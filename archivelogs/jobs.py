import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

from archivelogs.record_fetcher import (
    build_rows_from_video_items_with_like_fallback,
    fetch_upload_video_ids,
    filter_recordable_video_items,
)
from archivelogs.sheets import (
    CHANNEL_MASTER_HEADER,
    append_rows,
    get_channel_master_worksheet,
    get_record_worksheet,
    get_search_target_worksheet,
    get_status_worksheet,
)
from archivelogs.youtube_client import fetch_videos_bulk, get_youtube_client

LOGGER = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))
ROUTINE_RECORD_CHANNEL_ID = "UCojaLfI34qEb0pCTtbjDeEg"
ROUTINE_STATUS_CHANNEL_IDS = ["UCojaLfI34qEb0pCTtbjDeEg", "UC24z3yE1Mig66jwaSbZI0UA"]
STATUS_COLS = 35


def configure_logging():
    logging.basicConfig(level=(logging.DEBUG if os.environ.get("DEBUG_YOUTUBE_STATS") == "1" else logging.INFO), format="%(message)s")


def _safe_div(numerator, denominator):
    if not denominator:
        return 0
    return round(float(numerator) / float(denominator), 4)


def _parse_youtube_datetime_to_jst(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(JST)
    except Exception:
        return None


def _calc_activity_months(published_at: Optional[datetime], now_jst: datetime) -> int:
    if not published_at:
        return 0
    months = (now_jst.year - published_at.year) * 12 + (now_jst.month - published_at.month)
    return max(months, 1)


def _fetch_channel_playlists(youtube, channel_id: str, limit: int = 50) -> List[dict]:
    items: List[dict] = []
    page_token = None
    while len(items) < limit:
        req = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id, maxResults=min(50, limit - len(items)), pageToken=page_token)
        resp = req.execute() or {}
        chunk = resp.get("items", [])
        items.extend(chunk)
        page_token = resp.get("nextPageToken")
        if not page_token or not chunk:
            break
    return items[:limit]


def _build_playlist_metrics(youtube, channel_id: str, video_count: int):
    playlists = _fetch_channel_playlists(youtube, channel_id)
    playlist_count = len(playlists)
    sorted_items = sorted(playlists, key=lambda x: int((x.get("contentDetails") or {}).get("itemCount", 0) or 0), reverse=True)
    top = []
    for pl in sorted_items[:5]:
        title = (pl.get("snippet") or {}).get("title", "")
        count = int((pl.get("contentDetails") or {}).get("itemCount", 0) or 0)
        top.append(f"{title}（{count}本）")
    while len(top) < 5:
        top.append("")
    return _safe_div(playlist_count, video_count), top


def _fetch_recent_video_items(youtube, channel_id: str) -> List[dict]:
    ids = fetch_upload_video_ids(youtube, channel_id, 50)
    by_id = fetch_videos_bulk(youtube, ids)
    ordered = [by_id[v] for v in ids if v in by_id]
    return filter_recordable_video_items(ordered, max_results=50)


def _build_recent_metrics(items: Sequence[dict], subscriber_count: int, now_jst: datetime, days: int):
    threshold = now_jst - timedelta(days=days)
    scoped = []
    for it in items:
        published = _parse_youtube_datetime_to_jst((it.get("snippet") or {}).get("publishedAt", ""))
        if published and published >= threshold:
            scoped.append(it)
    if not scoped:
        return [0, 0, "", 0, 0, 0, 0]
    views = [int((it.get("statistics") or {}).get("viewCount", 0) or 0) for it in scoped]
    total_views = sum(views)
    posts = len(scoped)
    top_item = max(scoped, key=lambda it: int((it.get("statistics") or {}).get("viewCount", 0) or 0))
    top_views = int((top_item.get("statistics") or {}).get("viewCount", 0) or 0)
    top_title = (top_item.get("snippet") or {}).get("title", "")
    return [total_views, posts, top_title, top_views, _safe_div(top_views, total_views), _safe_div(total_views, posts), _safe_div(total_views, subscriber_count)]


def _parse_date(v: str) -> Optional[date]:
    if not v:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(v[:10], fmt).date()
        except Exception:
            pass
    return None


def _dedupe_search_targets(targets, exclude_ids: Sequence[str]):
    seen, out = set(), []
    excluded = duplicated = 0
    exclude = set(exclude_ids)
    for i, r in enumerate(targets):
        cid = (r[0] if r else "").strip()
        if not cid:
            continue
        if cid in exclude:
            excluded += 1
            continue
        if cid in seen:
            duplicated += 1
            continue
        seen.add(cid)
        out.append((cid, i))
    return out, excluded, duplicated


def _fetch_channels_light_bulk(youtube, channel_ids: Sequence[str]):
    out = {}
    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i : i + 50]
        resp = youtube.channels().list(part="snippet,statistics,contentDetails", id=",".join(chunk), maxResults=50).execute() or {}
        for it in resp.get("items", []):
            cid = it.get("id")
            if cid:
                out[cid] = it
    return out


def _fetch_latest_upload_published_at(youtube, uploads_playlist_id: str) -> str:
    if not uploads_playlist_id:
        return ""
    resp = youtube.playlistItems().list(part="snippet", playlistId=uploads_playlist_id, maxResults=1).execute() or {}
    items = resp.get("items", [])
    if not items:
        return ""
    return ((items[0].get("snippet") or {}).get("publishedAt") or "")


def _build_status_row(youtube, channel_id):
    now_jst = datetime.now(JST)
    r = youtube.channels().list(part="snippet,statistics,contentDetails", id=channel_id, maxResults=1).execute().get("items", [])
    if not r:
        return None
    it = r[0]
    sn = it.get("snippet") or {}
    st = it.get("statistics") or {}
    subscriber_count = int(st.get("subscriberCount", 0) or 0)
    video_count = int(st.get("videoCount", 0) or 0)
    view_count = int(st.get("viewCount", 0) or 0)
    published_dt = _parse_youtube_datetime_to_jst(sn.get("publishedAt", ""))
    open_date = published_dt.strftime("%Y/%m/%d") if published_dt else ""
    activity_months = _calc_activity_months(published_dt, now_jst)
    playlist_ratio, top_playlists = _build_playlist_metrics(youtube, channel_id, video_count)
    recent_items = _fetch_recent_video_items(youtube, channel_id)
    recent10 = _build_recent_metrics(recent_items, subscriber_count, now_jst, 10)
    recent30 = _build_recent_metrics(recent_items, subscriber_count, now_jst, 30)
    row = [now_jst.strftime("%Y/%m/%d"), channel_id, sn.get("title", ""), subscriber_count, video_count, view_count, open_date, activity_months, _safe_div(subscriber_count, activity_months), _safe_div(subscriber_count, video_count), _safe_div(view_count, video_count), _safe_div(view_count, subscriber_count), _safe_div(subscriber_count, view_count), playlist_ratio, _safe_div(video_count, activity_months), _safe_div(video_count, subscriber_count), *top_playlists, *recent10, *recent30]
    if len(row) != STATUS_COLS:
        raise ValueError(f"status row length mismatch: {len(row)} != {STATUS_COLS}")
    return row


def _select_status_batch(candidates, today: date, batch_limit: int):
    def key(x):
        if x["unseen"]:
            return (0, 0, 0, x["order"])
        return (1, -x["effective_age_days"], 0 if x["changed"] else 1, x["order"])

    sorted_items = sorted(candidates, key=key)
    return sorted_items[: int(batch_limit)]


def run_daily_auto_jobs(api_key: str, batch_limit: int = 30, dry_run: bool = False) -> Dict:
    configure_logging()
    yt = get_youtube_client(api_key)
    ws_record = get_record_worksheet(create=not dry_run)
    ws_status = get_status_worksheet(create=not dry_run)
    ws_search = get_search_target_worksheet(create=not dry_run)
    ws_master = get_channel_master_worksheet(create=not dry_run)

    ids = fetch_upload_video_ids(yt, ROUTINE_RECORD_CHANNEL_ID, 50)
    by_id = fetch_videos_bulk(yt, ids)
    items = filter_recordable_video_items([by_id[v] for v in ids if v in by_id], max_results=50)
    record_rows, diag = build_rows_from_video_items_with_like_fallback(yt, items, datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S"))
    record_appended = 0
    if record_rows and not dry_run:
        append_rows(ws_record, record_rows)
        record_appended = len(record_rows)

    now_jst = datetime.now(JST)
    today = now_jst.date()
    routine_status, failed = [], []
    for cid in ROUTINE_STATUS_CHANNEL_IDS:
        row = _build_status_row(yt, cid)
        if row:
            routine_status.append(row)
        else:
            failed.append(cid)
    routine_status_appended = 0
    if routine_status and not dry_run:
        append_rows(ws_status, routine_status)
        routine_status_appended = len(routine_status)

    targets = ws_search.get_all_values()[1:] if ws_search else []
    deduped, excluded_count, duplicated_count = _dedupe_search_targets(targets, ROUTINE_STATUS_CHANNEL_IDS)
    channel_ids = [cid for cid, _ in deduped]

    master_rows = ws_master.get_all_values()[1:] if ws_master else []
    header_idx = {k: i for i, k in enumerate(CHANNEL_MASTER_HEADER)}
    master_by_id = {(r[0] if r else "").strip(): r for r in master_rows if r and r[0].strip()}

    light_map = _fetch_channels_light_bulk(yt, channel_ids)
    master_updates = []
    candidates = []
    light_success = light_failed = changed_count = unchanged_count = unseen_count = 0

    for cid, order in deduped:
        prev = master_by_id.get(cid, [""] * len(CHANNEL_MASTER_HEADER))
        prev_video = int(prev[header_idx["last_video_count"]]) if prev[header_idx["last_video_count"]] else None
        prev_detail = _parse_date(prev[header_idx["last_detail_fetched_at"]])
        prev_latest = prev[header_idx["latest_upload_published_at"]]
        prev_error = int(prev[header_idx["error_count"]] or 0) if len(prev) > header_idx["error_count"] else 0

        item = light_map.get(cid)
        if not item:
            light_failed += 1
            LOGGER.warning("light fetch failed: channel_id=%s reason=not_found", cid)
            master_updates.append({"channel_id": cid, "error_count": prev_error + 1})
            continue

        light_success += 1
        sn, st, cd = item.get("snippet") or {}, item.get("statistics") or {}, item.get("contentDetails") or {}
        curr_video = int(st.get("videoCount", 0) or 0)
        delta = "" if prev_video is None else curr_video - prev_video
        unseen = prev_video is None or not prev_detail
        changed = unseen or (delta != 0)
        if unseen:
            unseen_count += 1
        if changed:
            changed_count += 1
        else:
            unchanged_count += 1

        latest_upload = prev_latest
        uploads = ((cd.get("relatedPlaylists") or {}).get("uploads") or "")
        if (not prev_latest) or (delta != "" and delta != 0):
            latest_upload = _fetch_latest_upload_published_at(yt, uploads)

        days_since = (today - prev_detail).days if prev_detail else 10**6
        effective_age = days_since if changed else days_since - 3
        candidates.append({"channel_id": cid, "order": order, "unseen": unseen, "changed": changed, "effective_age_days": effective_age})

        master_updates.append({
            "channel_id": cid,
            "チャンネル名": sn.get("title", ""),
            "uploads_playlist_id": uploads,
            "last_light_fetched_at": now_jst.strftime("%Y/%m/%d"),
            "last_video_count": curr_video,
            "last_subscriber_count": int(st.get("subscriberCount", 0) or 0),
            "last_view_count": int(st.get("viewCount", 0) or 0),
            "latest_upload_published_at": latest_upload,
            "last_video_count_delta": delta,
            "error_count": 0,
        })

    selected = _select_status_batch(candidates, today, batch_limit)
    picked = [x["channel_id"] for x in selected]

    batch, ok, ng = [], [], []
    for cid in picked:
        row = _build_status_row(yt, cid)
        if row:
            batch.append(row)
            ok.append(cid)
            for m in master_updates:
                if m["channel_id"] == cid:
                    m["last_detail_fetched_at"] = now_jst.strftime("%Y/%m/%d")
                    break
        else:
            ng.append(cid)

    status_batch_appended = 0
    if batch and not dry_run:
        append_rows(ws_status, batch)
        status_batch_appended = len(batch)

    if not dry_run and ws_master:
        new_rows = []
        for u in master_updates:
            row = [""] * len(CHANNEL_MASTER_HEADER)
            row[0] = u["channel_id"]
            old = master_by_id.get(u["channel_id"])
            if old:
                row = (old + [""] * len(CHANNEL_MASTER_HEADER))[: len(CHANNEL_MASTER_HEADER)]
            for k, v in u.items():
                if k in header_idx:
                    row[header_idx[k]] = str(v) if v is not None else ""
            new_rows.append(row)
        append_rows(ws_master, new_rows)

    return {
        "dry_run": dry_run,
        "record_target_count": len(items),
        "record_rows_planned": len(record_rows),
        "record_rows_appended": record_appended,
        "routine_status_planned": len(routine_status),
        "routine_status_appended": routine_status_appended,
        "status_batch_picked": len(picked),
        "status_batch_planned": len(batch),
        "status_batch_appended": status_batch_appended,
        "routine": {"record_count": len(record_rows), "status_count": len(routine_status), "failed_status_ids": failed},
        "status_batch": {"picked_count": len(picked), "ok_items": ok, "ng_items": ng, "filled_count": 0},
        "diag": diag,
        "status_batch_source_count": len(targets),
        "status_batch_excluded_routine_count": excluded_count,
        "status_batch_excluded_duplicate_count": duplicated_count,
        "status_batch_changed_count": changed_count,
        "status_batch_unchanged_count": unchanged_count,
        "status_batch_unseen_count": unseen_count,
        "status_batch_light_fetch_success_count": light_success,
        "status_batch_light_fetch_failed_count": light_failed,
    }
