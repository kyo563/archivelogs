import logging
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

from archivelogs.youtube_client import fallback_fetch_like_count_diagnostic, fetch_videos_bulk

LOGGER = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))


def parse_iso8601_duration(duration):
    m = re.match(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$", duration or "")
    if not m:
        return 0
    d, h, mi, s = [int(x or 0) for x in m.groups()]
    return d * 86400 + h * 3600 + mi * 60 + s


def fetch_upload_video_ids(youtube, channel_id, max_results=50):
    ch = youtube.channels().list(part="contentDetails", id=channel_id, maxResults=1).execute().get("items", [])
    if not ch:
        return []
    up = ch[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    ids, token = [], None
    while len(ids) < max_results:
        r = youtube.playlistItems().list(part="contentDetails", playlistId=up, maxResults=min(50, max_results - len(ids)), pageToken=token).execute()
        ids += [((it.get("contentDetails") or {}).get("videoId")) for it in r.get("items", []) if (it.get("contentDetails") or {}).get("videoId")]
        token = r.get("nextPageToken")
        if not token:
            break
    return ids


def filter_recordable_video_items(items, max_results=50):
    out = []
    for it in items:
        sn = it.get("snippet") or {}
        st = it.get("status") or {}
        if st.get("privacyStatus") != "public" or st.get("uploadStatus") != "processed":
            continue
        if sn.get("liveBroadcastContent") in ("live", "upcoming"):
            continue
        out.append(it)
    return sorted(out, key=lambda x: (x.get("snippet", {}).get("publishedAt") or ""))[:max_results]


def parse_stat_value(stats, key):
    if key not in stats:
        return ""
    v = stats.get(key)
    return "" if v in (None, "") else int(v)


def resolve_video_id(s):
    s = (s or "").strip()
    if "youtube.com/watch" in s and "v=" in s:
        try:
            return parse_qs(urlparse(s).query).get("v", [None])[0]
        except Exception:
            pass
    if "youtu.be/" in s:
        return s.split("youtu.be/")[1].split("?")[0].split("/")[0][:11]
    if "youtube.com/shorts/" in s:
        return s.split("shorts/")[1].split("?")[0].split("/")[0][:11]
    return s if len(s) == 11 and "/" not in s and " " not in s else None


def extract_video_id_from_title_cell(title_cell):
    m = re.search(r"watch\?v=([a-zA-Z0-9_-]{11})", (title_cell or ""))
    return m.group(1) if m else resolve_video_id(title_cell)


def build_record_row_from_video_item(item, logged_at_str):
    sn = item.get("snippet") or {}
    st = item.get("statistics") or {}
    cd = item.get("contentDetails") or {}
    ld = item.get("liveStreamingDetails") or {}
    vid = item.get("id") or ""
    dur = parse_iso8601_duration(cd.get("duration", "PT0S"))
    tp = "live" if ld else ("short" if dur <= 119 else "video")
    p = sn.get("publishedAt")
    pub = ""
    if p:
        try:
            pub = datetime.fromisoformat(p.replace("Z", "+00:00")).astimezone(JST).strftime("%Y/%m/%d %H:%M:%S")
        except Exception:
            pass
    title = (sn.get("title") or "").replace("\n", " ").strip().replace('"', '""')
    title_cell = f'=HYPERLINK("https://www.youtube.com/watch?v={vid}","{title}")'
    return [logged_at_str, tp, title_cell, pub, dur, parse_stat_value(st, "viewCount"), parse_stat_value(st, "likeCount"), parse_stat_value(st, "commentCount")], ("likeCount" not in st)


def build_rows_from_video_items_with_like_fallback(youtube, items, logged_at_str):
    out, miss, packed = [], [], []
    for it in items:
        vid = it.get("id")
        if not vid:
            continue
        row, m = build_record_row_from_video_item(it, logged_at_str)
        packed.append((vid, row, it))
        if m:
            miss.append(vid)
    fs, mf = 0, 0
    mn, ms, ml = 0, 0, 0
    for vid, row, it in packed:
        if vid in miss:
            fb = fallback_fetch_like_count_diagnostic(youtube, vid)
            if fb.get("success"):
                row[6] = fb.get("like_count", "")
                fs += 1
            else:
                mf += 1
                reason = fb.get("final_reason", "")
                if reason == "no_item_returned":
                    mn += 1
                elif reason == "statistics_missing":
                    ms += 1
                elif reason == "likeCount_missing":
                    ml += 1
                s = it.get("statistics") or {}
                a1 = (fb.get("attempts") or [{}])[0] if (fb.get("attempts") or []) else {}
                a2 = (fb.get("attempts") or [{}, {}])[1] if len(fb.get("attempts") or []) > 1 else {}
                title = ((it.get("snippet") or {}).get("title", "") or "").replace("\n", " ")[:120]
                LOGGER.warning(
                    "[record-fetch][missing-likeCount] video_id=%s title=%s initial_stats_keys=%s initial_viewCount=%s initial_commentCount=%s fallback_final_reason=%s fallback_attempt1_returned=%s fallback_attempt1_stats_keys=%s fallback_attempt1_has_likeCount=%s fallback_attempt2_returned=%s fallback_attempt2_stats_keys=%s fallback_attempt2_has_likeCount=%s fallback_privacyStatus=%s fallback_uploadStatus=%s fallback_liveBroadcastContent=%s",
                    vid,
                    title,
                    list(s.keys()),
                    s.get("viewCount", ""),
                    s.get("commentCount", ""),
                    reason,
                    a1.get("returned", False),
                    a1.get("statistics_keys", []),
                    a1.get("has_likeCount", False),
                    a2.get("returned", False),
                    a2.get("statistics_keys", []),
                    a2.get("has_likeCount", False),
                    a2.get("privacyStatus", ""),
                    a2.get("uploadStatus", ""),
                    a2.get("liveBroadcastContent", ""),
                )
                if os.environ.get("DEBUG_YOUTUBE_STATS") == "1":
                    LOGGER.warning("[record-fetch][missing-likeCount][debug] video_id=%s fallback_attempt1_raw_statistics=%s fallback_attempt2_raw_statistics=%s", vid, a1.get("raw_statistics", {}), a2.get("raw_statistics", {}))
        out.append(row)
    return out, {"bulk_count": len(items), "missing_initial": len(miss), "fallback_success": fs, "missing_final": mf, "missing_no_item": mn, "missing_statistics_missing": ms, "missing_likeCount_missing": ml}


def build_rows_with_like_fallback(youtube, video_ids, logged_at_str):
    by_id = fetch_videos_bulk(youtube, video_ids)
    items = [by_id[v] for v in video_ids if v in by_id]
    return build_rows_from_video_items_with_like_fallback(youtube, items, logged_at_str)
