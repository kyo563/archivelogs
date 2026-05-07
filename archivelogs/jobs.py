import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from archivelogs.record_fetcher import (
    build_rows_from_video_items_with_like_fallback,
    fetch_upload_video_ids,
    filter_recordable_video_items,
)
from archivelogs.sheets import append_rows, get_record_worksheet, get_search_target_worksheet, get_status_worksheet
from archivelogs.youtube_client import fetch_videos_bulk, get_youtube_client

LOGGER = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))
ROUTINE_RECORD_CHANNEL_ID = "UCojaLfI34qEb0pCTtbjDeEg"
ROUTINE_STATUS_CHANNEL_IDS = ["UCojaLfI34qEb0pCTtbjDeEg", "UC24z3yE1Mig66jwaSbZI0UA"]


def configure_logging():
    logging.basicConfig(
        level=(logging.DEBUG if os.environ.get("DEBUG_YOUTUBE_STATS") == "1" else logging.INFO),
        format="%(message)s",
    )


def _build_status_row(youtube, channel_id):
    r = youtube.channels().list(part="snippet,statistics", id=channel_id, maxResults=1).execute().get("items", [])
    if not r:
        return None
    it = r[0]
    sn = it.get("snippet") or {}
    st = it.get("statistics") or {}
    date = datetime.now(JST).strftime("%Y/%m/%d")
    return [date, channel_id, sn.get("title", ""), int(st.get("subscriberCount", 0) or 0), int(st.get("videoCount", 0) or 0), int(st.get("viewCount", 0) or 0), "", 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", "", "", "", 0, 0, "", 0, 0, 0, 0, 0, 0, "", 0, 0, 0, 0]


def run_daily_auto_jobs(api_key: str, batch_limit: int = 30, dry_run: bool = False) -> Dict:
    configure_logging()
    yt = get_youtube_client(api_key)
    ws_record = get_record_worksheet(create=not dry_run)
    ws_status = get_status_worksheet(create=not dry_run)
    ws_search = get_search_target_worksheet(create=not dry_run)

    ids = fetch_upload_video_ids(yt, ROUTINE_RECORD_CHANNEL_ID, 50)
    by_id = fetch_videos_bulk(yt, ids)
    items = filter_recordable_video_items([by_id[v] for v in ids if v in by_id], max_results=50)
    record_rows, diag = build_rows_from_video_items_with_like_fallback(yt, items, datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S"))

    record_appended = 0
    if record_rows and not dry_run:
        append_rows(ws_record, record_rows)
        record_appended = len(record_rows)

    routine_status = []
    failed = []
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
    picked = [r[0].strip() for r in targets if len(r) >= 1 and r[0].strip()][: int(batch_limit)]
    batch = []
    ok = []
    ng = []
    for cid in picked:
        row = _build_status_row(yt, cid)
        if row:
            batch.append(row)
            ok.append(cid)
        else:
            ng.append(cid)
    status_batch_appended = 0
    if batch and not dry_run:
        append_rows(ws_status, batch)
        status_batch_appended = len(batch)

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
    }
