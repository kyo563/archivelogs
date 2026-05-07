import logging, re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from archivelogs.youtube_client import fetch_videos_bulk, fallback_fetch_like_count_item

LOGGER = logging.getLogger(__name__)
JST = timezone(timedelta(hours=9))

def parse_stat_value(stats: Dict, key: str):
    if key not in stats:
        return ""
    v = stats.get(key)
    if v is None or v == "":
        return ""
    return int(v)

def resolve_video_id(url_or_id: str) -> Optional[str]:
    s = (url_or_id or "").strip()
    if not s:
        return None
    if "youtube.com/watch" in s and "v=" in s:
        try:
            vid = parse_qs(urlparse(s).query).get("v", [None])[0]
            if vid:
                return vid
        except Exception:
            pass
    if "youtu.be/" in s:
        return s.split("youtu.be/")[1].split("?")[0].split("/")[0][:11]
    if "youtube.com/shorts/" in s:
        return s.split("shorts/")[1].split("?")[0].split("/")[0][:11]
    return s if len(s) == 11 and "/" not in s and " " not in s else None

def extract_video_id_from_title_cell(title_cell: str) -> Optional[str]:
    t = (title_cell or "").strip()
    m = re.search(r'watch\?v=([a-zA-Z0-9_-]{11})', t)
    if m:
        return m.group(1)
    return resolve_video_id(t)

def build_record_row_from_video_item(item: Dict, logged_at_str: str) -> Tuple[List, bool]:
    snippet = item.get("snippet") or {}
    stats = item.get("statistics") or {}
    video_id = item.get("id") or ""
    published_str = ""
    p = snippet.get("publishedAt")
    if p:
        try:
            published_str = datetime.fromisoformat(p.replace("Z", "+00:00")).astimezone(JST).strftime("%Y/%m/%d %H:%M:%S")
        except Exception:
            pass
    view_count = parse_stat_value(stats, "viewCount")
    like_missing = "likeCount" not in stats
    like_count = parse_stat_value(stats, "likeCount")
    comment_count = parse_stat_value(stats, "commentCount")
    title = (snippet.get("title") or "").replace("\n", " ").strip().replace('"', '""')
    title_cell = f'=HYPERLINK("https://www.youtube.com/watch?v={video_id}","{title}")'
    return [logged_at_str, "video", title_cell, published_str, 0, view_count, like_count, comment_count], like_missing

def build_rows_with_like_fallback(youtube, video_ids: List[str], logged_at_str: str) -> Tuple[List[List], Dict[str, int]]:
    by_id = fetch_videos_bulk(youtube, video_ids)
    packed, missing_ids = [], []
    for vid in video_ids:
        item = by_id.get(vid)
        if not item or item.get("id") != vid:
            continue
        row, missing = build_record_row_from_video_item(item, logged_at_str)
        packed.append((vid, row, item))
        if missing:
            missing_ids.append(vid)
    out, fallback_success, missing_final = [], 0, 0
    for vid, row, item in packed:
        if vid in missing_ids:
            fb = fallback_fetch_like_count_item(youtube, vid)
            if fb and "likeCount" in ((fb.get("statistics") or {})):
                row[6] = parse_stat_value((fb.get("statistics") or {}), "likeCount")
                fallback_success += 1
            else:
                missing_final += 1
                s = item.get("statistics") or {}
                LOGGER.warning("[record-fetch][missing-likeCount] video_id=%s title=%s stats_keys=%s status=%s viewCount=%s commentCount=%s", vid, (item.get("snippet") or {}).get("title", ""), list(s.keys()), (item.get("status") or {}).get("privacyStatus"), s.get("viewCount"), s.get("commentCount"))
        out.append(row)
    return out, {"bulk_count": len(by_id), "missing_initial": len(missing_ids), "fallback_success": fallback_success, "missing_final": missing_final}
