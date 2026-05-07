import logging, time
from typing import Dict, List, Optional

LOGGER = logging.getLogger(__name__)
VIDEO_PART_FULL = "id,snippet,contentDetails,statistics,status,liveStreamingDetails"

def get_youtube_client(api_key: str):
    from googleapiclient.discovery import build
    return build("youtube", "v3", developerKey=api_key)

def fetch_videos_bulk(youtube, video_ids: List[str]) -> Dict[str, Dict]:
    if not video_ids:
        return {}
    resp = youtube.videos().list(part=VIDEO_PART_FULL, id=",".join(video_ids), maxResults=min(50, len(video_ids))).execute()
    items = resp.get("items", [])
    return {it.get("id"): it for it in items if it.get("id")}

def _fetch_single(youtube, video_id: str, part: str) -> Optional[Dict]:
    resp = youtube.videos().list(part=part, id=video_id, maxResults=1).execute()
    items = resp.get("items", [])
    return items[0] if items else None

def fallback_fetch_like_count_item(youtube, video_id: str, sleep_seconds: float = 0.2) -> Optional[Dict]:
    for idx, part in enumerate(["id,statistics", "id,snippet,statistics,status"]):
        item = _fetch_single(youtube, video_id, part)
        if item and "likeCount" in ((item.get("statistics") or {})):
            return item
        if idx == 0:
            time.sleep(sleep_seconds)
    return None
