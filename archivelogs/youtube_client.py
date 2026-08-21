import logging
import os
import time


VIDEO_PART_FULL = "id,snippet,contentDetails,statistics,status,liveStreamingDetails"
TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
LOGGER = logging.getLogger(__name__)


def get_youtube_client(api_key: str):
    from googleapiclient.discovery import build

    return build("youtube", "v3", developerKey=api_key)


def execute_with_retry(builder, retries=4, base_delay_seconds=1.0):
    """Execute an idempotent YouTube API request with bounded retries."""
    for attempt in range(retries):
        try:
            return builder().execute()
        except Exception as exc:
            status = getattr(getattr(exc, "resp", None), "status", None)
            transient_network_error = isinstance(exc, (ConnectionError, OSError, TimeoutError))
            can_retry = status in TRANSIENT_STATUS_CODES or (status is None and transient_network_error)
            if attempt == retries - 1 or not can_retry:
                raise
            delay = base_delay_seconds * (2**attempt)
            LOGGER.warning(
                "[youtube][retry] attempt=%s/%s status=%s delay_seconds=%.1f",
                attempt + 1,
                retries,
                status or "network",
                delay,
            )
            time.sleep(delay)


# Backward-compatible private alias for existing callers.
_execute = execute_with_retry


def fetch_videos_bulk(youtube, video_ids, part=VIDEO_PART_FULL):
    out = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i : i + 50]
        resp = execute_with_retry(
            lambda: youtube.videos().list(part=part, id=",".join(chunk), maxResults=50)
        )
        items = resp.get("items", [])
        got = {it.get("id") for it in items if it.get("id")}
        missing = [video_id for video_id in chunk if video_id not in got]
        if missing:
            LOGGER.warning(
                "[youtube][videos.list][missing] requested=%s missing=%s",
                len(chunk),
                ",".join(missing),
            )
        for item in items:
            if item.get("id"):
                out[item["id"]] = item
    return out


def _fetch_single(youtube, video_id, part):
    resp = execute_with_retry(
        lambda: youtube.videos().list(part=part, id=video_id, maxResults=1)
    )
    items = resp.get("items", [])
    return items[0] if items else None


def _attempt_info(part, item):
    info = {"part": part, "returned": bool(item)}
    stats = (item or {}).get("statistics") or {}
    info["statistics_keys"] = list(stats.keys())
    info["has_likeCount"] = "likeCount" in stats
    if item:
        status = item.get("status") or {}
        snippet = item.get("snippet") or {}
        info["privacyStatus"] = status.get("privacyStatus", "")
        info["uploadStatus"] = status.get("uploadStatus", "")
        info["liveBroadcastContent"] = snippet.get("liveBroadcastContent", "")
        if os.environ.get("DEBUG_YOUTUBE_STATS") == "1":
            info["raw_statistics"] = stats
    else:
        info["privacyStatus"] = ""
        info["uploadStatus"] = ""
        info["liveBroadcastContent"] = ""
    return info


def fallback_fetch_like_count_diagnostic(youtube, video_id, sleep_seconds=0.2):
    attempts = []
    last_item = None
    for index, part in enumerate(["id,statistics", "id,snippet,statistics,status"]):
        item = _fetch_single(youtube, video_id, part)
        attempts.append(_attempt_info(part, item))
        if item:
            last_item = item
            stats = item.get("statistics")
            if isinstance(stats, dict) and "likeCount" in stats:
                like_count = stats.get("likeCount")
                return {
                    "video_id": video_id,
                    "success": True,
                    "item": item,
                    "like_count": int(like_count) if str(like_count or "").isdigit() else like_count or "",
                    "attempts": attempts,
                    "final_reason": "ok",
                }
        if index == 0:
            time.sleep(sleep_seconds)

    if not last_item:
        final_reason = "no_item_returned"
    elif "statistics" not in last_item or last_item.get("statistics") is None:
        final_reason = "statistics_missing"
    else:
        final_reason = "likeCount_missing"

    return {
        "video_id": video_id,
        "success": False,
        "item": last_item,
        "like_count": "",
        "attempts": attempts,
        "final_reason": final_reason,
    }


def fallback_fetch_like_count_item(youtube, video_id, sleep_seconds=0.2):
    diag = fallback_fetch_like_count_diagnostic(
        youtube, video_id, sleep_seconds=sleep_seconds
    )
    return diag.get("item") if diag.get("success") else None
