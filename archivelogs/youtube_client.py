import logging,time,os
from googleapiclient.errors import HttpError
VIDEO_PART_FULL="id,snippet,contentDetails,statistics,status,liveStreamingDetails"
LOGGER=logging.getLogger(__name__)

def get_youtube_client(api_key:str):
    from googleapiclient.discovery import build
    return build("youtube","v3",developerKey=api_key)

def _execute(builder,retries=3):
    for i in range(retries):
        try:return builder().execute()
        except Exception as e:
            code=getattr(getattr(e,"resp",None),"status",None)
            if i==retries-1 or code not in (429,500,502,503,504): raise
            time.sleep(1.2*(i+1))

def fetch_videos_bulk(youtube, video_ids, part=VIDEO_PART_FULL):
    out={}
    for i in range(0,len(video_ids),50):
        chunk=video_ids[i:i+50]
        resp=_execute(lambda: youtube.videos().list(part=part,id=','.join(chunk),maxResults=50))
        items=resp.get("items",[])
        got={it.get("id") for it in items if it.get("id")}
        miss=[x for x in chunk if x not in got]
        if miss: LOGGER.warning("[youtube][videos.list][missing] requested=%s missing=%s",len(chunk),','.join(miss))
        for it in items:
            if it.get("id"): out[it["id"]]=it
    return out

def _fetch_single(youtube, video_id, part):
    resp=_execute(lambda: youtube.videos().list(part=part,id=video_id,maxResults=1))
    items=resp.get("items",[])
    return items[0] if items else None



def _attempt_info(part, item):
    info = {"part": part, "returned": bool(item)}
    stats = (item or {}).get("statistics") or {}
    info["statistics_keys"] = list(stats.keys())
    info["has_likeCount"] = "likeCount" in stats
    if item:
        st = item.get("status") or {}
        sn = item.get("snippet") or {}
        info["privacyStatus"] = st.get("privacyStatus", "")
        info["uploadStatus"] = st.get("uploadStatus", "")
        info["liveBroadcastContent"] = sn.get("liveBroadcastContent", "")
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
    for i, part in enumerate(["id,statistics", "id,snippet,statistics,status"]):
        item = _fetch_single(youtube, video_id, part)
        attempts.append(_attempt_info(part, item))
        if item:
            last_item = item
            stats = item.get("statistics")
            if isinstance(stats, dict) and "likeCount" in stats:
                return {
                    "video_id": video_id,
                    "success": True,
                    "item": item,
                    "like_count": int(stats.get("likeCount")) if str(stats.get("likeCount", "")).isdigit() else stats.get("likeCount", ""),
                    "attempts": attempts,
                    "final_reason": "ok",
                }
        if i == 0:
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
    diag = fallback_fetch_like_count_diagnostic(youtube, video_id, sleep_seconds=sleep_seconds)
    return diag.get("item") if diag.get("success") else None
