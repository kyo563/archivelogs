import logging,time
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

def fallback_fetch_like_count_item(youtube, video_id, sleep_seconds=0.2):
    for i,part in enumerate(["id,statistics","id,snippet,statistics,status"]):
        it=_fetch_single(youtube,video_id,part)
        if it and "likeCount" in ((it.get("statistics") or {})): return it
        if i==0: time.sleep(sleep_seconds)
    return None
