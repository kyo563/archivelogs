import argparse,json
from archivelogs.config import get_required_env
from archivelogs.youtube_client import get_youtube_client, fetch_videos_bulk, VIDEO_PART_FULL
from archivelogs.record_fetcher import resolve_video_id, parse_stat_value, parse_iso8601_duration

def main():
    p=argparse.ArgumentParser(); p.add_argument("ids",nargs="+"); a=p.parse_args()
    raw=[]
    for x in a.ids: raw.extend([i.strip() for i in x.split(",") if i.strip()])
    video_ids=[v for v in (resolve_video_id(x) for x in raw) if v]
    yt=get_youtube_client(get_required_env("YOUTUBE_API_KEY"))
    by_id=fetch_videos_bulk(yt, video_ids, part=VIDEO_PART_FULL)
    print("requested video IDs:", video_ids)
    print("returned video IDs:", list(by_id.keys()))
    print("missing returned IDs:", [x for x in video_ids if x not in by_id])
    for vid in video_ids:
        it=by_id.get(vid)
        if not it: continue
        s=it.get("statistics") or {}; sn=it.get("snippet") or {}; st=it.get("status") or {}; cd=it.get("contentDetails") or {}
        print("video_id:", vid)
        print("title:", sn.get("title",""))
        print("contentDetails.duration:", cd.get("duration",""))
        print("parsed duration_sec:", parse_iso8601_duration(cd.get("duration","")))
        print("liveStreamingDetails raw:", json.dumps(it.get("liveStreamingDetails") or {}, ensure_ascii=False))
        print("status raw:", json.dumps(st, ensure_ascii=False))
        print("snippet.liveBroadcastContent:", sn.get("liveBroadcastContent",""))
        print("statistics raw:", json.dumps(s, ensure_ascii=False))
        print("has likeCount key:", "likeCount" in s)
        print("parsed view_count:", parse_stat_value(s,"viewCount"))
        print("parsed like_count:", parse_stat_value(s,"likeCount"))
        print("parsed comment_count:", parse_stat_value(s,"commentCount"))

if __name__=="__main__": main()
