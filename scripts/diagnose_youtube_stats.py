import argparse, json
from archivelogs.config import get_required_env
from archivelogs.youtube_client import get_youtube_client
from archivelogs.record_fetcher import resolve_video_id, parse_stat_value

def main():
    p=argparse.ArgumentParser()
    p.add_argument("ids", nargs="+")
    a=p.parse_args()
    raw=[]
    for x in a.ids:
        raw.extend([i for i in x.split(",") if i])
    video_ids=[resolve_video_id(x) for x in raw]
    video_ids=[v for v in video_ids if v]
    yt=get_youtube_client(get_required_env("YOUTUBE_API_KEY"))
    resp=yt.videos().list(part="id,snippet,statistics,status", id=",".join(video_ids), maxResults=min(50,len(video_ids))).execute()
    for it in resp.get("items",[]):
        s=it.get("statistics") or {}
        sn=it.get("snippet") or {}
        st=it.get("status") or {}
        print("video_id:", it.get("id"))
        print("title:", sn.get("title",""))
        print("privacyStatus:", st.get("privacyStatus",""))
        print("uploadStatus:", st.get("uploadStatus",""))
        print("liveBroadcastContent:", sn.get("liveBroadcastContent",""))
        print("statistics raw:", json.dumps(s, ensure_ascii=False))
        print("statistics.keys:", list(s.keys()))
        print("parsed view_count:", parse_stat_value(s,"viewCount"))
        print("parsed like_count:", parse_stat_value(s,"likeCount"))
        print("parsed comment_count:", parse_stat_value(s,"commentCount"))
        print("has likeCount key:", "likeCount" in s)

if __name__=="__main__":
    main()
