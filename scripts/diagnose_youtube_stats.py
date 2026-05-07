import argparse
import json

from archivelogs.config import get_required_env
from archivelogs.record_fetcher import parse_iso8601_duration, parse_stat_value, resolve_video_id
from archivelogs.youtube_client import VIDEO_PART_FULL, get_youtube_client


def _fetch_single(youtube, video_id, part):
    resp = youtube.videos().list(part=part, id=video_id, maxResults=1).execute()
    items = resp.get("items", [])
    return items[0] if items else None


def _print_item(video_id, part, item):
    print(f"part: {part}")
    if not item:
        print("returned: False")
        return
    print("returned: True")
    s = item.get("statistics") or {}
    sn = item.get("snippet") or {}
    st = item.get("status") or {}
    cd = item.get("contentDetails") or {}
    lsd = item.get("liveStreamingDetails")

    print("statistics.keys():", list(s.keys()))
    print("has likeCount key:", "likeCount" in s)
    print("has viewCount key:", "viewCount" in s)
    print("has commentCount key:", "commentCount" in s)
    print("privacyStatus:", st.get("privacyStatus", ""))
    print("uploadStatus:", st.get("uploadStatus", ""))
    print("liveBroadcastContent:", sn.get("liveBroadcastContent", ""))
    print("contentDetails.duration:", cd.get("duration", ""))
    print("parsed duration_sec:", parse_iso8601_duration(cd.get("duration", "")))
    print("has liveStreamingDetails:", bool(lsd))
    print("statistics raw:", json.dumps(s, ensure_ascii=False))
    print("parsed view_count:", parse_stat_value(s, "viewCount"))
    print("parsed like_count:", parse_stat_value(s, "likeCount"))
    print("parsed comment_count:", parse_stat_value(s, "commentCount"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("ids", nargs="+")
    p.add_argument("--compare-parts", action="store_true")
    a = p.parse_args()

    raw = []
    for x in a.ids:
        raw.extend([i.strip() for i in x.split(",") if i.strip()])
    video_ids = [v for v in (resolve_video_id(x) for x in raw) if v]

    yt = get_youtube_client(get_required_env("YOUTUBE_API_KEY"))
    parts = [VIDEO_PART_FULL]
    if a.compare_parts:
        parts = [
            "id,statistics",
            "id,snippet,statistics,status",
            "id,snippet,contentDetails,statistics,status,liveStreamingDetails",
        ]

    print("requested video IDs:", video_ids)
    for vid in video_ids:
        print("video_id:", vid)
        for part in parts:
            _print_item(vid, part, _fetch_single(yt, vid, part))


if __name__ == "__main__":
    main()
