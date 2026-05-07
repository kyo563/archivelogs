import logging,re
from datetime import datetime,timedelta,timezone
from urllib.parse import parse_qs,urlparse
from archivelogs.youtube_client import fetch_videos_bulk,fallback_fetch_like_count_item
LOGGER=logging.getLogger(__name__)
JST=timezone(timedelta(hours=9))

def parse_iso8601_duration(duration):
    m=re.match(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$",duration or "")
    if not m:return 0
    d,h,mi,s=[int(x or 0) for x in m.groups()]
    return d*86400+h*3600+mi*60+s

def parse_stat_value(stats,key):
    if key not in stats:return ""
    v=stats.get(key)
    return "" if v in (None,"") else int(v)

def resolve_video_id(s):
    s=(s or "").strip()
    if "youtube.com/watch" in s and "v=" in s:
        try:return parse_qs(urlparse(s).query).get("v",[None])[0]
        except: pass
    if "youtu.be/" in s:return s.split("youtu.be/")[1].split("?")[0].split("/")[0][:11]
    if "youtube.com/shorts/" in s:return s.split("shorts/")[1].split("?")[0].split("/")[0][:11]
    return s if len(s)==11 and "/" not in s and " " not in s else None

def extract_video_id_from_title_cell(title_cell):
    m=re.search(r'watch\?v=([a-zA-Z0-9_-]{11})',(title_cell or ""))
    return m.group(1) if m else resolve_video_id(title_cell)

def build_record_row_from_video_item(item, logged_at_str):
    sn=item.get("snippet") or {}; st=item.get("statistics") or {}; cd=item.get("contentDetails") or {}; ld=item.get("liveStreamingDetails") or {}
    vid=item.get("id") or ""; dur=parse_iso8601_duration(cd.get("duration","PT0S"))
    tp="live" if ld else ("short" if dur<=119 else "video")
    p=sn.get("publishedAt"); pub=""
    if p:
        try: pub=datetime.fromisoformat(p.replace("Z","+00:00")).astimezone(JST).strftime("%Y/%m/%d %H:%M:%S")
        except: pass
    title=(sn.get("title") or "").replace("\n"," ").strip().replace('"','""')
    title_cell=f'=HYPERLINK("https://www.youtube.com/watch?v={vid}","{title}")'
    return [logged_at_str,tp,title_cell,pub,dur,parse_stat_value(st,"viewCount"),parse_stat_value(st,"likeCount"),parse_stat_value(st,"commentCount")],("likeCount" not in st)

def build_rows_with_like_fallback(youtube, video_ids, logged_at_str):
    by_id=fetch_videos_bulk(youtube, video_ids); out=[]; miss=[]; packed=[]
    for vid in video_ids:
        it=by_id.get(vid)
        if not it: continue
        row,m=build_record_row_from_video_item(it,logged_at_str); packed.append((vid,row,it));
        if m: miss.append(vid)
    fs=0; mf=0
    for vid,row,it in packed:
        if vid in miss:
            fb=fallback_fetch_like_count_item(youtube,vid)
            fb_stats = (fb.get("statistics") or {}) if fb else {}
            if fb and "likeCount" in fb_stats:
                row[6]=parse_stat_value(fb_stats,"likeCount"); fs+=1
            else:
                mf+=1; s=it.get("statistics") or {}
                LOGGER.warning("[record-fetch][missing-likeCount] video_id=%s title=%s stats_keys=%s status=%s viewCount=%s commentCount=%s",vid,(it.get("snippet") or {}).get("title",""),list(s.keys()),(it.get("status") or {}).get("privacyStatus"),s.get("viewCount"),s.get("commentCount"))
        out.append(row)
    return out,{"bulk_count":len(by_id),"missing_initial":len(miss),"fallback_success":fs,"missing_final":mf}
