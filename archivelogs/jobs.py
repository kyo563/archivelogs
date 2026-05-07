import logging,os
from datetime import datetime,timedelta,timezone
from archivelogs.youtube_client import get_youtube_client
from archivelogs.record_fetcher import build_rows_with_like_fallback
from archivelogs.sheets import get_record_worksheet,get_status_worksheet,get_search_target_worksheet,append_rows
LOGGER=logging.getLogger(__name__)
JST=timezone(timedelta(hours=9))
ROUTINE_RECORD_CHANNEL_ID="UCojaLfI34qEb0pCTtbjDeEg"
ROUTINE_STATUS_CHANNEL_IDS=["UCojaLfI34qEb0pCTtbjDeEg","UC24z3yE1Mig66jwaSbZI0UA"]

def configure_logging(): logging.basicConfig(level=(logging.DEBUG if os.environ.get("DEBUG_YOUTUBE_STATS")=="1" else logging.INFO),format="%(message)s")

def fetch_channel_upload_items(youtube, channel_id, max_results=50):
    ch=youtube.channels().list(part="contentDetails",id=channel_id,maxResults=1).execute().get("items",[])
    if not ch:return []
    up=ch[0].get("contentDetails",{}).get("relatedPlaylists",{}).get("uploads")
    ids=[]; token=None
    while len(ids)<max_results:
        r=youtube.playlistItems().list(part="contentDetails",playlistId=up,maxResults=min(50,max_results-len(ids)),pageToken=token).execute()
        ids += [((it.get("contentDetails") or {}).get("videoId")) for it in r.get("items",[]) if (it.get("contentDetails") or {}).get("videoId")]
        token=r.get("nextPageToken")
        if not token: break
    vids=youtube.videos().list(part="id,snippet,contentDetails,statistics,status,liveStreamingDetails",id=','.join(ids),maxResults=50).execute().get("items",[])
    out=[]
    for it in vids:
        sn=it.get("snippet") or {}; st=it.get("status") or {}
        if st.get("privacyStatus")!="public" or st.get("uploadStatus")!="processed": continue
        if sn.get("liveBroadcastContent") in ("live","upcoming"): continue
        out.append(it)
    return sorted(out,key=lambda x:(x.get("snippet",{}).get("publishedAt") or ""))[:max_results]

def _build_status_row(youtube, channel_id):
    # NOTE: Status詳細指標は暫定的に簡略化（旧 app.py の全分析列とは完全同等ではありません）
    r=youtube.channels().list(part="snippet,statistics",id=channel_id,maxResults=1).execute().get("items",[])
    if not r:return None
    it=r[0]; sn=it.get("snippet") or {}; st=it.get("statistics") or {}
    date=datetime.now(JST).strftime("%Y/%m/%d")
    return [date,channel_id,sn.get("title",""),int(st.get("subscriberCount",0) or 0),int(st.get("videoCount",0) or 0),int(st.get("viewCount",0) or 0),"",0,0,0,0,0,0,0,0,0,"","","","","",0,0,"",0,0,0,0,0,0,"",0,0,0,0]

def run_daily_auto_jobs(api_key:str,batch_limit:int=30):
    configure_logging(); yt=get_youtube_client(api_key)
    ws_record=get_record_worksheet(); ws_status=get_status_worksheet(); ws_search=get_search_target_worksheet()
    items=fetch_channel_upload_items(yt, ROUTINE_RECORD_CHANNEL_ID, 50)
    video_ids=[it.get("id") for it in items if it.get("id")]
    rows,diag=build_rows_with_like_fallback(yt,video_ids,datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S"))
    if rows: append_rows(ws_record,rows)
    routine_status=[]; failed=[]
    for cid in ROUTINE_STATUS_CHANNEL_IDS:
        row=_build_status_row(yt,cid)
        if row:routine_status.append(row)
        else:failed.append(cid)
    if routine_status: append_rows(ws_status,routine_status)
    targets=ws_search.get_all_values()[1:]
    picked=[r[0].strip() for r in targets if len(r)>=1 and r[0].strip()][:int(batch_limit)]
    batch=[]; ok=[]; ng=[]
    for cid in picked:
        row=_build_status_row(yt,cid)
        if row: batch.append(row); ok.append(cid)
        else: ng.append(cid)
    if batch: append_rows(ws_status,batch)
    return {"routine":{"record_count":len(rows),"status_count":len(routine_status),"failed_status_ids":failed},"status_batch":{"picked_count":len(picked),"ok_items":ok,"ng_items":ng,"filled_count":0},"diag":diag,"record_target_count":len(video_ids)}
