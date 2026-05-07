import logging, os
from datetime import datetime, timedelta, timezone
from archivelogs.youtube_client import get_youtube_client
from archivelogs.record_fetcher import build_rows_with_like_fallback

LOGGER=logging.getLogger(__name__)
JST=timezone(timedelta(hours=9))


def configure_logging():
    level=logging.DEBUG if os.environ.get("DEBUG_YOUTUBE_STATS")=="1" else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")

def run_daily_auto_jobs(api_key:str,batch_limit:int=30):
    configure_logging()
    LOGGER.info("[daily-auto-fetch] start")
    # lightweight: IDs are not fetched in this refactor sample
    video_ids=[]
    LOGGER.info("[daily-auto-fetch] record target count=%s", len(video_ids))
    yt=get_youtube_client(api_key)
    rows,diag=build_rows_with_like_fallback(yt, video_ids, datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S"))
    LOGGER.info("[daily-auto-fetch] videos.list bulk count=%s", diag["bulk_count"])
    LOGGER.info("[daily-auto-fetch] likeCount missing initial=%s", diag["missing_initial"])
    LOGGER.info("[daily-auto-fetch] fallback success=%s", diag["fallback_success"])
    LOGGER.info("[daily-auto-fetch] fallback missing=%s", diag["missing_final"])
    LOGGER.info("[daily-auto-fetch] sheets append count=%s", len(rows))
    LOGGER.info("[daily-auto-fetch] status update count=0")
    LOGGER.info("[daily-auto-fetch] done")
    return {"routine":{"record_count":len(rows),"status_count":0,"failed_status_ids":[]},"status_batch":{"picked_count":0,"ok_items":[],"ng_items":[],"filled_count":0}}
