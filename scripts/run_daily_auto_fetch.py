"""毎日の自動取得バッチを実行するスクリプト。"""
import os
from archivelogs.config import get_required_env
from archivelogs.jobs import run_daily_auto_jobs

def main() -> int:
    api_key = get_required_env("YOUTUBE_API_KEY")
    batch_limit = int(os.environ.get("STATUS_BATCH_LIMIT", "30"))
    result = run_daily_auto_jobs(api_key=api_key, batch_limit=batch_limit)
    routine = result["routine"]
    status_batch = result["status_batch"]
    print("[daily-auto-fetch] 完了")
    print(f"- routine: Record {routine['record_count']}件 / Status {routine['status_count']}件")
    print(f"- status_batch: 対象 {status_batch['picked_count']}件 / 成功 {len(status_batch['ok_items'])}件 / 失敗 {len(status_batch['ng_items'])}件")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
