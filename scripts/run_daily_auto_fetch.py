"""毎日の自動取得バッチを実行するスクリプト。"""

import os

from app import get_secret_value, run_daily_auto_jobs


def main() -> int:
    api_key = get_secret_value("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY が設定されていません。")

    batch_limit = int(os.environ.get("STATUS_BATCH_LIMIT", "30"))
    result = run_daily_auto_jobs(api_key=api_key, batch_limit=batch_limit)

    routine = result["routine"]
    status_batch = result["status_batch"]

    print("[daily-auto-fetch] 完了")
    print(
        f"- routine: Record {routine['record_count']}件 / Status {routine['status_count']}件"
    )
    if routine["failed_status_ids"]:
        print("- routine failed status ids: " + ", ".join(routine["failed_status_ids"]))

    print(
        f"- status_batch: 対象 {status_batch['picked_count']}件 / 成功 {len(status_batch['ok_items'])}件 / 失敗 {len(status_batch['ng_items'])}件"
    )
    print(f"- status_batch: チャンネル名補完 {status_batch['filled_count']}件")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
