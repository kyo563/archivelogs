"""毎日の自動取得バッチを実行するスクリプト。"""
import argparse
import os

from archivelogs.config import get_required_env
from archivelogs.jobs import run_daily_auto_jobs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = get_required_env("YOUTUBE_API_KEY")
    batch_limit = int(os.environ.get("STATUS_BATCH_LIMIT", "30"))
    result = run_daily_auto_jobs(api_key=api_key, batch_limit=batch_limit, dry_run=args.dry_run)
    diag = result.get("diag", {})
    print(f"[daily-auto-fetch] dry_run={'true' if args.dry_run else 'false'}")
    print(f"[daily-auto-fetch] record target count={result.get('record_target_count', 0)}")
    print(f"[daily-auto-fetch] record rows planned={result.get('record_rows_planned', 0)}")
    print(f"[daily-auto-fetch] record rows appended={result.get('record_rows_appended', 0)}")
    print(f"[daily-auto-fetch] videos.list bulk count={diag.get('bulk_count', 0)}")
    print(f"[daily-auto-fetch] likeCount missing initial={diag.get('missing_initial', 0)}")
    print(f"[daily-auto-fetch] fallback success={diag.get('fallback_success', 0)}")
    print(f"[daily-auto-fetch] fallback missing={diag.get('missing_final', 0)}")
    print(f"[daily-auto-fetch] routine status planned={result.get('routine_status_planned', 0)}")
    print(f"[daily-auto-fetch] routine status appended={result.get('routine_status_appended', 0)}")
    print(f"[daily-auto-fetch] status batch picked={result.get('status_batch_picked', 0)}")
    print(f"[daily-auto-fetch] status batch planned={result.get('status_batch_planned', 0)}")
    print(f"[daily-auto-fetch] status batch appended={result.get('status_batch_appended', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
