import argparse

from archivelogs.config import get_required_env
from archivelogs.record_fetcher import extract_video_id_from_title_cell
from archivelogs.sheets import get_record_worksheet
from archivelogs.youtube_client import fallback_fetch_like_count_diagnostic, fetch_videos_bulk, get_youtube_client


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--include-zero", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    ws = get_record_worksheet()
    rows = ws.get_all_values(value_render_option="FORMULA")
    yt = get_youtube_client(get_required_env("YOUTUBE_API_KEY"))

    targets, extracted, skipped = [], 0, 0
    for i, row in enumerate(rows[1:], start=2):
        like = row[6].strip() if len(row) >= 7 else ""
        title = row[2] if len(row) >= 3 else ""
        if like == "" or (a.include_zero and like == "0"):
            vid = extract_video_id_from_title_cell(title)
            if vid:
                targets.append((i, vid, like))
                extracted += 1
            else:
                skipped += 1

    missing_targets = [(r, vid, old) for r, vid, old in targets if old == ""]
    zero_targets = [(r, vid, old) for r, vid, old in targets if old == "0"]

    updates, api_ok, like_ok, missing = [], 0, 0, 0

    by_id = fetch_videos_bulk(yt, [x[1] for x in missing_targets], part="id,statistics") if missing_targets else {}
    for r, vid, old in missing_targets:
        it = by_id.get(vid)
        if not it:
            missing += 1
            continue
        api_ok += 1
        s = it.get("statistics") or {}
        if "likeCount" not in s:
            missing += 1
            continue
        new = int(s.get("likeCount") or 0)
        like_ok += 1
        updates.append((r, 7, str(new), old, vid))

    for r, vid, old in zero_targets:
        diag = fallback_fetch_like_count_diagnostic(yt, vid)
        if not diag.get("success"):
            missing += 1
            continue
        api_ok += 1
        new = int(diag.get("like_count", 0) or 0)
        like_ok += 1
        if new <= 0:
            continue
        updates.append((r, 7, str(new), old, vid))

    if updates and not a.dry_run:
        cells = [ws.cell(r, c) for r, c, _, _, _ in updates]
        for cell, (_, _, v, _, _) in zip(cells, updates):
            cell.value = v
        ws.update_cells(cells, value_input_option="USER_ENTERED")

    if a.dry_run and updates:
        for r, _, v, old, vid in updates:
            print(f"[dry-run] row={r} video_id={vid} old_likeCount={old or '<missing>'} new_likeCount={v}")

    planned_updates = len(updates)
    actual_updates = 0 if a.dry_run else len(updates)
    print(f"対象行数: {len(targets)}\nvideo_id抽出成功数: {extracted}\nAPI取得成功数: {api_ok}\nlikeCount取得成功数: {like_ok}\n更新予定件数: {planned_updates}\n実更新件数: {actual_updates}\nmissing継続件数: {missing}\nskipped件数: {skipped}")


if __name__ == "__main__":
    main()
