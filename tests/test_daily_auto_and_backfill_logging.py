import scripts.backfill_missing_like_counts as backfill_script
import scripts.run_daily_auto_fetch as daily_script
from archivelogs.jobs import run_daily_auto_jobs


def test_run_daily_auto_jobs_dry_run_no_append(monkeypatch):
    monkeypatch.setattr("archivelogs.jobs.get_youtube_client", lambda _: object())
    monkeypatch.setattr("archivelogs.jobs.get_record_worksheet", lambda create=True: object())
    monkeypatch.setattr("archivelogs.jobs.get_status_worksheet", lambda create=True: object())

    class SearchWS:
        def get_all_values(self):
            return [["channel_id"], ["cid1"]]

    monkeypatch.setattr("archivelogs.jobs.get_search_target_worksheet", lambda create=True: SearchWS())
    monkeypatch.setattr("archivelogs.jobs.fetch_upload_video_ids", lambda *_: ["vid1"])
    monkeypatch.setattr("archivelogs.jobs.fetch_videos_bulk", lambda *_: {"vid1": {"id": "vid1", "snippet": {"publishedAt": "2026-01-01T00:00:00Z", "liveBroadcastContent": "none", "title": "t"}, "status": {"privacyStatus": "public", "uploadStatus": "processed"}, "contentDetails": {"duration": "PT1M"}, "statistics": {"viewCount": "1", "likeCount": "2"}}})
    monkeypatch.setattr("archivelogs.jobs._build_status_row", lambda *_: None)
    called = {"n": 0}

    def _append(*_):
        called["n"] += 1

    monkeypatch.setattr("archivelogs.jobs.append_rows", _append)
    result = run_daily_auto_jobs(api_key="dummy", batch_limit=1, dry_run=True)
    assert called["n"] == 0
    assert result["record_rows_planned"] >= 0
    assert result["record_rows_appended"] == 0


def test_run_daily_auto_fetch_prints_dry_run(monkeypatch, capsys):
    monkeypatch.setattr("scripts.run_daily_auto_fetch.get_required_env", lambda _: "dummy")
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: type("A", (), {"dry_run": True})())
    monkeypatch.setattr("scripts.run_daily_auto_fetch.run_daily_auto_jobs", lambda **_: {"record_target_count": 3, "record_rows_planned": 2, "record_rows_appended": 0, "routine_status_planned": 1, "routine_status_appended": 0, "status_batch_picked": 0, "status_batch_planned": 0, "status_batch_appended": 0, "diag": {"bulk_count": 3, "missing_initial": 2, "fallback_success": 1, "missing_final": 1}})
    assert daily_script.main() == 0
    out = capsys.readouterr().out
    assert "[daily-auto-fetch] dry_run=true" in out


def test_backfill_dry_run_shows_planned_and_actual(monkeypatch, capsys):
    class DummyWS:
        def get_all_values(self, value_render_option=None):
            return [["date", "type", "title", "x", "x", "x", "like_count"], ["", "", '=HYPERLINK("https://www.youtube.com/watch?v=abcdefghijk","t")', "", "", "", ""]]

    monkeypatch.setattr("scripts.backfill_missing_like_counts.get_record_worksheet", lambda: DummyWS())
    monkeypatch.setattr("scripts.backfill_missing_like_counts.get_required_env", lambda _: "dummy")
    monkeypatch.setattr("scripts.backfill_missing_like_counts.get_youtube_client", lambda _: object())
    monkeypatch.setattr("scripts.backfill_missing_like_counts.fetch_videos_bulk", lambda *_args, **_kwargs: {"abcdefghijk": {"statistics": {"likeCount": "8"}}})
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: type("A", (), {"include_zero": False, "dry_run": True})())
    backfill_script.main()
    out = capsys.readouterr().out
    assert "更新予定件数: 1" in out
    assert "実更新件数: 0" in out
