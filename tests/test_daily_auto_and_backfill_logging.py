import scripts.run_daily_auto_fetch as daily_script
import scripts.backfill_missing_like_counts as backfill_script
from archivelogs.jobs import run_daily_auto_jobs


def test_run_daily_auto_jobs_contains_diag_and_record_target_count(monkeypatch):
    monkeypatch.setattr("archivelogs.jobs.get_youtube_client", lambda _: object())
    monkeypatch.setattr("archivelogs.jobs.get_record_worksheet", lambda: object())
    monkeypatch.setattr("archivelogs.jobs.get_status_worksheet", lambda: object())

    class SearchWS:
        def get_all_values(self):
            return [["channel_id"], ["cid1"]]

    monkeypatch.setattr("archivelogs.jobs.get_search_target_worksheet", lambda: SearchWS())
    monkeypatch.setattr("archivelogs.jobs.fetch_channel_upload_items", lambda *_: [{"id": "vid1"}, {"id": "vid2"}])
    monkeypatch.setattr("archivelogs.jobs.build_rows_with_like_fallback", lambda *_: ([], {"bulk_count": 2, "missing_initial": 1, "fallback_success": 1, "missing_final": 0}))
    monkeypatch.setattr("archivelogs.jobs._build_status_row", lambda *_: None)
    monkeypatch.setattr("archivelogs.jobs.append_rows", lambda *_: None)
    result = run_daily_auto_jobs(api_key="dummy", batch_limit=1)
    assert "diag" in result
    assert result["record_target_count"] == 2


def test_run_daily_auto_fetch_prints_diag(monkeypatch, capsys):
    monkeypatch.setattr("scripts.run_daily_auto_fetch.get_required_env", lambda _: "dummy")
    monkeypatch.setattr(
        "scripts.run_daily_auto_fetch.run_daily_auto_jobs",
        lambda **_: {
            "routine": {"record_count": 0, "status_count": 0, "failed_status_ids": []},
            "status_batch": {"picked_count": 0, "ok_items": [], "ng_items": [], "filled_count": 0},
            "record_target_count": 3,
            "diag": {"bulk_count": 3, "missing_initial": 2, "fallback_success": 1, "missing_final": 1},
        },
    )
    assert daily_script.main() == 0
    out = capsys.readouterr().out
    assert "[daily-auto-fetch] record target count=3" in out
    assert "[daily-auto-fetch] videos.list bulk count=3" in out
    assert "[daily-auto-fetch] likeCount missing initial=2" in out
    assert "[daily-auto-fetch] fallback success=1" in out
    assert "[daily-auto-fetch] fallback missing=1" in out


def test_backfill_dry_run_shows_planned_and_actual(monkeypatch, capsys):
    class DummyWS:
        def get_all_values(self, value_render_option=None):
            return [["date", "type", "title", "x", "x", "x", "like_count"], ["", "", '=HYPERLINK("https://www.youtube.com/watch?v=abcdefghijk","t")', "", "", "", ""]]

    monkeypatch.setattr("scripts.backfill_missing_like_counts.get_record_worksheet", lambda: DummyWS())
    monkeypatch.setattr("scripts.backfill_missing_like_counts.get_required_env", lambda _: "dummy")
    monkeypatch.setattr("scripts.backfill_missing_like_counts.get_youtube_client", lambda _: object())
    monkeypatch.setattr(
        "scripts.backfill_missing_like_counts.fetch_videos_bulk",
        lambda *_args, **_kwargs: {"abcdefghijk": {"statistics": {"likeCount": "8"}}},
    )
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda self: type("A", (), {"include_zero": False, "dry_run": True})())
    backfill_script.main()
    out = capsys.readouterr().out
    assert "更新予定件数: 1" in out
    assert "実更新件数: 0" in out
