import ast
import importlib
from pathlib import Path

import pytest

from archivelogs import config
from archivelogs.jobs import run_daily_auto_jobs


def test_streamlit_record_adapter_calls_core_row_builder(monkeypatch):
    app = importlib.import_module("app")
    called = {"n": 0}

    def _fake_builder(youtube, items, logged_at):
        called["n"] += 1
        return [[logged_at, "video", "t", "", 0, 0, 0, 0]], {"bulk_count": len(items), "missing_initial": 0, "fallback_success": 0, "missing_final": 0}

    monkeypatch.setattr("app.build_rows_from_video_items_with_like_fallback", _fake_builder)
    monkeypatch.setattr("app.get_youtube_client", lambda _api_key: object())
    monkeypatch.setattr("app.fetch_upload_video_ids", lambda *_, **__: ["vid1"])
    monkeypatch.setattr("app.fetch_videos_bulk", lambda *_: {"vid1": {"id": "vid1", "snippet": {"publishedAt": "2026-01-01T00:00:00Z", "liveBroadcastContent": "none", "title": "t"}, "status": {"privacyStatus": "public", "uploadStatus": "processed"}, "contentDetails": {"duration": "PT1M"}, "statistics": {}}})
    rows, diag = app.fetch_record_rows_via_core("dummy", "cid1", 1)

    assert called["n"] == 1
    assert len(rows) == 1
    assert diag["bulk_count"] == 1


def test_runtime_config_accepts_secrets_dict_injection():
    config.clear_runtime_config()
    config.set_runtime_config({"gcp_service_account": {"type": "service_account", "project_id": "p"}})
    info = config.load_service_account_info()
    assert info["type"] == "service_account"
    assert info["project_id"] == "p"
    config.clear_runtime_config()


def test_service_account_can_be_loaded_from_json_string(monkeypatch):
    config.clear_runtime_config()
    monkeypatch.setenv("GCP_SERVICE_ACCOUNT_JSON", '{"type":"service_account","project_id":"from_json"}')
    info = config.load_service_account_info()
    assert info["type"] == "service_account"
    assert info["project_id"] == "from_json"
    config.clear_runtime_config()


def test_run_daily_auto_jobs_normal_run_calls_append_rows(monkeypatch):
    monkeypatch.setattr("archivelogs.jobs.get_youtube_client", lambda _: object())
    monkeypatch.setattr("archivelogs.jobs.get_record_worksheet", lambda create=True: object())
    monkeypatch.setattr("archivelogs.jobs.get_status_worksheet", lambda create=True: object())

    class SearchWS:
        def get_all_values(self):
            return [["channel_id"], ["cid1"]]

    monkeypatch.setattr("archivelogs.jobs.get_search_target_worksheet", lambda create=True: SearchWS())
    monkeypatch.setattr("archivelogs.jobs.fetch_upload_video_ids", lambda *_: ["vid1"])
    monkeypatch.setattr(
        "archivelogs.jobs.fetch_videos_bulk",
        lambda *_: {
            "vid1": {
                "id": "vid1",
                "snippet": {"publishedAt": "2026-01-01T00:00:00Z", "liveBroadcastContent": "none", "title": "t"},
                "status": {"privacyStatus": "public", "uploadStatus": "processed"},
                "contentDetails": {"duration": "PT1M"},
                "statistics": {"viewCount": "1", "likeCount": "2"},
            }
        },
    )
    monkeypatch.setattr(
        "archivelogs.jobs.build_rows_from_video_items_with_like_fallback",
        lambda *_args, **_kwargs: ([["row"]], {"bulk_count": 1, "missing_initial": 0, "fallback_success": 0, "missing_final": 0}),
    )
    monkeypatch.setattr("archivelogs.jobs._build_status_row", lambda *_: ["s"])

    called = {"n": 0}

    def _append(*_):
        called["n"] += 1

    monkeypatch.setattr("archivelogs.jobs.append_rows", _append)
    result = run_daily_auto_jobs(api_key="dummy", batch_limit=1, dry_run=False)

    assert called["n"] >= 1
    assert result["record_rows_appended"] == 1


def test_run_daily_auto_fetch_does_not_import_app_module():
    source = Path("scripts/run_daily_auto_fetch.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    assert "app" not in imported


def test_import_app_is_not_fatal(monkeypatch):
    class DummySt:
        class errors:
            class StreamlitSecretNotFoundError(Exception):
                pass

        class _Secrets(dict):
            def get(self, key, default=None):
                return default

        secrets = _Secrets()
        session_state = {}

        def set_page_config(self, **_):
            return None

        def cache_resource(self, f):
            return f

        def markdown(self, *_args, **_kwargs):
            return None

        def write(self, *_args, **_kwargs):
            return None

        def table(self, *_args, **_kwargs):
            return None

        def button(self, *_args, **_kwargs):
            return False

        def info(self, *_args, **_kwargs):
            return None

        sidebar = type("Sidebar", (), {"text_input": staticmethod(lambda *_args, **_kwargs: "")})()

    monkeypatch.setitem(__import__("sys").modules, "streamlit", DummySt())
    monkeypatch.setitem(__import__("sys").modules, "streamlit.components", type("C", (), {})())
    monkeypatch.setitem(__import__("sys").modules, "streamlit.components.v1", type("CV1", (), {"html": staticmethod(lambda *_a, **_k: None)})())
    monkeypatch.setitem(__import__("sys").modules, "streamlit.errors", type("E", (), {"StreamlitSecretNotFoundError": Exception})())

    monkeypatch.setitem(__import__("sys").modules, "gspread", type("G", (), {})())
    monkeypatch.setitem(__import__("sys").modules, "googleapiclient", type("GA", (), {})())
    monkeypatch.setitem(__import__("sys").modules, "googleapiclient.discovery", type("D", (), {"build": staticmethod(lambda *_a, **_k: object())})())
    monkeypatch.setitem(__import__("sys").modules, "googleapiclient.errors", type("GE", (), {"HttpError": Exception})())
    monkeypatch.setitem(__import__("sys").modules, "google", type("GO", (), {})())
    monkeypatch.setitem(__import__("sys").modules, "google.oauth2", type("GO2", (), {})())
    monkeypatch.setitem(
        __import__("sys").modules,
        "google.oauth2.service_account",
        type("S", (), {"Credentials": type("C", (), {"from_service_account_info": staticmethod(lambda *_a, **_k: object())})})(),
    )

    try:
        importlib.import_module("app")
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"app import should not be fatal, but got: {exc}")


def test_build_record_diag_display_maps_core_keys():
    app = importlib.import_module("app")
    rows = [["a"], ["b"]]
    diag = {"bulk_count": 5, "missing_initial": 2, "fallback_success": 1, "missing_final": 1}
    out = app.build_record_diag_display(dry_run=True, rows=rows, appended_count=0, diag=diag)
    assert out == {
        "dry-run": True,
        "record rows planned": 2,
        "record rows appended": 0,
        "videos.list bulk count": 5,
        "likeCount missing initial": 2,
        "fallback success": 1,
        "fallback missing": 1,
    }


def test_append_record_rows_if_needed_dry_run_has_no_side_effects(monkeypatch):
    app = importlib.import_module("app")
    called = {"ws": 0, "append": 0, "refresh": 0}

    monkeypatch.setattr("app.get_record_worksheet", lambda: called.__setitem__("ws", called["ws"] + 1))
    monkeypatch.setattr("app.shared_append_rows", lambda *_: called.__setitem__("append", called["append"] + 1))
    monkeypatch.setattr("app.refresh_record_comment_counts", lambda *_: called.__setitem__("refresh", called["refresh"] + 1))

    appended, updated = app.append_record_rows_if_needed("dummy", [["row"]], dry_run=True)
    assert appended == 0 and updated == 0
    assert called == {"ws": 0, "append": 0, "refresh": 0}


def test_append_record_rows_if_needed_non_dry_run_calls_append(monkeypatch):
    app = importlib.import_module("app")
    called = {"append": 0}
    monkeypatch.setattr("app.get_record_worksheet", lambda: object())
    monkeypatch.setattr("app.shared_append_rows", lambda *_: called.__setitem__("append", called["append"] + 1))
    monkeypatch.setattr("app.refresh_record_comment_counts", lambda *_: 3)

    appended, updated = app.append_record_rows_if_needed("dummy", [["r1"], ["r2"]], dry_run=False)
    assert called["append"] == 1
    assert appended == 2
    assert updated == 3


def test_append_record_rows_if_needed_empty_rows_has_no_side_effects(monkeypatch):
    app = importlib.import_module("app")
    called = {"ws": 0, "append": 0, "refresh": 0}

    monkeypatch.setattr("app.get_record_worksheet", lambda: called.__setitem__("ws", called["ws"] + 1))
    monkeypatch.setattr("app.shared_append_rows", lambda *_: called.__setitem__("append", called["append"] + 1))
    monkeypatch.setattr("app.refresh_record_comment_counts", lambda *_: called.__setitem__("refresh", called["refresh"] + 1))

    appended, updated = app.append_record_rows_if_needed("dummy", [], dry_run=False)
    assert appended == 0 and updated == 0
    assert called == {"ws": 0, "append": 0, "refresh": 0}


def test_daily_auto_fetch_workflow_dispatch_has_dry_run_input():
    text = Path(".github/workflows/daily-auto-fetch.yml").read_text(encoding="utf-8")
    assert "workflow_dispatch:" in text
    assert "inputs:" in text
    assert "dry_run:" in text
    assert 'default: "true"' in text
    assert '--dry-run' in text
