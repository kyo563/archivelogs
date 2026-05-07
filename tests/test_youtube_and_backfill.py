from archivelogs.youtube_client import fetch_videos_bulk

def test_fetch_videos_bulk_chunking():
    calls=[]
    class V:
        def list(self, **kwargs): calls.append(kwargs["id"].split(",")); return self
        def execute(self): return {"items":[]}
    class Y:
        def videos(self): return V()
    fetch_videos_bulk(Y(), [f"id{i:09d}"[:11] for i in range(120)])
    assert len(calls)==3


from archivelogs.youtube_client import fallback_fetch_like_count_diagnostic


def test_fallback_diag_ok_first_attempt(monkeypatch):
    monkeypatch.setattr("archivelogs.youtube_client._fetch_single", lambda *_args, **_kwargs: {"id": "v1", "statistics": {"likeCount": "9"}})
    d = fallback_fetch_like_count_diagnostic(None, "v1", sleep_seconds=0)
    assert d["success"] is True
    assert d["final_reason"] == "ok"


def test_fallback_diag_no_item(monkeypatch):
    monkeypatch.setattr("archivelogs.youtube_client._fetch_single", lambda *_args, **_kwargs: None)
    d = fallback_fetch_like_count_diagnostic(None, "v1", sleep_seconds=0)
    assert d["success"] is False
    assert d["final_reason"] == "no_item_returned"


def test_fallback_diag_statistics_missing(monkeypatch):
    monkeypatch.setattr("archivelogs.youtube_client._fetch_single", lambda *_args, **_kwargs: {"id": "v1"})
    d = fallback_fetch_like_count_diagnostic(None, "v1", sleep_seconds=0)
    assert d["success"] is False
    assert d["final_reason"] == "statistics_missing"


def test_fallback_diag_likecount_missing(monkeypatch):
    monkeypatch.setattr("archivelogs.youtube_client._fetch_single", lambda *_args, **_kwargs: {"id": "v1", "statistics": {"viewCount": "1"}})
    d = fallback_fetch_like_count_diagnostic(None, "v1", sleep_seconds=0)
    assert d["success"] is False
    assert d["final_reason"] == "likeCount_missing"
