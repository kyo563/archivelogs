from datetime import datetime

from archivelogs.jobs import JST, ROUTINE_STATUS_CHANNEL_IDS, STATUS_COLS, _build_status_row, _dedupe_search_targets, _select_status_batch


class _Exec:
    def __init__(self, payload): self.payload = payload
    def execute(self): return self.payload


class _Channels:
    def list(self, **kwargs):
        return _Exec({"items": [{"snippet": {"title": "ch", "publishedAt": "2026-05-08T00:00:00Z"}, "statistics": {"subscriberCount": "0", "videoCount": "0", "viewCount": "0"}, "contentDetails": {}}]})


class _Playlists:
    def list(self, **kwargs):
        return _Exec({"items": [{"snippet": {"title": "B"}, "contentDetails": {"itemCount": 3}}, {"snippet": {"title": "A"}, "contentDetails": {"itemCount": 5}}]})


class _Youtube:
    def channels(self): return _Channels()
    def playlists(self): return _Playlists()


def test_build_status_row_len_and_core_fields(monkeypatch):
    monkeypatch.setattr("archivelogs.jobs.fetch_upload_video_ids", lambda *_: ["v1"])
    monkeypatch.setattr("archivelogs.jobs.fetch_videos_bulk", lambda *_: {"v1": {"snippet": {"publishedAt": "2026-05-01T00:00:00Z", "title": "t", "liveBroadcastContent": "none"}, "status": {"privacyStatus": "public", "uploadStatus": "processed"}, "statistics": {"viewCount": "10"}}})
    monkeypatch.setattr("archivelogs.jobs.filter_recordable_video_items", lambda items, max_results=50: items)
    monkeypatch.setattr("archivelogs.jobs.datetime", type("D", (), {"now": staticmethod(lambda tz=None: datetime(2026, 5, 8, 12, 0, 0, tzinfo=JST)), "fromisoformat": datetime.fromisoformat, "strptime": datetime.strptime}))
    row = _build_status_row(_Youtube(), "cid")
    assert len(row) == STATUS_COLS


def test_dedupe_exclude_routine():
    targets = [[ROUTINE_STATUS_CHANNEL_IDS[0]], ["  "], ["X"], ["X"], [ROUTINE_STATUS_CHANNEL_IDS[1]], ["Y"]]
    picked, excluded, dup = _dedupe_search_targets(targets, ROUTINE_STATUS_CHANNEL_IDS)
    assert [x[0] for x in picked] == ["X", "Y"]
    assert excluded == 2
    assert dup == 1


def test_priority_order_b_c_d_a():
    items = [
        {"channel_id": "A", "order": 0, "unseen": False, "changed": False, "effective_age_days": 9},
        {"channel_id": "B", "order": 1, "unseen": False, "changed": True, "effective_age_days": 11},
        {"channel_id": "C", "order": 2, "unseen": False, "changed": True, "effective_age_days": 10},
        {"channel_id": "D", "order": 3, "unseen": False, "changed": True, "effective_age_days": 9},
    ]
    out = _select_status_batch(items, datetime(2026, 5, 11).date(), 4)
    assert [x["channel_id"] for x in out] == ["B", "C", "D", "A"]


def test_unseen_is_highest_priority():
    items = [
        {"channel_id": "X", "order": 1, "unseen": True, "changed": True, "effective_age_days": 0},
        {"channel_id": "Y", "order": 0, "unseen": False, "changed": True, "effective_age_days": 999},
    ]
    out = _select_status_batch(items, datetime(2026, 5, 11).date(), 2)
    assert [x["channel_id"] for x in out] == ["X", "Y"]
