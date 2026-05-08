from datetime import datetime

from archivelogs.jobs import (
    JST,
    ROUTINE_STATUS_CHANNEL_IDS,
    STATUS_COLS,
    _build_status_row,
    _pick_status_batch_targets,
)


class _Exec:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        return self.payload


class _Channels:
    def list(self, **kwargs):
        return _Exec(
            {
                "items": [
                    {
                        "snippet": {"title": "ch", "publishedAt": "2026-05-08T00:00:00Z"},
                        "statistics": {"subscriberCount": "0", "videoCount": "0", "viewCount": "0"},
                        "contentDetails": {},
                    }
                ]
            }
        )


class _Playlists:
    def list(self, **kwargs):
        return _Exec(
            {
                "items": [
                    {"snippet": {"title": "B"}, "contentDetails": {"itemCount": 3}},
                    {"snippet": {"title": "A"}, "contentDetails": {"itemCount": 5}},
                ]
            }
        )


class _Youtube:
    def channels(self):
        return _Channels()

    def playlists(self):
        return _Playlists()


def test_build_status_row_len_and_core_fields(monkeypatch):
    monkeypatch.setattr("archivelogs.jobs.fetch_upload_video_ids", lambda *_: ["v1"])
    monkeypatch.setattr(
        "archivelogs.jobs.fetch_videos_bulk",
        lambda *_: {
            "v1": {
                "snippet": {"publishedAt": "2026-05-01T00:00:00Z", "title": "t", "liveBroadcastContent": "none"},
                "status": {"privacyStatus": "public", "uploadStatus": "processed"},
                "statistics": {"viewCount": "10"},
            }
        },
    )
    monkeypatch.setattr("archivelogs.jobs.filter_recordable_video_items", lambda items, max_results=50: items)
    monkeypatch.setattr("archivelogs.jobs.datetime", type("D", (), {"now": staticmethod(lambda tz=None: datetime(2026, 5, 8, 12, 0, 0, tzinfo=JST)), "fromisoformat": datetime.fromisoformat}))

    row = _build_status_row(_Youtube(), "cid")
    assert len(row) == STATUS_COLS
    assert row[6] == "2026/05/08"
    assert row[7] >= 1
    assert row[24] == 10


def test_pick_status_batch_targets_exclude_and_dedupe():
    targets = [[ROUTINE_STATUS_CHANNEL_IDS[0]], ["  "], ["X"], ["X"], [ROUTINE_STATUS_CHANNEL_IDS[1]], ["Y"]]
    picked, excluded, dup = _pick_status_batch_targets(targets, 2, ROUTINE_STATUS_CHANNEL_IDS)
    assert picked == ["X", "Y"]
    assert excluded == 2
    assert dup == 1
