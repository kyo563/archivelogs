from archivelogs.record_fetcher import *


def test_like_exists(): assert parse_stat_value({"likeCount":"12"},"likeCount")==12
def test_like_zero(): assert parse_stat_value({"likeCount":"0"},"likeCount")==0
def test_like_missing(): assert parse_stat_value({},"likeCount")==""
def test_comment_missing(): assert parse_stat_value({},"commentCount")==""
def test_hyperlink_extract(): assert extract_video_id_from_title_cell('=HYPERLINK("https://www.youtube.com/watch?v=xxxxxxxxxxx","t")')=="xxxxxxxxxxx"
def test_url_extract():
    assert resolve_video_id("https://www.youtube.com/watch?v=xxxxxxxxxxx")=="xxxxxxxxxxx"
    assert resolve_video_id("https://youtu.be/xxxxxxxxxxx")=="xxxxxxxxxxx"
    assert resolve_video_id("https://www.youtube.com/shorts/xxxxxxxxxxx")=="xxxxxxxxxxx"
    assert resolve_video_id("xxxxxxxxxxx")=="xxxxxxxxxxx"
def test_duration(): assert parse_iso8601_duration("PT1H2M3S")==3723
def test_short_live():
    row,_=build_record_row_from_video_item({"id":"x","snippet":{"title":"t"},"contentDetails":{"duration":"PT59S"},"statistics":{}},"2026/01/01 00:00:00"); assert row[1]=="short"
    row,_=build_record_row_from_video_item({"id":"x","snippet":{"title":"t"},"contentDetails":{"duration":"PT10M"},"statistics":{},"liveStreamingDetails":{"a":1}},"2026/01/01 00:00:00"); assert row[1]=="live"

def test_fallback_called_and_reflected(monkeypatch):
    monkeypatch.setattr('archivelogs.record_fetcher.fetch_videos_bulk', lambda y,ids:{ids[0]:{"id":ids[0],"snippet":{"title":"t"},"statistics":{},"contentDetails":{"duration":"PT10M"}}})
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_item', lambda y,vid:{"id":vid,"statistics":{"likeCount":"5"}})
    rows,diag=build_rows_with_like_fallback(None,["abcdefghijk"],"2026/01/01 00:00:00")
    assert rows[0][6]==5 and diag["fallback_success"]==1

def test_fallback_missing(monkeypatch):
    monkeypatch.setattr('archivelogs.record_fetcher.fetch_videos_bulk', lambda y,ids:{ids[0]:{"id":ids[0],"snippet":{"title":"t"},"statistics":{},"contentDetails":{"duration":"PT10M"}}})
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_item', lambda y,vid:None)
    rows,diag=build_rows_with_like_fallback(None,["abcdefghijk"],"2026/01/01 00:00:00")
    assert rows[0][6]=="" and diag["missing_final"]==1

def test_filter_recordable_video_items():
    items=[
        {"id":"1","snippet":{"publishedAt":"2026-01-01T00:00:00Z","liveBroadcastContent":"none"},"status":{"privacyStatus":"public","uploadStatus":"processed"}},
        {"id":"2","snippet":{"publishedAt":"2026-01-02T00:00:00Z","liveBroadcastContent":"live"},"status":{"privacyStatus":"public","uploadStatus":"processed"}},
    ]
    out=filter_recordable_video_items(items)
    assert [x["id"] for x in out]==["1"]
