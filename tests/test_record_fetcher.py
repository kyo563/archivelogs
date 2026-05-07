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
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":True,"like_count":5})
    rows,diag=build_rows_with_like_fallback(None,["abcdefghijk"],"2026/01/01 00:00:00")
    assert rows[0][6]==5 and diag["fallback_success"]==1

def test_fallback_missing(monkeypatch):
    monkeypatch.setattr('archivelogs.record_fetcher.fetch_videos_bulk', lambda y,ids:{ids[0]:{"id":ids[0],"snippet":{"title":"t"},"statistics":{},"contentDetails":{"duration":"PT10M"}}})
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":False,"final_reason":"no_item_returned","attempts":[]})
    rows,diag=build_rows_with_like_fallback(None,["abcdefghijk"],"2026/01/01 00:00:00")
    assert rows[0][6]=="" and diag["missing_final"]==1

def test_filter_recordable_video_items():
    items=[
        {"id":"1","snippet":{"publishedAt":"2026-01-01T00:00:00Z","liveBroadcastContent":"none"},"status":{"privacyStatus":"public","uploadStatus":"processed"}},
        {"id":"2","snippet":{"publishedAt":"2026-01-02T00:00:00Z","liveBroadcastContent":"live"},"status":{"privacyStatus":"public","uploadStatus":"processed"}},
    ]
    out=filter_recordable_video_items(items)
    assert [x["id"] for x in out]==["1"]


def test_fallback_missing_reason_likecount(monkeypatch):
    monkeypatch.setattr('archivelogs.record_fetcher.fetch_videos_bulk', lambda y,ids:{ids[0]:{"id":ids[0],"snippet":{"title":"t"},"statistics":{},"contentDetails":{"duration":"PT10M"}}})
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":False,"final_reason":"likeCount_missing","attempts":[{"returned":True,"statistics_keys":["viewCount"],"has_likeCount":False},{"returned":True,"statistics_keys":["viewCount","commentCount"],"has_likeCount":False,"privacyStatus":"public","uploadStatus":"processed","liveBroadcastContent":"none"}]})
    rows,diag=build_rows_with_like_fallback(None,["abcdefghijk"],"2026/01/01 00:00:00")
    assert rows[0][6]=="" and diag["missing_final"]==1 and diag["missing_likeCount_missing"]==1

def test_fallback_missing_reason_no_item(monkeypatch):
    monkeypatch.setattr('archivelogs.record_fetcher.fetch_videos_bulk', lambda y,ids:{ids[0]:{"id":ids[0],"snippet":{"title":"t"},"statistics":{},"contentDetails":{"duration":"PT10M"}}})
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":False,"final_reason":"no_item_returned","attempts":[{"returned":False},{"returned":False}]})
    _rows,diag=build_rows_with_like_fallback(None,["abcdefghijk"],"2026/01/01 00:00:00")
    assert diag["missing_no_item"]==1


def test_zero_like_recheck_targeted_by_view(monkeypatch):
    item={"id":"abcdefghijk","snippet":{"title":"t"},"statistics":{"likeCount":"0","viewCount":"10","commentCount":"0"},"contentDetails":{"duration":"PT10M"}}
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":True,"like_count":0})
    rows,diag=build_rows_from_video_items_with_like_fallback(None,[item],"2026/01/01 00:00:00")
    assert rows[0][6]==0 and diag["zero_like_initial"]==1

def test_zero_like_recheck_targeted_by_comment(monkeypatch):
    item={"id":"abcdefghijk","snippet":{"title":"t"},"statistics":{"likeCount":"0","viewCount":"0","commentCount":"2"},"contentDetails":{"duration":"PT10M"}}
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":True,"like_count":0})
    _rows,diag=build_rows_from_video_items_with_like_fallback(None,[item],"2026/01/01 00:00:00")
    assert diag["zero_like_initial"]==1

def test_zero_like_recheck_not_targeted_when_no_signal(monkeypatch):
    item={"id":"abcdefghijk","snippet":{"title":"t"},"statistics":{"likeCount":"0","viewCount":"0","commentCount":"0"},"contentDetails":{"duration":"PT10M"}}
    called={"n":0}
    def _fb(_y,_v):
        called["n"]+=1
        return {"success":True,"like_count":99}
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', _fb)
    rows,diag=build_rows_from_video_items_with_like_fallback(None,[item],"2026/01/01 00:00:00")
    assert rows[0][6]==0 and diag["zero_like_initial"]==0 and called["n"]==0

def test_zero_like_recheck_updates_when_positive(monkeypatch):
    item={"id":"abcdefghijk","snippet":{"title":"t"},"statistics":{"likeCount":"0","viewCount":"1"},"contentDetails":{"duration":"PT10M"}}
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":True,"like_count":"12"})
    rows,diag=build_rows_from_video_items_with_like_fallback(None,[item],"2026/01/01 00:00:00")
    assert rows[0][6]==12 and diag["zero_like_recheck_success"]==1

def test_zero_like_recheck_still_zero(monkeypatch):
    item={"id":"abcdefghijk","snippet":{"title":"t"},"statistics":{"likeCount":"0","viewCount":"1"},"contentDetails":{"duration":"PT10M"}}
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":True,"like_count":"0"})
    rows,diag=build_rows_from_video_items_with_like_fallback(None,[item],"2026/01/01 00:00:00")
    assert rows[0][6]==0 and diag["zero_like_still_zero"]==1

def test_zero_like_recheck_likecount_missing_reason(monkeypatch):
    item={"id":"abcdefghijk","snippet":{"title":"t"},"statistics":{"likeCount":"0","viewCount":"1"},"contentDetails":{"duration":"PT10M"}}
    monkeypatch.setattr('archivelogs.record_fetcher.fallback_fetch_like_count_diagnostic', lambda y,vid:{"success":False,"final_reason":"likeCount_missing"})
    rows,diag=build_rows_from_video_items_with_like_fallback(None,[item],"2026/01/01 00:00:00")
    assert rows[0][6]==0 and diag["zero_like_recheck_likeCount_missing"]==1
