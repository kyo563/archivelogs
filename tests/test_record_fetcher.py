from archivelogs.record_fetcher import parse_stat_value, extract_video_id_from_title_cell, resolve_video_id

def test_like_exists():
    s={"viewCount":"100","likeCount":"12","commentCount":"3"}
    assert parse_stat_value(s,"likeCount")==12

def test_like_zero():
    s={"viewCount":"100","likeCount":"0","commentCount":"3"}
    assert parse_stat_value(s,"likeCount")==0

def test_like_missing():
    s={"viewCount":"100","commentCount":"3"}
    assert parse_stat_value(s,"likeCount")==""

def test_comment_missing():
    s={"viewCount":"100","likeCount":"12"}
    assert parse_stat_value(s,"commentCount")==""

def test_hyperlink_extract():
    assert extract_video_id_from_title_cell('=HYPERLINK("https://www.youtube.com/watch?v=xxxxxxxxxxx","title")')=="xxxxxxxxxxx"

def test_url_extract():
    assert resolve_video_id("https://youtu.be/xxxxxxxxxxx")=="xxxxxxxxxxx"
    assert resolve_video_id("https://www.youtube.com/shorts/xxxxxxxxxxx")=="xxxxxxxxxxx"
    assert resolve_video_id("xxxxxxxxxxx")=="xxxxxxxxxxx"
