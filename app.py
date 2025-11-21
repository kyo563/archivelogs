import streamlit as st
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import gspread

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
import re
from urllib.parse import urlparse, parse_qs

# ====================================
# 共通設定
# ====================================

st.set_page_config(page_title="YouTube ログ収集ツール", layout="wide")

# タイムゾーン（日本時間固定）
JST = timezone(timedelta(hours=9))

# スプレッドシート関連
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = st.secrets.get("SPREADSHEET_ID", None)
RECORD_SHEET_NAME = st.secrets.get("WORKSHEET_NAME", "record")
STATUS_SHEET_NAME = "Status"

if SPREADSHEET_ID is None:
    st.error('st.secrets["SPREADSHEET_ID"] が設定されていません。')
    st.stop()

# record シートのヘッダー
RECORD_HEADER = [
    "logged_at",
    "type",
    "title",
    "published_at",
    "duration_sec",
    "view_count",
    "like_count",
]

# Status シートのヘッダー
STATUS_HEADER = [
    "logged_at",                 # 1: 取得日時（JST）
    "channel_id",               # 2
    "channel_title",            # 3
    "subscriber_count",         # 4
    "video_count",              # 5
    "view_count",               # 6
    "channel_published_at",     # 7
    "months_active",            # 8
    "subs_per_month",           # 9
    "subs_per_video",           # 10
    "views_per_video",          # 11
    "views_per_sub",            # 12
    "subs_per_total_view",      # 13
    "playlists_per_video",      # 14
    "videos_per_month",         # 15
    "videos_per_subscriber",    # 16
    "top_playlist_1",           # 17
    "top_playlist_2",           # 18
    "top_playlist_3",           # 19
    "top_playlist_4",           # 20
    "top_playlist_5",           # 21
    "total_views_last10",       # 22
    "num_videos_last10",        # 23
    "top_title_last10",         # 24
    "top_views_last10",         # 25
    "top_share_last10",         # 26
    "avg_views_per_video_last10",  # 27
    "views_per_sub_last10",     # 28
    "total_views_last30",       # 29
    "num_videos_last30",        # 30
    "top_title_last30",         # 31
    "top_views_last30",         # 32
    "top_share_last30",         # 33
    "avg_views_per_video_last30",  # 34
    "views_per_sub_last30",     # 35
]


# ====================================
# YouTube / Sheets クライアント
# ====================================

# YouTube API キー
API_KEY = st.secrets.get("YOUTUBE_API_KEY", None)
if not API_KEY:
    API_KEY = st.sidebar.text_input("YouTube API Key (一時入力可)", type="password")


@st.cache_resource
def get_youtube_client(api_key: str):
    if not api_key:
        raise RuntimeError("YouTube API key is not configured.")
    return build("youtube", "v3", developerKey=api_key)


@st.cache_resource
def get_gspread_client():
    sa_info = st.secrets.get("gcp_service_account")
    if sa_info is None:
        raise RuntimeError('st.secrets["gcp_service_account"] が設定されていません。')
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)


def get_record_worksheet():
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(RECORD_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=RECORD_SHEET_NAME, rows=1000, cols=20)
        ws.append_row(RECORD_HEADER)
        return ws

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(RECORD_HEADER)
    return ws


def get_status_worksheet():
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(STATUS_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=STATUS_SHEET_NAME, rows=1000, cols=50)
        ws.append_row(STATUS_HEADER)
        return ws

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(STATUS_HEADER)
    return ws


def append_rows(ws, rows: List[List]):
    """gspread の append_rows がない場合も考慮してラップ"""
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except AttributeError:
        for r in rows:
            ws.append_row(r, value_input_option="USER_ENTERED")


# ====================================
# 各種ユーティリティ
# ====================================

def parse_iso8601_duration(duration: str) -> int:
    """
    ISO8601 の duration (例: PT1H2M3S) を秒数に変換
    """
    if not duration:
        return 0
    pattern = re.compile(
        r"^PT"
        r"(?:(\d+)H)?"
        r"(?:(\d+)M)?"
        r"(?:(\d+)S)?"
        r"$"
    )
    m = pattern.match(duration)
    if not m:
        return 0
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def resolve_channel_id_simple(url_or_id: str, api_key: str) -> Optional[str]:
    """
    URL / ID / 表示名 からチャンネルID(UC〜)を推定して返す。
    - 既に UC〜24桁 → それを返す
    - URLに channel/UC〜 が含まれていれば抜き出す
    - それ以外は search().list(type=channel) で検索し、最初のチャンネルIDを返す
    """
    s = (url_or_id or "").strip()
    if not s:
        return None

    if s.startswith("UC") and len(s) == 24:
        return s

    if "channel/" in s:
        return s.split("channel/")[1].split("/")[0]

    youtube = get_youtube_client(api_key)
    try:
        resp = youtube.search().list(
            q=s,
            type="channel",
            part="id,snippet",
            maxResults=3,
        ).execute()
        items = resp.get("items", [])
        if not items:
            return None
        return items[0].get("id", {}).get("channelId")
    except Exception:
        return None


def resolve_video_id(url_or_id: str) -> Optional[str]:
    """
    YouTube URL / 動画ID から videoId を抽出
    """
    s = (url_or_id or "").strip()
    if not s:
        return None

    # watch URL
    if "youtube.com/watch" in s and "v=" in s:
        try:
            parsed = urlparse(s)
            qs = parse_qs(parsed.query)
            vid = qs.get("v", [None])[0]
            if vid:
                return vid
        except Exception:
            pass

    # youtu.be短縮
    if "youtu.be/" in s:
        return s.split("youtu.be/")[1].split("?")[0].split("/")[0]

    # shorts URL
    if "youtube.com/shorts/" in s:
        return s.split("shorts/")[1].split("?")[0].split("/")[0]

    # 素の動画IDっぽいもの
    if len(s) == 11 and "/" not in s and " " not in s:
        return s

    return None


# ====================================
# record シート向け：動画取得
# ====================================

def fetch_channel_upload_items(channel_id: str, max_results: int, api_key: str) -> List[Dict]:
    """
    チャンネルの uploads プレイリストから最新 max_results 件の video item を取得
    （非公開、未処理、ライブ前のものは除外）
    """
    youtube = get_youtube_client(api_key)

    # uploads プレイリストID取得
    try:
        ch_resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id,
            maxResults=1,
        ).execute()
        items = ch_resp.get("items", [])
        if not items:
            return []
        uploads_playlist_id = (
            items[0]
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads")
        )
        if not uploads_playlist_id:
            return []
    except Exception:
        return []

    # uploads プレイリストから videoId を取得
    try:
        pl_resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=max_results,
        ).execute()
    except Exception:
        return []

    video_ids = []
    for it in pl_resp.get("items", []):
        cd = it.get("contentDetails", {}) or {}
        vid = cd.get("videoId")
        if vid:
            video_ids.append(vid)

    if not video_ids:
        return []

    # 動画詳細
    try:
        v_resp = youtube.videos().list(
            part="snippet,contentDetails,statistics,status,liveStreamingDetails",
            id=",".join(video_ids),
            maxResults=max_results,
        ).execute()
    except Exception:
        return []

    filtered: List[Dict] = []
    for it in v_resp.get("items", []):
        snippet = it.get("snippet", {}) or {}
        status = it.get("status", {}) or {}

        # 公開済み・public のみ
        if status.get("privacyStatus") != "public":
            continue
        if status.get("uploadStatus") != "processed":
            continue

        # まだライブ前 / 実況中を除外
        if snippet.get("liveBroadcastContent") in ("live", "upcoming"):
            continue

        filtered.append(it)

    # 投稿日時の昇順（古い順）に並べ替え
    filtered_sorted = sorted(
        filtered,
        key=lambda x: (x.get("snippet", {}).get("publishedAt") or ""),
    )
    return filtered_sorted


def fetch_single_video_item(video_id: str, api_key: str) -> Optional[Dict]:
    youtube = get_youtube_client(api_key)
    try:
        resp = youtube.videos().list(
            part="snippet,contentDetails,statistics,status,liveStreamingDetails",
            id=video_id,
            maxResults=1,
        ).execute()
    except Exception:
        return None

    items = resp.get("items", [])
    if not items:
        return None

    it = items[0]
    snippet = it.get("snippet", {}) or {}
    status = it.get("status", {}) or {}

    if status.get("privacyStatus") != "public":
        return None
    if status.get("uploadStatus") != "processed":
        return None
    if snippet.get("liveBroadcastContent") in ("live", "upcoming"):
        return None

    return it


def build_record_row_from_video_item(item: Dict, logged_at_str: str) -> List:
    snippet = item.get("snippet", {}) or {}
    content = item.get("contentDetails", {}) or {}
    stats = item.get("statistics", {}) or {}
    live_details = item.get("liveStreamingDetails", {}) or {}
    video_id = item.get("id")

    duration_iso = content.get("duration", "PT0S")
    duration_sec = parse_iso8601_duration(duration_iso)

    # 種別判定
    vtype = "video"
    if live_details:
        vtype = "live"
    elif duration_sec <= 61:
        vtype = "short"

    # 投稿日（JST）
    published_raw = snippet.get("publishedAt")
    if published_raw:
        try:
            published_dt_utc = datetime.fromisoformat(
                published_raw.replace("Z", "+00:00")
            )
            published_dt_jst = published_dt_utc.astimezone(JST)
            published_str = published_dt_jst.strftime("%Y/%m/%d %H:%M:%S")
        except Exception:
            published_str = ""
    else:
        published_str = ""

    view_count = int(stats.get("viewCount", 0) or 0)
    like_count = int(stats.get("likeCount", 0) or 0)

    # タイトル＋ハイパーリンク（Sheets の HYPERLINK 関数）
    title_raw = (snippet.get("title") or "").replace("\n", " ").strip()
    title_escaped = title_raw.replace('"', '""')  # ダブルクオート二重化
    url = f"https://www.youtube.com/watch?v={video_id}"
    title_cell = f'=HYPERLINK("{url}","{title_escaped}")'

    return [
        logged_at_str,
        vtype,
        title_cell,
        published_str,
        duration_sec,
        view_count,
        like_count,
    ]


# ====================================
# Status シート向け：チャンネル統計
# ====================================

def get_channel_basic(channel_id: str, api_key: str) -> Optional[Dict]:
    youtube = get_youtube_client(api_key)
    try:
        resp = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id,
            maxResults=1,
        ).execute()
    except Exception:
        return None

    items = resp.get("items", [])
    if not items:
        return None

    it = items[0]
    snippet = it.get("snippet", {}) or {}
    stats = it.get("statistics", {}) or {}
    uploads = it.get("contentDetails", {}).get("relatedPlaylists", {}) or {}

    return {
        "channelId": channel_id,
        "title": snippet.get("title"),
        "publishedAt": snippet.get("publishedAt"),
        "subscriberCount": int(stats.get("subscriberCount", 0) or 0),
        "videoCount": int(stats.get("videoCount", 0) or 0),
        "viewCount": int(stats.get("viewCount", 0) or 0),
        "uploadsPlaylistId": uploads.get("uploads"),
    }


def get_playlists_meta(channel_id: str, api_key: str) -> List[Dict]:
    youtube = get_youtube_client(api_key)
    pls: List[Dict] = []
    next_page: Optional[str] = None

    try:
        while True:
            resp = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page,
            ).execute()
            for pl in resp.get("items", []):
                pls.append(
                    {
                        "playlistId": pl.get("id"),
                        "title": pl.get("snippet", {}).get("title"),
                        "itemCount": int(pl.get("contentDetails", {}).get("itemCount", 0) or 0),
                    }
                )
            next_page = resp.get("nextPageToken")
            if not next_page:
                break
    except Exception:
        pass

    return pls


def search_video_ids_published_after(
    channel_id: str,
    days: int,
    api_key: str,
) -> List[str]:
    youtube = get_youtube_client(api_key)
    video_ids: List[str] = []

    published_after = (
        datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=days)
    ).isoformat().replace("+00:00", "Z")

    next_page: Optional[str] = None

    try:
        while True:
            resp = youtube.search().list(
                part="id",
                channelId=channel_id,
                publishedAfter=published_after,
                type="video",
                maxResults=50,
                pageToken=next_page,
            ).execute()
            for item in resp.get("items", []):
                vid = item.get("id", {}).get("videoId")
                if vid:
                    video_ids.append(vid)
            next_page = resp.get("nextPageToken")
            if not next_page:
                break
    except Exception:
        pass

    return video_ids


def get_videos_stats(video_ids: Tuple[str, ...], api_key: str) -> Dict[str, Dict]:
    youtube = get_youtube_client(api_key)
    out: Dict[str, Dict] = {}

    if not video_ids:
        return out

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i: i + 50]
        try:
            resp = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(chunk),
                maxResults=50,
            ).execute()
            for it in resp.get("items", []):
                vid = it.get("id")
                if not vid:
                    continue
                snippet = it.get("snippet", {}) or {}
                stats = it.get("statistics", {}) or {}
                out[vid] = {
                    "title": snippet.get("title", "") or "",
                    "viewCount": int(stats.get("viewCount", 0) or 0),
                    "likeCount": int(stats.get("likeCount", 0) or 0),
                }
        except Exception:
            continue

    return out


# ====================================
# UI
# ====================================

st.title("YouTube ログ収集ツール")

if not API_KEY:
    st.warning("左サイドバーから YouTube API Key を入力してください。")

tab_logs, tab_status = st.tabs(["動画ログ収集（record）", "チャンネルステータス（Status）"])

# ---------------------------
# タブ1：動画ログ収集（record）
# ---------------------------
with tab_logs:
    st.subheader("動画ログ収集（record シート）")

    col_in1, col_in2 = st.columns(2)
    with col_in1:
        channel_input = st.text_input(
            "チャンネルID / URL（最新50件を record に追記）",
            key="channel_input",
        )
    with col_in2:
        video_input = st.text_input(
            "動画ID / URL（単体動画を record に追記）",
            key="video_input",
        )

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        fetch_channel_btn = st.button("チャンネルから最新50件を record へ追記")
    with col_btn2:
        fetch_video_btn = st.button("動画1本を record へ追記")

    # --- チャンネルから最新50件 ---
    if fetch_channel_btn:
        if not API_KEY:
            st.error("APIキー未設定です。サイドバーまたは secrets に設定してください。")
        else:
            channel_id = resolve_channel_id_simple(channel_input, API_KEY)
            if not channel_id:
                st.error("チャンネルIDを解決できませんでした。URL / ID / 表示名を確認してください。")
            else:
                try:
                    ws_record = get_record_worksheet()
                except Exception as e:
                    st.error(f"record シートの取得に失敗しました: {e}")
                else:
                    items = fetch_channel_upload_items(channel_id, max_results=50, api_key=API_KEY)
                    if not items:
                        st.warning("取得対象となる公開済み動画が見つかりませんでした。")
                    else:
                        now_jst = datetime.now(JST)
                        logged_at_str = now_jst.strftime("%Y/%m/%d %H:%M:%S")
                        rows = [
                            build_record_row_from_video_item(it, logged_at_str)
                            for it in items
                        ]
                        append_rows(ws_record, rows)
                        st.success(f"{len(rows)} 件の動画を record シートに追記しました。")

    # --- 単体動画 ---
    if fetch_video_btn:
        if not API_KEY:
            st.error("APIキー未設定です。サイドバーまたは secrets に設定してください。")
        else:
            vid = resolve_video_id(video_input)
            if not vid:
                st.error("動画ID / URL を解釈できませんでした。")
            else:
                try:
                    ws_record = get_record_worksheet()
                except Exception as e:
                    st.error(f"record シートの取得に失敗しました: {e}")
                else:
                    item = fetch_single_video_item(vid, API_KEY)
                    if not item:
                        st.warning("対象の公開済み動画が見つかりませんでした（非公開 / 未処理 / ライブ前など）。")
                    else:
                        now_jst = datetime.now(JST)
                        logged_at_str = now_jst.strftime("%Y/%m/%d %H:%M:%S")
                        row = build_record_row_from_video_item(item, logged_at_str)
                        ws_record.append_row(row, value_input_option="USER_ENTERED")
                        st.success("1件の動画を record シートに追記しました。")

# ---------------------------
# タブ2：チャンネルステータス（Status）
# ---------------------------
with tab_status:
    st.subheader("チャンネルステータス記録（Status シート）")

    status_input = st.text_input(
        "チャンネルID / URL（Status にスナップショットを1行追加）",
        key="status_input",
    )
    status_btn = st.button("Status シートに最新スナップショットを追記")

    if status_btn:
        if not API_KEY:
            st.error("APIキー未設定です。サイドバーまたは secrets に設定してください。")
        else:
            channel_id = resolve_channel_id_simple(status_input, API_KEY)
            if not channel_id:
                st.error("チャンネルIDを解決できませんでした。URL / ID / 表示名を確認してください。")
            else:
                basic = get_channel_basic(channel_id, API_KEY)
                if not basic:
                    st.error("チャンネル情報の取得に失敗しました。")
                else:
                    # 基本情報
                    now_jst = datetime.now(JST)
                    data_datetime_str = now_jst.strftime("%Y/%m/%d %H:%M:%S")

                    published_at_raw = basic.get("publishedAt")
                    published_dt = None
                    if published_at_raw:
                        try:
                            published_dt = datetime.fromisoformat(
                                published_at_raw.replace("Z", "+00:00")
                            )
                        except Exception:
                            published_dt = None

                    if published_dt:
                        days_active = (datetime.utcnow().replace(tzinfo=timezone.utc) - published_dt).days
                        months_active = round(days_active / 30, 2)
                    else:
                        months_active = None

                    subs = basic.get("subscriberCount", 0)
                    vids_total = basic.get("videoCount", 0)
                    views_total = basic.get("viewCount", 0)

                    # 直近10日・30日
                    ids_10 = search_video_ids_published_after(channel_id, 10, API_KEY)
                    stats_10 = get_videos_stats(tuple(ids_10), API_KEY) if ids_10 else {}
                    total_views_last10 = sum(v.get("viewCount", 0) for v in stats_10.values())
                    num_videos_last10 = len(stats_10)

                    ids_30 = search_video_ids_published_after(channel_id, 30, API_KEY)
                    stats_30 = get_videos_stats(tuple(ids_30), API_KEY) if ids_30 else {}
                    total_views_last30 = sum(v.get("viewCount", 0) for v in stats_30.values())
                    num_videos_last30 = len(stats_30)

                    # 直近10日のトップ動画
                    if num_videos_last10 > 0:
                        top_vid_10 = max(
                            stats_10.items(),
                            key=lambda kv: kv[1]["viewCount"],
                        )
                        top_info_10 = top_vid_10[1]
                        top_title_last10 = (top_info_10.get("title") or "").replace("\n", " ").strip()
                        top_views_last10 = top_info_10["viewCount"]
                        top_share_last10 = (
                            round(
                                (top_views_last10 / total_views_last10)
                                if total_views_last10 > 0 else 0.0,
                                4,
                            )
                        )
                    else:
                        top_title_last10 = ""
                        top_views_last10 = 0
                        top_share_last10 = 0.0

                    # 直近30日のトップ動画
                    if num_videos_last30 > 0:
                        top_vid_30 = max(
                            stats_30.items(),
                            key=lambda kv: kv[1]["viewCount"],
                        )
                        top_info_30 = top_vid_30[1]
                        top_title_last30 = (top_info_30.get("title") or "").replace("\n", " ").strip()
                        top_views_last30 = top_info_30["viewCount"]
                        top_share_last30 = (
                            round(
                                (top_views_last30 / total_views_last30)
                                if total_views_last30 > 0 else 0.0,
                                4,
                            )
                        )
                    else:
                        top_title_last30 = ""
                        top_views_last30 = 0
                        top_share_last30 = 0.0

                    # 各種指標
                    views_per_sub = round((views_total / subs), 2) if subs > 0 else 0.0
                    subs_per_total_view = (
                        round((subs / views_total), 5) if views_total > 0 else 0.0
                    )
                    views_per_video = (
                        round((views_total / vids_total), 2) if vids_total > 0 else 0.0
                    )

                    views_per_sub_last10 = (
                        round((total_views_last10 / subs), 5) if subs > 0 else 0.0
                    )
                    views_per_sub_last30 = (
                        round((total_views_last30 / subs), 5) if subs > 0 else 0.0
                    )

                    avg_views_per_video_last10 = (
                        round((total_views_last10 / num_videos_last10), 2)
                        if num_videos_last10 > 0 else 0.0
                    )
                    avg_views_per_video_last30 = (
                        round((total_views_last30 / num_videos_last30), 2)
                        if num_videos_last30 > 0 else 0.0
                    )

                    playlists_meta = get_playlists_meta(channel_id, API_KEY)
                    playlist_count = len(playlists_meta)
                    playlists_sorted = sorted(
                        playlists_meta,
                        key=lambda x: x["itemCount"],
                        reverse=True,
                    )
                    top5_playlists = playlists_sorted[:5]
                    while len(top5_playlists) < 5:
                        top5_playlists.append({"title": "-", "itemCount": "-"})

                    playlists_per_video = (
                        round((playlist_count / vids_total), 5) if vids_total > 0 else 0.0
                    )
                    videos_per_month = (
                        round((vids_total / months_active), 2)
                        if months_active is not None and months_active > 0
                        else 0.0
                    )
                    videos_per_subscriber = (
                        round((vids_total / subs), 5) if subs > 0 else 0.0
                    )
                    subs_per_month = (
                        round((subs / months_active), 2)
                        if months_active is not None and months_active > 0
                        else 0.0
                    )
                    subs_per_video = (
                        round((subs / vids_total), 2) if vids_total > 0 else 0.0
                    )

                    # プレイリスト列
                    playlist_cols = []
                    for pl in top5_playlists:
                        title = (pl.get("title", "") or "").replace("\n", " ").strip()
                        ic = pl.get("itemCount", "")
                        if ic in ("", None, "-"):
                            playlist_cols.append(title)
                        else:
                            playlist_cols.append(f"{title}→{ic}")

                    # Status 行データ
                    status_row = [
                        data_datetime_str,                                 # logged_at
                        channel_id,                                        # channel_id
                        basic.get("title") or "",                          # channel_title
                        subs,                                              # subscriber_count
                        vids_total,                                        # video_count
                        views_total,                                       # view_count
                        published_dt.strftime("%Y-%m-%d") if published_dt else "",  # channel_published_at
                        months_active if months_active is not None else "",        # months_active
                        subs_per_month,                                    # subs_per_month
                        subs_per_video,                                    # subs_per_video
                        views_per_video,                                   # views_per_video
                        views_per_sub,                                     # views_per_sub
                        subs_per_total_view,                               # subs_per_total_view
                        playlists_per_video,                               # playlists_per_video
                        videos_per_month,                                  # videos_per_month
                        videos_per_subscriber,                             # videos_per_subscriber
                        *playlist_cols,                                    # top_playlist_1〜5
                        total_views_last10,                                # total_views_last10
                        num_videos_last10,                                 # num_videos_last10
                        top_title_last10,                                  # top_title_last10
                        top_views_last10,                                  # top_views_last10
                        top_share_last10,                                  # top_share_last10
                        avg_views_per_video_last10,                        # avg_views_per_video_last10
                        views_per_sub_last10,                              # views_per_sub_last10
                        total_views_last30,                                # total_views_last30
                        num_videos_last30,                                 # num_videos_last30
                        top_title_last30,                                  # top_title_last30
                        top_views_last30,                                  # top_views_last30
                        top_share_last30,                                  # top_share_last30
                        avg_views_per_video_last30,                        # avg_views_per_video_last30
                        views_per_sub_last30,                              # views_per_sub_last30
                    ]

                    try:
                        ws_status = get_status_worksheet()
                        ws_status.append_row(status_row, value_input_option="USER_ENTERED")
                        st.success("Status シートに最新スナップショットを 1 行追記しました。")
                    except Exception as e:
                        st.error(f"Status シートへの書き込みに失敗しました: {e}")
                    else:
                        # ざっくり画面にも出しておく
                        st.write("---")
                        st.write(f"チャンネル名: {basic.get('title')}")
                        st.write(f"登録者数: {subs}")
                        st.write(f"動画本数: {vids_total}")
                        st.write(f"総再生回数: {views_total}")
                        st.write(f"活動開始日: {published_dt.strftime('%Y-%m-%d') if published_dt else '不明'}")
                        st.write(f"活動月数: {months_active if months_active is not None else '-'}")
                        st.write(f"累計登録者数/活動月: {subs_per_month}")
                        st.write(f"累計動画あたり総再生回数: {views_per_video}")
                        st.write(f"直近10日 合計再生数: {total_views_last10}（投稿数: {num_videos_last10}）")
                        st.write(f"直近30日 合計再生数: {total_views_last30}（投稿数: {num_videos_last30}）")
