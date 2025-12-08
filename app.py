"""YouTube チャンネルの統計を収集する Streamlit アプリ。

このファイルは純粋な Python コードのみで構成し、誤ってパッチヘッダーや
シェルコマンドの断片が混入しないように保守する。
"""

import json
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import gspread
import streamlit as st
import streamlit.components.v1 as components
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# ====================================
# 共通設定
# ====================================

st.set_page_config(page_title="ログ収集ツール", layout="wide")

# タイムゾーン（日本時間固定）
JST = timezone(timedelta(hours=9))

# スプレッドシート関連
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = st.secrets.get("SPREADSHEET_ID", None)
RECORD_SHEET_NAME = st.secrets.get("WORKSHEET_NAME", "record")
STATUS_SHEET_NAME = "Status"

if SPREADSHEET_ID is None:
    raise RuntimeError('st.secrets["SPREADSHEET_ID"] が設定されていません。')

# record シートのヘッダー
RECORD_HEADER = [
    "logged_at",       # 取得日時（JST, yyyy/mm/dd hh:mm:ss）
    "type",            # video / live / short
    "title",           # HYPERLINK付きタイトル
    "published_at",    # 公開日時（JST, yyyy/mm/dd hh:mm:ss）
    "duration_sec",    # 秒数
    "view_count",      # 再生数
    "like_count",      # 高評価数
]

# Status シートのヘッダー（日本語）
STATUS_HEADER = [
    "取得日時",                  # logged_at（JST, yyyy/mm/dd）
    "チャンネルID",              # channel_id
    "チャンネル名",              # channel_title
    "登録者数",                  # subscriber_count
    "動画本数",                  # video_count
    "総再生回数",                # view_count
    "チャンネル開設日",          # channel_published_at（JST, yyyy/mm/dd）
    "活動月数",                  # months_active
    "累計登録者数/活動月",       # subs_per_month
    "累計登録者数/動画",         # subs_per_video
    "累計動画あたり総再生回数",  # views_per_video
    "累計総再生回数/登録者数",   # views_per_sub
    "1再生あたり登録者増",       # subs_per_total_view
    "動画あたりプレイリスト数",   # playlists_per_video
    "活動月あたり動画本数",      # videos_per_month
    "登録者あたり動画本数",      # videos_per_subscriber
    "上位プレイリスト1",         # top_playlist_1
    "上位プレイリスト2",         # top_playlist_2
    "上位プレイリスト3",         # top_playlist_3
    "上位プレイリスト4",         # top_playlist_4
    "上位プレイリスト5",         # top_playlist_5
    "直近10日合計再生数",        # total_views_last10
    "直近10日投稿数",            # num_videos_last10
    "直近10日トップ動画タイトル",# top_title_last10
    "直近10日トップ動画再生数",  # top_views_last10
    "直近10日トップ動画シェア",  # top_share_last10
    "直近10日平均再生数/動画",   # avg_views_per_video_last10
    "直近10日視聴/登録比",       # views_per_sub_last10
    "直近30日合計再生数",        # total_views_last30
    "直近30日投稿数",            # num_videos_last30
    "直近30日トップ動画タイトル",# top_title_last30
    "直近30日トップ動画再生数",  # top_views_last30
    "直近30日トップ動画シェア",  # top_share_last30
    "直近30日平均再生数/動画",   # avg_views_per_video_last30
    "直近30日視聴/登録比",       # views_per_sub_last30
]

# YouTube Data API の概算クオータ
QUOTA_UNITS = {
    "channels.list": 1,
    "playlistItems.list": 1,
    "videos.list": 1,
    "search.list": 100,
    "playlists.list": 1,
}


# ====================================
# クオータ管理
# ====================================

def ensure_quota_state() -> Dict:
    if "quota_usage" not in st.session_state:
        st.session_state["quota_usage"] = {"total": 0, "by_endpoint": {}}
    return st.session_state["quota_usage"]


def add_quota_usage(endpoint: str, count: int = 1):
    usage = ensure_quota_state()
    units = QUOTA_UNITS.get(endpoint, 0) * count
    usage["total"] += units
    usage["by_endpoint"][endpoint] = usage["by_endpoint"].get(endpoint, 0) + units


def reset_quota_usage():
    st.session_state["quota_usage"] = {"total": 0, "by_endpoint": {}}


def render_quota_summary(label: str):
    usage = ensure_quota_state()
    st.markdown(f"### 概算クオータ（{label}）")
    st.write(f"概算クオータ {usage['total']} 単位（累計）")
    if usage["by_endpoint"]:
        rows = [
            {"エンドポイント": k, "概算単位": v}
            for k, v in sorted(
                usage["by_endpoint"].items(), key=lambda kv: kv[1], reverse=True
            )
        ]
        st.table(rows)
    else:
        st.write("まだ計測されたリクエストがありません。")

    if st.button("リセット", key=f"reset_quota_{label}"):
        reset_quota_usage()
        st.info("クオータ概算をリセットしました。")


# ====================================
# API キー / クライアント
# ====================================

def get_api_key_from_ui() -> Optional[str]:
    """
    secrets に YOUTUBE_API_KEY があればそれを使い、
    無ければサイドバーで手入力してもらう。
    """
    key = st.secrets.get("YOUTUBE_API_KEY", None)
    if not key:
        key = st.sidebar.text_input("YouTube API Key (一時入力可)", type="password")
    return key


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


# ====================================
# スプレッドシートユーティリティ
# ====================================

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
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except AttributeError:
        # 古い gspread 互換
        for r in rows:
            ws.append_row(r, value_input_option="USER_ENTERED")


# ====================================
# 共通ユーティリティ
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
    """
    s = (url_or_id or "").strip()
    if not s:
        return None

    # 生のチャンネルID（UC〜で始まる24桁）
    if s.startswith("UC") and len(s) == 24:
        return s

    # https://www.youtube.com/channel/UC... 形式
    if "channel/" in s:
        return s.split("channel/")[1].split("/")[0]

    youtube = get_youtube_client(api_key)
    try:
        add_quota_usage("search.list")
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
    URL or ID から videoId を抜き出す。
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

    # youtu.be
    if "youtu.be/" in s:
        return s.split("youtu.be/")[1].split("?")[0].split("/")[0]

    # shorts
    if "youtube.com/shorts/" in s:
        return s.split("shorts/")[1].split("?")[0].split("/")[0]

    # 素の videoId
    if len(s) == 11 and "/" not in s and " " not in s:
        return s

    return None


# ====================================
# record 用 YouTube処理
# ====================================

def fetch_channel_upload_items(channel_id: str, max_results: int, api_key: str) -> List[Dict]:
    """
    チャンネルのアップロード済み動画（公開・処理済・アーカイブ済みのみ）を
    公開日時の古い順に max_results 件まで取得。
    """
    youtube = get_youtube_client(api_key)
    # API の仕様上 1 回で取得できる件数は 50 件のため、上限を固定する
    max_results = min(max_results, 50)

    # uploads プレイリストID取得
    try:
        add_quota_usage("channels.list")
        ch_resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id,
            maxResults=1,
        ).execute()
        items = ch_resp.get("items", [])
        if not items:
            st.warning("チャンネルのアップロード情報が取得できませんでした。")
            return []
        uploads_playlist_id = (
            items[0]
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads")
        )
        if not uploads_playlist_id:
            st.warning("アップロード動画のプレイリストが見つかりませんでした。")
            return []
    except Exception as e:
        st.warning(f"チャンネル情報の取得に失敗しました: {e}")
        return []

    # playlistItems で videoId を取得
    video_ids: List[str] = []
    next_page: Optional[str] = None
    try:
        while True:
            remaining = max_results - len(video_ids)
            if remaining <= 0:
                break
            add_quota_usage("playlistItems.list")
            pl_resp = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=min(50, remaining),
                pageToken=next_page,
            ).execute()
            for it in pl_resp.get("items", []):
                cd = it.get("contentDetails", {}) or {}
                vid = cd.get("videoId")
                if vid:
                    video_ids.append(vid)
            next_page = pl_resp.get("nextPageToken")
            if not next_page:
                break
    except Exception as e:
        st.warning(f"アップロード動画の取得に失敗しました: {e}")
        return []

    if not video_ids:
        return []

    # video 本体
    try:
        add_quota_usage("videos.list")
        v_resp = youtube.videos().list(
            part="snippet,contentDetails,statistics,status,liveStreamingDetails",
            id=",".join(video_ids),
            maxResults=max_results,
        ).execute()
    except Exception as e:
        st.warning(f"動画情報の取得に失敗しました: {e}")
        return []

    filtered: List[Dict] = []
    for it in v_resp.get("items", []):
        snippet = it.get("snippet", {}) or {}
        status = it.get("status", {}) or {}

        # 公開済み・処理済みのみ
        if status.get("privacyStatus") != "public":
            continue
        if status.get("uploadStatus") != "processed":
            continue

        # ライブ中 / 予約中は除外（アーカイブになってから）
        if snippet.get("liveBroadcastContent") in ("live", "upcoming"):
            continue

        filtered.append(it)

    # 公開日時（昇順）でソート
    filtered_sorted = sorted(
        filtered,
        key=lambda x: (x.get("snippet", {}).get("publishedAt") or ""),
    )
    return filtered_sorted[:max_results]


def fetch_single_video_item(video_id: str, api_key: str) -> Optional[Dict]:
    """
    指定 videoId の動画を1件取得（公開・処理済み・アーカイブのみ）。
    """
    youtube = get_youtube_client(api_key)
    try:
        add_quota_usage("videos.list")
        resp = youtube.videos().list(
            part="snippet,contentDetails,statistics,status,liveStreamingDetails",
            id=video_id,
            maxResults=1,
        ).execute()
    except Exception as e:
        st.warning(f"動画情報の取得に失敗しました: {e}")
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
    """
    video API の item から record シート1行分を構成。
    """
    snippet = item.get("snippet", {}) or {}
    content = item.get("contentDetails", {}) or {}
    stats = item.get("statistics", {}) or {}
    live_details = item.get("liveStreamingDetails", {}) or {}
    video_id = item.get("id")

    # 長さ
    duration_iso = content.get("duration", "PT0S")
    duration_sec = parse_iso8601_duration(duration_iso)

    # 種別判定
    vtype = "video"
    if live_details:
        vtype = "live"
    elif duration_sec <= 61:
        vtype = "short"

    # 公開日時（JST）
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

    # タイトル（改行潰し）＋HYPERLINK
    title_raw = (snippet.get("title") or "").replace("\n", " ").strip()
    title_escaped = title_raw.replace('"', '""')
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
# Status 用 YouTube処理
# ====================================

def get_channel_basic(channel_id: str, api_key: str) -> Optional[Dict]:
    youtube = get_youtube_client(api_key)
    try:
        add_quota_usage("channels.list")
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
            add_quota_usage("playlists.list")
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
            add_quota_usage("search.list")
            resp = youtube.search().list(
                part="id",
                channelId=channel_id,
                publishedAfter=published_after,
                type="video",
                maxResults=50,
                order="date",  # 期間内の取りこぼしを避けるため公開日の降順で取得
                pageToken=next_page,
            ).execute()
            for item in resp.get("items", []):
                vid = item.get("id", {}).get("videoId")
                if vid:
                    video_ids.append(vid)
            next_page = resp.get("nextPageToken")
            if not next_page:
                break
    except Exception as e:
        st.warning(f"期間内動画の検索に失敗しました: {e}")

    return video_ids


def get_videos_stats(video_ids: Tuple[str, ...], api_key: str) -> Dict[str, Dict]:
    youtube = get_youtube_client(api_key)
    out: Dict[str, Dict] = {}

    if not video_ids:
        return out

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i: i + 50]
        try:
            add_quota_usage("videos.list")
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
        except Exception as e:
            st.warning(f"動画統計情報の取得に失敗しました: {e}")
            continue

    return out


def compute_channel_status(channel_id: str, api_key: str) -> Optional[Dict]:
    """
    チャンネルステータス（Statusシート1行分＋TXT要約に必要な情報）をまとめて計算する。
    """
    basic = get_channel_basic(channel_id, api_key)
    if not basic:
        return None

    now_jst = datetime.now(JST)
    data_date_str = now_jst.strftime("%Y/%m/%d")

    # チャンネル開設日・活動月数
    published_at_raw = basic.get("publishedAt")
    published_dt: Optional[datetime] = None
    channel_published_str = ""
    months_active: Optional[float] = None

    if published_at_raw:
        try:
            published_dt = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
        except Exception:
            published_dt = None

    if published_dt:
        days_active = (
            datetime.utcnow().replace(tzinfo=timezone.utc) - published_dt
        ).days
        months_active = round(days_active / 30, 2)
        published_dt_jst = published_dt.astimezone(JST)
        channel_published_str = published_dt_jst.strftime("%Y/%m/%d")

    subs = basic.get("subscriberCount", 0)
    vids_total = basic.get("videoCount", 0)
    views_total = basic.get("viewCount", 0)

    # プレイリスト情報
    playlists_meta = get_playlists_meta(channel_id, api_key)
    playlist_count = len(playlists_meta)
    playlists_sorted = sorted(
        playlists_meta,
        key=lambda x: x["itemCount"],
        reverse=True,
    )
    top5_playlists = playlists_sorted[:5]
    while len(top5_playlists) < 5:
        top5_playlists.append({"title": "-", "itemCount": 0})

    playlist_cols: List[str] = []
    for pl in top5_playlists:
        title = (pl.get("title", "") or "").replace("\n", " ").strip()
        item_count = pl.get("itemCount", 0)
        if title == "-" and item_count == 0:
            playlist_cols.append("-")
        else:
            playlist_cols.append(f"{title} ({item_count}本)")

    # 集計指標
    subs_per_month = (
        round(subs / months_active, 2)
        if months_active is not None and months_active > 0
        else 0.0
    )
    subs_per_video = round(subs / vids_total, 2) if vids_total > 0 else 0.0
    views_per_video = round(views_total / vids_total, 2) if vids_total > 0 else 0.0
    views_per_sub = round(views_total / subs, 2) if subs > 0 else 0.0
    subs_per_total_view = (
        round(subs / views_total, 5) if views_total > 0 else 0.0
    )
    playlists_per_video = (
        round(playlist_count / vids_total, 5) if vids_total > 0 else 0.0
    )
    videos_per_month = (
        round(vids_total / months_active, 2)
        if months_active is not None and months_active > 0
        else 0.0
    )
    videos_per_subscriber = round(vids_total / subs, 5) if subs > 0 else 0.0

    # 直近10日
    ids_10 = search_video_ids_published_after(channel_id, 10, api_key)
    stats_10 = get_videos_stats(tuple(ids_10), api_key) if ids_10 else {}
    total_views_last10 = sum(v.get("viewCount", 0) for v in stats_10.values())
    num_videos_last10 = len(stats_10)

    if num_videos_last10 > 0:
        top_vid_10 = max(
            stats_10.items(),
            key=lambda kv: kv[1]["viewCount"],
        )
        top_info_10 = top_vid_10[1]
        top_views_last10 = top_info_10["viewCount"]
        top_share_last10 = (
            round(top_views_last10 / total_views_last10, 4)
            if total_views_last10 > 0
            else 0.0
        )
        top_title_last10 = (top_info_10.get("title") or "").replace("\n", " ").strip()
    else:
        top_title_last10 = ""
        top_views_last10 = 0
        top_share_last10 = 0.0

    avg_views_per_video_last10 = (
        round(total_views_last10 / num_videos_last10, 2)
        if num_videos_last10 > 0
        else 0.0
    )
    views_per_sub_last10 = (
        round(total_views_last10 / subs, 5) if subs > 0 else 0.0
    )

    # 直近30日
    ids_30 = search_video_ids_published_after(channel_id, 30, api_key)
    stats_30 = get_videos_stats(tuple(ids_30), api_key) if ids_30 else {}
    total_views_last30 = sum(v.get("viewCount", 0) for v in stats_30.values())
    num_videos_last30 = len(stats_30)

    if num_videos_last30 > 0:
        top_vid_30 = max(
            stats_30.items(),
            key=lambda kv: kv[1]["viewCount"],
        )
        top_info_30 = top_vid_30[1]
        top_views_last30 = top_info_30["viewCount"]
        top_share_last30 = (
            round(top_views_last30 / total_views_last30, 4)
            if total_views_last30 > 0
            else 0.0
        )
        top_title_last30 = (top_info_30.get("title") or "").replace("\n", " ").strip()
    else:
        top_title_last30 = ""
        top_views_last30 = 0
        top_share_last30 = 0.0

    avg_views_per_video_last30 = (
        round(total_views_last30 / num_videos_last30, 2)
        if num_videos_last30 > 0
        else 0.0
    )
    views_per_sub_last30 = (
        round(total_views_last30 / subs, 5) if subs > 0 else 0.0
    )

    return {
        "basic": basic,
        "data_date_str": data_date_str,
        "channel_id": channel_id,
        "channel_title": basic.get("title") or "",
        "subs": subs,
        "vids_total": vids_total,
        "views_total": views_total,
        "channel_published_str": channel_published_str,
        "months_active": months_active,
        "subs_per_month": subs_per_month,
        "subs_per_video": subs_per_video,
        "views_per_video": views_per_video,
        "views_per_sub": views_per_sub,
        "subs_per_total_view": subs_per_total_view,
        "playlist_count": playlist_count,
        "playlists_per_video": playlists_per_video,
        "videos_per_month": videos_per_month,
        "videos_per_subscriber": videos_per_subscriber,
        "top5_playlists": top5_playlists,
        "playlist_cols": playlist_cols,
        "total_views_last10": total_views_last10,
        "num_videos_last10": num_videos_last10,
        "top_title_last10": top_title_last10,
        "top_views_last10": top_views_last10,
        "top_share_last10": top_share_last10,
        "avg_views_per_video_last10": avg_views_per_video_last10,
        "views_per_sub_last10": views_per_sub_last10,
        "total_views_last30": total_views_last30,
        "num_videos_last30": num_videos_last30,
        "top_title_last30": top_title_last30,
        "top_views_last30": top_views_last30,
        "top_share_last30": top_share_last30,
        "avg_views_per_video_last30": avg_views_per_video_last30,
        "views_per_sub_last30": views_per_sub_last30,
    }


def build_status_row(status: Dict) -> List:
    """
    Status シート1行分の配列を構成する。
    """
    return [
        status["data_date_str"],
        status["channel_id"],
        status["channel_title"],
        status["subs"],
        status["vids_total"],
        status["views_total"],
        status["channel_published_str"],
        status["months_active"] if status["months_active"] is not None else "",
        status["subs_per_month"],
        status["subs_per_video"],
        status["views_per_video"],
        status["views_per_sub"],
        status["subs_per_total_view"],
        status["playlists_per_video"],
        status["videos_per_month"],
        status["videos_per_subscriber"],
        *status["playlist_cols"],
        status["total_views_last10"],
        status["num_videos_last10"],
        status["top_title_last10"],
        status["top_views_last10"],
        status["top_share_last10"],
        status["avg_views_per_video_last10"],
        status["views_per_sub_last10"],
        status["total_views_last30"],
        status["num_videos_last30"],
        status["top_title_last30"],
        status["top_views_last30"],
        status["top_share_last30"],
        status["avg_views_per_video_last30"],
        status["views_per_sub_last30"],
    ]


def build_status_summary_text(status: Dict) -> str:
    """
    ChatGPT へのコピペを前提とした、説明付きステータステキストを生成する。
    各数値が何を意味するかをラベルと補足で明示している。
    """
    lines: List[str] = []
    lines.append("=== チャンネルステータス（ChatGPT解析用）===")
    lines.append("")
    # 基本情報
    lines.append("■ 基本情報")
    lines.append(f"取得日時（JST）: {status['data_date_str']}")
    lines.append(f"チャンネルID: {status['channel_id']}")
    lines.append(f"チャンネル名: {status['channel_title']}")
    lines.append(f"登録者数（現在のチャンネル登録者数）: {status['subs']}")
    lines.append(f"動画本数（公開済み動画の本数）: {status['vids_total']}")
    lines.append(f"総再生回数（公開済み動画の累計再生回数）: {status['views_total']}")
    lines.append(f"チャンネル開設日（JST）: {status['channel_published_str']}")
    lines.append(f"活動月数（チャンネル開設からの経過月数）: {status['months_active']}")
    lines.append("")
    # 累計指標
    lines.append("■ 累計指標")
    lines.append(
        f"累計登録者数/活動月（1ヶ月あたりの平均登録者増加数）: {status['subs_per_month']}"
    )
    lines.append(
        f"累計登録者数/動画（動画1本あたりの平均登録者数）: {status['subs_per_video']}"
    )
    lines.append(
        f"累計動画あたり総再生回数（動画1本あたりの平均再生数）: {status['views_per_video']}"
    )
    lines.append(
        f"累計総再生回数/登録者数（登録者1人あたりの平均再生数）: {status['views_per_sub']}"
    )
    lines.append(
        f"1再生あたり登録者増（総再生回数に対する登録者数比）: {status['subs_per_total_view']}"
    )
    lines.append(
        f"動画あたりプレイリスト数（動画1本あたりに所属するプレイリスト数の平均）: {status['playlists_per_video']}"
    )
    lines.append(
        f"活動月あたり動画本数（1ヶ月あたりの動画投稿本数）: {status['videos_per_month']}"
    )
    lines.append(
        f"登録者あたり動画本数（登録者1人あたりに対応する動画本数）: {status['videos_per_subscriber']}"
    )
    lines.append("")
    # プレイリスト
    lines.append("■ 上位プレイリスト（件数順）")
    for i, pl in enumerate(status["top5_playlists"], start=1):
        title = (pl.get("title") or "").replace("\n", " ")
        count = pl.get("itemCount", 0)
        lines.append(f"{i}位: {title}（登録動画本数: {count}本）")
    lines.append("")
    # 直近10日
    lines.append("■ 直近10日（直近10日間に公開された動画）")
    lines.append(f"直近10日合計再生数: {status['total_views_last10']}")
    lines.append(f"直近10日投稿数（動画本数）: {status['num_videos_last10']}")
    lines.append(
        f"直近10日トップ動画タイトル（期間内で最も再生された動画）: {status['top_title_last10']}"
    )
    lines.append(f"直近10日トップ動画再生数: {status['top_views_last10']}")
    lines.append(
        f"直近10日トップ動画シェア（直近10日合計再生数に対するトップ動画再生数の割合）: {status['top_share_last10']}"
    )
    lines.append(
        f"直近10日平均再生/動画（直近10日合計再生数 ÷ 直近10日投稿数）: {status['avg_views_per_video_last10']}"
    )
    lines.append(
        f"直近10日視聴/登録比（直近10日合計再生数 ÷ 現在の登録者数）: {status['views_per_sub_last10']}"
    )
    lines.append("")
    # 直近30日
    lines.append("■ 直近30日（直近30日間に公開された動画）")
    lines.append(f"直近30日合計再生数: {status['total_views_last30']}")
    lines.append(f"直近30日投稿数（動画本数）: {status['num_videos_last30']}")
    lines.append(
        f"直近30日トップ動画タイトル（期間内で最も再生された動画）: {status['top_title_last30']}"
    )
    lines.append(f"直近30日トップ動画再生数: {status['top_views_last30']}")
    lines.append(
        f"直近30日トップ動画シェア（直近30日合計再生数に対するトップ動画再生数の割合）: {status['top_share_last30']}"
    )
    lines.append(
        f"直近30日平均再生/動画（直近30日合計再生数 ÷ 直近30日投稿数）: {status['avg_views_per_video_last30']}"
    )
    lines.append(
        f"直近30日視聴/登録比（直近30日合計再生数 ÷ 現在の登録者数）: {status['views_per_sub_last30']}"
    )

    return "\n".join(lines)


def build_status_numeric_text(status: Dict) -> str:
    """
    数値に対応する値だけを順番に並べたテキスト（TXT保存用）を生成する。
    ChatGPT に貼るときは、こちらではなく build_status_summary_text の方を使う。
    """
    lines: List[str] = []

    # 基本情報
    lines.append(status["data_date_str"])
    lines.append(status["channel_id"])
    lines.append(status["channel_title"])
    lines.append(str(status["subs"]))
    lines.append(str(status["vids_total"]))
    lines.append(str(status["views_total"]))

    # 開設日をハイフン区切りに寄せる（例: 2022-05-02）
    opened = status["channel_published_str"] or ""
    lines.append(opened.replace("/", "-") if opened else "")

    # 累計指標
    def _fmt(v):
        return "" if v is None else str(v)

    lines.append(_fmt(status["months_active"]))
    lines.append(_fmt(status["subs_per_month"]))
    lines.append(_fmt(status["subs_per_video"]))
    lines.append(_fmt(status["views_per_video"]))
    lines.append(_fmt(status["views_per_sub"]))
    lines.append(_fmt(status["subs_per_total_view"]))
    lines.append(_fmt(status["playlists_per_video"]))
    lines.append(_fmt(status["videos_per_month"]))
    lines.append(_fmt(status["videos_per_subscriber"]))

    # 上位プレイリスト（タイトル→本数）
    for pl in status["top5_playlists"]:
        title = (pl.get("title") or "").replace("\n", " ")
        count = pl.get("itemCount", 0)
        lines.append(f"{title}→{count}")

    # 直近10日
    lines.append(str(status["total_views_last10"]))
    lines.append(str(status["num_videos_last10"]))
    lines.append(status["top_title_last10"])
    lines.append(str(status["top_views_last10"]))
    lines.append(str(status["top_share_last10"]))
    lines.append(str(status["avg_views_per_video_last10"]))
    lines.append(str(status["views_per_sub_last10"]))

    # 直近30日
    lines.append(str(status["total_views_last30"]))
    lines.append(str(status["num_videos_last30"]))
    lines.append(status["top_title_last30"])
    lines.append(str(status["top_views_last30"]))
    lines.append(str(status["top_share_last30"]))
    lines.append(str(status["avg_views_per_video_last30"]))
    lines.append(str(status["views_per_sub_last30"]))

    return "\n".join(lines)


# ====================================
# UI 本体
# ====================================

st.title("ログ収集ツール")

# API キー入力はここで一度だけ
api_key = get_api_key_from_ui()


def run_config_diagnostics(api_key: Optional[str]):
    """APIキーとスプレッドシート接続の簡易チェックを行う。"""

    with st.sidebar.expander("設定チェック", expanded=False):
        st.write("YouTube API とスプレッドシート接続の動作確認を行います。")
        if st.button("接続を検証", key="run_config_check"):
            if not api_key:
                st.error("YouTube API Key が未入力のため検証できません。")
            else:
                try:
                    yt = get_youtube_client(api_key)
                    add_quota_usage("videos.list")
                    yt.videos().list(
                        part="id",
                        id="dQw4w9WgXcQ",
                        maxResults=1,
                    ).execute()
                    st.success("YouTube API に接続できました。")
                except Exception as e:  # APIキー無効や権限不足などを可視化
                    st.error(f"YouTube API への接続に失敗しました: {e}")

            try:
                spreadsheet = get_gspread_client().open_by_key(SPREADSHEET_ID)
                st.success(f"スプレッドシート『{spreadsheet.title}』に接続できました。")
            except Exception as e:
                st.error(f"スプレッドシートへの接続に失敗しました: {e}")


run_config_diagnostics(api_key)

tab_logs, tab_status, tab_status_txt = st.tabs(
    ["ログ（Record）", "ステータス（Status）", "ステータス解析（TXT/コピーのみ）"]
)

# ----------------------------
# タブ1: 動画ログ収集（record）
# ----------------------------
with tab_logs:
    st.subheader("Recordシート")
    render_quota_summary("Record")

    if not api_key:
        st.info("サイドバーから YouTube API Key を入力してください。")
    else:
        channel_input = st.text_input("チャンネルURL / ID（直近50件を取得）", "")
        video_input = st.text_input("動画URL / ID（任意・1件だけ取得）", "")

        col1, col2 = st.columns(2)
        with col1:
            run_recent_btn = st.button("直近50件を Record に追記")
        with col2:
            run_single_btn = st.button("この動画だけ Record に追記")

        ws_record = get_record_worksheet()

        # 直近50件（チャンネル）
        if run_recent_btn:
            if not channel_input.strip():
                st.error("チャンネルURL / ID を入力してください。")
            else:
                channel_id = resolve_channel_id_simple(channel_input, api_key)
                if not channel_id:
                    st.error("チャンネルIDを解決できませんでした。")
                else:
                    with st.spinner("直近50件を取得中..."):
                        items = fetch_channel_upload_items(
                            channel_id, max_results=50, api_key=api_key
                        )
                    if not items:
                        st.warning("取得できる動画がありませんでした。")
                    else:
                        now_jst = datetime.now(JST)
                        logged_at_str = now_jst.strftime("%Y/%m/%d %H:%M:%S")
                        rows = [
                            build_record_row_from_video_item(it, logged_at_str)
                            for it in items
                        ]
                        append_rows(ws_record, rows)
                        st.success(f"{len(rows)}件の動画ログを Record シートに追記しました。")

        # 単一動画
        if run_single_btn:
            if not video_input.strip():
                st.error("動画URL / ID を入力してください。")
            else:
                vid = resolve_video_id(video_input)
                if not vid:
                    st.error("動画IDを解決できませんでした。URL / ID を確認してください。")
                else:
                    with st.spinner("動画情報を取得中..."):
                        item = fetch_single_video_item(vid, api_key)
                    if not item:
                        st.error("指定した動画が取得できませんでした（非公開・処理中・ライブ中などの可能性）。")
                    else:
                        now_jst = datetime.now(JST)
                        logged_at_str = now_jst.strftime("%Y/%m/%d %H:%M:%S")
                        row = build_record_row_from_video_item(item, logged_at_str)
                        append_rows(ws_record, [row])
                        st.success("1件の動画ログを Record シートに追記しました。")

# ----------------------------
# タブ2: チャンネルステータス（Status）
# ----------------------------
with tab_status:
    st.subheader("Statusシート")
    render_quota_summary("Status")

    if not api_key:
        st.info("サイドバーから YouTube API Key を入力してください。")
    else:
        url_or_id = st.text_input("URL / ID / 表示名 を入力（チャンネル）", "")

        status_btn = st.button("このチャンネルのステータスを Status に1行追記")

        if status_btn:
            if not url_or_id.strip():
                st.error("URL / ID / 表示名 を入力してください。")
            else:
                channel_id = resolve_channel_id_simple(url_or_id, api_key)
                if not channel_id:
                    st.error("チャンネルIDを解決できませんでした。")
                else:
                    with st.spinner("チャンネルステータスを取得中..."):
                        status = compute_channel_status(channel_id, api_key)
                    if not status:
                        st.error("チャンネル情報の取得に失敗しました。")
                    else:
                        status_row = build_status_row(status)
                        ws_status = get_status_worksheet()
                        append_rows(ws_status, [status_row])

                        st.success("Status シートにチャンネルステータスを1行追記しました。")
                        st.write(f"チャンネル名: {status['channel_title']}")
                        st.write(f"登録者数: {status['subs']}")
                        st.write(f"動画本数: {status['vids_total']}")
                        st.write(f"総再生回数: {status['views_total']}")

                        preview = dict(zip(STATUS_HEADER, status_row))
                        st.markdown("#### 取得結果の全項目プレビュー")
                        st.table(
                            [
                                {"項目": key, "値": preview.get(key, "")}
                                for key in STATUS_HEADER
                            ]
                        )

# ----------------------------
# タブ3: チャンネルステータス解析（TXT/コピーのみ）
# ----------------------------
with tab_status_txt:
    st.subheader("ステータス解析（TXT/コピーのみ）")
    render_quota_summary("Status解析")

    if not api_key:
        st.info("サイドバーから YouTube API Key を入力してください。")
    else:
        url_or_id_txt = st.text_input(
            "URL / ID / 表示名 を入力（チャンネル、TXT/コピー用）",
            key="status_txt_channel_input",
        )
        analyze_btn = st.button("このチャンネルのステータスを取得（TXT/コピー用）")

        if analyze_btn:
            if not url_or_id_txt.strip():
                st.error("URL / ID / 表示名 を入力してください。")
            else:
                channel_id = resolve_channel_id_simple(url_or_id_txt, api_key)
                if not channel_id:
                    st.error("チャンネルIDを解決できませんでした。")
                else:
                    with st.spinner("チャンネルステータスを取得中..."):
                        status = compute_channel_status(channel_id, api_key)
                    if not status:
                        st.error("チャンネル情報の取得に失敗しました。")
                    else:
                        # ChatGPT解析用（ラベル付きテキスト）
                        summary_text = build_status_summary_text(status)
                        # 数値のみテキスト
                        numeric_text = build_status_numeric_text(status)

                        st.markdown("#### 集計結果（説明付き：ChatGPT解析用プレビュー）")
                        st.text(summary_text)

                        st.markdown("#### 数値のみテキスト（TXT出力用プレビュー）")
                        st.text(numeric_text)

                        # TXT ダウンロードボタン（数値のみ）
                        numeric_bytes = numeric_text.encode("utf-8")
                        st.download_button(
                            label="📄 TXT（数値のみ）をダウンロード",
                            data=numeric_bytes,
                            file_name="channel_status_numeric.txt",
                            mime="text/plain",
                        )

                        # クリップボードコピー（説明付きテキスト）
                        components.html(
                            f"""
                            <button onclick="navigator.clipboard.writeText({json.dumps(summary_text)})"
                                style="
                                    background-color: #FF4B4B;
                                    color: white;
                                    border: none;
                                    padding: 0.4rem 1rem;
                                    border-radius: 0.3rem;
                                    cursor: pointer;
                                    font-size: 0.9rem;
                                    margin-top: 0.5rem;
                                ">
                                📋 集計結果（説明付き）をコピー
                            </button>
                            """,
                            height=80,
                        )
