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
SEARCH_TARGET_SHEET_NAME = "検索対象"

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
    "comment_count",   # コメント数
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
ROUTINE_RECORD_CHANNEL_ID = "UCojaLfI34qEb0pCTtbjDeEg"
ROUTINE_STATUS_CHANNEL_IDS = [
    "UCojaLfI34qEb0pCTtbjDeEg",
    "UC24z3yE1Mig66jwaSbZI0UA",
]

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

@st.cache_resource
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
    elif len(first_row) < len(RECORD_HEADER):
        # 既存ヘッダーは残しつつ、不足している末尾列のみ追加する
        missing_headers = RECORD_HEADER[len(first_row):]
        ws.update_cell(1, len(first_row) + 1, missing_headers[0])
        for idx, header in enumerate(missing_headers[1:], start=len(first_row) + 2):
            ws.update_cell(1, idx, header)
    return ws


@st.cache_resource
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


@st.cache_resource
def get_search_target_worksheet():
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SEARCH_TARGET_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SEARCH_TARGET_SHEET_NAME, rows=1000, cols=10)
        ws.append_row(["チャンネルID", "チャンネル名"])
        return ws

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(["チャンネルID", "チャンネル名"])
    return ws


def read_search_targets() -> List[Dict[str, str]]:
    ws = get_search_target_worksheet()
    rows = ws.get_all_values()
    targets: List[Dict[str, str]] = []
    for row in rows[1:]:
        channel_id = row[0].strip() if len(row) >= 1 else ""
        channel_name = row[1].strip() if len(row) >= 2 else ""
        if not channel_id:
            continue
        targets.append({"channel_id": channel_id, "channel_name": channel_name})
    return targets


def parse_status_date(date_str: str) -> Optional[datetime]:
    text = (date_str or "").strip()
    if not text:
        return None
    for fmt in ("%Y/%m/%d", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def get_latest_status_dates() -> Dict[str, datetime]:
    ws = get_status_worksheet()
    rows = ws.get_all_values()
    if not rows:
        return {}

    header = rows[0]
    try:
        date_idx = header.index("取得日時")
    except ValueError:
        date_idx = 0
    try:
        channel_idx = header.index("チャンネルID")
    except ValueError:
        channel_idx = 1

    latest_map: Dict[str, datetime] = {}
    for row in rows[1:]:
        channel_id = row[channel_idx].strip() if len(row) > channel_idx else ""
        if not channel_id:
            continue
        dt = parse_status_date(row[date_idx] if len(row) > date_idx else "")
        if not dt:
            continue
        prev = latest_map.get(channel_id)
        if prev is None or dt > prev:
            latest_map[channel_id] = dt
    return latest_map


def get_latest_channel_titles_from_status() -> Dict[str, str]:
    """Status シートの履歴から channel_id ごとの最新チャンネル名を作る。"""
    ws = get_status_worksheet()
    rows = ws.get_all_values()
    if not rows:
        return {}

    header = rows[0]
    try:
        date_idx = header.index("取得日時")
    except ValueError:
        date_idx = 0
    try:
        channel_idx = header.index("チャンネルID")
    except ValueError:
        channel_idx = 1
    try:
        title_idx = header.index("チャンネル名")
    except ValueError:
        title_idx = 2

    latest_map: Dict[str, Tuple[datetime, str]] = {}
    for row in rows[1:]:
        channel_id = row[channel_idx].strip() if len(row) > channel_idx else ""
        channel_title = row[title_idx].strip() if len(row) > title_idx else ""
        if not channel_id or not channel_title:
            continue
        dt = parse_status_date(row[date_idx] if len(row) > date_idx else "")
        if not dt:
            dt = datetime.min

        prev = latest_map.get(channel_id)
        if prev is None or dt > prev[0]:
            latest_map[channel_id] = (dt, channel_title)

    return {k: v[1] for k, v in latest_map.items()}


def fill_missing_channel_names_on_search_target() -> int:
    """検索対象シートで A列にIDがあり B列が空の行だけを、Statusの既存名で補完する。"""
    ws = get_search_target_worksheet()
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return 0

    titles_map = get_latest_channel_titles_from_status()
    if not titles_map:
        return 0

    updates: List[Tuple[int, int, str]] = []
    for idx, row in enumerate(rows[1:], start=2):
        channel_id = row[0].strip() if len(row) >= 1 else ""
        channel_name = row[1].strip() if len(row) >= 2 else ""
        if not channel_id or channel_name:
            continue

        title = titles_map.get(channel_id, "")
        if title:
            updates.append((idx, 2, title))

    update_cells_in_column(ws, updates)
    return len(updates)


def sort_targets_by_staleness(targets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    latest_map = get_latest_status_dates()

    def sort_key(target: Dict[str, str]):
        dt = latest_map.get(target["channel_id"])
        if dt is None:
            return (0, datetime.min)
        return (1, dt)

    return sorted(targets, key=sort_key)


def append_rows(ws, rows: List[List]):
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    except AttributeError:
        # 古い gspread 互換
        for r in rows:
            ws.append_row(r, value_input_option="USER_ENTERED")


def update_cells_in_column(ws, row_col_values: List[Tuple[int, int, str]]):
    """指定セルをまとめて更新する。引数は (row, col, value) の配列。"""
    if not row_col_values:
        return
    cells = [ws.cell(r, c) for r, c, _ in row_col_values]
    for cell, (_, _, value) in zip(cells, row_col_values):
        cell.value = value
    ws.update_cells(cells, value_input_option="USER_ENTERED")


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


def extract_video_id_from_title_cell(title_cell: str) -> Optional[str]:
    """record シートの title 列から videoId を抽出する。"""
    text = (title_cell or "").strip()
    if not text:
        return None

    # HYPERLINK("https://www.youtube.com/watch?v=...", "...")
    m = re.search(r"watch\?v=([a-zA-Z0-9_-]{11})", text)
    if m:
        return m.group(1)

    # 万一 URL / videoId がそのまま入っているケースにも対応
    return resolve_video_id(text)


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
    # commentCount は既存の videos.list 応答を利用する（追加API呼び出しはしない）
    comment_count_raw = stats.get("commentCount")
    comment_count = int(comment_count_raw) if comment_count_raw is not None else ""

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
        comment_count,
    ]


def fetch_comment_counts(video_ids: List[str], api_key: str) -> Dict[str, str]:
    """videos.list(part=statistics) だけでコメント数をまとめて取得する。"""
    youtube = get_youtube_client(api_key)
    result: Dict[str, str] = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        try:
            add_quota_usage("videos.list")
            resp = youtube.videos().list(
                part="statistics",
                id=",".join(chunk),
                maxResults=len(chunk),
            ).execute()
        except Exception as e:
            st.warning(f"コメント数の取得に失敗しました: {e}")
            continue

        for item in resp.get("items", []):
            vid = item.get("id")
            stats = item.get("statistics", {}) or {}
            raw = stats.get("commentCount")
            if vid:
                result[vid] = str(raw) if raw is not None else ""
    return result


def refresh_record_comment_counts(ws, api_key: str) -> int:
    """record シート H 列をコメント数で更新し、更新件数を返す。"""
    rows = ws.get_all_values()
    if len(rows) <= 1:
        return 0

    targets: List[Tuple[int, str]] = []
    for row_idx, row in enumerate(rows[1:], start=2):
        title_cell = row[2] if len(row) >= 3 else ""
        video_id = extract_video_id_from_title_cell(title_cell)
        if video_id:
            targets.append((row_idx, video_id))

    if not targets:
        return 0

    unique_video_ids = list(dict.fromkeys([vid for _, vid in targets]))
    counts = fetch_comment_counts(unique_video_ids, api_key)

    updates: List[Tuple[int, int, str]] = []
    for row_idx, video_id in targets:
        if video_id in counts:
            comment_value = counts[video_id]
            updates.append((row_idx, 8, comment_value))  # H列

    update_cells_in_column(ws, updates)
    return len(updates)


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
    解析用の「=== 集計結果 ===」形式テキストを生成する。
    （そのままChatGPTに投げ込むことを想定した説明付きフォーマット）
    """
    lines: List[str] = []

    # 見出し
    lines.append("=== 集計結果 ===")
    lines.append("")

    # 基本情報
    lines.append("■ 基本情報")
    lines.append(f"データ取得日: {status['data_date_str']}（このツールで集計を行った日）")
    lines.append(f"チャンネルID: {status['channel_id']}（UCから始まる固有ID）")
    lines.append(f"チャンネル名: {status['channel_title']}")
    lines.append(f"登録者数: {status['subs']}（現在の登録者総数）")
    lines.append(f"動画本数: {status['vids_total']}（公開済み動画の本数）")
    lines.append(f"総再生回数: {status['views_total']}（公開済み動画の累計再生数）")
    lines.append(f"活動開始日: {status['channel_published_str']}（チャンネル作成日）")

    months_active = status.get("months_active")
    months_str = "" if months_active is None else str(months_active)
    lines.append(f"活動月数: {months_str}（チャンネル開設からの日数 ÷ 30 を概算）")
    lines.append("")

    # 累計指標
    lines.append(f"累計登録者数/活動月: {status['subs_per_month']}（現在の登録者数 ÷ 活動月数）")
    lines.append(f"累計登録者数/動画: {status['subs_per_video']}（現在の登録者数 ÷ 動画本数）")
    lines.append(f"累計動画あたり総再生回数: {status['views_per_video']}（総再生回数 ÷ 動画本数）")
    lines.append(f"累計総再生回数/登録者数: {status['views_per_sub']}（総再生回数 ÷ 登録者数）")
    lines.append(f"1再生あたり登録者増: {status['subs_per_total_view']}（登録者数 ÷ 総再生回数）")
    lines.append(f"動画あたりプレイリスト数: {status['playlists_per_video']}（プレイリスト総数 ÷ 動画本数）")
    lines.append(f"活動月あたり動画本数: {status['videos_per_month']}（動画本数 ÷ 活動月数）")
    lines.append(f"登録者あたり動画本数: {status['videos_per_subscriber']}（動画本数 ÷ 登録者数）")
    lines.append("")

    # 上位プレイリスト
    lines.append("■ 上位プレイリスト（件数順）")
    for i, pl in enumerate(status["top5_playlists"], start=1):
        title = (pl.get("title") or "").replace("\n", " ")
        count = pl.get("itemCount", 0)
        lines.append(f"{i}位: {title} → {count}本")
    lines.append("")

    # 直近指標
    lines.append("■ 直近指標")

    # 直近10日
    lines.append(
        f"直近10日 合計再生数: {status['total_views_last10']}（直近10日間に公開された動画の再生数合計）"
    )
    lines.append(
        f"直近10日 投稿数: {status['num_videos_last10']}（直近10日間に公開された公開動画本数）"
    )

    if status["num_videos_last10"] > 0:
        share10_pct = status["top_share_last10"] * 100
        lines.append("直近10日 トップ動画:")
        lines.append(
            f"- 『{status['top_title_last10']}』 — "
            f"views: {status['top_views_last10']}（この動画単体の再生数） | "
            f"share: {share10_pct:.2f}%（直近10日の合計再生数に占める割合）"
        )
    else:
        lines.append("直近10日 トップ動画: データなし")

    lines.append(
        f"直近10日 平均再生: {status['avg_views_per_video_last10']}（直近10日間の合計再生数 ÷ 投稿数）"
    )
    lines.append(
        f"直近10日 視聴/登録比: {status['views_per_sub_last10']}（直近10日の合計再生数 ÷ 現在の登録者数）"
    )

    # 直近30日
    lines.append(
        f"直近30日 合計再生数: {status['total_views_last30']}（直近30日間に公開された動画の再生数合計）"
    )
    lines.append(
        f"直近30日 投稿数: {status['num_videos_last30']}（直近30日間に公開された公開動画本数）"
    )

    if status["num_videos_last30"] > 0:
        share30_pct = status["top_share_last30"] * 100
        lines.append("直近30日 トップ動画:")
        lines.append(
            f"- 『{status['top_title_last30']}』 — "
            f"views: {status['top_views_last30']}（この動画単体の再生数） | "
            f"share: {share30_pct:.2f}%（直近30日の合計再生数に占める割合）"
        )
    else:
        lines.append("直近30日 トップ動画: データなし")

    lines.append(
        f"直近30日 平均再生: {status['avg_views_per_video_last30']}（直近30日間の合計再生数 ÷ 投稿数）"
    )
    lines.append(
        f"直近30日 視聴/登録比: {status['views_per_sub_last30']}（直近30日の合計再生数 ÷ 現在の登録者数）"
    )

    return "\n".join(lines)


def build_status_numeric_text(status: Dict) -> str:
    """
    数字主体の簡素なテキスト（提示されたサンプル形式に近いもの）を生成する。
    """
    lines: List[str] = []

    # 基本情報
    lines.append(str(status["data_date_str"]))      # 取得日
    lines.append(str(status["channel_id"]))         # チャンネルID
    lines.append(str(status["channel_title"]))      # チャンネル名
    lines.append(str(status["subs"]))               # 登録者数
    lines.append(str(status["vids_total"]))         # 動画本数
    lines.append(str(status["views_total"]))        # 総再生回数
    lines.append(str(status["channel_published_str"]))  # 開設日

    # 活動月数と各種指標
    months_active = status.get("months_active")
    lines.append("" if months_active is None else str(months_active))
    lines.append(str(status["subs_per_month"]))
    lines.append(str(status["subs_per_video"]))
    lines.append(str(status["views_per_video"]))
    lines.append(str(status["views_per_sub"]))
    lines.append(str(status["subs_per_total_view"]))
    lines.append(str(status["playlists_per_video"]))
    lines.append(str(status["videos_per_month"]))
    lines.append(str(status["videos_per_subscriber"]))

    # 上位プレイリスト（「タイトル→本数」形式）
    for pl in status["top5_playlists"]:
        title = (pl.get("title") or "").replace("\n", " ")
        count = pl.get("itemCount", 0)
        # サンプルに合わせて「タイトル→数値」
        lines.append(f"{title}→{count}")

    # 直近10日ブロック
    lines.append(str(status["total_views_last10"]))           # 合計再生数
    lines.append(str(status["num_videos_last10"]))            # 投稿数
    lines.append(str(status["top_title_last10"]))             # トップ動画タイトル
    lines.append(str(status["top_views_last10"]))             # トップ動画再生数
    lines.append(str(status["top_share_last10"]))             # トップ動画シェア
    lines.append(str(status["avg_views_per_video_last10"]))   # 平均再生/動画
    lines.append(str(status["views_per_sub_last10"]))         # 視聴/登録比

    # 直近30日ブロック
    lines.append(str(status["total_views_last30"]))
    lines.append(str(status["num_videos_last30"]))
    lines.append(str(status["top_title_last30"]))
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
    ["ログ（Record）", "ステータス（Status）", "分析（TXT/コピー）"]
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

        col1, col2, col3 = st.columns(3)
        with col1:
            run_recent_btn = st.button("直近50件を Record に追記")
        with col2:
            run_single_btn = st.button("この動画だけ Record に追記")
        with col3:
            refresh_comments_btn = st.button("Record のコメント数を更新（H列）")

        routine_btn = st.button("ルーティン")

        if routine_btn:
            ws_record = get_record_worksheet()
            ws_status = get_status_worksheet()
            with st.spinner("ルーティンを実行中..."):
                record_items = fetch_channel_upload_items(
                    ROUTINE_RECORD_CHANNEL_ID,
                    max_results=50,
                    api_key=api_key,
                )

                record_count = 0
                if record_items:
                    logged_at_str = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")
                    record_rows = [
                        build_record_row_from_video_item(it, logged_at_str)
                        for it in record_items
                    ]
                    append_rows(ws_record, record_rows)
                    record_count = len(record_rows)

                status_rows: List[List] = []
                failed_status_ids: List[str] = []
                for channel_id in ROUTINE_STATUS_CHANNEL_IDS:
                    status = compute_channel_status(channel_id, api_key)
                    if status:
                        status_rows.append(build_status_row(status))
                    else:
                        failed_status_ids.append(channel_id)

                if status_rows:
                    append_rows(ws_status, status_rows)

            st.success(
                f"ルーティン完了: Record {record_count}件 / Status {len(status_rows)}件を追記しました。"
            )
            if failed_status_ids:
                st.warning(
                    "Status 取得に失敗したチャンネルID: "
                    + ", ".join(failed_status_ids)
                )

        if refresh_comments_btn:
            ws_record = get_record_worksheet()
            with st.spinner("Record のコメント数を更新中..."):
                updated_count = refresh_record_comment_counts(ws_record, api_key)
            st.success(f"{updated_count}件のコメント数を H 列に反映しました。")

        # 直近50件（チャンネル）
        if run_recent_btn:
            ws_record = get_record_worksheet()
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
            ws_record = get_record_worksheet()
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

        st.markdown("---")
        st.markdown("#### 検索対象シートから古い順に一括更新")
        st.caption("検索対象シートのA列（チャンネルID）を読み込み、Status の最終取得日時が古い順に追記します。")
        batch_limit = st.number_input(
            "今回更新する最大件数",
            min_value=1,
            max_value=100,
            value=30,
            step=1,
            key="status_batch_limit",
        )
        batch_btn = st.button("検索対象シートを読み込み、古い順で Status に追記")

        if batch_btn:
            with st.spinner("検索対象を読み込み、順次ステータスを取得中..."):
                filled_count = fill_missing_channel_names_on_search_target()
                if filled_count:
                    st.info(f"検索対象シートのチャンネル名を {filled_count} 件補完しました（Statusシートの既存データを利用）。")
                targets = read_search_targets()
                if not targets:
                    st.warning("検索対象シートにチャンネルIDがありません。A列を確認してください。")
                else:
                    ordered_targets = sort_targets_by_staleness(targets)
                    picked = ordered_targets[: int(batch_limit)]

                    ws_status = get_status_worksheet()
                    result_rows: List[List] = []
                    ok_items: List[str] = []
                    ng_items: List[str] = []
                    progress = st.progress(0.0)

                    for idx, target in enumerate(picked, start=1):
                        channel_id = target["channel_id"]
                        status = compute_channel_status(channel_id, api_key)
                        if status:
                            result_rows.append(build_status_row(status))
                            title = status.get("channel_title") or target.get("channel_name") or ""
                            ok_items.append(f"{channel_id} {title}".strip())
                        else:
                            ng_items.append(channel_id)
                        progress.progress(idx / len(picked))

                    if result_rows:
                        append_rows(ws_status, result_rows)

                    st.success(
                        f"一括更新が完了しました（成功: {len(ok_items)}件 / 失敗: {len(ng_items)}件）。"
                    )
                    if ok_items:
                        st.markdown("**成功したチャンネル**")
                        st.write("\n".join(f"- {x}" for x in ok_items))
                    if ng_items:
                        st.markdown("**失敗したチャンネルID**")
                        st.write("\n".join(f"- {x}" for x in ng_items))

# ----------------------------
# タブ3: チャンネルステータス解析（TXT/コピーのみ）
# ----------------------------
with tab_status_txt:
    st.subheader("簡易解析")
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
                        # 説明付きテキスト & 数値のみテキストを生成
                        summary_text = build_status_summary_text(status)
                        numeric_text = build_status_numeric_text(status)

                        # 取得ボタン直下：TXT（数値のみ）ダウンロード
                        numeric_bytes = numeric_text.encode("utf-8")
                        st.download_button(
                            label="📄 TXTのみをダウンロード",
                            data=numeric_bytes,
                            file_name="channel_status_numeric.txt",
                            mime="text/plain",
                        )

                        # 取得ボタン直下：説明付きテキストをクリップボードにコピー
                        components.html(
                            f"""
<div>
  <button id="copySummaryBtn"
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
  <span id="copySummaryStatus"
      style="margin-left: 0.5rem; font-size: 0.85rem; color: #333;">
  </span>

  <script>
    const textToCopy = {json.dumps(summary_text)};
    const btn = document.getElementById("copySummaryBtn");
    const status = document.getElementById("copySummaryStatus");

    btn.addEventListener("click", async () => {{
      try {{
        await navigator.clipboard.writeText(textToCopy);
        status.textContent = "コピーしました。";
      }} catch (err) {{
        status.textContent = "コピーに失敗しました: " + err;
      }}
    }});
  </script>
</div>
                            """,
                            height=100,
                        )

                        # 下にプレビュー（必要なときだけスクロールして確認）
                        st.markdown("#### 集計結果（説明付き：ChatGPT解析用プレビュー）")
                        st.text(summary_text)
