import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import gspread
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

# ===== ページ設定 =====
st.set_page_config(page_title="ログ収集ツール", layout="centered")

# ===== 共通設定（JST & 日付フォーマット） =====
JST = timezone(timedelta(hours=9))
DT_FORMAT = "%Y/%m/%d %H:%M:%S"  # 例: 2025/11/20 16:33:14

# ===== YT API キー入力 =====
DEFAULT_API_KEY = st.secrets.get("YOUTUBE_API_KEY", "")
API_KEY = st.text_input("YouTube API キー", value=DEFAULT_API_KEY, type="password")

# ===== YouTube クライアント =====
@st.cache_resource
def get_youtube_client(api_key: str):
    if not api_key:
        raise RuntimeError("YouTube APIキーが設定されていません。")
    return build("youtube", "v3", developerKey=api_key)


# ===== Google Sheets 接続 =====
@st.cache_resource
def get_worksheet():
    try:
        sa_info = st.secrets["gcp_service_account"]
        spreadsheet_id = st.secrets["SPREADSHEET_ID"]
        worksheet_name = st.secrets.get("WORKSHEET_NAME", "record")

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(worksheet_name)
        return ws
    except KeyError as e:
        st.error(f"secrets に必要なキーがありません: {e}")
        st.stop()
    except Exception as e:
        st.error(f"スプレッドシートへの接続に失敗しました: {e}")
        st.stop()


# ===== チャンネルID解決（ID/URL/表示名対応） =====
@st.cache_data(ttl=3600)
def resolve_channel_id_simple(url_or_id: str, api_key: str) -> Optional[str]:
    s = (url_or_id or "").strip()
    if not s:
        return None

    # 生のチャンネルID（UC〜24桁）
    if s.startswith("UC") and len(s) == 24:
        return s

    # https://www.youtube.com/channel/UC... 形式
    if "channel/" in s:
        return s.split("channel/")[1].split("/")[0]

    # @ハンドルやチャンネル名など → search API で検索
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


# ===== 動画ID抽出（URL/ID両対応） =====
def extract_video_id(s: str) -> Optional[str]:
    s = s.strip()
    if not s:
        return None

    # 通常の watch URL
    if "watch?v=" in s:
        part = s.split("watch?v=")[1]
        vid = part.split("&")[0]
        return vid or None

    # youtu.be 短縮URL
    if "youtu.be/" in s:
        part = s.split("youtu.be/")[1]
        vid = part.split("?")[0].split("&")[0]
        return vid or None

    # 生の11桁IDっぽい場合
    if len(s) == 11 and " " not in s and "/" not in s:
        return s

    return None


# ===== ISO8601 duration → 秒数 =====
def parse_duration_to_seconds(duration: str) -> int:
    # 例: PT1H2M3S / PT15M / PT45S
    if not duration or not duration.startswith("PT"):
        return 0

    d = duration.replace("PT", "")
    hours = minutes = seconds = 0

    num = ""
    for ch in d:
        if ch.isdigit():
            num += ch
        else:
            if ch == "H":
                hours = int(num or 0)
            elif ch == "M":
                minutes = int(num or 0)
            elif ch == "S":
                seconds = int(num or 0)
            num = ""

    return hours * 3600 + minutes * 60 + seconds


# ===== 動画タイプ判定（video / short / live） =====
def classify_video_type(
    snippet: Dict,
    content_details: Dict,
    live_details: Optional[Dict],
    duration_sec: int,
) -> Optional[str]:
    # ライブ関連情報がある場合
    if live_details:
        actual_start = live_details.get("actualStartTime")
        actual_end = live_details.get("actualEndTime")
        # アーカイブ済みライブのみ対象（actualEndTime がある）
        if actual_start and actual_end:
            return "live"
        # 配信前・配信中はログ対象外
        return None

    # 1分未満はショートとみなす
    if duration_sec > 0 and duration_sec < 60:
        return "short"

    return "video"


# ===== 単一動画の情報取得 → ログ1行分に変換 =====
def fetch_video_row(video_id: str, api_key: str, logged_at_str: str) -> Optional[List]:
    youtube = get_youtube_client(api_key)

    try:
        resp = youtube.videos().list(
            part="snippet,contentDetails,liveStreamingDetails,statistics,status",
            id=video_id,
            maxResults=1,
        ).execute()
    except HttpError as e:
        st.error(f"YouTube API エラー（videos.list）: {e}")
        return None

    items = resp.get("items", [])
    if not items:
        return None

    it = items[0]
    snippet = it.get("snippet", {}) or {}
    content = it.get("contentDetails", {}) or {}
    live_details = it.get("liveStreamingDetails") or {}
    stats = it.get("statistics", {}) or {}
    status = it.get("status", {}) or {}

    # 公開ステータスチェック（public 以外は対象外）
    if status.get("privacyStatus") != "public":
        return None

    # 公開日時（UTC） → JST
    published_raw = snippet.get("publishedAt")
    if not published_raw:
        return None

    try:
        dt_utc = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
    except Exception:
        # パースできない場合は対象外扱い
        return None

    # 未来日時（予約投稿でまだ公開前）は除外
    now_utc = datetime.now(timezone.utc)
    if dt_utc > now_utc:
        return None

    dt_jst = dt_utc.astimezone(JST)
    published_at_str = dt_jst.strftime(DT_FORMAT)

    # 再生時間
    duration_iso = content.get("duration", "PT0S")
    duration_sec = parse_duration_to_seconds(duration_iso)

    # タイプ判定
    vtype = classify_video_type(snippet, content, live_details, duration_sec)
    if vtype is None:
        # 配信前・配信中のライブなどはスキップ
        return None

    # タイトル（改行→スペース & ダブルクォートエスケープ）
    raw_title = (snippet.get("title") or "").replace("\n", " ")
    safe_title = raw_title.replace('"', '""')

    url = f"https://www.youtube.com/watch?v={video_id}"
    # HYPERLINK 形式でセルに保存
    title_cell = f'=HYPERLINK("{url}", "{safe_title}")'

    # 再生数・高評価数
    view_count = int(stats.get("viewCount", 0) or 0)
    like_count = int(stats.get("likeCount", 0) or 0)

    # ===== シートのヘッダーに対応した行データ =====
    # A: logged_at（JST）、B: type、C: title（ハイパーリンク）、
    # D: published_at（JST）、E: duration_sec、F: view_count、G: like_count
    row = [
        logged_at_str,
        vtype,
        title_cell,
        published_at_str,
        duration_sec,
        view_count,
        like_count,
    ]
    return row


# ===== チャンネルから直近 N 件の動画ログを取得 =====
def fetch_recent_rows_for_channel(
    channel_input: str,
    api_key: str,
    logged_at_str: str,
    limit: int = 50,  # ★ デフォルト 50 件に変更
) -> List[List]:
    youtube = get_youtube_client(api_key)
    channel_id = resolve_channel_id_simple(channel_input, api_key)
    if not channel_id:
        st.error("チャンネルID/URLを解決できませんでした。")
        return []

    try:
        # 日付の新しい順に最大50件取得して、その中から条件を満たすものを最大 limit 件
        resp = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            type="video",
            order="date",
            maxResults=50,  # ★ 20 → 50 に拡大
        ).execute()
    except HttpError as e:
        st.error(f"YouTube API エラー（search.list）: {e}")
        return []

    items = resp.get("items", [])
    candidate_ids: List[str] = []

    # search 段階でざっくりフィルタ（live/upcomingっぽいものは避ける）
    for item in items:
        vid = item.get("id", {}).get("videoId")
        if not vid:
            continue

        snippet = item.get("snippet", {}) or {}
        live_flag = snippet.get("liveBroadcastContent")
        # upcoming / live はここで弾く（アーカイブ済み live は liveBroadcastContent='none'）
        if live_flag in ("upcoming", "live"):
            continue

        candidate_ids.append(vid)

    rows: List[List] = []
    for vid in candidate_ids:
        if len(rows) >= limit:
            break
        row = fetch_video_row(vid, api_key, logged_at_str)
        if row is not None:
            rows.append(row)

    return rows


# ===== UI =====
st.title("YouTube 投稿ログ収集ツール")

st.markdown(
    """
- チャンネル単位　動画単体 
"""
)

st.write("---")

channel_input = st.text_input("チャンネルID または チャンネルURL（直近50件を取得）", "")
video_input = st.text_input("動画URL または 動画ID（単体取得・任意）", "")

ws = None
if st.button("ログを取得してスプレッドシートに追記"):
    if not API_KEY:
        st.error("YouTube APIキーを入力してください。")
        st.stop()

    # シート接続
    ws = get_worksheet()

    # 共通の logged_at（JST）
    logged_at_dt = datetime.now(JST)
    logged_at_str = logged_at_dt.strftime(DT_FORMAT)

    rows_to_append: List[List] = []

    # 1) 動画URL/ID が入っていれば単体優先
    if video_input.strip():
        vid = extract_video_id(video_input.strip())
        if not vid:
            st.error("動画URL/IDを解釈できませんでした。")
            st.stop()

        row = fetch_video_row(vid, API_KEY, logged_at_str)
        if row is None:
            st.warning("対象動画は未公開・非公開・配信前などのためログ対象外でした。")
            st.stop()
        rows_to_append.append(row)

    # 2) そうでなければチャンネル入力から直近50件
    elif channel_input.strip():
        rows_to_append = fetch_recent_rows_for_channel(
            channel_input.strip(), API_KEY, logged_at_str, limit=50  # ★ 5 → 50
        )
        if not rows_to_append:
            st.warning("条件に合致する公開済み動画が見つかりませんでした。")
            st.stop()
    else:
        st.error("チャンネルID/URL か 動画URL/ID のどちらかを入力してください。")
        st.stop()

    # ===== スプレッドシートへ書き込み =====
    try:
        if len(rows_to_append) == 1:
            ws.append_row(rows_to_append[0], value_input_option="USER_ENTERED")
        else:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    except Exception as e:
        st.error(f"スプレッドシートへの書き込みに失敗しました: {e}")
        st.stop()

    st.success(f"{len(rows_to_append)} 件のログを書き込みました。")
    st.write("最後に追加した行のプレビュー：")
    st.table(rows_to_append)
