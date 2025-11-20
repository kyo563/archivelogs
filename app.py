import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

import streamlit as st
from googleapiclient.discovery import build
import gspread
from google.oauth2.service_account import Credentials


# ===== Streamlit 基本設定 =====
st.set_page_config(page_title="YouTube 動画ログ取得ツール", layout="centered")

st.title("YouTube 動画ログ取得ツール")
st.write("公開済み動画の情報を取得し、スプレッドシートにログとして蓄積します。")


# ===== Google Sheets 接続 =====
@st.cache_resource
def get_gsheet_worksheet():
    """
    Streamlit secrets に保存されたサービスアカウント情報から
    Google Sheets のワークシートを返す。
    必要な secrets:
      - gcp_service_account : サービスアカウント JSON を辞書として保存
      - SPREADSHEET_ID      : 対象スプレッドシートの ID
      - WORKSHEET_NAME      : 対象シート名（省略時は 'Sheet1'）
    """
    sa_info = st.secrets["gcp_service_account"]
    spreadsheet_id = st.secrets["SPREADSHEET_ID"]
    worksheet_name = st.secrets.get("WORKSHEET_NAME", "Sheet1")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)


# ===== YouTube クライアント =====
def get_youtube_client(api_key: str):
    if not api_key:
        raise RuntimeError("YouTube API キーが未入力です。")
    return build("youtube", "v3", developerKey=api_key)


# ===== ユーティリティ =====
def extract_channel_id(text: str) -> Optional[str]:
    """
    入力文字列からチャンネルID (UC〜) を推定する。
    - そのまま UC〜24文字 → それを返す
    - https://www.youtube.com/channel/UC... → 抜き出して返す
    それ以外は None
    """
    if not text:
        return None
    s = text.strip()

    # 生のチャンネルID
    if s.startswith("UC") and len(s) == 24:
        return s

    # URL 内の channel/UC... を抜き出し
    if "youtube.com/channel/" in s:
        part = s.split("youtube.com/channel/")[1]
        cid = part.split("/")[0]
        if cid.startswith("UC") and len(cid) == 24:
            return cid

    return None


def extract_video_id(text: str) -> Optional[str]:
    """
    入力文字列から videoId を推定する。
    - そのまま 11文字程度の ID
    - https://www.youtube.com/watch?v=... 形式
    - https://youtu.be/... 形式
    """
    if not text:
        return None
    s = text.strip()

    # watch?v=xxxx
    if "watch?v=" in s:
        part = s.split("watch?v=")[1]
        vid = part.split("&")[0]
        return vid

    # youtu.be/xxxx
    if "youtu.be/" in s:
        part = s.split("youtu.be/")[1]
        vid = part.split("?")[0]
        return vid

    # それ以外はそのまま返す（単体ID想定）
    return s


def iso8601_duration_to_seconds(duration: str) -> Optional[int]:
    """
    ISO8601 形式の duration (例: 'PT1H2M3S') を秒数に変換。
    変換できない場合は None。
    """
    if not duration:
        return None
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    if not m:
        return None
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def classify_video_type(
    snippet: Dict,
    content: Dict,
    live_details: Dict,
    duration_sec: Optional[int],
) -> str:
    """
    動画タイプを 'live' / 'short' / 'video' のいずれかに分類する。
    - live: liveStreamingDetails があり actualEndTime もある（終了済みライブ）
    - short: live ではなく、duration <= 61秒 または #shorts を含む
    - video: 上記以外
    """
    # live (終了済みライブ)
    if live_details and live_details.get("actualEndTime"):
        return "live"

    # short 判定
    title = (snippet.get("title") or "").lower()
    description = (snippet.get("description") or "").lower()
    if duration_sec is not None and duration_sec <= 61:
        return "short"
    if "#shorts" in title or "#shorts" in description:
        return "short"

    # 通常動画
    return "video"


def build_video_record(item: Dict) -> Optional[Dict]:
    """
    videos().list の 1 item からログ用のレコードを構築。
    条件に合わない動画（非公開・限定公開・配信前など）は None を返す。
    """
    snippet = item.get("snippet") or {}
    stats = item.get("statistics") or {}
    content = item.get("contentDetails") or {}
    live_details = item.get("liveStreamingDetails") or {}
    status = item.get("status") or {}

    # 公開状態チェック
    privacy = status.get("privacyStatus")
    if privacy != "public":
        return None

    # ライブ・プレミアの配信前 / 配信中を除外
    live_flag = snippet.get("liveBroadcastContent")
    actual_end = live_details.get("actualEndTime")
    if live_flag in ("upcoming", "live") and not actual_end:
        return None

    # duration → 秒
    duration_sec = iso8601_duration_to_seconds(content.get("duration", ""))

    # タイプ分類
    vtype = classify_video_type(snippet, content, live_details, duration_sec)

    video_id = item.get("id")
    if not video_id:
        return None

    # 日時
    published_at = snippet.get("publishedAt")  # ISO8601 (UTC)

    # 再生数・高評価数
    view_count = int(stats.get("viewCount", 0) or 0)
    like_count = int(stats.get("likeCount", 0) or 0)

    # タイトル（改行除去）
    title_raw = (snippet.get("title") or "").replace("\n", " ").strip()

    # ログ取得時刻 (UTC)
    logged_at = datetime.now(timezone.utc).isoformat()

    url = f"https://www.youtube.com/watch?v={video_id}"

    # HYPERLINK 用にダブルクォートだけエスケープ（"" に置換）
    title_escaped = title_raw.replace('"', '""')
    hyperlink_formula = f'=HYPERLINK("{url}","{title_escaped}")'

    return {
        "logged_at": logged_at,
        "channel_id": snippet.get("channelId"),
        "video_id": video_id,
        "url": url,
        "type": vtype,
        "title_formula": hyperlink_formula,
        "published_at": published_at,
        "duration_sec": duration_sec,
        "view_count": view_count,
        "like_count": like_count,
    }


def get_latest_video_records_for_channel(
    youtube,
    channel_id: str,
    limit: int = 5,
) -> List[Dict]:
    """
    指定チャンネルの uploads プレイリストから、公開済みの動画を新しい順に走査し、
    条件に合うものを最大 limit 件ログレコードとして返す。
    返却時は published_at 昇順（古い→新しい）にソートして返す。
    """
    # 1) uploads プレイリスト ID を取得
    ch_resp = youtube.channels().list(
        part="contentDetails",
        id=channel_id,
        maxResults=1,
    ).execute()
    ch_items = ch_resp.get("items", [])
    if not ch_items:
        raise ValueError("チャンネルが見つかりませんでした。")

    uploads_pl = (
        ch_items[0]
        .get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads")
    )
    if not uploads_pl:
        raise ValueError("uploads プレイリストが取得できませんでした。")

    records: List[Dict] = []
    next_page: Optional[str] = None

    while len(records) < limit:
        pl_resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_pl,
            maxResults=50,
            pageToken=next_page,
        ).execute()
        pl_items = pl_resp.get("items", [])
        if not pl_items:
            break

        video_ids = [
            it.get("contentDetails", {}).get("videoId")
            for it in pl_items
            if it.get("contentDetails", {}).get("videoId")
        ]

        if video_ids:
            v_resp = youtube.videos().list(
                part="snippet,statistics,contentDetails,liveStreamingDetails,status",
                id=",".join(video_ids),
                maxResults=len(video_ids),
            ).execute()
            for v_item in v_resp.get("items", []):
                rec = build_video_record(v_item)
                if rec is not None:
                    records.append(rec)
                    if len(records) >= limit:
                        break

        next_page = pl_resp.get("nextPageToken")
        if not next_page:
            break

    # 公開日時で昇順にソート（古い→新しい）
    def sort_key(r: Dict):
        pa = r.get("published_at")
        try:
            return datetime.fromisoformat(pa.replace("Z", "+00:00")) if pa else datetime.min
        except Exception:
            return datetime.min

    records.sort(key=sort_key)
    return records[:limit]


def get_single_video_record(youtube, video_id: str) -> Optional[Dict]:
    """
    単一動画のログレコードを取得。
    条件に合わない動画の場合は None。
    """
    resp = youtube.videos().list(
        part="snippet,statistics,contentDetails,liveStreamingDetails,status",
        id=video_id,
        maxResults=1,
    ).execute()
    items = resp.get("items", [])
    if not items:
        return None
    return build_video_record(items[0])


def append_records_to_sheet(worksheet, records: List[Dict]):
    """
    レコードをシートに追記する。
    列順:
      logged_at, channel_id, video_id, url, type,
      title (HYPERLINK), published_at, duration_sec, view_count, like_count
    """
    for rec in records:
        row = [
            rec.get("logged_at", ""),
            rec.get("channel_id", ""),
            rec.get("video_id", ""),
            rec.get("url", ""),
            rec.get("type", ""),
            rec.get("title_formula", ""),
            rec.get("published_at", ""),
            rec.get("duration_sec", ""),
            rec.get("view_count", ""),
            rec.get("like_count", ""),
        ]
        worksheet.append_row(row, value_input_option="USER_ENTERED")


# ===== UI =====

st.subheader("設定")

api_key = st.text_input(
    "YouTube API キー（必須）",
    type="password",
    help="Google Cloud Console で取得した API キーを入力してください。",
)

st.markdown("---")

st.subheader("入力")

col1, col2 = st.columns(2)

with col1:
    channel_input = st.text_input(
        "チャンネルID / チャンネルURL（チャンネルモード・任意）",
        placeholder="例: UCxxxxxxxxxxxxxxxxxxxxxx または https://www.youtube.com/channel/UC...",
    )

with col2:
    video_input = st.text_input(
        "動画URL / 動画ID（単体動画モード・任意）",
        placeholder="例: https://www.youtube.com/watch?v=xxxx",
    )

st.write(
    "- チャンネルモード: チャンネルのみ入力 → 直近 5 件の公開済み動画を取得\n"
    "- 単体動画モード: 動画のみ入力 → その動画 1 件だけを取得\n"
    "- 両方入力 / 両方空の場合はエラーになります"
)

run_btn = st.button("データ取得してスプレッドシートに追記")


if run_btn:
    # 入力チェック
    if not api_key:
        st.error("YouTube API キーを入力してください。")
        st.stop()

    has_channel = bool(channel_input.strip())
    has_video = bool(video_input.strip())

    if has_channel and has_video:
        st.error("チャンネルか動画か、どちらか片方だけを入力してください。")
        st.stop()
    if not has_channel and not has_video:
        st.error("チャンネルID/URL または 動画URL/ID のどちらかを入力してください。")
        st.stop()

    # クライアント初期化
    try:
        yt = get_youtube_client(api_key)
    except Exception as e:
        st.error(f"YouTube クライアントの初期化に失敗しました: {e}")
        st.stop()

    try:
        ws = get_gsheet_worksheet()
    except Exception as e:
        st.error(f"スプレッドシートへの接続に失敗しました: {e}")
        st.stop()

    records: List[Dict] = []

    if has_channel:
        # チャンネルモード
        channel_id = extract_channel_id(channel_input)
        if not channel_id:
            st.error("チャンネルID / URL の形式が想定外です。UC〜 または /channel/UC〜 形式で入力してください。")
            st.stop()

        try:
            records = get_latest_video_records_for_channel(yt, channel_id, limit=5)
        except Exception as e:
            st.error(f"チャンネルから動画を取得中にエラーが発生しました: {e}")
            st.stop()

        if not records:
            st.warning("条件に合致する公開済み動画が見つかりませんでした。")
            st.stop()

        mode_label = "チャンネルモード（直近5件）"

    else:
        # 単体動画モード
        video_id = extract_video_id(video_input)
        if not video_id:
            st.error("動画URL / ID の形式が想定外です。")
            st.stop()

        rec = get_single_video_record(yt, video_id)
        if rec is None:
            st.error("条件に合致する動画が見つかりませんでした（非公開 / 配信前 などの可能性があります）。")
            st.stop()

        records = [rec]
        mode_label = "単体動画モード"

    # スプレッドシートに追記
    try:
        append_records_to_sheet(ws, records)
    except Exception as e:
        st.error(f"スプレッドシートへの書き込みに失敗しました: {e}")
        st.stop()

    # 結果表示
    st.success(f"{mode_label} のデータ取得とスプレッドシートへの追記が完了しました。")

    # 画面にもテーブル表示（確認用）
    display_rows = []
    for r in records:
        display_rows.append(
            {
                "logged_at": r.get("logged_at"),
                "channel_id": r.get("channel_id"),
                "video_id": r.get("video_id"),
                "url": r.get("url"),
                "type": r.get("type"),
                "title": r.get("title_formula"),
                "published_at": r.get("published_at"),
                "duration_sec": r.get("duration_sec"),
                "view_count": r.get("view_count"),
                "like_count": r.get("like_count"),
            }
        )
    st.subheader("今回取得したレコード")
    st.dataframe(display_rows)
