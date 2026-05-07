import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

from archivelogs.config import get_required_env, get_secret_value, load_service_account_info

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
STATUS_SHEET_NAME = "Status"
SEARCH_TARGET_SHEET_NAME = "検索対象"
RECORD_HEADER = ["logged_at", "type", "title", "published_at", "duration_sec", "view_count", "like_count", "comment_count"]
STATUS_HEADER = ["取得日時", "チャンネルID", "チャンネル名", "登録者数", "動画本数", "総再生回数", "チャンネル開設日", "活動月数", "累計登録者数/活動月", "累計登録者数/動画", "累計動画あたり総再生回数", "累計総再生回数/登録者数", "1再生あたり登録者増", "動画あたりプレイリスト数", "活動月あたり動画本数", "登録者あたり動画本数", "上位プレイリスト1", "上位プレイリスト2", "上位プレイリスト3", "上位プレイリスト4", "上位プレイリスト5", "直近10日合計再生数", "直近10日投稿数", "直近10日トップ動画タイトル", "直近10日トップ動画再生数", "直近10日トップ動画シェア", "直近10日平均再生数/動画", "直近10日視聴/登録比", "直近30日合計再生数", "直近30日投稿数", "直近30日トップ動画タイトル", "直近30日トップ動画再生数", "直近30日トップ動画シェア", "直近30日平均再生数/動画", "直近30日視聴/登録比"]


def get_client():
    creds = Credentials.from_service_account_info(load_service_account_info(), scopes=SCOPES)
    return gspread.authorize(creds)


def _sheet():
    return get_client().open_by_key(get_required_env("SPREADSHEET_ID"))


def get_record_worksheet(create=True):
    ws_name = get_secret_value("WORKSHEET_NAME", "record") or "record"
    sh = _sheet()
    try:
        ws = sh.worksheet(ws_name)
    except gspread.WorksheetNotFound:
        if not create:
            return None
        ws = sh.add_worksheet(title=ws_name, rows=1000, cols=20)
        ws.append_row(RECORD_HEADER)
        return ws
    h = ws.row_values(1)
    if create:
        if not h:
            ws.append_row(RECORD_HEADER)
        elif len(h) < len(RECORD_HEADER):
            ws.update(f"{rowcol_to_a1(1, len(h)+1)}:{rowcol_to_a1(1, len(RECORD_HEADER))}", [RECORD_HEADER[len(h) :]])
    return ws


def get_status_worksheet(create=True):
    sh = _sheet()
    try:
        ws = sh.worksheet(STATUS_SHEET_NAME)
    except gspread.WorksheetNotFound:
        if not create:
            return None
        ws = sh.add_worksheet(title=STATUS_SHEET_NAME, rows=1000, cols=50)
        ws.append_row(STATUS_HEADER)
        return ws
    h = ws.row_values(1)
    if create:
        if not h:
            ws.append_row(STATUS_HEADER)
        elif h != STATUS_HEADER:
            raise RuntimeError("Status シートのヘッダーが STATUS_HEADER と一致しません。")
    return ws


def get_search_target_worksheet(create=True):
    sh = _sheet()
    try:
        ws = sh.worksheet(SEARCH_TARGET_SHEET_NAME)
    except gspread.WorksheetNotFound:
        if not create:
            return None
        ws = sh.add_worksheet(title=SEARCH_TARGET_SHEET_NAME, rows=1000, cols=10)
        ws.append_row(["チャンネルID", "チャンネル名"])
        return ws
    if create and not ws.row_values(1):
        ws.append_row(["チャンネルID", "チャンネル名"])
    return ws


def append_rows(ws, rows, value_input_option="USER_ENTERED"):
    if rows and ws:
        ws.append_rows(rows, value_input_option=value_input_option)


def update_cells_in_column(ws, row_col_values):
    if not row_col_values or not ws:
        return
    cells = [ws.cell(r, c) for r, c, _ in row_col_values]
    for cell, (_, _, v) in zip(cells, row_col_values):
        cell.value = v
    ws.update_cells(cells, value_input_option="USER_ENTERED")
