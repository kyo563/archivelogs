import gspread
from google.oauth2.service_account import Credentials
from archivelogs.config import load_service_account_info, get_required_env
SCOPES=["https://www.googleapis.com/auth/spreadsheets"]

def get_client():
    creds = Credentials.from_service_account_info(load_service_account_info(), scopes=SCOPES)
    return gspread.authorize(creds)

def get_record_worksheet():
    sh = get_client().open_by_key(get_required_env("SPREADSHEET_ID"))
    return sh.worksheet(get_required_env("WORKSHEET_NAME") if get_required_env("WORKSHEET_NAME") else "record")
