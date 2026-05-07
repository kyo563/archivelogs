import json, os

def get_secret_value(key: str, default=None):
    v = os.environ.get(key)
    return default if v is None else v

def get_required_env(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise RuntimeError(f"{key} が設定されていません。")
    return v

def load_service_account_info() -> dict:
    raw = get_required_env("GCP_SERVICE_ACCOUNT_JSON")
    return json.loads(raw)
