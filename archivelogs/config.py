import json
import os
from typing import Any

_RUNTIME_CONFIG: dict[str, Any] = {}


def set_runtime_config(overrides: dict):
    if not isinstance(overrides, dict):
        raise TypeError("overrides は dict である必要があります。")
    _RUNTIME_CONFIG.update(overrides)


def clear_runtime_config():
    _RUNTIME_CONFIG.clear()


def get_secret_value(key: str, default=None):
    if key in _RUNTIME_CONFIG:
        value = _RUNTIME_CONFIG.get(key)
        return default if value is None else value
    v = os.environ.get(key)
    return default if v is None else v


def get_required_env(key: str) -> str:
    v = get_secret_value(key)
    if not v:
        raise RuntimeError(f"{key} が設定されていません。")
    return v


def load_service_account_info() -> dict:
    runtime_dict = get_secret_value("gcp_service_account")
    if isinstance(runtime_dict, dict):
        return runtime_dict

    raw = get_secret_value("GCP_SERVICE_ACCOUNT_JSON")
    if raw:
        return json.loads(raw)

    raise RuntimeError("gcp_service_account / GCP_SERVICE_ACCOUNT_JSON が設定されていません。")
