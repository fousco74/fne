# fne/api/client.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import requests
import frappe
from fne.utils import get_password_from_env_or_settings

@dataclass
class FNEConfig:
    base_url: str
    api_key: str
    timeout: int = 30

class FNEApiError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}

def get_fne_config():
    s = frappe.get_cached_doc("FNE Settings")
    env = s.environment or "SANDBOX"
    base_url = s.base_url_sandbox if env == "SANDBOX" else (s.base_url_prod or "{PLACEHOLDER_BASE_URL_PROD}")
    api_key = get_password_from_env_or_settings("FNE_API_KEY", "FNE Settings", "api_key")
    return FNEConfig(base_url=base_url.rstrip("/"), api_key=api_key, timeout=int(s.http_timeout_seconds or 30))

def _session():
    sess = requests.Session()
    sess.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return sess

def post(path: str, json: Dict[str, Any]) -> Dict[str, Any]:
    cfg = get_fne_config()
    url = f"{cfg.base_url}{path}"
    sess = _session()
    resp = sess.post(
        url,
        json=json,
        headers={"Authorization": f"Bearer {cfg.api_key}"},
        timeout=cfg.timeout,
    )
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    if resp.status_code >= 400:
        raise FNEApiError(
            message=data.get("message") or f"FNE HTTP {resp.status_code}",
            status_code=resp.status_code,
            payload=data,
        )
    return data
