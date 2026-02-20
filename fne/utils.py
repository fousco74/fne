# fne/utils.py
import hashlib
import json
import random
from datetime import datetime, timedelta
import frappe
from frappe.utils import now

# fne/utils.py
from datetime import datetime

def now_utc():
    # datetime UTC "naïf" (sans timezone) -> le plus compatible avec Frappe
    return frappe.utils.now_datetime()

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)

def jitter_seconds(base: int, ratio: float = 0.25) -> int:
    # +/- 25%
    delta = int(base * ratio)
    return max(0, base + random.randint(-delta, delta))

def exp_backoff_seconds(attempt: int, base: int = 30, cap: int = 3600) -> int:
    # attempt starts at 1
    val = min(cap, base * (2 ** (attempt - 1)))
    return jitter_seconds(val)

def get_password_from_env_or_settings(env_key: str, settings_doctype: str, fieldname: str) -> str:
    v = frappe.conf.get(env_key)
    if v:
        return v
    return frappe.get_cached_doc(settings_doctype).get_password(fieldname)

def toast(user: str, title: str, message: str, indicator: str = "orange"):
    try:
        frappe.publish_realtime(
            event="fne_toast",
            message={"title": title, "message": message, "indicator": indicator},
            user=user,
        )
    except Exception:
        frappe.log_error(frappe.get_traceback(), "FNE toast publish failed")
