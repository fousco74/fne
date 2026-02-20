# fne/services/notifications.py
from __future__ import annotations
import frappe
from fne.utils import toast

def notify_sticker_low(balance=None, warning=False):
    msg = f"Alerte sticker FNE. warning={warning} balance={balance}"
    # toast current user
    try:
        toast(frappe.session.user, "FNE", msg, "orange")
    except Exception:
        pass

    # system notification
    try:
        n = frappe.get_doc({
            "doctype": "Notification Log",
            "subject": "Alerte stock sticker FNE",
            "email_content": msg,
            "for_user": frappe.session.user if frappe.session.user else "Administrator",
            "type": "Alert",
        })
        n.insert(ignore_permissions=True)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "FNE Notification failed")
