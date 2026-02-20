# fne/services/guards.py
import frappe

def is_prod_environment() -> bool:
    s = frappe.get_cached_doc("FNE Settings")
    return (s.environment == "PRODUCTION")

def require_fne_enabled():
    s = frappe.get_cached_doc("FNE Settings")
    if not is_prod_environment():
        return 

    if not s.remote_control_enabled:
        return

    # cached status
    status = frappe.cache().get_value("fne:remote_enabled")
    if status is False:
        frappe.throw("FNE est désactivé à distance (PROD). Contactez votre intégrateur.", frappe.PermissionError)
