# fne/services/certification.py
from __future__ import annotations
from typing import Optional, Dict, Any, List
import frappe
from fne.constants import (
    STATUS_QUEUED, STATUS_CERTIFIED, STATUS_FAILED, STATUS_DISABLED,
    FNE_INVOICE_TYPE_SALE, FNE_INVOICE_TYPE_PURCHASE, FNE_INVOICE_TYPE_REFUND,
)
from fne.services.guards import require_fne_enabled
from fne.services import mapping
from fne.jobs.certify_document_job import enqueue_certification
from frappe.exceptions import DuplicateEntryError


def _get_settings():
    return frappe.get_cached_doc("FNE Settings")

def on_pos_invoice_submit(doc, method=None):
    s = _get_settings()
    if s.certify_on != "SUBMIT" or doc.is_consolidated == 1:
        return
   
    if doc.is_return:
        fne_type = FNE_INVOICE_TYPE_REFUND
    else:
        fne_type = FNE_INVOICE_TYPE_SALE

    fne_docname = ensure_fne_document("POS Invoice", doc.name, fne_type)
    doc.custom_fne_document = fne_docname
    doc.save(ignore_permissions=True)
    enqueue_certification("POS Invoice", doc.name, fne_type, fne_docname=fne_docname, force=False)

def on_sales_invoice_submit(doc, method=None):
    s = _get_settings()
    if s.certify_on != "SUBMIT" or doc.is_consolidated == 1:
        return

    if doc.is_return:
        fne_type = FNE_INVOICE_TYPE_REFUND
    else:
        fne_type = FNE_INVOICE_TYPE_SALE

    fne_docname = ensure_fne_document("Sales Invoice", doc.name, fne_type)
    doc.custom_fne_document = fne_docname
    doc.save(ignore_permissions=True)    
    enqueue_certification("Sales Invoice", doc.name, fne_type, fne_docname=fne_docname, force=False)


def on_purchase_invoice_submit(doc, method=None):
    s = _get_settings()
    if s.certify_on != "SUBMIT":
        return
    if not _is_agricole_purchase(doc):
        return
    
    if doc.is_return:
        fne_type = FNE_INVOICE_TYPE_REFUND
    else:
        fne_type = FNE_INVOICE_TYPE_PURCHASE

    fne_docname = ensure_fne_document("Purchase Invoice", doc.name, fne_type)
    doc.custom_fne_document = fne_docname
    doc.save(ignore_permissions=True)
    enqueue_certification("Purchase Invoice", doc.name, fne_type, fne_docname=fne_docname, force=False)


def _is_agricole_purchase(doc) -> bool:
    if getattr(doc, "custom_is_agricole", 0):
        return True

    item_codes = [row.item_code for row in (getattr(doc, "items", []) or []) if row.item_code]
    if not item_codes:
        return False

    rows = frappe.db.get_values(
        "Item",
        filters={"name": ["in", item_codes]},
        fieldname=["name", "custom_is_agricole"],
        as_dict=True,
    )
    return any(r.get("custom_is_agricole") for r in rows)



def ensure_fne_document(doctype: str, docname: str, fne_type: str) -> str:
    existing = frappe.db.get_value(
        "FNE Document",
        {"reference_doctype": doctype, "reference_name": docname, "fne_invoice_type": fne_type},
        "name",
    )
    if existing:
        return existing

    try:
        d = frappe.get_doc({
            "doctype": "FNE Document",
            "reference_doctype": doctype,
            "reference_name": docname,
            "fne_invoice_type": fne_type,
            "status": STATUS_QUEUED,
            "attempts": 0,
        })
        d.insert(ignore_permissions=True)
        return d.name
    except DuplicateEntryError:
        # quelqu’un l’a créé entre-temps
        return frappe.db.get_value(
            "FNE Document",
            {"reference_doctype": doctype, "reference_name": docname, "fne_invoice_type": fne_type},
            "name",
        )


def certify_now(doctype: str, docname: str, fne_type: str) -> str:
    require_fne_enabled()
    fne_docname = ensure_fne_document(doctype, docname, fne_type)
    enqueue_certification(doctype, docname, fne_type, fne_docname=fne_docname, force=True)
    return fne_docname
