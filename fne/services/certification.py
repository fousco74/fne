# fne/services/certification.py
from __future__ import annotations
import frappe
from frappe.exceptions import DuplicateEntryError

from fne.constants import (
    STATUS_QUEUED,
    FNE_INVOICE_TYPE_SALE,
    FNE_INVOICE_TYPE_PURCHASE,
    FNE_INVOICE_TYPE_REFUND,
)

from fne.services.guards import require_fne_enabled
from fne.jobs.certify_document_job import enqueue_certification


def _get_settings():
    return frappe.get_cached_doc("FNE Settings")


# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def _get_fne_type(doc, purchase=False):
    if doc.is_return:
        return FNE_INVOICE_TYPE_REFUND
    return FNE_INVOICE_TYPE_PURCHASE if purchase else FNE_INVOICE_TYPE_SALE


def _link_fne_document(doc, fne_docname):
    doc.db_set("custom_fne_document", fne_docname, update_modified=True)


# ---------------------------------------------------
# Hooks
# ---------------------------------------------------

def on_pos_invoice_submit(doc, method=None):
    s = _get_settings()

    if s.certify_on != "SUBMIT" or doc.is_consolidated:
        return

    fne_type    = _get_fne_type(doc)
    fne_docname = ensure_fne_document("POS Invoice", doc.name, fne_type)
    _link_fne_document(doc, fne_docname)

    enqueue_certification(
        "POS Invoice",
        doc.name,
        fne_type,
        fne_docname=fne_docname,
        force=False,
    )


def on_sales_invoice_submit(doc, method=None):
    s = _get_settings()

    if s.certify_on != "SUBMIT" or doc.is_consolidated:
        return

    fne_type    = _get_fne_type(doc)
    fne_docname = ensure_fne_document("Sales Invoice", doc.name, fne_type)
    _link_fne_document(doc, fne_docname)

    enqueue_certification(
        "Sales Invoice",
        doc.name,
        fne_type,
        fne_docname=fne_docname,
        force=False,
    )


def on_purchase_invoice_submit(doc, method=None):
    s = _get_settings()

    # Les retours Purchase ne passent pas par ce hook (pas de refund FNE sur achat)
    if s.certify_on != "SUBMIT" or doc.is_return:
        return

    if not _is_agricole_purchase(doc):
        return

    fne_type    = _get_fne_type(doc, purchase=True)
    fne_docname = ensure_fne_document("Purchase Invoice", doc.name, fne_type)
    _link_fne_document(doc, fne_docname)

    enqueue_certification(
        "Purchase Invoice",
        doc.name,
        fne_type,
        fne_docname=fne_docname,
        force=False,
    )


# ---------------------------------------------------
# Business Logic
# ---------------------------------------------------

def _is_agricole_purchase(doc) -> bool:
    if getattr(doc, "custom_is_agricole", 0):
        return True

    item_codes = [row.item_code for row in (doc.items or []) if row.item_code]

    if not item_codes:
        return False

    return frappe.db.exists(
        "Item",
        {
            "name": ["in", item_codes],
            "custom_is_agricole": 1,
        },
    )


# ---------------------------------------------------
# FNE Document management
# ---------------------------------------------------

def ensure_fne_document(doctype: str, docname: str, fne_type: str) -> str:

    existing = frappe.db.get_value(
        "FNE Document",
        {
            "reference_doctype": doctype,
            "reference_name":    docname,
            "fne_invoice_type":  fne_type,
        },
        "name",
    )

    if existing:
        return existing

    try:
        doc = frappe.get_doc({
            "doctype":           "FNE Document",
            "reference_doctype": doctype,
            "reference_name":    docname,
            "fne_invoice_type":  fne_type,
            "status":            STATUS_QUEUED,
            "attempts":          0,
        })
        doc.insert(ignore_permissions=True)
        return doc.name

    except DuplicateEntryError:
        return frappe.db.get_value(
            "FNE Document",
            {
                "reference_doctype": doctype,
                "reference_name":    docname,
                "fne_invoice_type":  fne_type,
            },
            "name",
        )


# ---------------------------------------------------
# Manual certification
# ---------------------------------------------------

def certify_now(doctype: str, docname: str, fne_type: str) -> str:

    require_fne_enabled()

    doc = frappe.get_doc(doctype, docname)

    # Les avoirs Purchase ne passent pas par le circuit FNE
    if doctype == "Purchase Invoice" and doc.is_return:
        frappe.throw(frappe._("Les avoirs fournisseurs ne sont pas certifiables via FNE."))

    existing = frappe.db.get_value(
        "FNE Document",
        {
            "reference_doctype": doctype,
            "reference_name":    docname,
            "fne_invoice_type":  fne_type,
        },
        ["name", "status", "token_url"],
        as_dict=True,
    )

    if existing and existing.token_url:
        frappe.throw(frappe._("Ce document a déjà été certifié (token FNE existant)."))

    if existing:
        # Remise à zéro du document existant (FAILED/DEAD → QUEUED)
        # On conserve l'historique des tentatives au lieu de supprimer/recréer
        frappe.db.set_value(
            "FNE Document",
            existing.name,
            {
                "status":        STATUS_QUEUED,
                "last_error":    "",
                "next_retry_at": None,
            },
            update_modified=True,
        )
        fne_docname = existing.name
    else:
        fne_docname = ensure_fne_document(doctype, docname, fne_type)

    _link_fne_document(doc, fne_docname)

    enqueue_certification(
        doctype,
        docname,
        fne_type,
        fne_docname=fne_docname,
        force=True,
    )

    return fne_docname