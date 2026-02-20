# fne/api/public.py
from __future__ import annotations
import frappe
from frappe import _
from fne.services.guards import require_fne_enabled
from fne.services.certification import certify_now
from fne.jobs.fetch_pdf_job import enqueue_pdf_fetch

@frappe.whitelist()
def certify_document(doctype: str, docname: str, fne_type: str):
    require_fne_enabled()
    return {"fne_document": certify_now(doctype, docname, fne_type)}

@frappe.whitelist()
def fetch_pdf(fne_document: str):
    require_fne_enabled()
    enqueue_pdf_fetch(fne_document, force=True)
    return {"ok": True}

@frappe.whitelist()
def get_fne_status(doctype: str, docname: str):
    rows = frappe.get_all(
        "FNE Document",
        filters={"reference_doctype": doctype, "reference_name": docname},
        fields=["name", "status", "fne_reference", "token_url", "pdf_file", "last_error", "fne_invoice_type"],
        order_by="modified desc",
        limit=5,
    )
    return rows
