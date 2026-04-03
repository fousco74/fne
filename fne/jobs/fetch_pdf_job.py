# fne/jobs/fetch_pdf_job.py
from __future__ import annotations
import frappe

def enqueue_pdf_fetch(fne_document: str, force: bool = False):
    s = frappe.get_cached_doc("FNE Settings")
    if not s.enable_pdf_fetch:
        return
    frappe.enqueue(
        "fne.jobs.fetch_pdf_job.run",
        queue="long",
        job_name=f"fne:pdf:{fne_document}",
        enqueue_after_commit=True,  # ✅
        fne_document=fne_document,
        force=force,
    )

def run(fne_document: str, force: bool = False):
    doc = frappe.get_doc("FNE Document", fne_document)
    if (not force) and doc.pdf_file:
        return

    from fne.services.pdf_fetch import fetch_and_attach_pdf
    fetch_and_attach_pdf(doc)
