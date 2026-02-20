# fne/jobs/retry_scheduler_job.py
from __future__ import annotations
import frappe
from fne.constants import STATUS_FAILED, STATUS_DEAD

def run_retry_scheduler():
    # find FAILED with next_retry_at <= now
    now = frappe.utils.now_datetime()
    rows = frappe.get_all(
        "FNE Document",
        filters={
            "status": STATUS_FAILED,
            "next_retry_at": ("<=", now),
            "attempts": ("<=", 10),
        },
        fields=["name", "reference_doctype", "reference_name", "fne_invoice_type"],
        limit=200,
        order_by="next_retry_at asc",
    )
    for r in rows:
        # enqueue certification again
        try:
            frappe.enqueue(
                "fne.jobs.certify_document_job.run",
                queue="default",
                job_name=f"fne:retry:{r.name}",
                doctype=r.reference_doctype,
                docname=r.reference_name,
                fne_type=r.fne_invoice_type,
                enqueue_after_commit=True,  # ✅
                fne_docname=r.name,
                force=True,
            )
        except Exception:
            frappe.log_error(frappe.get_traceback(), f"FNE retry enqueue failed: {r.name}")

    # mark as DEAD if attempts exceeded
    frappe.db.sql(
        """
        UPDATE `tabFNE Document`
        SET status=%s
        WHERE status=%s AND attempts > 10
        """,
        (STATUS_DEAD, STATUS_FAILED),
    )
