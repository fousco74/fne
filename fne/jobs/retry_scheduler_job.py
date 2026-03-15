# fne/jobs/retry_scheduler_job.py
from __future__ import annotations
import frappe
from fne.constants import STATUS_FAILED, STATUS_DEAD


def run_retry_scheduler():
	now = frappe.utils.now_datetime()

	# ── Re-enqueue retryable docs ──────────────────────────────────────────
	rows = frappe.get_all(
		"FNE Document",
		filters={
			"status":        STATUS_FAILED,
			"next_retry_at": ("<=", now),
			"attempts":      ("<=", 10),
		},
		fields=["name", "reference_doctype", "reference_name", "fne_invoice_type"],
		limit=200,
		order_by="next_retry_at asc",
	)

	for r in rows:
		try:
			frappe.enqueue(
				"fne.jobs.certify_document_job.run",
				queue="default",
				job_name=f"fne:retry:{r.name}",
				doctype=r.reference_doctype,
				docname=r.reference_name,
				fne_type=r.fne_invoice_type,
				enqueue_after_commit=True,
				fne_docname=r.name,
				force=True,
			)
		except Exception:
			frappe.log_error(frappe.get_traceback(), f"FNE retry enqueue failed: {r.name}")

	# ── Promote over-limit docs to DEAD and notify ─────────────────────────
	dead_count = frappe.db.sql(
		"""
		SELECT COUNT(*) FROM `tabFNE Document`
		WHERE status = %s AND attempts > 10
		""",
		(STATUS_FAILED,),
	)[0][0] or 0

	if dead_count:
		frappe.db.sql(
			"""
			UPDATE `tabFNE Document`
			SET status = %s
			WHERE status = %s AND attempts > 10
			""",
			(STATUS_DEAD, STATUS_FAILED),
		)
		frappe.db.commit()

		try:
			from fne.services.notifications import notify_retry_batch_dead
			notify_retry_batch_dead(dead_count)
		except Exception:
			frappe.log_error(frappe.get_traceback(), "FNE retry-batch-dead notification failed")
