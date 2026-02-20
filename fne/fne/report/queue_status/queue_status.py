# fne/fne/report/fne_queue_status/fne_queue_status.py
from __future__ import annotations
import frappe

def execute(filters=None):
    columns = [
        {"label": "Bucket", "fieldname": "bucket", "fieldtype": "Data", "width": 200},
        {"label": "Count", "fieldname": "cnt", "fieldtype": "Int", "width": 80},
    ]
    data = frappe.db.sql(
        """
        SELECT
          CASE
            WHEN status='QUEUED' THEN 'Queued'
            WHEN status='FAILED' THEN 'Retry Pending'
            WHEN status='DEAD' THEN 'Dead-letter'
            WHEN status='PDF_PENDING' THEN 'PDF Pending'
            ELSE status
          END as bucket,
          COUNT(*) cnt
        FROM `tabFNE Document`
        GROUP BY bucket
        ORDER BY cnt DESC
        """,
        as_dict=True,
    )
    return columns, data
