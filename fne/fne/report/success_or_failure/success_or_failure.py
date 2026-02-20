# fne/fne/report/fne_success_failure/fne_success_failure.py
from __future__ import annotations
import frappe

def execute(filters=None):
    columns = [
        {"label": "Status", "fieldname": "status", "fieldtype": "Data", "width": 140},
        {"label": "Count", "fieldname": "cnt", "fieldtype": "Int", "width": 80},
    ]
    data = frappe.db.sql(
        """
        SELECT status, COUNT(*) cnt
        FROM `tabFNE Document`
        GROUP BY status
        ORDER BY cnt DESC
        """,
        as_dict=True,
    )
    return columns, data
