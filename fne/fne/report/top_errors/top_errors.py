# fne/fne/report/fne_top_errors/fne_top_errors.py
from __future__ import annotations
import frappe

def execute(filters=None):
    columns = [
        {"label": "Erreur", "fieldname": "err", "fieldtype": "Data", "width": 400},
        {"label": "Count", "fieldname": "cnt", "fieldtype": "Int", "width": 80},
    ]
    data = frappe.db.sql(
        """
        SELECT LEFT(IFNULL(last_error,''), 380) as err, COUNT(*) cnt
        FROM `tabFNE Document`
        WHERE status IN ('FAILED','DEAD')
        GROUP BY err
        ORDER BY cnt DESC
        LIMIT 50
        """,
        as_dict=True,
    )
    return columns, data
