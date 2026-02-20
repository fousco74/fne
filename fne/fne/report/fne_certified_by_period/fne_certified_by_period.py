# fne/fne/report/fne_certified_by_period/fne_certified_by_period.py
from __future__ import annotations
import frappe

def execute(filters=None):
    filters = filters or {}
    columns = [
        {"label": "Date", "fieldname": "day", "fieldtype": "Date", "width": 110},
        {"label": "Type", "fieldname": "fne_invoice_type", "fieldtype": "Data", "width": 90},
        {"label": "Certifiées", "fieldname": "cnt", "fieldtype": "Int", "width": 90},
    ]
    cond, vals = _conds(filters)
    data = frappe.db.sql(
        f"""
        SELECT DATE(certified_at) as day, fne_invoice_type, COUNT(*) as cnt
        FROM `tabFNE Document`
        WHERE certified_at IS NOT NULL {cond}
        GROUP BY DATE(certified_at), fne_invoice_type
        ORDER BY day DESC
        """,
        vals,
        as_dict=True,
    )
    return columns, data

def _conds(filters):
    cond = ""
    vals = {}
    if filters.get("from_date"):
        cond += " AND certified_at >= %(from_date)s"
        vals["from_date"] = filters["from_date"]
    if filters.get("to_date"):
        cond += " AND certified_at <= %(to_date)s"
        vals["to_date"] = filters["to_date"]
    if filters.get("company"):
        cond += " AND company = %(company)s"
        vals["company"] = filters["company"]
    return cond, vals
