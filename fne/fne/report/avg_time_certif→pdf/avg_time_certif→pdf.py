# fne/fne/report/fne_avg_time/fne_avg_time.py
from __future__ import annotations
import frappe


def execute(filters=None):
    filters = filters or {}

    columns = [
        {
            "label": "Type FNE",
            "fieldname": "fne_invoice_type",
            "fieldtype": "Data",
            "width": 120,
        },
        {
            "label": "Temps moyen (sec)",
            "fieldname": "avg_sec",
            "fieldtype": "Float",
            "width": 150,
        },
        {
            "label": "Temps moyen (min)",
            "fieldname": "avg_min",
            "fieldtype": "Float",
            "width": 150,
        },
        {
            "label": "Nombre de documents",
            "fieldname": "cnt",
            "fieldtype": "Int",
            "width": 150,
        },
    ]

    conditions = []
    values = {}

    # 🔎 Filtres optionnels
    if filters.get("from_date"):
        conditions.append("certified_at >= %(from_date)s")
        values["from_date"] = filters["from_date"]

    if filters.get("to_date"):
        conditions.append("certified_at <= %(to_date)s")
        values["to_date"] = filters["to_date"]

    if filters.get("fne_invoice_type"):
        conditions.append("fne_invoice_type = %(fne_invoice_type)s")
        values["fne_invoice_type"] = filters["fne_invoice_type"]

    where_clause = " AND ".join(conditions)
    if where_clause:
        where_clause = " AND " + where_clause

    data = frappe.db.sql(
        f"""
        SELECT
            fne_invoice_type,
            AVG(
                GREATEST(
                    TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at),
                    0
                )
            ) AS avg_sec,
            AVG(
                GREATEST(
                    TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at),
                    0
                )
            ) / 60 AS avg_min,
            COUNT(*) AS cnt
        FROM `tabFNE Document`
        WHERE
            certified_at IS NOT NULL
            AND pdf_fetched_at IS NOT NULL
            {where_clause}
        GROUP BY fne_invoice_type
        ORDER BY avg_sec DESC
        """,
        values,
        as_dict=True,
    )

    return columns, data
