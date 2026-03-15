# fne/fne/report/fne_certified_by_period/fne_certified_by_period.py
from __future__ import annotations
from collections import defaultdict
import frappe

_TYPE_COLORS = {
	"sale":     "#5e64ff",
	"purchase": "#28a745",
	"refund":   "#fd7e14",
}

_PERIOD_SQL = {
	"Jour":  "DATE(certified_at)",
	"Semaine": "DATE(DATE_SUB(certified_at, INTERVAL WEEKDAY(certified_at) DAY))",
	"Mois":  "DATE_FORMAT(certified_at, '%Y-%m-01')",
}


def execute(filters=None):
	filters = filters or {}
	period      = filters.get("period") or "Jour"
	period_expr = _PERIOD_SQL.get(period, _PERIOD_SQL["Jour"])

	columns = [
		{"label": "Période",    "fieldname": "day",              "fieldtype": "Date", "width": 120},
		{"label": "Type",       "fieldname": "fne_invoice_type", "fieldtype": "Data", "width": 100},
		{"label": "Certifiées", "fieldname": "cnt",              "fieldtype": "Int",  "width": 100},
	]

	cond, vals = _conds(filters)

	data = frappe.db.sql(
		f"""
		SELECT
			{period_expr}   AS day,
			fne_invoice_type,
			COUNT(*)         AS cnt
		FROM `tabFNE Document`
		WHERE certified_at IS NOT NULL {cond}
		GROUP BY {period_expr}, fne_invoice_type
		ORDER BY day ASC
		""",
		vals,
		as_dict=True,
	)

	# ─── Build chart (one dataset per invoice type) ─────────────────────────
	days_set = sorted({str(r.day) for r in data})
	types    = sorted({r.fne_invoice_type for r in data})

	pivot: dict = defaultdict(lambda: defaultdict(int))
	for r in data:
		pivot[r.fne_invoice_type][str(r.day)] += r.cnt

	datasets = [
		{"name": t, "values": [pivot[t].get(d, 0) for d in days_set]}
		for t in types
	]

	chart = {
		"data": {
			"labels":   days_set,
			"datasets": datasets,
		},
		"type":   "line",
		"colors": [_TYPE_COLORS.get(t, "#888888") for t in types],
		"lineOptions": {"regionFill": 1, "hideDots": 0},
		"axisOptions": {"xIsSeries": 1},
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	grand_total = sum(r.cnt for r in data)
	by_type: dict = defaultdict(int)
	for r in data:
		by_type[r.fne_invoice_type] += r.cnt

	summary = [{"label": "Total certifiées", "value": grand_total, "indicator": "green"}]
	for t, cnt in sorted(by_type.items()):
		color = "blue" if t == "sale" else ("green" if t == "purchase" else "orange")
		summary.append({"label": f"  {t}", "value": cnt, "indicator": color})

	return columns, data, None, chart, summary


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
	if filters.get("fne_invoice_type"):
		cond += " AND fne_invoice_type = %(fne_invoice_type)s"
		vals["fne_invoice_type"] = filters["fne_invoice_type"]
	return cond, vals
