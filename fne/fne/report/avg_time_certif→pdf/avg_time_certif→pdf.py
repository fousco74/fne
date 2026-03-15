# fne/fne/report/avg_time_certif→pdf/avg_time_certif→pdf.py
from __future__ import annotations
import frappe


def execute(filters=None):
	filters = filters or {}

	columns = [
		{"label": "Type FNE",          "fieldname": "fne_invoice_type", "fieldtype": "Data",  "width": 120},
		{"label": "Médiane (sec)",      "fieldname": "med_sec",          "fieldtype": "Float", "width": 130},
		{"label": "Moyenne (sec)",      "fieldname": "avg_sec",          "fieldtype": "Float", "width": 130},
		{"label": "Min (sec)",          "fieldname": "min_sec",          "fieldtype": "Float", "width": 110},
		{"label": "Max (sec)",          "fieldname": "max_sec",          "fieldtype": "Float", "width": 110},
		{"label": "Moyenne (min)",      "fieldname": "avg_min",          "fieldtype": "Float", "width": 130},
		{"label": "Documents",          "fieldname": "cnt",              "fieldtype": "Int",   "width": 100},
	]

	conditions = []
	values = {}

	if filters.get("from_date"):
		conditions.append("certified_at >= %(from_date)s")
		values["from_date"] = filters["from_date"]
	if filters.get("to_date"):
		conditions.append("certified_at <= %(to_date)s")
		values["to_date"] = filters["to_date"]
	if filters.get("fne_invoice_type"):
		conditions.append("fne_invoice_type = %(fne_invoice_type)s")
		values["fne_invoice_type"] = filters["fne_invoice_type"]

	where_clause = ("AND " + " AND ".join(conditions)) if conditions else ""

	data = frappe.db.sql(
		f"""
		SELECT
			fne_invoice_type,
			ROUND(AVG(GREATEST(TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at), 0)), 1) AS avg_sec,
			ROUND(MIN(GREATEST(TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at), 0)), 1) AS min_sec,
			ROUND(MAX(GREATEST(TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at), 0)), 1) AS max_sec,
			ROUND(AVG(GREATEST(TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at), 0)) / 60, 2) AS avg_min,
			COUNT(*) AS cnt
		FROM `tabFNE Document`
		WHERE certified_at IS NOT NULL AND pdf_fetched_at IS NOT NULL
		{where_clause}
		GROUP BY fne_invoice_type
		ORDER BY avg_sec DESC
		""",
		values,
		as_dict=True,
	)

	# MariaDB lacks MEDIAN – compute in Python
	for row in data:
		median_raw = frappe.db.sql(
			f"""
			SELECT GREATEST(TIMESTAMPDIFF(SECOND, certified_at, pdf_fetched_at), 0) AS diff
			FROM `tabFNE Document`
			WHERE certified_at IS NOT NULL AND pdf_fetched_at IS NOT NULL
			  AND fne_invoice_type = %(fne_type)s
			  {where_clause}
			ORDER BY diff
			""",
			{**values, "fne_type": row.fne_invoice_type},
			as_dict=True,
		)
		diffs = [r.diff for r in median_raw if r.diff is not None]
		if diffs:
			mid = len(diffs) // 2
			row["med_sec"] = (
				diffs[mid] if len(diffs) % 2
				else round((diffs[mid - 1] + diffs[mid]) / 2, 1)
			)
		else:
			row["med_sec"] = 0.0

	# ─── Chart ─────────────────────────────────────────────────────────────
	labels = [r.fne_invoice_type for r in data]
	avg_v  = [float(r.avg_sec or 0) for r in data]
	med_v  = [float(r.med_sec or 0) for r in data]
	max_v  = [float(r.max_sec or 0) for r in data]

	chart = {
		"data": {
			"labels":   labels,
			"datasets": [
				{"name": "Moyenne (sec)", "values": avg_v},
				{"name": "Médiane (sec)", "values": med_v},
				{"name": "Max (sec)",     "values": max_v},
			],
		},
		"type":   "bar",
		"colors": ["#5e64ff", "#28a745", "#fd7e14"],
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	total_cnt   = sum(r.cnt for r in data)
	all_avgs    = [float(r.avg_sec) for r in data if r.avg_sec is not None]
	overall_avg = round(sum(all_avgs) / len(all_avgs), 1) if all_avgs else 0

	summary = [
		{"label": "Documents analysés",     "value": total_cnt,   "indicator": "blue"},
		{"label": "Moyenne globale (sec)",   "value": overall_avg,
			"indicator": "green" if overall_avg < 60 else ("orange" if overall_avg < 300 else "red")},
	]

	return columns, data, None, chart, summary
