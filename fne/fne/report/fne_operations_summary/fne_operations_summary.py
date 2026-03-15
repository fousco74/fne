# fne/fne/report/fne_operations_summary/fne_operations_summary.py
"""
Tableau de bord opérationnel jour par jour :
soumissions, certifications, PDF prêts, échecs, dead-letters, taux de succès.
"""
from __future__ import annotations
import frappe


def execute(filters=None):
	filters = filters or {}

	columns = [
		{"label": "Date",          "fieldname": "day",       "fieldtype": "Date",    "width": 110},
		{"label": "Soumis",        "fieldname": "submitted", "fieldtype": "Int",     "width": 90},
		{"label": "Certifiés",     "fieldname": "certified", "fieldtype": "Int",     "width": 90},
		{"label": "PDF prêt",      "fieldname": "pdf_ready", "fieldtype": "Int",     "width": 90},
		{"label": "Échoués",       "fieldname": "failed",    "fieldtype": "Int",     "width": 90},
		{"label": "Dead-letters",  "fieldname": "dead",      "fieldtype": "Int",     "width": 100},
		{"label": "Taux cert. %",  "fieldname": "cert_rate", "fieldtype": "Percent", "width": 110},
		{"label": "Taux PDF %",    "fieldname": "pdf_rate",  "fieldtype": "Percent", "width": 100},
	]

	cond = ""
	vals: dict = {}
	if filters.get("from_date"):
		cond += " AND creation >= %(from_date)s"
		vals["from_date"] = filters["from_date"]
	if filters.get("to_date"):
		cond += " AND creation <= %(to_date)s"
		vals["to_date"] = filters["to_date"]
	if filters.get("fne_invoice_type"):
		cond += " AND fne_invoice_type = %(fne_invoice_type)s"
		vals["fne_invoice_type"] = filters["fne_invoice_type"]

	raw = frappe.db.sql(
		f"""
		SELECT
			DATE(creation)                                                AS day,
			COUNT(*)                                                      AS submitted,
			SUM(status IN ('CERTIFIED', 'PDF_PENDING', 'PDF_READY'))     AS certified,
			SUM(status = 'PDF_READY')                                    AS pdf_ready,
			SUM(status = 'FAILED')                                       AS failed,
			SUM(status = 'DEAD')                                         AS dead
		FROM `tabFNE Document`
		WHERE 1=1 {cond}
		GROUP BY DATE(creation)
		ORDER BY day ASC
		""",
		vals,
		as_dict=True,
	)

	data = []
	for r in raw:
		submitted = int(r.submitted or 0)
		certified = int(r.certified or 0)
		pdf_ready = int(r.pdf_ready or 0)
		data.append({
			"day":       r.day,
			"submitted": submitted,
			"certified": certified,
			"pdf_ready": pdf_ready,
			"failed":    int(r.failed or 0),
			"dead":      int(r.dead or 0),
			"cert_rate": round(100.0 * certified / submitted, 1) if submitted else 0.0,
			"pdf_rate":  round(100.0 * pdf_ready  / certified, 1) if certified else 0.0,
		})

	# ─── Chart ─────────────────────────────────────────────────────────────
	labels   = [str(r["day"]) for r in data]
	sub_v    = [r["submitted"] for r in data]
	cert_v   = [r["certified"] for r in data]
	pdf_v    = [r["pdf_ready"] for r in data]
	failed_v = [r["failed"]    for r in data]

	chart = {
		"data": {
			"labels":   labels,
			"datasets": [
				{"name": "Soumis",    "values": sub_v},
				{"name": "Certifiés", "values": cert_v},
				{"name": "PDF prêt",  "values": pdf_v},
				{"name": "Échoués",   "values": failed_v},
			],
		},
		"type":   "line",
		"colors": ["#4099ff", "#5e64ff", "#28a745", "#dc3545"],
		"lineOptions": {"regionFill": 0, "hideDots": 0},
		"axisOptions": {"xIsSeries": 1},
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	total_sub  = sum(r["submitted"] for r in data)
	total_cert = sum(r["certified"] for r in data)
	total_pdf  = sum(r["pdf_ready"] for r in data)
	total_fail = sum(r["failed"]    for r in data)
	total_dead = sum(r["dead"]      for r in data)
	cert_rate  = round(100.0 * total_cert / total_sub,  1) if total_sub  else 0.0
	pdf_rate   = round(100.0 * total_pdf  / total_cert, 1) if total_cert else 0.0

	summary = [
		{"label": "Total soumis",        "value": total_sub,  "indicator": "blue"},
		{"label": "Total certifiés",     "value": total_cert, "indicator": "green"},
		{"label": "Total PDF prêts",     "value": total_pdf,  "indicator": "green"},
		{"label": "Échecs",              "value": total_fail, "indicator": "orange"},
		{"label": "Dead-letters",        "value": total_dead, "indicator": "red"},
		{"label": "Taux cert. global",   "value": f"{cert_rate} %",
			"indicator": "green" if cert_rate >= 95 else ("orange" if cert_rate >= 80 else "red")},
		{"label": "Taux PDF global",     "value": f"{pdf_rate} %",
			"indicator": "green" if pdf_rate >= 90 else ("orange" if pdf_rate >= 70 else "red")},
	]

	return columns, data, None, chart, summary
