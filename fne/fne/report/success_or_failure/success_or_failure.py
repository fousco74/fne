# fne/fne/report/success_or_failure/success_or_failure.py
from __future__ import annotations
import frappe

_STATUS_COLORS = {
	"PDF_READY":   "#28a745",
	"CERTIFIED":   "#5e64ff",
	"PDF_PENDING": "#fd7e14",
	"PDF_FAILED":  "#e67e22",
	"QUEUED":      "#4099ff",
	"FAILED":      "#ff5858",
	"DEAD":        "#dc3545",
	"DISABLED":    "#aaaaaa",
}

_SUCCESS_STATUSES = {"CERTIFIED", "PDF_PENDING", "PDF_READY"}


def execute(filters=None):
	columns = [
		{"label": "Statut",       "fieldname": "status",   "fieldtype": "Data",    "width": 140},
		{"label": "Nombre",       "fieldname": "cnt",      "fieldtype": "Int",     "width": 100},
		{"label": "% Total",      "fieldname": "pct",      "fieldtype": "Percent", "width": 100},
		{"label": "% Cumulé",     "fieldname": "cum_pct",  "fieldtype": "Percent", "width": 120},
	]

	raw = frappe.db.sql(
		"""
		SELECT status, COUNT(*) AS cnt
		FROM `tabFNE Document`
		GROUP BY status
		ORDER BY cnt DESC
		""",
		as_dict=True,
	)

	total = sum(r.cnt for r in raw) or 1
	cumulative = 0
	data = []
	for r in raw:
		pct = round(100.0 * r.cnt / total, 1)
		cumulative += r.cnt
		data.append({
			"status":  r.status,
			"cnt":     r.cnt,
			"pct":     pct,
			"cum_pct": round(100.0 * cumulative / total, 1),
		})

	# ─── Chart ─────────────────────────────────────────────────────────────
	labels = [r["status"] for r in data]
	values = [r["cnt"]    for r in data]
	colors = [_STATUS_COLORS.get(s, "#888888") for s in labels]

	chart = {
		"data": {
			"labels":   labels,
			"datasets": [{"name": "Documents", "values": values}],
		},
		"type":   "bar",
		"colors": colors,
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	success    = sum(r["cnt"] for r in data if r["status"] in _SUCCESS_STATUSES)
	pdf_failed = sum(r["cnt"] for r in data if r["status"] == "PDF_FAILED")
	failures   = sum(r["cnt"] for r in data if r["status"] in ("FAILED", "DEAD"))
	rate       = round(100.0 * success / total, 1)

	summary = [
		{"label": "Total",                  "value": total,      "indicator": "blue"},
		{"label": "Succès",                 "value": success,    "indicator": "green"},
		{"label": "PDF non récupéré",       "value": pdf_failed, "indicator": "orange"},
		{"label": "Échecs certif. / Dead",  "value": failures,   "indicator": "red"},
		{"label": "Taux de succès",         "value": f"{rate} %",
			"indicator": "green" if rate >= 90 else ("orange" if rate >= 70 else "red")},
	]

	return columns, data, None, chart, summary
