# fne/fne/report/queue_status/queue_status.py
from __future__ import annotations
import frappe

_COLOR_MAP = {
	"Queued":         "#4099ff",
	"Certified":      "#5e64ff",
	"PDF Pending":    "#fd7e14",
	"PDF Ready":      "#28a745",
	"PDF Failed":     "#e67e22",
	"Retry Pending":  "#ff5858",
	"Dead-letter":    "#dc3545",
	"Disabled":       "#aaaaaa",
}

_STATUS_LABEL = {
	"QUEUED":      "Queued",
	"CERTIFIED":   "Certified",
	"PDF_PENDING": "PDF Pending",
	"PDF_READY":   "PDF Ready",
	"PDF_FAILED":  "PDF Failed",
	"FAILED":      "Retry Pending",
	"DEAD":        "Dead-letter",
	"DISABLED":    "Disabled",
}


def execute(filters=None):
	columns = [
		{"label": "Statut",   "fieldname": "bucket", "fieldtype": "Data",    "width": 200},
		{"label": "Nombre",   "fieldname": "cnt",    "fieldtype": "Int",     "width": 100},
		{"label": "% Total",  "fieldname": "pct",    "fieldtype": "Percent", "width": 110},
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

	data = []
	for r in raw:
		label = _STATUS_LABEL.get(r.status, r.status)
		data.append({
			"bucket": label,
			"cnt":    r.cnt,
			"pct":    round(100.0 * r.cnt / total, 1),
		})

	# ─── Chart ─────────────────────────────────────────────────────────────
	labels = [r["bucket"] for r in data]
	values = [r["cnt"]    for r in data]
	colors = [_COLOR_MAP.get(lbl, "#888888") for lbl in labels]

	chart = {
		"data": {
			"labels":   labels,
			"datasets": [{"name": "Documents", "values": values}],
		},
		"type":   "donut",
		"colors": colors,
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	certified   = sum(r["cnt"] for r in data if r["bucket"] in ("Certified", "PDF Ready", "PDF Pending"))
	queued      = sum(r["cnt"] for r in data if r["bucket"] == "Queued")
	pdf_failed  = sum(r["cnt"] for r in data if r["bucket"] == "PDF Failed")
	problems    = sum(r["cnt"] for r in data if r["bucket"] in ("Retry Pending", "Dead-letter"))

	summary = [
		{"label": "Total documents",    "value": total,      "indicator": "blue"},
		{"label": "Certifiés / Done",   "value": certified,  "indicator": "green"},
		{"label": "En attente",         "value": queued,     "indicator": "orange"},
		{"label": "PDF non récupéré",   "value": pdf_failed, "indicator": "orange"},
		{"label": "Erreurs certif.",    "value": problems,   "indicator": "red"},
	]

	return columns, data, None, chart, summary
