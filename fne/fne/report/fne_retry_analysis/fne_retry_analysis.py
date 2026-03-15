# fne/fne/report/fne_retry_analysis/fne_retry_analysis.py
"""
Analyse les patterns de retry : distribution des tentatives, ratio Dead vs Failed,
erreurs les plus fréquentes par nombre de tentatives.
"""
from __future__ import annotations
from collections import defaultdict
import frappe


def execute(filters=None):
	filters = filters or {}

	columns = [
		{"label": "Tentatives",     "fieldname": "attempts",   "fieldtype": "Int",     "width": 110},
		{"label": "Total docs",     "fieldname": "total",      "fieldtype": "Int",     "width": 110},
		{"label": "FAILED",         "fieldname": "failed_cnt", "fieldtype": "Int",     "width": 100},
		{"label": "DEAD",           "fieldname": "dead_cnt",   "fieldtype": "Int",     "width": 100},
		{"label": "% Dead",         "fieldname": "pct_dead",   "fieldtype": "Percent", "width": 100},
		{"label": "Erreur fréq.",   "fieldname": "top_error",  "fieldtype": "Data",    "width": 300},
	]

	cond = ""
	vals: dict = {}
	if filters.get("from_date"):
		cond += " AND creation >= %(from_date)s"
		vals["from_date"] = filters["from_date"]
	if filters.get("to_date"):
		cond += " AND creation <= %(to_date)s"
		vals["to_date"] = filters["to_date"]

	# Distribution par nombre de tentatives
	raw = frappe.db.sql(
		f"""
		SELECT
			IFNULL(attempts, 0)    AS attempts,
			COUNT(*)               AS total,
			SUM(status = 'FAILED') AS failed_cnt,
			SUM(status = 'DEAD')   AS dead_cnt
		FROM `tabFNE Document`
		WHERE status IN ('FAILED', 'DEAD') {cond}
		GROUP BY attempts
		ORDER BY attempts ASC
		""",
		vals,
		as_dict=True,
	)

	# Pour chaque bucket d'attempts, trouver l'erreur la plus fréquente
	top_errors: dict = {}
	for r in raw:
		err_raw = frappe.db.sql(
			f"""
			SELECT LEFT(IFNULL(last_error, ''), 200) AS err, COUNT(*) AS cnt
			FROM `tabFNE Document`
			WHERE status IN ('FAILED','DEAD')
			  AND IFNULL(attempts, 0) = %(att)s
			  {cond}
			GROUP BY err
			ORDER BY cnt DESC
			LIMIT 1
			""",
			{**vals, "att": r.attempts},
			as_dict=True,
		)
		top_errors[r.attempts] = (err_raw[0].err or "") if err_raw else ""

	data = []
	for r in raw:
		total = int(r.total or 0) or 1
		data.append({
			"attempts":   int(r.attempts),
			"total":      int(r.total or 0),
			"failed_cnt": int(r.failed_cnt or 0),
			"dead_cnt":   int(r.dead_cnt or 0),
			"pct_dead":   round(100.0 * int(r.dead_cnt or 0) / total, 1),
			"top_error":  top_errors.get(r.attempts, "")[:200],
		})

	# ─── Chart : Failed vs Dead par nombre de tentatives ───────────────────
	labels   = [str(r["attempts"]) for r in data]
	failed_v = [r["failed_cnt"] for r in data]
	dead_v   = [r["dead_cnt"]   for r in data]

	chart = {
		"data": {
			"labels":   labels,
			"datasets": [
				{"name": "FAILED (retryable)", "values": failed_v},
				{"name": "DEAD (définitif)",   "values": dead_v},
			],
		},
		"type":   "bar",
		"colors": ["#ff5858", "#dc3545"],
		"barOptions": {"stacked": 1},
		"axisOptions": {"xAxisMode": "tick"},
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	total_docs = sum(r["total"] for r in data)
	total_dead = sum(r["dead_cnt"] for r in data)
	max_att    = max((r["attempts"] for r in data), default=0)
	avg_att    = (
		round(sum(r["attempts"] * r["total"] for r in data) / total_docs, 1)
		if total_docs else 0
	)

	summary = [
		{"label": "Total en erreur",    "value": total_docs, "indicator": "red"},
		{"label": "Dead-letters",       "value": total_dead, "indicator": "red"},
		{"label": "Moy. tentatives",    "value": avg_att,    "indicator": "orange"},
		{"label": "Max tentatives",     "value": max_att,    "indicator": "orange"},
	]

	return columns, data, None, chart, summary
