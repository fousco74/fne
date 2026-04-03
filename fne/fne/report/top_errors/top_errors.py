# fne/fne/report/top_errors/top_errors.py
from __future__ import annotations
import frappe


def _categorize_error(err: str) -> str:
	if not err:
		return "Inconnu"
	lower = err.lower()
	if "400" in lower or "bad request" in lower:
		return "HTTP 400 (Bad Request)"
	if "401" in lower or "unauthorized" in lower:
		return "HTTP 401 (Auth)"
	if "403" in lower or "forbidden" in lower:
		return "HTTP 403 (Forbidden)"
	if "404" in lower or "not found" in lower:
		return "HTTP 404 (Not Found)"
	if "429" in lower or "rate limit" in lower or "too many" in lower:
		return "HTTP 429 (Rate Limit)"
	if "500" in lower:
		return "HTTP 500 (Server Error)"
	if "502" in lower or "bad gateway" in lower:
		return "HTTP 502 (Bad Gateway)"
	if "503" in lower or "unavailable" in lower:
		return "HTTP 503 (Unavailable)"
	if "504" in lower or "timeout" in lower or "timed out" in lower:
		return "HTTP 504 / Timeout"
	if "connection" in lower or "refused" in lower or "network" in lower:
		return "Erreur réseau"
	if "json" in lower or "decode" in lower or "parse" in lower:
		return "Erreur parsing réponse"
	if "mapping" in lower or "mapper" in lower:
		return "Erreur mapping items"
	if "pdf fetch failed" in lower or "no direct pdf endpoint" in lower or "playwright" in lower:
		return "Erreur récupération PDF"
	if "not certified" in lower or "fne document" in lower:
		return "Erreur logique FNE"
	return "Autre"


def execute(filters=None):
	filters = filters or {}

	columns = [
		{"label": "Erreur",          "fieldname": "err",       "fieldtype": "Data",     "width": 340},
		{"label": "Catégorie",       "fieldname": "category",  "fieldtype": "Data",     "width": 200},
		{"label": "Occurrences",     "fieldname": "cnt",       "fieldtype": "Int",      "width": 100},
		{"label": "Dead-letters",    "fieldname": "dead_cnt",  "fieldtype": "Int",      "width": 110},
		{"label": "Dernière fois",   "fieldname": "last_seen", "fieldtype": "Datetime", "width": 160},
	]

	status_filter = "('FAILED','DEAD')"
	sf = filters.get("status_filter", "FAILED, PDF_FAILED et DEAD")
	if sf == "FAILED, PDF_FAILED et DEAD":
		status_filter = "('FAILED','PDF_FAILED','DEAD')"
	elif sf == "DEAD uniquement":
		status_filter = "('DEAD')"
	elif sf == "FAILED uniquement":
		status_filter = "('FAILED')"
	elif sf == "PDF_FAILED uniquement":
		status_filter = "('PDF_FAILED')"

	raw = frappe.db.sql(
		f"""
		SELECT
			LEFT(IFNULL(last_error, ''), 380)  AS err,
			COUNT(*)                            AS cnt,
			SUM(status = 'DEAD')               AS dead_cnt,
			MAX(modified)                       AS last_seen
		FROM `tabFNE Document`
		WHERE status IN {status_filter}
		GROUP BY err
		ORDER BY cnt DESC
		LIMIT 50
		""",
		as_dict=True,
	)

	data = []
	for r in raw:
		data.append({
			"err":       r.err or "(vide)",
			"category":  _categorize_error(r.err or ""),
			"cnt":       r.cnt,
			"dead_cnt":  int(r.dead_cnt or 0),
			"last_seen": r.last_seen,
		})

	# ─── Chart – top 10 erreurs ────────────────────────────────────────────
	top10   = data[:10]
	labels  = [
		(r["err"][:55] + "…") if len(r["err"]) > 55 else r["err"]
		for r in top10
	]
	total_v = [r["cnt"]      for r in top10]
	dead_v  = [r["dead_cnt"] for r in top10]

	chart = {
		"data": {
			"labels":   labels,
			"datasets": [
				{"name": "Total",       "values": total_v},
				{"name": "Dead-letter", "values": dead_v},
			],
		},
		"type":   "bar",
		"colors": ["#ff5858", "#dc3545"],
	}

	# ─── Summary ───────────────────────────────────────────────────────────
	total_docs    = sum(r["cnt"]      for r in data)
	total_dead    = sum(r["dead_cnt"] for r in data)
	unique_errors = len(data)

	summary = [
		{"label": "Documents en erreur", "value": total_docs,    "indicator": "red"},
		{"label": "Dead-letters",        "value": total_dead,    "indicator": "red"},
		{"label": "Erreurs uniques",     "value": unique_errors, "indicator": "orange"},
	]

	return columns, data, None, chart, summary
