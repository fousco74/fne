# fne/services/notifications.py
from __future__ import annotations
import frappe
from fne.utils import toast


def _admin_user() -> str:
	return frappe.session.user if (frappe.session and frappe.session.user) else "Administrator"


def notify_sticker_low(balance=None, warning=False):
	level   = "critique" if warning else "bas"
	msg     = f"Stock sticker FNE {level}. Solde actuel : {balance}"
	subject = f"Alerte stock sticker FNE ({level})"

	try:
		toast(_admin_user(), "FNE – Sticker", msg, "red" if warning else "orange")
	except Exception:
		pass

	try:
		frappe.get_doc({
			"doctype":       "Notification Log",
			"subject":       subject,
			"email_content": msg,
			"for_user":      _admin_user(),
			"type":          "Alert",
		}).insert(ignore_permissions=True)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "FNE sticker notification failed")


def notify_dead_document(
	fne_docname: str,
	reference_doctype: str,
	reference_name: str,
	error: str,
):
	"""Notifie qu'un FNE Document est passé en DEAD (erreur non-retryable)."""
	subject = f"FNE Dead-letter : {reference_doctype} {reference_name}"
	msg = (
		f"Le document FNE <b>{fne_docname}</b> est passé en statut DEAD.<br>"
		f"Référence : {reference_doctype} {reference_name}<br>"
		f"Erreur : {error[:500]}"
	)

	try:
		toast(_admin_user(), "FNE – Dead-letter", subject, "red")
	except Exception:
		pass

	try:
		frappe.get_doc({
			"doctype":       "Notification Log",
			"subject":       subject,
			"email_content": msg,
			"for_user":      _admin_user(),
			"type":          "Alert",
		}).insert(ignore_permissions=True)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "FNE dead-letter notification failed")


def notify_retry_batch_dead(count: int):
	"""Notifie lorsque le scheduler marque un lot de documents comme DEAD (tentatives épuisées)."""
	if count <= 0:
		return

	subject = f"FNE : {count} document(s) passé(s) en DEAD (tentatives épuisées)"
	msg = (
		f"{count} document(s) FNE ont épuisé toutes leurs tentatives de retry "
		f"et sont maintenant en statut DEAD. Consultez le rapport <i>FNE Retry Analysis</i>."
	)

	try:
		toast(_admin_user(), "FNE – Dead-letters", subject, "red")
	except Exception:
		pass

	try:
		frappe.get_doc({
			"doctype":       "Notification Log",
			"subject":       subject,
			"email_content": msg,
			"for_user":      _admin_user(),
			"type":          "Alert",
		}).insert(ignore_permissions=True)
	except Exception:
		frappe.log_error(frappe.get_traceback(), "FNE retry batch dead notification failed")
