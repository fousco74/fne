# fne/jobs/certify_document_job.py
from __future__ import annotations
from typing import Optional, Dict, Any
from frappe.utils import get_fullname
from frappe.utils import add_to_date
import time
from frappe.exceptions import TimestampMismatchError, QueryDeadlockError

import frappe
from fne.constants import STATUS_CERTIFIED, STATUS_FAILED, STATUS_PDF_PENDING, RETRIABLE_HTTP
from fne.services.guards import require_fne_enabled
from fne.services.mapping import (
    _get_customer, _get_supplier, resolve_template, resolve_client_ncc,
    resolve_establishment_pos, build_items_sale, build_items_purchase, resolve_custom_taxes_global
)
from fne.api.client import post, FNEApiError
from fne.utils import json_dumps, now_utc, exp_backoff_seconds
from fne.jobs.fetch_pdf_job import enqueue_pdf_fetch

def enqueue_certification(doctype: str, docname: str, fne_type: str,
                          fne_docname: Optional[str] = None, force: bool = False):

    job_id = f"fne:certify:{doctype}:{docname}:{fne_type}"

    frappe.enqueue(
        "fne.jobs.certify_document_job.run",
        queue="default",
        job_id=job_id,
        deduplicate=True,
        enqueue_after_commit=True,
        doctype=doctype,
        docname=docname,
        fne_type=fne_type,
        fne_docname=fne_docname,
        force=force,
    )


from frappe.exceptions import DoesNotExistError

def _load_fne_doc(doctype, docname, fne_type, fne_docname):
    if fne_docname:
        try:
            return frappe.get_doc("FNE Document", fne_docname)
        except DoesNotExistError:
            # fallback: retrouver par référence
            fne_docname = None

    name = frappe.db.get_value(
        "FNE Document",
        {"reference_doctype": doctype, "reference_name": docname, "fne_invoice_type": fne_type},
        "name",
    )
    if not name:
        frappe.throw(f"FNE Document introuvable pour {doctype} {docname} ({fne_type})")
    return frappe.get_doc("FNE Document", name)



def run(doctype: str, docname: str, fne_type: str, fne_docname: Optional[str] = None, force: bool = False):
    require_fne_enabled()

    fne_doc = _load_fne_doc(doctype, docname, fne_type, fne_docname)

    # idempotency local
    if not force and fne_doc.fne_invoice_id and fne_doc.status in (STATUS_CERTIFIED, STATUS_PDF_PENDING):
        return

    src = frappe.get_doc(doctype, docname)

    #
    est, pos = resolve_establishment_pos(src)
    s = frappe.get_cached_doc("FNE Settings")



    payload: Dict[str, Any] = {
        "paymentMethod": s.payment_method_default or "cash",
        "template": "B2C",
        "pointOfSale": pos,
        "establishment": est,
        "commercialMessage": s.default_commercial_message or "",
        "footer": s.default_footer or "",
        "foreignCurrency": "",
        "foreignCurrencyRate": 0,
    }

    if (doctype == "Sales Invoice" or doctype == "POS Invoice") and fne_type == "sale":
        customer = _get_customer(src)
        template = resolve_template(customer)
        seller_user  = getattr(src, "owner", None) or getattr(src, "modified_by", None)

        payload.update({
            "invoiceType": "sale",
            "template": template,
            "clientCompanyName": src.customer_name or customer.customer_name,
            "clientPhone": getattr(customer, "mobile_no", "") or "",
            "clientEmail": getattr(customer, "email_id", "") or "",
            "clientSellerName": get_fullname(seller_user )if seller_user else "",
            "items": build_items_sale(src),
            "customTaxes": resolve_custom_taxes_global(src),
            "discount": float(getattr(src, "additional_discount_percentage", 0) or 0),
        })
        
        if doctype == "POS Invoice":
            payload["isRne"] = True
        ncc = resolve_client_ncc(customer, template)
        if ncc:
            payload["clientNcc"] = ncc

        _persist_request_payload(fne_doc.name,payload)


        try:
            data = post("/external/invoices/sign", payload)
        except FNEApiError as e:
            _handle_error(fne_doc, e)
            return

        _persist_success_sign(fne_doc, src, data)
        enqueue_pdf_fetch(fne_doc.name)
            

    elif doctype == "Purchase Invoice" and fne_type == "purchase":
        supplier = _get_supplier(src)
        template = resolve_template(customer)
        seller_user = getattr(src, "owner", None) or getattr(src, "modified_by", None)
        payload.update({
            "invoiceType": "purchase",
            "template": template,
            "clientCompanyName": supplier.supplier_name,
            "clientPhone": getattr(supplier, "mobile_no", "") or "",
            "clientEmail": getattr(supplier, "email_id", "") or "",
            "clientSellerName":  get_fullname(seller_user) if seller_user else "",
            "items": build_items_purchase(src),
            "discount": float(getattr(src, "additional_discount_percentage", 0) or 0),
        })
        
       
        payload["clientNcc"] = supplier.tax_id or ""

        _persist_request_payload(fne_doc.name,payload)



        try:
            data = post("/external/invoices/sign", payload)
        except FNEApiError as e:
            _handle_error(fne_doc, e)
            return

        _persist_success_sign(fne_doc, src, data)
        enqueue_pdf_fetch(fne_doc.name)
    

    elif (doctype == "Sales Invoice" or doctype == "POS Invoice" or doctype == "Purchase Invoice") and fne_type == "refund":
        # Refund based on original invoice id + item ids
        if not getattr(src, "return_against", None):
            fne_doc.status = STATUS_FAILED
            fne_doc.last_error = "Return invoice missing return_against"
            fne_doc.save(ignore_permissions=True)
            return
        
        if doctype == "POS Invoice":
            payload["isRne"] = True

        orig_si = frappe.get_doc(doctype, src.return_against)
        
        if not orig_si.custom_fne_document:
            fne_doc.status = STATUS_FAILED
            fne_doc.last_error = f"Original invoice not certified (FNE Document not found) {fne_docname}"
            fne_doc.save(ignore_permissions=True)
            return

        orig_fne = frappe.get_doc("FNE Document", orig_si.custom_fne_document)
        if not orig_fne.fne_invoice_id:
            fne_doc.status = STATUS_FAILED
            fne_doc.last_error = "Original FNE invoice id missing"
            fne_doc.save(ignore_permissions=True)
            return

        items_payload = _build_refund_items(src, orig_fne)
        payload = {"items": items_payload}
        
        

        _persist_request_payload(fne_doc.name,payload)



        try:
            data = post(f"/external/invoices/{orig_fne.fne_invoice_id}/refund", payload)
        except FNEApiError as e:
            _handle_error(fne_doc, e)
            return

        _persist_success_refund(fne_doc, src, orig_fne, data)
        enqueue_pdf_fetch(fne_doc.name)

    else:
        fne_doc.status = STATUS_FAILED
        fne_doc.last_error = f"Unsupported: {doctype} / {fne_type}"
        fne_doc.save(ignore_permissions=True)

def _build_refund_items(return_si, orig_fne_doc) -> list:
    # Strategy:
    # 1) If return row has field 'sales_invoice_item' use it
    # 2) else match by item_code FIFO from orig item map
    map_rows = list(orig_fne_doc.get("items_map") or [])
    by_erp_row = {r.erp_row_name: r.fne_item_id for r in map_rows if r.erp_row_name and r.fne_item_id}

    fifo = {}
    for r in map_rows:
        if r.item_code and r.fne_item_id:
            fifo.setdefault(r.item_code, []).append(r.fne_item_id)

    out = []
    for row in return_si.items:
        link_field = getattr(row, "sales_invoice_item", None) or getattr(row, "si_detail", None)
        fne_item_id = None
        if link_field and link_field in by_erp_row:
            fne_item_id = by_erp_row[link_field]
        else:
            if row.item_code in fifo and fifo[row.item_code]:
                fne_item_id = fifo[row.item_code].pop(0)

        if not fne_item_id:
            frappe.throw(f"Impossible de mapper l'item retour {row.item_code} vers un item FNE (facture d'origine).")

        out.append({"id": fne_item_id, "quantity": float(abs(row.qty))})
    return out

def _persist_success_sign(fne_doc, src, data: dict):
    inv = data.get("invoice") or {}
    inv_items = (inv.get("items") or [])

    # Prépare les rows à insérer (sans toucher à la DB)
    item_rows = []
    erp_updates = []  # (doctype, name, field, value)

    for idx, erp_row in enumerate(src.items):
        if idx < len(inv_items) and inv_items[idx].get("id"):
            fne_item_id = inv_items[idx]["id"]
            item_rows.append({
                "erp_row_name": erp_row.name,
                "item_code": erp_row.item_code,
                "fne_item_id": fne_item_id,
                "quantity": float(abs(erp_row.qty)),
                "amount": float(erp_row.rate),
            })
            erp_updates.append((erp_row.doctype, erp_row.name, "custom_fne_item_id", fne_item_id))

    updates = {
        "response_json": json_dumps(data),
        "fne_reference": data.get("reference"),
        "token_url": data.get("token"),
        "warning": 1 if data.get("warning") else 0,
        "balance_sticker": data.get("balance_sticker") or data.get("balance_funds"),
        "fne_invoice_id": inv.get("id"),
        "status": STATUS_CERTIFIED,
        "certified_at": now_utc(),
    }

    # Écriture DB robuste : lock + retry
    for attempt in range(5):
        try:
            frappe.db.rollback()

            locked = frappe.get_doc("FNE Document", fne_doc.name, for_update=True)
            for k, v in updates.items():
                setattr(locked, k, v)

            locked.set("items_map", [])
            for r in item_rows:
                locked.append("items_map", r)

            locked.save(ignore_permissions=True, ignore_version=True)
            frappe.db.commit()
            break

        except (QueryDeadlockError, TimestampMismatchError):
            frappe.db.rollback()
            time.sleep(min(2 ** attempt, 8))
    else:
        # 5 tentatives échouées
        raise

    # IMPORTANT : mettre à jour les lignes ERP APRÈS avoir commit le FNE doc
    for dt, name, field, value in erp_updates:
        frappe.db.set_value(dt, name, field, value, update_modified=False)
    frappe.db.commit()

    _handle_sticker_warning(data)

def _persist_success_refund(fne_doc, return_si, orig_fne_doc, data: dict):
    updates = {
        "response_json": json_dumps(data),
        "fne_reference": data.get("reference"),
        "token_url": data.get("token"),
        "warning": 1 if data.get("warning") else 0,
        "balance_sticker": data.get("balance_sticker") or data.get("balance_funds"),
        "status": STATUS_CERTIFIED,
        "certified_at": now_utc(),
    }

    for attempt in range(5):
        try:
            frappe.db.rollback()

            locked = frappe.get_doc("FNE Document", fne_doc.name, for_update=True)
            for k, v in updates.items():
                setattr(locked, k, v)

            locked.save(ignore_permissions=True, ignore_version=True)
            frappe.db.commit()
            break

        except (QueryDeadlockError, TimestampMismatchError):
            frappe.db.rollback()
            time.sleep(min(2 ** attempt, 8))
    else:
        raise

    try:
        return_si.db_set("custom_fne_document", fne_doc.name, update_modified=False)
        return_si.db_set("custom_fne_status", updates["status"], update_modified=False)
    except Exception:
        pass

    _handle_sticker_warning(data)

def _handle_sticker_warning(data: dict):
    s = frappe.get_cached_doc("FNE Settings")
    warning = bool(data.get("warning"))
    balance = data.get("balance_sticker") or data.get("balance_funds")
    threshold = int(s.sticker_warning_threshold or 0)

    if warning or (balance is not None and threshold and int(balance) <= threshold):
        from fne.services.notifications import notify_sticker_low
        notify_sticker_low(balance=balance, warning=warning)

        if s.block_on_sticker_warning:
            frappe.throw("Stock sticker faible/alerte FNE: certification bloquée par paramètre (FNE Settings).")

def _handle_error(fne_doc, e: FNEApiError):
    attempts = int(fne_doc.attempts or 0) + 1
    status_code = e.status_code or 0
    response_json = json_dumps(e.payload or {})
    last_error = str(e)

    updates = {
        "attempts": attempts,
        "last_error": last_error,
        "response_json": response_json,
    }

    if status_code in RETRIABLE_HTTP:
        delay = exp_backoff_seconds(attempts, base=30, cap=3600)
        updates["next_retry_at"] = add_to_date(now_utc(), seconds=delay)
        updates["status"] = STATUS_FAILED
    else:
        updates["status"] = "DEAD"

    # Retry avec verrou pour éviter TimestampMismatchError
    for attempt in range(5):
        try:
            frappe.db.rollback()
            locked = frappe.get_doc("FNE Document", fne_doc.name, for_update=True)

            for k, v in updates.items():
                setattr(locked, k, v)

            locked.save(ignore_permissions=True, ignore_version=True)
            frappe.db.commit()
            break

        except (TimestampMismatchError, QueryDeadlockError):
            frappe.db.rollback()
            time.sleep(min(2 ** attempt, 8))
    else:
        raise



def _persist_request_payload(fne_doc_name: str, payload: dict):
    frappe.db.set_value(
        "FNE Document",
        fne_doc_name,
        "request_payload_json",
        json_dumps(payload),
        update_modified=True,
    )
    frappe.db.commit()
