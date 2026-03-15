# fne/jobs/certify_document_job.py
from __future__ import annotations
from typing import Optional, Dict, Any, List
import time

import frappe
from frappe.exceptions import DoesNotExistError, TimestampMismatchError, QueryDeadlockError
from frappe.utils import get_fullname, add_to_date

from fne.constants import STATUS_CERTIFIED, STATUS_DEAD, STATUS_FAILED, STATUS_PDF_PENDING, RETRIABLE_HTTP
from fne.services.guards import require_fne_enabled
from fne.services.mapping import (
    _get_customer, _get_supplier, resolve_template, resolve_client_ncc,
    resolve_establishment_pos, resolve_custom_taxes_global
)
from fne.api.client import post, FNEApiError
from fne.utils import json_dumps, now_utc, exp_backoff_seconds
from fne.jobs.fetch_pdf_job import enqueue_pdf_fetch


# ═══════════════════════════════════════════════════════════════════════════════
# MAPPING TABLES  (ERPNext → FNE spec)
# ═══════════════════════════════════════════════════════════════════════════════

# Annexe 1 – paymentMethod
_PAYMENT_METHOD_MAP: Dict[str, str] = {
    # cash
    "cash":             "cash",
    "espèces":          "cash",
    "especes":          "cash",
    "paiement en espèces": "cash",
    # card
    "card":             "card",
    "credit card":      "card",
    "carte":            "card",
    "carte bancaire":   "card",
    "debit card":       "card",
    # cheque
    "cheque":           "check",
    "chèque":           "check",
    "check":            "check",
    # mobile-money
    "mobile money":     "mobile-money",
    "mobile-money":     "mobile-money",
    "orange money":     "mobile-money",
    "mtn money":        "mobile-money",
    "wave":             "mobile-money",
    "moov money":       "mobile-money",
    # transfer
    "bank transfer":    "transfer",
    "virement":         "transfer",
    "virement bancaire":"transfer",
    "transfer":         "transfer",
    # deferred
    "deferred":         "deferred",
    "à terme":          "deferred",
    "a terme":          "deferred",
    "crédit":           "deferred",
    "credit":           "deferred",
}

# Annexe 1 – foreignCurrency (ERPNext currency name → ISO code FNE)
_CURRENCY_MAP: Dict[str, str] = {
    "XOF":                          "XOF",
    "CFA":                          "XOF",
    "Franc CFA":                    "XOF",
    "USD":                          "USD",
    "US Dollar":                    "USD",
    "Dollar":                       "USD",
    "EUR":                          "EUR",
    "Euro":                         "EUR",
    "JPY":                          "JPY",
    "Yen":                          "JPY",
    "CAD":                          "CAD",
    "Canadian Dollar":              "CAD",
    "GBP":                          "GBP",
    "Pound Sterling":               "GBP",
    "Livre Sterling":               "GBP",
    "AUD":                          "AUD",
    "Australian Dollar":            "AUD",
    "CNH":                          "CNH",
    "CNY":                          "CNH",
    "Yuan":                         "CNH",
    "CHF":                          "CHF",
    "Swiss Franc":                  "CHF",
    "Franc Suisse":                 "CHF",
    "HKD":                          "HKD",
    "Hong Kong Dollar":             "HKD",
    "NZD":                          "NZD",
    "New Zealand Dollar":           "NZD",
}

# Annexe 1 – taxes (ERPNext account/tax name → FNE code)
# Doit être enrichi selon les comptes TVA configurés dans ERPNext
_TAX_ACCOUNT_MAP: Dict[str, str] = {
    # TVA normale 18%
    "tva":              "TVA",
    "tva 18":           "TVA",
    "tva normal":       "TVA",
    "tva normale":      "TVA",
    "tva 18%":          "TVA",
    "tva a":            "TVA",
    # TVA réduit 9%
    "tva 9":            "TVAB",
    "tva reduit":       "TVAB",
    "tva réduit":       "TVAB",
    "tva b":            "TVAB",
    "tva 9%":           "TVAB",
    # TVA exo conv 0%
    "tvac":             "TVAC",
    "tva exo":          "TVAC",
    "tva exo conv":     "TVAC",
    "tva exonéré":      "TVAC",
    "tva exonere":      "TVAC",
    "tva 0":            "TVAC",
    "tva c":            "TVAC",
    # TVA exo légale (TEE/RME) 0%
    "tvad":             "TVAD",
    "tva exo leg":      "TVAD",
    "tva exo légale":   "TVAD",
    "tva d":            "TVAD",
    "tee":              "TVAD",
    "rme":              "TVAD",
}

# Société de référence (XOF) – si la devise correspond, pas de foreignCurrency
_BASE_CURRENCY = "XOF"


# ═══════════════════════════════════════════════════════════════════════════════
# RESOLVERS
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_payment_method(src, fallback: str = "cash") -> str:
    """mode_of_payment ERPNext → valeur FNE (cash/card/check/mobile-money/transfer/deferred)."""
    raw = getattr(src, "mode_of_payment", None) or ""
    return _PAYMENT_METHOD_MAP.get(raw.strip().lower(), fallback)


def _resolve_foreign_currency(src) -> dict:
    """
    Utilise les champs natifs ERPNext :
      - currency           → devise de la facture
      - conversion_rate    → taux de conversion vers la devise de base
    Si la devise == XOF (devise de base), foreignCurrency = "" et rate = 0.
    Sinon mappe vers le code ISO FNE.
    """
    erp_currency = (getattr(src, "currency", None) or "").strip()
    fne_currency  = _CURRENCY_MAP.get(erp_currency, erp_currency)

    # Devise de base → pas de conversion étrangère
    if not erp_currency or fne_currency == _BASE_CURRENCY:
        return {"foreignCurrency": "", "foreignCurrencyRate": 0}

    rate = float(getattr(src, "conversion_rate", 0) or 0)
    return {
        "foreignCurrency":     fne_currency,
        "foreignCurrencyRate": rate if rate else 0,
    }



def _resolve_tax_code(account_head: str) -> str:
    """
    Résout le code TVA FNE depuis le nom du compte ERPNext.
    Cherche une correspondance dans _TAX_ACCOUNT_MAP (insensible à la casse).
    Fallback = "TVA" (TVA normale 18%).
    """
    if not account_head:
        return "TVA"
    key = account_head.strip().lower()
    # Cherche d'abord une correspondance exacte
    if key in _TAX_ACCOUNT_MAP:
        return _TAX_ACCOUNT_MAP[key]
    # Cherche si la clé contient un fragment connu
    for fragment, code in _TAX_ACCOUNT_MAP.items():
        if fragment in key:
            return code
    return "TVA"


def _get_item_tax_code(erp_row, doc_taxes) -> str:
    """
    Détermine le code TVA FNE pour une ligne d'article.
    Priorité :
      1. item_tax_template de la ligne
      2. Parcours des taxes du document pour trouver le compte associé
      3. Fallback "TVA"
    """
    # 1. item_tax_template
    item_tax_tpl = getattr(erp_row, "item_tax_template", None) or ""
    if item_tax_tpl:
        code = _resolve_tax_code(item_tax_tpl)
        if code:
            return code

    # 2. Premier compte de taxe du document
    for tax_row in (doc_taxes or []):
        account = getattr(tax_row, "account_head", "") or ""
        code = _resolve_tax_code(account)
        if code:
            return code

    return "TVA"


def _maybe_add_client_ncc(payload: dict, ncc_value: str, template: str):
    """clientNcc : O si B2B, omis sinon (sauf si valeur présente)."""
    if template == "B2B":
        payload["clientNcc"] = ncc_value or ""
    elif ncc_value:
        payload["clientNcc"] = ncc_value


def _resolve_measurement_unit(erp_row) -> str:
    """Unité de mesure : stock_uom ou uom de la ligne."""
    return (
        getattr(erp_row, "uom", None)
        or getattr(erp_row, "stock_uom", None)
        or ""
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ITEM BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def build_items_sale(src) -> List[dict]:
    """
    Construit le tableau items pour une facture de vente (API #1).
    Champs obligatoires : taxes (O), description (O), quantity (O), amount (O)
    Champs optionnels  : customTaxes (N), reference (N), discount (N), measurementUnit (N)
    """
    doc_taxes = getattr(src, "taxes", []) or []
    doc_custom_taxes = _resolve_custom_taxes_per_item(src)

    items = []
    for row in (src.items or []):
        tax_code = _get_item_tax_code(row, doc_taxes)

        item: Dict[str, Any] = {
            # Obligatoires
            "taxes":       [tax_code],
            "description": row.item_name or row.description or row.item_code or "",
            "quantity":    float(abs(row.qty)),
            "amount":      float(row.rate),
        }

        # Optionnels
        if row.item_code:
            item["reference"] = row.item_code

        discount = float(getattr(row, "discount_percentage", 0) or 0)
        if discount:
            item["discount"] = discount

        unit = _resolve_measurement_unit(row)
        if unit:
            item["measurementUnit"] = unit

        # customTaxes par item (N) – name (O si non vide), amount (O si non vide)
        row_custom_taxes = doc_custom_taxes.get(row.name, [])
        if row_custom_taxes:
            item["customTaxes"] = row_custom_taxes

        items.append(item)
    return items


def build_items_purchase(src) -> List[dict]:
    """
    Construit le tableau items pour un bordereau d'achat (API #3).
    Pas de champ taxes pour les purchases selon la spec.
    Champs obligatoires : description (O), quantity (O), amount (O)
    Champs optionnels  : reference (N), discount (N), measurementUnit (N)
    """
    items = []
    for row in (src.items or []):
        item: Dict[str, Any] = {
            # Obligatoires
            "description": row.item_name or row.description or row.item_code or "",
            "quantity":    float(abs(row.qty)),
            "amount":      float(row.rate),
        }

        # Optionnels
        if row.item_code:
            item["reference"] = row.item_code

        discount = float(getattr(row, "discount_percentage", 0) or 0)
        if discount:
            item["discount"] = discount

        unit = _resolve_measurement_unit(row)
        if unit:
            item["measurementUnit"] = unit

        items.append(item)
    return items


def _resolve_custom_taxes_per_item(src) -> Dict[str, list]:
    """
    Retourne un dict {erp_row_name: [{name, amount}]} pour les taxes personnalisées
    par ligne d'article (lu depuis custom_item_taxes si présent).
    """
    result: Dict[str, list] = {}
    for row in (getattr(src, "custom_item_taxes", None) or []):
        ref = getattr(row, "item_row", None) or ""
        name = getattr(row, "tax_name", None) or getattr(row, "name", None) or ""
        amount = float(getattr(row, "tax_rate", 0) or getattr(row, "amount", 0) or 0)
        if ref and name:
            result.setdefault(ref, []).append({"name": name, "amount": amount})
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# BASE PAYLOAD BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_is_rne(src) -> dict:
    """
    Résout les champs isRne / rne (lien vers un RNE) depuis les custom fields ERPNext.
    isRne est OBLIGATOIRE selon la spec DGI.
    """
    is_rne = bool(getattr(src, "custom_is_rne", False))
    result: Dict[str, Any] = {"isRne": is_rne}
    if is_rne:
        result["rne"] = getattr(src, "custom_rne_number", None) or ""
    return result


def _resolve_template_b2f(base_template: str, src) -> str:
    """
    Surcharge le template en B2F si la facture utilise une devise étrangère
    (spec DGI : foreignCurrency obligatoire pour B2F).
    """
    currency_info = _resolve_foreign_currency(src)
    if currency_info.get("foreignCurrency"):
        return "B2F"
    return base_template


def _build_base_payload(src, est: str, pos: str, s) -> Dict[str, Any]:
    """
    Socle commun à tous les types de factures.
    Tous les champs sont résolus automatiquement depuis ERPNext.
    """
    currency_info = _resolve_foreign_currency(src)

    payload: Dict[str, Any] = {
        # ── Obligatoires ──────────────────────────────────────────────────────
        "establishment":     est,                                         # O
        "pointOfSale":       pos,                                         # O
        "template":          "B2C",   # surchargé par l'appelant          # O
        "clientCompanyName": "",      # surchargé par l'appelant          # O
        "clientPhone":       "",      # surchargé par l'appelant          # O
        "clientEmail":       "",      # surchargé par l'appelant          # O
        "items":             [],      # surchargé par l'appelant          # O
        # isRne (O) — lien vers RNE, obligatoire selon spec DGI
        **_resolve_is_rne(src),
        # foreignCurrency (N) + foreignCurrencyRate (O si currency non vide, 0 sinon)
        **currency_info,
        # ── Optionnels ────────────────────────────────────────────────────────
        "paymentMethod":     _resolve_payment_method(
                                src, fallback=s.payment_method_default or "cash"),
        "commercialMessage": (
            getattr(src, "custom_commercial_message", None)
            or s.default_commercial_message or ""
        ),
        "footer": (
            getattr(src, "custom_footer", None)
            or s.default_footer or ""
        ),
        "description": getattr(src, "custom_fne_description", None) or "",
        # discount (N) sur le total HT — champ natif ERPNext
        "discount": float(getattr(src, "additional_discount_percentage", 0) or 0),
    }
    return payload


# ═══════════════════════════════════════════════════════════════════════════════
# ENQUEUE
# ═══════════════════════════════════════════════════════════════════════════════

def enqueue_certification(
    doctype: str,
    docname: str,
    fne_type: str,
    fne_docname: Optional[str] = None,
    force: bool = False,
):
    frappe.enqueue(
        "fne.jobs.certify_document_job.run",
        queue="default",
        job_id=f"fne:certify:{doctype}:{docname}:{fne_type}",
        deduplicate=True,
        enqueue_after_commit=True,
        doctype=doctype,
        docname=docname,
        fne_type=fne_type,
        fne_docname=fne_docname,
        force=force,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FNE DOCUMENT LOADER
# ═══════════════════════════════════════════════════════════════════════════════

def _load_fne_doc(doctype, docname, fne_type, fne_docname):
    if fne_docname:
        try:
            return frappe.get_doc("FNE Document", fne_docname)
        except DoesNotExistError:
            fne_docname = None

    name = frappe.db.get_value(
        "FNE Document",
        {"reference_doctype": doctype, "reference_name": docname, "fne_invoice_type": fne_type},
        "name",
    )
    if not name:
        frappe.throw(f"FNE Document introuvable pour {doctype} {docname} ({fne_type})")
    return frappe.get_doc("FNE Document", name)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN JOB
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    doctype: str,
    docname: str,
    fne_type: str,
    fne_docname: Optional[str] = None,
    force: bool = False,
):
    require_fne_enabled()

    fne_doc = _load_fne_doc(doctype, docname, fne_type, fne_docname)

    # Idempotence locale
    if not force and fne_doc.fne_invoice_id and fne_doc.status in (STATUS_CERTIFIED, STATUS_PDF_PENDING):
        return

    src = frappe.get_doc(doctype, docname)
    est, pos = resolve_establishment_pos(src)
    s = frappe.get_cached_doc("FNE Settings")

    # ── SALE ──────────────────────────────────────────────────────────────────
    if doctype == "Sales Invoice" and fne_type == "sale":
        customer = _get_customer(src)
        template  = _resolve_template_b2f(resolve_template(customer), src)
        seller    = getattr(src, "owner", None) or getattr(src, "modified_by", None)

        payload = _build_base_payload(src, est, pos, s)
        payload.update({
            "invoiceType":       "sale",                                        # O
            "template":          template,                                      # O
            "clientCompanyName": src.customer_name or customer.customer_name,  # O
            "clientPhone":       getattr(customer, "mobile_no",  "") or "",    # O
            "clientEmail":       getattr(customer, "email_id",   "") or "",    # O
            "clientSellerName":  get_fullname(seller) if seller else "",       # N
            "items":             build_items_sale(src),                        # O
        })
        # customTaxes globaux (N) – omis si vide (l'API rejette un array vide)
        global_custom_taxes = resolve_custom_taxes_global(src)
        if global_custom_taxes:
            payload["customTaxes"] = global_custom_taxes

        # clientNcc : O si B2B
        ncc = resolve_client_ncc(customer, template)
        _maybe_add_client_ncc(payload, ncc or "", template)

        _persist_request_payload(fne_doc.name, payload)

        try:
            data = post("/external/invoices/sign", payload)
        except FNEApiError as e:
            _handle_error(fne_doc, e)
            return

        _persist_success_sign(fne_doc, src, data)
        enqueue_pdf_fetch(fne_doc.name)

    # ── PURCHASE ──────────────────────────────────────────────────────────────
    elif doctype == "Purchase Invoice" and fne_type == "purchase":
        supplier = _get_supplier(src)
        template  = _resolve_template_b2f(resolve_template(supplier), src)
        seller    = getattr(src, "owner", None) or getattr(src, "modified_by", None)

        payload = _build_base_payload(src, est, pos, s)
        payload.update({
            "invoiceType":       "purchase",                                   # O
            "template":          template,                                     # O
            "clientCompanyName": supplier.supplier_name,                       # O
            "clientPhone":       getattr(supplier, "mobile_no", "") or "",     # O
            "clientEmail":       getattr(supplier, "email_id",  "") or "",     # O
            "clientSellerName":  get_fullname(seller) if seller else "",       # N
            "items":             build_items_purchase(src),                    # O
            # Pas de taxes ni customTaxes globaux pour les bordereaux d'achat
        })

        # clientNcc : O si B2B (tax_id fournisseur)
        _maybe_add_client_ncc(payload, supplier.tax_id or "", template)

        _persist_request_payload(fne_doc.name, payload)

        try:
            data = post("/external/invoices/sign", payload)
        except FNEApiError as e:
            _handle_error(fne_doc, e)
            return

        _persist_success_sign(fne_doc, src, data)
        enqueue_pdf_fetch(fne_doc.name)

    # ── REFUND ────────────────────────────────────────────────────────────────
    elif fne_type == "refund":
        if not getattr(src, "return_against", None):
            _fail_fne_doc(fne_doc, "Return invoice missing return_against")
            return
        
        orig = frappe.get_doc(doctype, src.return_against)

        if not orig.custom_fne_document:
            _fail_fne_doc(fne_doc, f"Original invoice not certified (FNE Document not found) {fne_docname}")
            return

        orig_fne = frappe.get_doc("FNE Document", orig.custom_fne_document)

        if not orig_fne.fne_invoice_id:
            _fail_fne_doc(fne_doc, "Original FNE invoice id missing")
            return

        # Refund payload : items avec id (O) + quantity (O)
        payload = {"items": _build_refund_items(src, orig_fne)}

        _persist_request_payload(fne_doc.name, payload)

        try:
            data = post(f"/external/invoices/{orig_fne.fne_invoice_id}/refund", payload)
        except FNEApiError as e:
            _handle_error(fne_doc, e)
            return

        _persist_success_refund(fne_doc, src, orig_fne, data)
        enqueue_pdf_fetch(fne_doc.name)

    else:
        _fail_fne_doc(fne_doc, f"Unsupported: {doctype} / {fne_type}")


# ═══════════════════════════════════════════════════════════════════════════════
# REFUND ITEM MAPPER
# ═══════════════════════════════════════════════════════════════════════════════

def _build_refund_items(return_si, orig_fne_doc) -> list:
    map_rows   = list(orig_fne_doc.get("items_map") or [])
    by_erp_row = {r.erp_row_name: r.fne_item_id for r in map_rows if r.erp_row_name and r.fne_item_id}

    fifo: Dict[str, list] = {}
    for r in map_rows:
        if r.item_code and r.fne_item_id:
            fifo.setdefault(r.item_code, []).append(r.fne_item_id)

    out = []
    for row in return_si.items:
        link_field  = getattr(row, "sales_invoice_item", None) or getattr(row, "si_detail", None)
        fne_item_id = by_erp_row.get(link_field) if link_field else None

        if not fne_item_id and row.item_code in fifo and fifo[row.item_code]:
            fne_item_id = fifo[row.item_code].pop(0)

        if not fne_item_id:
            frappe.throw(
                f"Impossible de mapper l'item retour {row.item_code} vers un item FNE (facture d'origine)."
            )

        out.append({
            "id":       fne_item_id,           # O
            "quantity": float(abs(row.qty)),    # O
        })
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# PERSIST HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _fail_fne_doc(fne_doc, error: str):
    """Business-logic failures that retrying cannot fix → DEAD immediately."""
    fne_doc.status     = STATUS_DEAD
    fne_doc.last_error = error
    fne_doc.save(ignore_permissions=True)
    try:
        from fne.services.notifications import notify_dead_document
        notify_dead_document(fne_doc.name, fne_doc.reference_doctype, fne_doc.reference_name, error)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "FNE dead-letter notification failed")


def _persist_request_payload(fne_doc_name: str, payload: dict):
    frappe.db.set_value(
        "FNE Document",
        fne_doc_name,
        "request_payload_json",
        json_dumps(payload),
        update_modified=True,
    )
    frappe.db.commit()


def _persist_success_sign(fne_doc, src, data: dict):
    inv       = data.get("invoice") or {}
    inv_items = inv.get("items") or []

    item_rows   = []
    erp_updates = []

    for idx, erp_row in enumerate(src.items):
        if idx < len(inv_items) and inv_items[idx].get("id"):
            fne_item_id = inv_items[idx]["id"]
            item_rows.append({
                "erp_row_name": erp_row.name,
                "item_code":    erp_row.item_code,
                "fne_item_id":  fne_item_id,
                "quantity":     float(abs(erp_row.qty)),
                "amount":       float(erp_row.rate),
            })
            erp_updates.append((erp_row.doctype, erp_row.name, "custom_fne_item_id", fne_item_id))

    updates = {
        "response_json":   json_dumps(data),
        "fne_reference":   data.get("reference"),
        "token_url":       data.get("token"),
        "warning":         1 if data.get("warning") else 0,
        "balance_sticker": data.get("balance_sticker") or data.get("balance_funds"),
        "fne_invoice_id":  inv.get("id"),
        "status":          STATUS_CERTIFIED,
        "certified_at":    now_utc(),
    }

    _db_write_with_retry(fne_doc.name, updates, item_rows)

    for dt, name, field, value in erp_updates:
        frappe.db.set_value(dt, name, field, value, update_modified=False)
    frappe.db.commit()

    _handle_sticker_warning(data)


def _persist_success_refund(fne_doc, return_si, orig_fne_doc, data: dict):
    updates = {
        "response_json":   json_dumps(data),
        "fne_reference":   data.get("reference"),
        "token_url":       data.get("token"),
        "warning":         1 if data.get("warning") else 0,
        "balance_sticker": data.get("balance_sticker") or data.get("balance_funds"),
        "status":          STATUS_CERTIFIED,
        "certified_at":    now_utc(),
    }

    _db_write_with_retry(fne_doc.name, updates)

    try:
        return_si.db_set("custom_fne_document", fne_doc.name,     update_modified=False)
        return_si.db_set("custom_fne_status",   STATUS_CERTIFIED,  update_modified=False)
    except Exception:
        pass

    _handle_sticker_warning(data)


def _db_write_with_retry(fne_doc_name: str, updates: dict, item_rows: Optional[list] = None):
    """Écrit le FNE Document avec lock optimiste + retry sur deadlock / timestamp mismatch."""
    for attempt in range(5):
        try:
            frappe.db.rollback()
            locked = frappe.get_doc("FNE Document", fne_doc_name, for_update=True)

            for k, v in updates.items():
                setattr(locked, k, v)

            if item_rows is not None:
                locked.set("items_map", [])
                for r in item_rows:
                    locked.append("items_map", r)

            locked.save(ignore_permissions=True, ignore_version=True)
            frappe.db.commit()
            return

        except (QueryDeadlockError, TimestampMismatchError):
            frappe.db.rollback()
            time.sleep(min(2 ** attempt, 8))

    raise RuntimeError(f"Impossible de sauvegarder FNE Document {fne_doc_name} après 5 tentatives.")


# ═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

def _handle_error(fne_doc, e: FNEApiError):
    attempts    = int(fne_doc.attempts or 0) + 1
    status_code = e.status_code or 0
    is_retriable = status_code in RETRIABLE_HTTP

    error_msg = f"[HTTP {status_code}] {e}" if status_code else str(e)

    updates = {
        "attempts":      attempts,
        "last_error":    error_msg,
        "response_json": json_dumps(e.payload or {}),
        "status":        STATUS_FAILED if is_retriable else STATUS_DEAD,
    }

    if is_retriable:
        delay = exp_backoff_seconds(attempts, base=30, cap=3600)
        updates["next_retry_at"] = add_to_date(now_utc(), seconds=delay)
    else:
        # Functional HTTP error → notify dead-letter immediately
        try:
            from fne.services.notifications import notify_dead_document
            notify_dead_document(
                fne_doc.name, fne_doc.reference_doctype, fne_doc.reference_name, error_msg
            )
        except Exception:
            frappe.log_error(frappe.get_traceback(), "FNE dead-letter notification failed")

    _db_write_with_retry(fne_doc.name, updates)


# ═══════════════════════════════════════════════════════════════════════════════
# STICKER WARNING
# ═══════════════════════════════════════════════════════════════════════════════

def _handle_sticker_warning(data: dict):
    s         = frappe.get_cached_doc("FNE Settings")
    warning   = bool(data.get("warning"))
    balance   = data.get("balance_sticker") or data.get("balance_funds")
    threshold = int(s.sticker_warning_threshold or 0)

    if warning or (balance is not None and threshold and int(balance) <= threshold):
        from fne.services.notifications import notify_sticker_low
        notify_sticker_low(balance=balance, warning=warning)

        if s.block_on_sticker_warning:
            frappe.throw("Stock sticker faible/alerte FNE : certification bloquée (FNE Settings).")