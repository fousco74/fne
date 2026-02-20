# fne/services/mapping.py
from __future__ import annotations
from typing import Dict, Any, Tuple, List, Optional
import frappe
from typing import Tuple

def _get_customer(doc) -> frappe.model.document.Document:
    data = frappe.get_doc("Customer", doc.customer)
    if not data.email_id:
        data.email_id = frappe.get_value('Address',data.customer_primary_address, 'email_id') or frappe.get_value('Address',data.customer_primary_contact, 'email_id')
    
    if not data.mobile_no:
        data.mobile_no = frappe.get_value('Address',data.customer_primary_address, 'phone') or frappe.get_value('Address',data.customer_primary_contact, 'phone')
    return data

def _get_supplier(doc) -> frappe.model.document.Document:
    data = frappe.get_doc("Supplier", doc.supplier)
    if not data.email_id:
        data.email_id = frappe.get_value('Address',data.supplier_primary_address, 'email_id') or frappe.get_value('Address',data.supplier_primary_contact, 'email_id')
    
    if not data.mobile_no:
        data.mobile_no = frappe.get_value('Address',data.supplier_primary_address, 'phone') or frappe.get_value('Address',data.supplier_primary_contact, 'phone')

    return data

def _field(doc, *names, default=None):
    for n in names:
        if hasattr(doc, n):
            v = doc.get(n)
            if v not in (None, ""):
                return v
    return default

def resolve_template(customer) -> str:
    return _field(customer, "custom_template", "custom_fne_template", default="B2C")

def resolve_client_ncc(customer, template: str) -> Optional[str]:
    return _field(customer, "tax_id", "custom_ncc") or ""



def resolve_establishment_pos(erp_doc=None) -> Tuple[str, str]:
    """
    Resolve establishment (company) and point of sale (POS)

    Priority:
    - Company: erp_doc.company -> FNE Settings.default_company
    - POS:
        if FNE Settings.use_pos:
            erp_doc.pos_profile -> FNE Settings.default_pos_profile
        else:
            empty string
    """

    s = frappe.get_cached_doc("FNE Settings")

    # --- Establishment ---
    company = None
    if erp_doc and getattr(erp_doc, "company", None):
        company = erp_doc.company
    else:
        company = s.default_company

    if not company:
        frappe.throw("Aucune société définie pour déterminer l'établissement FNE.")

    est = frappe.get_value("Company", company, "name")

    # --- POS ---
    pos = ""

    if s.use_pos:
        if erp_doc and getattr(erp_doc, "pos_profile", None):
            pos = erp_doc.pos_profile
            
    pos = s.standard_pos
    
    if not pos:
        frappe.throw("POS requis mais aucun POS Profile défini.")
    return est, pos


def build_items_sale(doc) -> List[Dict[str, Any]]:
    items = []
    for row in doc.items:
        item = frappe.get_cached_doc("Item", row.item_code)
        items.append({
            "taxes": resolve_taxes_sale(doc, row),
            "customTaxes": resolve_custom_taxes_item(doc, row),
            "reference": row.item_code,
            "description": row.description or row.item_name or row.item_code,
            "quantity": float(abs(row.qty)),
            "amount": float(row.rate),
            "discount": float(row.discount_percentage or 0) if hasattr(row, "discount_percentage") else 0,
            "measurementUnit": row.uom,
        })
    return items

def build_items_purchase(doc) -> List[Dict[str, Any]]:
    items = []
    for row in doc.items:
        items.append({
            "reference": row.item_code,
            "description": row.description or row.item_name or row.item_code,
            "quantity": float(abs(row.qty)),
            "amount": float(row.rate),
            "discount": float(getattr(row, "discount_percentage", 0) or 0),
            "measurementUnit": row.uom,
        })
    return items

def resolve_taxes_sale(doc, row) -> List[str]:
    """SIMPLE mode: map effective tax % to TVA codes."""
    s = frappe.get_cached_doc("FNE Settings")
    if s.tax_mapping_mode == "MANUAL":
        return [s.default_vat_code or "TVA"]

    # Attempt: compute total tax rate on row from doc.taxes
    # This is approximative: good enough for v1; integrator can override.
    rate = 0.0
    for t in getattr(doc, "taxes", []) or []:
        if getattr(t, "charge_type", "") in ("On Net Total", "On Previous Row Amount", "On Previous Row Total"):
            try:
                rate += float(t.rate or 0)
            except Exception:
                pass

    company = frappe.get_cached_doc("Company", doc.company)
    regime = (company.get("custom_regime_dimposition") or "").upper()

    if abs(rate - 18.0) < 0.01:
        return ["TVA"]
    if abs(rate - 9.0) < 0.01:
        return ["TVAB"]
    if abs(rate - 0.0) < 0.01:
        # choose TVAD for RME/TEE else TVAC
        return ["TVAD" if "RME" in regime else "TVAC"]

    return [s.default_vat_code or "TVA"]

def resolve_custom_taxes_item(doc, row) -> List[Dict[str, Any]]:
    # Placeholder: extend with mapping table if needed
    return []

def resolve_custom_taxes_global(doc) -> List[Dict[str, Any]]:
    return []
