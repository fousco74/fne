# fne/services/pdf_fetch.py
from __future__ import annotations
import re
import time
from typing import Optional, Tuple, List
import requests
import frappe
from fne.utils import sha256_bytes, now_utc
from fne.constants import STATUS_PDF_READY, STATUS_PDF_PENDING, STATUS_FAILED

EXPORT_BTN_REGEX = re.compile(r'href="(blob:[^"]+)"', re.IGNORECASE)

def fetch_and_attach_pdf(fne_doc):
    s = frappe.get_cached_doc("FNE Settings")
    if not fne_doc.token_url:
        fne_doc.status = STATUS_FAILED
        fne_doc.last_error = "Missing token_url for PDF fetch"
        fne_doc.save(ignore_permissions=True)
        return

    # 1) Try NETWORK_TRACE
    pdf_bytes = None
    err1 = None
    if s.pdf_fetch_strategy in ("NETWORK_TRACE_FIRST", "NETWORK_TRACE_ONLY"):
        try:
            pdf_bytes = _network_trace_fetch_pdf(fne_doc.token_url, s)
        except Exception as e:
            err1 = str(e)

    # 2) Fallback HEADLESS
    err2 = None
    if not pdf_bytes and s.pdf_fetch_strategy in ("NETWORK_TRACE_FIRST", "HEADLESS_FIRST", "HEADLESS_ONLY"):
        try:
            pdf_bytes = _headless_playwright_fetch_pdf(fne_doc.token_url, s)
        except Exception as e:
            err2 = str(e)

    if not pdf_bytes:
        fne_doc.status = STATUS_FAILED
        fne_doc.last_error = f"PDF fetch failed. network_trace={err1} headless={err2}"
        fne_doc.save(ignore_permissions=True)
        return

    _attach_pdf(fne_doc, pdf_bytes)

def _attach_pdf(fne_doc, pdf_bytes: bytes):
    h = sha256_bytes(pdf_bytes)
    filename = f"FNE-{fne_doc.fne_reference or fne_doc.name}.pdf"

    filedoc = frappe.get_doc({
        "doctype": "File",
        "file_name": filename,
        "attached_to_doctype": fne_doc.reference_doctype,
        "attached_to_name": fne_doc.reference_name,
        "content": pdf_bytes,
        "is_private": 1,
    })
    filedoc.save(ignore_permissions=True)

    fne_doc.pdf_file = filedoc.file_url
    fne_doc.pdf_sha256 = h
    fne_doc.status = STATUS_PDF_READY
    fne_doc.pdf_fetched_at = now_utc()
    fne_doc.save(ignore_permissions=True)

    # push back to ERP doc attach fields
    try:
        src = frappe.get_doc(fne_doc.reference_doctype, fne_doc.reference_name)
        src.db_set("custom_fne_pdf", filedoc.file_url, update_modified=False)
        src.db_set("custom_fne_status", fne_doc.status, update_modified=False)
    except Exception:
        pass

def _network_trace_fetch_pdf(token_url: str, s) -> bytes:
    """
    Attempt to discover real PDF endpoint used by the verification page.
    - If user configured pdf_endpoint_template, try it.
    - Else: autodiscovery by scanning HTML+JS for 'pdf'/'export' endpoints.
    """
    sess = requests.Session()
    sess.headers.update({"User-Agent": "ERPNext-FNE/1.0"})

    # poll if page not ready
    max_wait = int(s.pdf_max_wait_seconds or 25)
    poll = float(s.pdf_poll_interval_seconds or 2)

    html = None
    for _ in range(max(1, int(max_wait / poll))):
        r = sess.get(token_url, timeout=int(s.http_timeout_seconds or 30))
        r.raise_for_status()
        html = r.text
        if "Exporter" in html or "export" in html.lower():
            break
        time.sleep(poll)

    if not html:
        raise RuntimeError("Token page not reachable")

    # If integrator already knows internal endpoint, use it:
    if s.pdf_endpoint_template:
        # Example: "/fr/verification/{uuid}/pdf" or "/api/invoices/{uuid}/export"
        uuid = token_url.rstrip("/").split("/")[-1]
        url = _join_base(token_url, s.pdf_endpoint_template.format(uuid=uuid))
        pdf = _try_get_pdf(sess, url)
        if pdf:
            return pdf

    # Autodiscovery: scan HTML script src, then scan js
    candidates = _extract_candidate_urls_from_html(token_url, html)

    for url in candidates:
        pdf = _try_get_pdf(sess, url)
        if pdf:
            return pdf

    raise RuntimeError("NETWORK_TRACE: no direct PDF endpoint discovered (configure pdf_endpoint_template or use HEADLESS).")

def _join_base(token_url: str, path: str) -> str:
    # token_url example: http://54.247.95.108/fr/verification/<uuid>
    # base = http://54.247.95.108
    m = re.match(r"^(https?://[^/]+)", token_url)
    base = m.group(1) if m else token_url
    if not path.startswith("/"):
        path = "/" + path
    return base + path

def _extract_candidate_urls_from_html(token_url: str, html: str) -> List[str]:
    base = re.match(r"^(https?://[^/]+)", token_url).group(1)
    urls = set()

    # search for explicit links containing pdf/export/download
    for pat in ("pdf", "export", "download", "invoice"):
        for m in re.finditer(rf"""["'](\/[^"']*{pat}[^"']*)["']""", html, re.IGNORECASE):
            urls.add(base + m.group(1))

    # collect script src and scan them
    for m in re.finditer(r"""<script[^>]+src=["']([^"']+)["']""", html, re.IGNORECASE):
        src = m.group(1)
        if src.startswith("/"):
            src = base + src
        try:
            js = requests.get(src, timeout=20).text
            for pat in ("pdf", "export", "download"):
                for mm in re.finditer(rf"""["'](\/[^"']*{pat}[^"']*)["']""", js, re.IGNORECASE):
                    urls.add(base + mm.group(1))
        except Exception:
            continue

    return list(urls)

def _try_get_pdf(sess: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = sess.get(url, timeout=20)
        if r.status_code == 200 and ("application/pdf" in (r.headers.get("Content-Type") or "").lower()):
            return r.content
    except Exception:
        return None
    return None

def _headless_playwright_fetch_pdf(token_url: str, s) -> bytes:
    """
    HEADLESS_FALLBACK:
    open token_url, wait export button, read blob URL, fetch it in-page, return bytes.
    """
    from playwright.sync_api import sync_playwright

    max_wait = int(s.pdf_max_wait_seconds or 25) * 1000

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(accept_downloads=False)
        page = context.new_page()
        page.goto(token_url, wait_until="networkidle", timeout=max_wait)

        # Wait for Exporter button/link
        page.wait_for_selector('a[download]', timeout=max_wait)

        # Ensure href is blob: by forcing click (some apps set href after click)
        try:
            page.click("text=Exporter", timeout=5000)
        except Exception:
            pass

        # Get blob href
        href = page.eval_on_selector('a[download]', "el => el.getAttribute('href')")
        if not href or not href.startswith("blob:"):
            # sometimes href is on <a> wrapping the button
            href = page.eval_on_selector('a[download]', "el => el.href")
        if not href or not str(href).startswith("blob:"):
            raise RuntimeError(f"HEADLESS: Export blob not found (href={href})")

        # Fetch blob inside browser and return base64
        b64 = page.evaluate(
            """async (u) => {
                const res = await fetch(u);
                const blob = await res.blob();
                const ab = await blob.arrayBuffer();
                let binary = '';
                const bytes = new Uint8Array(ab);
                const chunkSize = 0x8000;
                for (let i = 0; i < bytes.length; i += chunkSize) {
                    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize));
                }
                return btoa(binary);
            }""",
            href,
        )

        browser.close()

    import base64
    return base64.b64decode(b64)
