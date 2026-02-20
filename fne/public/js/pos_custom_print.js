frappe.provide("erpnext.PointOfSale");

// ------------------------
// 1) Helper print PDF
// ------------------------
function print_pdf_url(file_url) {
  const url = frappe.urllib.get_full_url(file_url);

  const iframe = document.createElement("iframe");
  iframe.style.position = "fixed";
  iframe.style.right = "0";
  iframe.style.bottom = "0";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  iframe.src = url;

  iframe.onload = () => {
    try {
      iframe.contentWindow.focus();
      iframe.contentWindow.print();
    } finally {
      setTimeout(() => iframe.remove(), 2000);
    }
  };

  document.body.appendChild(iframe);
}

// ------------------------
// 2) UI Loader + Polling
// ------------------------
const FNE = {
  poll_interval_ms: 2000,
  max_wait_ms: 120000,        // 2 min
  docname_max_wait_ms: 30000, // 30s pour détecter docname après submit

  _poller: null,
  _doc_timer: null,
  _in_flight: false,
  _loading: false,
  _observer: null,

  selectors: {
    complete_btn: ".submit-order-btn",
    summary_print_btn: ".summary-btns .print-btn",
    summary_btns_wrap: ".summary-btns",
    new_order_btn: ".summary-btns .new-btn",
  },

  loader_html(label) {
    return `
      <div class="fne-waiting" style="display:flex;align-items:center;gap:8px;padding:8px 10px;border:1px solid var(--border-color,#d1d8dd);border-radius:6px;margin-bottom:6px;">
        <i class="fa fa-spinner fa-spin" aria-hidden="true"></i>
        <span>${label || __("Certification FNE en cours...")}</span>
      </div>
    `;
  },

  // Observe DOM changes because POS injects summary buttons AFTER submit
  start_dom_guard() {
    if (this._observer) return;

    const target =
      (window.cur_pos && cur_pos.wrapper && cur_pos.wrapper.get(0)) ||
      document.body;

    this._observer = new MutationObserver(() => {
      // Tant qu’on est en attente, on ré-applique le hide/loader
      // dès que le DOM bouge (ex: apparition de Print Receipt après validation)
      if (this._loading) this.enter_loading();
    });

    this._observer.observe(target, { childList: true, subtree: true });
  },

  stop_dom_guard() {
    if (this._observer) {
      this._observer.disconnect();
      this._observer = null;
    }
  },

  enter_loading() {
    if (this._loading) {
      // déjà en mode loading, mais on doit quand même re-hide si boutons ajoutés après
      // donc on ne return pas trop tôt
    } else {
      this._loading = true;
    }

    // 1) Complete Order: hide + loader after it
    const $complete = $(this.selectors.complete_btn).first();
    if ($complete.length) {
      $complete.hide();
      if (!$complete.next(".fne-waiting").length) {
        $(this.loader_html(__("Certification FNE en cours..."))).insertAfter($complete);
      }
    }

    // 2) Print Receipt: hide + loader in summary area
    const $print = $(this.selectors.summary_print_btn).first();
    if ($print.length) {
      $print.hide();
      const $wrap = $print.closest(this.selectors.summary_btns_wrap);
      if ($wrap.length && !$wrap.find(".fne-waiting").length) {
        $wrap.prepend(this.loader_html(__("Reçu certifié en génération...")));
      }
    }
  },

  exit_loading() {
    this._loading = false;
    this.stop_dom_guard();

    $(".fne-waiting").remove();

    // Restore buttons if present
    const $complete = $(this.selectors.complete_btn).first();
    if ($complete.length) $complete.show();

    const $print = $(this.selectors.summary_print_btn).first();
    if ($print.length) $print.show();
  },

  clear_timers_only() {
    if (this._poller) clearInterval(this._poller);
    if (this._doc_timer) clearInterval(this._doc_timer);
    this._poller = null;
    this._doc_timer = null;
    this._in_flight = false;
  },

  stop_all() {
    this.clear_timers_only();
    this.exit_loading();
  },

  get_current_docname() {
    return (
      (window.cur_pos &&
        cur_pos.order_summary &&
        cur_pos.order_summary.doc &&
        cur_pos.order_summary.doc.name) ||
      (window.cur_pos && cur_pos.frm && cur_pos.frm.doc && cur_pos.frm.doc.name) ||
      null
    );
  },

  wait_for_docname_then_poll_pdf() {
    // relance proprement
    this.clear_timers_only();

    this.enter_loading();
    this.start_dom_guard();

    const start = Date.now();
    this._doc_timer = setInterval(() => {
      const docname = this.get_current_docname();

      if (docname) {
        clearInterval(this._doc_timer);
        this._doc_timer = null;
        this.poll_pdf(docname);
        return;
      }

      if (Date.now() - start > this.docname_max_wait_ms) {
        clearInterval(this._doc_timer);
        this._doc_timer = null;
        this.exit_loading();
        frappe.msgprint({
          title: __("Info"),
          message: __("Facture POS non détectée. Réessayez après validation du paiement."),
          indicator: "orange",
        });
      }
    }, 400);
  },

  poll_pdf(docname) {
    const start = Date.now();

    if (this._poller) clearInterval(this._poller);

    this._poller = setInterval(() => {
      if (this._in_flight) return;
      this._in_flight = true;

      frappe.db
        .get_value("POS Invoice", docname, "custom_fne_pdf")
        .then((r) => {
          const file_url = r && r.message && r.message.custom_fne_pdf;

          if (file_url) {
            clearInterval(this._poller);
            this._poller = null;
            this._in_flight = false;

            this.exit_loading();
            frappe.show_alert({ message: __("Reçu certifié disponible."), indicator: "green" });
            return;
          }

          if (Date.now() - start > this.max_wait_ms) {
            clearInterval(this._poller);
            this._poller = null;
            this._in_flight = false;

            this.exit_loading();
            frappe.msgprint({
              title: __("Certification en attente"),
              message: __(
                "Le reçu certifié n’est pas encore prêt. Vous pouvez réessayer d’imprimer dans quelques instants."
              ),
              indicator: "orange",
            });
          } else {
            this._in_flight = false;
          }
        })
        .catch(() => {
          this._in_flight = false;
          // on laisse continuer jusqu'au timeout
        });
    }, this.poll_interval_ms);
  },
};

// ------------------------
// 3) Patch POS once loaded
// ------------------------
frappe.require("point-of-sale.bundle.js", function () {
  const timer = setInterval(() => {
    if (!window.cur_pos || !cur_pos.order_summary || !cur_pos.order_summary.print_receipt) return;

    // éviter patch multiple
    if (cur_pos.__fne_ui_patched) {
      clearInterval(timer);
      return;
    }

    // A) Patch print_receipt
    const original_print = cur_pos.order_summary.print_receipt.bind(cur_pos.order_summary);

    cur_pos.order_summary.print_receipt = function () {
      const docname =
        (this.doc && this.doc.name) ||
        (cur_pos.frm && cur_pos.frm.doc && cur_pos.frm.doc.name);

      if (!docname) {
        frappe.msgprint(__("Facture POS introuvable pour l’impression."));
        return;
      }

      frappe.db
        .get_value("POS Invoice", docname, "custom_fne_pdf")
        .then((r) => {
          const file_url = r && r.message && r.message.custom_fne_pdf;

          if (file_url) {
            print_pdf_url(file_url);
          } else {
            // On attend le PDF certifié (pas de fallback)
            frappe.show_alert({ message: __("Certification FNE en cours..."), indicator: "orange" });
            FNE.wait_for_docname_then_poll_pdf();
          }
        })
        .catch(() => {
          // on évite d'imprimer un reçu non certifié
          frappe.msgprint(__("Impossible de vérifier le reçu certifié. Réessayez."));
        });
    };

    // B) Au clic sur Complete Order => lancer l'attente (sans bloquer la soumission ERPNext)
    $(document).on("click.fne", FNE.selectors.complete_btn, function () {
      FNE.wait_for_docname_then_poll_pdf();
    });

    // C) Si New Order => stop tout (évite loader bloqué)
    $(document).on("click.fne", FNE.selectors.new_order_btn, function () {
      FNE.stop_all();
    });

    // D) Sécurité : si l'écran summary est injecté après submit, le MutationObserver re-hide automatiquement
    // (rien de plus à faire ici)

    cur_pos.__fne_ui_patched = true;
    clearInterval(timer);
  }, 300);
});
