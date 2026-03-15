// fne/public/js/sales_invoice.js
;(function () {

    frappe.ui.form.on("Sales Invoice", {
        refresh: async function (frm) {
            if (frm.doc.docstatus != 1) return;

            ["FNE: Statut", "FNE: Certifier", "FNE: Récupérer PDF"].forEach(label => {
                frm.remove_custom_button(label);
            });

            frm.add_custom_button(__("FNE: Statut"), () => showStatusDialog(frm));

            const latestFNE = await getLatestFNEStatus(frm);

            if (!latestFNE || !latestFNE.token_url) {
                addCertifyButton(frm);
            } else if (!latestFNE.pdf_file) {
                addFetchPDFButton(frm, latestFNE.name);
            }
        }
    });

    // ---------- Fonctions utilitaires ----------

    async function getLatestFNEStatus(frm) {
        try {
            const res = await frappe.call({
                method: "fne.api.public.get_fne_status",
                args: { doctype: "Sales Invoice", docname: frm.doc.name }
            });
            const rows = res.message || [];
            return rows.length ? rows[0] : null;
        } catch (e) {
            console.error("FNE sales_invoice: erreur récupération statut", e);
            return null;
        }
    }

    function addCertifyButton(frm) {
        frm.add_custom_button(__("FNE: Certifier"), async () => {
            try {
                await frappe.call({
                    method: "fne.api.public.certify_document",
                    args: {
                        doctype: "Sales Invoice",
                        docname: frm.doc.name,
                        fne_type: frm.doc.is_return ? "refund" : "sale"
                    }
                });
                frm.reload_doc();
            } catch (e) {
                frappe.msgprint(__("Erreur lors de la certification."));
            }
        });
    }

    function addFetchPDFButton(frm, fneDocumentName) {
        frm.add_custom_button(__("FNE: Récupérer PDF"), async () => {
            try {
                await frappe.call({
                    method: "fne.api.public.fetch_pdf",
                    args: { fne_document: fneDocumentName }
                });
                frm.reload_doc();
            } catch (e) {
                frappe.msgprint(__("Erreur lors de la récupération du PDF."));
            }
        });
    }

    async function showStatusDialog(frm) {
        try {
            const r = await frappe.call({
                method: "fne.api.public.get_fne_status",
                args: { doctype: "Sales Invoice", docname: frm.doc.name }
            });
            const rows = r.message || [];
            if (!rows.length) {
                frappe.msgprint({ title: __("Statut FNE"), message: __("Aucun journal FNE trouvé."), indicator: "orange" });
                return;
            }
            const html = rows.map(x => `
                <div style="margin-bottom:8px">
                    <b>${x.fne_invoice_type}</b> — ${x.status}<br/>
                    Ref: ${x.fne_reference || "-"}<br/>
                    Token: ${x.token_url ? `<a href="${x.token_url}" target="_blank">${x.token_url}</a>` : "-"}<br/>
                    PDF: ${x.pdf_file ? `<a href="${x.pdf_file}" target="_blank">Télécharger</a>` : "-"}<br/>
                    Err: ${x.last_error || "-"}
                </div>
            `).join("");
            frappe.msgprint({ title: __("Statut FNE"), message: html, indicator: "blue" });
        } catch (e) {
            frappe.msgprint(__("Impossible de récupérer le statut FNE."));
        }
    }

})();
