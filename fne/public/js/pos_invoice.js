// fne/public/js/pos_invoice.js

frappe.ui.form.on("POS Invoice", {
    refresh: async function (frm) {
        if (frm.doc.docstatus != 1) return;

        // Nettoyer les anciens boutons pour éviter les doublons
        ["FNE: Statut", "FNE: Certifier", "FNE: Récupérer PDF"].forEach(label => {
            frm.remove_custom_button(label);
        });

        // Bouton "Statut" toujours visible
        frm.add_custom_button(__("FNE: Statut"), () => showPOSStatusDialog(frm));

        const status = frm.doc.custom_fne_status;

        if (status === "FAILED" || status === "DEAD") {
            frm.add_custom_button(__("FNE: Certifier"), async () => {
                try {
                    await frappe.call({
                        method: "fne.api.public.certify_document",
                        args: {
                            doctype: "POS Invoice",
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

        if (status === "CERTIFIED" && !frm.doc.custom_fne_pdf) {
            frm.add_custom_button(__("FNE: Récupérer PDF"), async () => {
                try {
                    const r = await frappe.call({
                        method: "fne.api.public.get_fne_status",
                        args: { doctype: "POS Invoice", docname: frm.doc.name }
                    });
                    const latest = (r.message || [])[0];
                    if (!latest) {
                        frappe.msgprint(__("Aucun journal FNE trouvé."));
                        return;
                    }
                    await frappe.call({
                        method: "fne.api.public.fetch_pdf",
                        args: { fne_document: latest.name }
                    });
                    frm.reload_doc();
                } catch (e) {
                    frappe.msgprint(__("Erreur lors de la récupération du PDF."));
                }
            });
        }
    }
});

async function showPOSStatusDialog(frm) {
    try {
        const r = await frappe.call({
            method: "fne.api.public.get_fne_status",
            args: { doctype: "POS Invoice", docname: frm.doc.name }
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
