// fne/public/js/sales_invoice.js

frappe.ui.form.on("Sales Invoice", {
    refresh: async function (frm) {
        // N'afficher les boutons FNE que pour les documents soumis (docstatus = 1)
        if (frm.doc.docstatus != 1) return;

        // 1. Nettoyer les anciens boutons FNE pour éviter les doublons
        ["FNE: Statut", "FNE: Certifier", "FNE: Récupérer PDF"].forEach(label => {
            frm.remove_custom_button(label);
        });

        // 2. Ajouter le bouton "Statut" (toujours visible)
        frm.add_custom_button(__("FNE: Statut"), () => showStatusDialog(frm));

        // 3. Récupérer le dernier document FNE et afficher le bon bouton
        const latestFNE = await getLatestFNEStatus(frm);

        if (!latestFNE || !latestFNE.token_url) {
            addCertifyButton(frm);
        } else {
          if (!latestFNE.pdf_file){
            addFetchPDFButton(frm, latestFNE.name);
          }
        }
    }
});

// ---------- Fonctions utilitaires ----------

async function getLatestFNEStatus(frm) {
    try {
        const res = await frappe.call({
            method: "fne.api.public.get_fne_status",
            args: {
                doctype: "Sales Invoice",
                docname: frm.doc.name
            }
        });
        const rows = res.message || [];
        // Suppose que le tableau est trié du plus récent au plus ancien
        return rows.length ? rows[0] : null;
    } catch (e) {
        console.error("Erreur lors de la récupération du statut FNE", e);
        return null;
    }
}

function addCertifyButton(frm) {
    frm.add_custom_button(__("FNE: Certifier"), async () => {
        const fne_type = frm.doc.is_return ? "refund" : "sale";
        try {
            await frappe.call({
                method: "fne.api.public.certify_document",
                args: {
                    doctype: "Sales Invoice",
                    docname: frm.doc.name,
                    fne_type: fne_type
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

function showStatusDialog(frm) {
    frappe.call({
        method: "fne.api.public.get_fne_status",
        args: {
            doctype: "Sales Invoice",
            docname: frm.doc.name
        }
    }).then(r => {
        const rows = r.message || [];
        const html = rows.map(x => `
            <div style="margin-bottom:8px">
                <b>${x.fne_invoice_type}</b> - ${x.status}<br/>
                Ref: ${x.fne_reference || "-"}<br/>
                Token: ${x.token_url ? `<a href="${x.token_url}" target="_blank">${x.token_url}</a>` : "-"}<br/>
                PDF: ${x.pdf_file ? `<a href="${x.pdf_file}" target="_blank">Télécharger</a>` : "-"}<br/>
                Err: ${x.last_error || "-"}
            </div>
        `).join("");
        frappe.msgprint({
            title: __("Statut FNE"),
            message: html,
            indicator: "blue"
        });
    });
}