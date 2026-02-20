// fne/public/js/purchase_invoice.js
frappe.ui.form.on("Purchase Invoice", {
  refresh(frm) {


      if(frm.doc.custom_fne_status == "FAILED" || frm.doc.custom_fne_status == "DEAD" || (!frm.doc.custom_fne_status && frm.docstatus ==1)){

        frm.add_custom_button(__("FNE: Statut"), () => {
          frappe.call("fne.api.public.get_fne_status", { doctype: "Purchase Invoice", docname: frm.doc.name })
            .then((r) => {
              const rows = r.message || [];
              const html = rows.map(x =>
                `<div style="margin-bottom:8px">
                  <b>${x.fne_invoice_type}</b> - ${x.status}<br/>
                  Ref: ${x.fne_reference || "-"}<br/>
                  Token: ${x.token_url ? `<a href="${x.token_url}" target="_blank">${x.token_url}</a>` : "-"}<br/>
                  PDF: ${x.pdf_file ? `<a href="${x.pdf_file}" target="_blank">Télécharger</a>` : "-"}<br/>
                  Err: ${x.last_error || "-"}
                </div>`
              ).join("");
              frappe.msgprint({ title: __("Statut FNE"), message: html, indicator: "blue" });
            });
        });

      }

      if(frm.doc.custom_fne_status == "CERTIFIED"){


          frm.add_custom_button(__("FNE: Certifier (Bordereau)"), () => {
            frappe.call("fne.api.public.certify_document", {
              doctype: "Purchase Invoice",
              docname: frm.doc.name,
              fne_type: "purchase",
            }).then(() => frm.reload_doc());
          });

      }   

    frm.add_custom_button(__("FNE: Récupérer PDF"), () => {
      frappe.call("fne.api.public.get_fne_status", { doctype: "Purchase Invoice", docname: frm.doc.name })
        .then((r) => {
          const latest = (r.message || [])[0];
          if (!latest) return frappe.msgprint(__("Aucun journal FNE trouvé."));
          return frappe.call("fne.api.public.fetch_pdf", { fne_document: latest.name });
        })
        .then(() => frm.reload_doc());
    });

  },
  custom_is_agricole(frm){
    if (frm.doc.custom_is_agricole){


      frm.set_query("item_code", "items", function (doc, cdt, cdn){
        return {
          filters :{
            custom_is_agricole : 1
          }
        };
      })
    }else{
      frm.set_query("item_code", "items", function  (doc, cdt, cdn){
        return {
          filters: {}
        };
      })
    }
  }
});
