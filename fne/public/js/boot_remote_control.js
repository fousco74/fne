// fne/public/js/boot_remote_control.js
/*
frappe.ready(() => {
  // Run only in Desk
  if (!frappe.session || !frappe.session.user) return;

  frappe.call({
    method: "fne.api.remote_control.get_remote_status",
    args: {},
    callback: (r) => {
      const st = r.message;
      if (!st) return;

      // if disabled: popup + block FNE buttons via global flag
      if (st.enforced && st.enabled === false) {
        frappe.boot.fne_disabled = true;
        frappe.msgprint({
          title: __("FNE désactivé"),
          message: __("FNE est désactivé à distance (PROD). Les actions de certification sont bloquées."),
          indicator: "red",
        });
      }
    },
  });
});

*/
