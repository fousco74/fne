frappe.realtime.on("fne_toast", (data) => {
  if (!data) return;
  frappe.show_alert(
    { message: `${data.title || "FNE"}: ${data.message || ""}`, indicator: data.indicator || "orange" },
    10
  );
});
