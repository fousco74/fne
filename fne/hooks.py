app_name = "fne"
app_title = "FNE"
app_publisher = "KONE Fousseni"
app_description = "Facture Normalisé Electronique"
app_email = "fkone@amoaman.com"
app_license = "mit"

fixtures = [
	{"dt": "Custom Field",    "filters": [["name", "like", "%-custom_%"]]},
	{"dt": "Property Setter", "filters": [["name", "like", "%FNE%"]]},
	"FNE Settings"
]


# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
add_to_apps_screen = [
	{
		"name": "fne",
		"logo": "/assets/fne/images/fne_logo.png",
		"title": "FNE",
		"route": "/fne/FNE",
	}
]

# Includes in <head>
# ------------------

app_include_js = [
	"/assets/fne/js/fne_common.js",
]

# include js in doctype views
doctype_js = {
	"Sales Invoice":   "public/js/sales_invoice.js",
	"Purchase Invoice": "public/js/purchase_invoice.js",
}

# Home Pages
# ----------

# Document Events
# ---------------
doc_events = {
	"Sales Invoice": {
		"on_submit": "fne.services.certification.on_sales_invoice_submit",
	},
	"Purchase Invoice": {
		"on_submit": "fne.services.certification.on_purchase_invoice_submit",
	},
	# "POS Invoice": {
	#     "on_submit": "fne.services.certification.on_pos_invoice_submit",
	# },
}

# Scheduled Tasks
# ---------------
scheduler_events = {
	"cron": {
		# Retry des certifications FAILED toutes les 10 minutes
		"*/10 * * * *": [
			"fne.jobs.retry_scheduler_job.run_retry_scheduler",
		],
	}
}

# Testing
# -------
# before_tests = "fne.install.before_tests"
