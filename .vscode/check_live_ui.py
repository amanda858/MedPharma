import urllib.request, urllib.parse, http.cookiejar, json, re
base = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
req = urllib.request.Request(base+"/hub/api/login", data=json.dumps({"username":"admin","password":"admin123"}).encode(), headers={"Content-Type":"application/json"})
op.open(req, timeout=60).read()

# Fetch hub and leads iframe pages
hub = op.open(base+"/hub", timeout=60).read().decode()
print("=== HUB ===")
print("panel-leads active:", 'id="panel-leads"' in hub and re.search(r'class="hub-panel active"\s+id="panel-leads"', hub) is not None)
print("Live Build present:", "Live Build" in hub)
print("Lab Leads present:", "Lab Leads" in hub)
print("Contact Form present:", "Contact Form" in hub)
print("iframe src:", re.findall(r'<iframe[^>]*id="frame-leads"[^>]*src="([^"]*)"', hub))

leads = op.open(base+"/admin/leads/", timeout=60).read().decode()
print("\n=== LEADS IFRAME PAGE ===")
print("len:", len(leads))
print("Live Build present:", "Live Build" in leads)
print("Lab Intelligence present:", "Lab Intelligence" in leads)
print("Contact Form present:", "Contact Form" in leads)
print("scrub form id:", "id=\"scrubFile\"" in leads or "scrubFile" in leads)
print("title:", re.search(r"<title>([^<]*)</title>", leads).group(1))
m = re.search(r"<h1[^>]*>([^<]+)</h1>", leads)
print("h1:", m.group(1) if m else None)
