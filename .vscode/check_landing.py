import urllib.request, urllib.parse, http.cookiejar, re, json
base = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
data = urllib.parse.urlencode({"username":"rcm","password":"rcm123"}).encode()
try:
    opener.open(base+"/hub/login", data=data, timeout=30).read()
except Exception as e:
    print("login err", e)
html = opener.open(base+"/hub", timeout=30).read().decode()
out = {}
out["active_nav_items"] = re.findall(r'<div class="nav-item[^"]*\bactive\b[^"]*"[^>]*data-panel="([^"]+)"', html)
m2 = re.search(r"navTo\('(\w+)'\);\s*\}\s*function updateSubProfileBar", html)
out["default_navTo"] = m2.group(1) if m2 else None
out["has_panel_leads"] = '<div class="hub-panel" id="panel-leads"' in html
out["has_lab_leads_text"] = "Lab Leads" in html
print(json.dumps(out, indent=2))
