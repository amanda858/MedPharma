import urllib.request, urllib.parse, http.cookiejar, re, json
base = "https://medpharma-hub.onrender.com"
cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
# Try multiple credential pairs
creds_list = [("rcm","rcm123"),("admin","admin"),("jess","jess123"),("admin","admin123")]
loggedin = None
for u,p in creds_list:
    try:
        req = urllib.request.Request(base+"/hub/api/login", data=json.dumps({"username":u,"password":p}).encode(), headers={"Content-Type":"application/json"})
        r = opener.open(req, timeout=30)
        body = r.read().decode()
        if r.status==200 and '"ok":true' in body:
            loggedin = u; break
    except Exception as e:
        pass
print("logged_in_as:", loggedin)
html = opener.open(base+"/hub", timeout=30).read().decode()
out = {}
out["html_size"] = len(html)
out["active_nav_items"] = re.findall(r'<div class="nav-item[^"]*\bactive\b[^"]*"[^>]*data-panel="([^"]+)"', html)
out["all_nav_items"] = re.findall(r'data-panel="([^"]+)"', html)[:20]
m = re.findall(r"navTo\('(\w+)'\)", html)
out["all_navTo_calls"] = list(dict.fromkeys(m))
out["has_panel_leads_div"] = '<div class="hub-panel" id="panel-leads"' in html
out["has_leadsIframe"] = 'id="leadsIframe"' in html
out["has_Lab_Leads_text"] = "Lab Leads" in html
# Find what the DOMContentLoaded does at the end
m3 = re.search(r"DOMContentLoaded.{0,3000}?navTo\('(\w+)'\)", html, re.S)
out["DOMContentLoaded_first_navTo"] = m3.group(1) if m3 else None
print(json.dumps(out, indent=2))
