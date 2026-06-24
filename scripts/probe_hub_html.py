#!/usr/bin/env python3
import urllib.request
r = urllib.request.urlopen("https://medpharma-hub.onrender.com/hub", timeout=60)
body = r.read().decode("utf-8", "replace")
print("len:", len(body))
for needle in ("Enter as Admin", "enterAsAdmin", "No client accounts yet", "Lead Source", "loadLeadsPanel", "showNewClientFromSelector"):
    print(needle, "->", needle in body)
