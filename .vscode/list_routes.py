import re, pathlib
p = pathlib.Path("/workspaces/CVOPro/app/leads_app.py")
src = p.read_text()
print("FILE SIZE:", len(src))
for m in re.finditer(r'@app\.(get|post|put|delete)\(\s*["\']([^"\']+)["\']', src):
    print(m.group(1).upper(), m.group(2))
print("---hub_app routing---")
hub = pathlib.Path("/workspaces/CVOPro/app/hub_app.py").read_text()
for kw in ["leads_app","mount","include_router","WSGIMiddleware","app.mount"]:
    for ln in hub.splitlines():
        if kw in ln:
            print(repr(ln.strip()))
