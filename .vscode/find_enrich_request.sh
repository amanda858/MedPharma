#!/bin/bash
set -e
cd /workspaces/CVOPro
echo "=== git log -S 'class EnrichRequest' on leads_app.py ==="
git log --oneline -S "class EnrichRequest" -- app/leads_app.py | head -10
echo
echo "=== current leads_app.py: lines defining classes near 1900-2050 ==="
grep -n "^class\|^async def\|^def " app/leads_app.py | sed -n '1,200p' | tail -40
echo
echo "=== last 5 lines that mention EnrichRequest in git history ==="
git log -p -S "EnrichRequest" --all -- app/leads_app.py | head -60
