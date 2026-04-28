#!/bin/bash
set -e
cd /workspaces/CVOPro
git add app/leads_app.py app/templates/index.html
git commit -m "feat: searchable national-pull lead browser (specialty/state/q/heat)" -- app/leads_app.py app/templates/index.html
git push origin main
