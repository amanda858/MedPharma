#!/bin/bash
set -e
cd /workspaces/CVOPro
git add app/leads_app.py app/national_pull.py .vscode/tasks.json .vscode/smoke_pull_changes.py .vscode/diag_pull_state.py .vscode/wait_and_test_search.py .vscode/find_enrich_request.sh .vscode/trigger_national_pull.py .vscode/check_national_search.py .vscode/wait_for_search_endpoint.py .vscode/push_search_ui.sh
git commit -m "fix: restore deleted EnrichRequest class + add scoped pull params (states/per_state/specialty)"
git push origin main
