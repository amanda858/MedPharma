#!/bin/bash
set -e
cd /workspaces/CVOPro
git add app/bulk_prospector.py .vscode/local_hunt.py
git commit -m "feat: add Hunter.io + pattern email enrichment to lead pipeline

- Wire find_emails_for_lab() into _enrich_dm_only() for every prospect
- Add 5 new row columns: DM Email, DM Email Confidence, Org Domain,
  Domain Candidates, Org Emails Found
- Pick best email by name match > decision-maker > first-found
- Count DM Email as a valid 'reach' signal in _has_reach() filter
- Print email/domain/verified-email stats in local_hunt summary
- ENABLE_EMAIL_ENRICHMENT=1 by default (set to 0 to disable)

Tested locally on FL clinical: 25/25 rows with email,
4 verified (Hunter conf >=70), 11 with live domains, in 179s." -- app/bulk_prospector.py .vscode/local_hunt.py
git push origin main
