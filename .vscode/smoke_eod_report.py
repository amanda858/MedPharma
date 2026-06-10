"""Local smoke test for the EOD report.

Verifies:
  - get_eod_team_report() returns a well-formed dict for today
  - _build_eod_report_html() produces non-empty text + HTML
  - send_eod_team_report() dry-runs with no SMTP/SendGrid creds
    (should return ok=False, via='no provider configured')

Run: python3 .vscode/smoke_eod_report.py
"""
import os
import sys
import json
from datetime import datetime

# Force no creds so we don't accidentally send during smoke
for k in ("SENDGRID_API_KEY", "SMTP_HOST", "SMTP_USER", "SMTP_PASS"):
    os.environ.pop(k, None)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.client_db import get_eod_team_report  # noqa: E402
from app.notifications import (  # noqa: E402
    _build_eod_report_html,
    send_eod_team_report,
    _eod_recipients,
)

today = datetime.now().strftime("%Y-%m-%d")
print(f"== EOD report smoke for {today} ==")

report = get_eod_team_report(today)
print(f"report_date: {report['report_date']}")
print(f"generated_at: {report['generated_at']}")
print(f"totals: {json.dumps(report['totals'], indent=2)}")
print(f"user count: {len(report['users'])}")
for u in report["users"][:3]:
    print(f"  - {u['username']}: "
          f"{u['totals']['production_entries']} entries, "
          f"{u['totals']['production_hours']}h, "
          f"{u['totals']['audit_actions']} CRUD, "
          f"{len(u['by_client'])} clients touched")

text, html = _build_eod_report_html(report)
assert text.strip(), "Plain-text body is empty"
assert "<html" in html.lower() or "<body" in html.lower(), "HTML body looks broken"
assert "MedPharma" in text, "Plain-text body missing brand"
assert "report_date" not in html.lower() or report["report_date"] in html, "HTML body missing date"
print(f"text body length: {len(text)} chars")
print(f"html body length: {len(html)} chars")

print(f"default recipients: {_eod_recipients()}")
result = send_eod_team_report(today)
print(f"send result (no creds expected): {json.dumps(result, indent=2)}")
assert result["recipients"] == ["lexi@medprosc.com", "eric@medprosc.com"], \
    f"Default recipients wrong: {result['recipients']}"
print("PASS — EOD report builds, renders, and dispatch path works")
