"""Seed a synthetic day of activity into a TEMP DB, then run the EOD
report against it to prove the aggregator handles real data correctly.

Does NOT touch the production DB. Uses a tmpfile sqlite db that's
created, populated, queried, then deleted.
"""
import os
import sys
import json
import tempfile
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Point the app at a tmp DB BEFORE importing app modules
_tmpfd, tmpdb = tempfile.mkstemp(suffix="_eod_smoke.db")
os.close(_tmpfd)
os.environ["DB_PATH"] = tmpdb

from app.client_db import init_client_hub_db, get_db, get_eod_team_report  # noqa: E402
from app.notifications import _build_eod_report_html  # noqa: E402

print(f"Using temp DB: {tmpdb}")
init_client_hub_db()

today = datetime.now().strftime("%Y-%m-%d")
now_iso = datetime.now().isoformat(timespec="seconds")
hour_ago = (datetime.now() - timedelta(hours=1, minutes=20)).isoformat(timespec="seconds")
morning = (datetime.now() - timedelta(hours=4)).isoformat(timespec="seconds")

conn = get_db()
cur = conn.cursor()

# 1) Two client accounts (different companies)
cur.execute(
    "INSERT INTO clients (username,password,salt,company,contact_name,email,role) "
    "VALUES ('acmelab','x','s','Acme Diagnostics','','contact@acme.com','client')"
)
acme_id = cur.lastrowid
cur.execute(
    "INSERT INTO clients (username,password,salt,company,contact_name,email,role) "
    "VALUES ('bioscan','x','s','BioScan Lab','','contact@bioscan.com','client')"
)
bio_id = cur.lastrowid

# 2) Production entries (the gold)
prod_entries = [
    # (client, user, category, task, qty, hours)
    (acme_id, "susan",   "Claims",        "Worked AR aging 60-90 day bucket", 25, 3.0),
    (acme_id, "susan",   "Credentialing", "Submitted CAQH attestation for 3 providers", 3, 1.5),
    (bio_id,  "susan",   "Claims",        "Posted ERA payments batch 4521", 18, 1.2),
    (acme_id, "jessica", "Enrollment",    "Cigna Medicare enrollment packet", 1, 2.4),
    (acme_id, "jessica", "EDI",           "Set up 837P clearinghouse trading partner", 1, 1.1),
    (bio_id,  "rcm",     "Claims",        "Denial appeals - 14 claims to BCBS", 14, 2.8),
    (bio_id,  "rcm",     "Notes",         "Updated payor follow-up notes", 12, 0.5),
]
for cid, user, cat, task, qty, hrs in prod_entries:
    cur.execute(
        "INSERT INTO team_production "
        "(client_id, work_date, username, category, task_description, "
        " quantity, time_spent, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (cid, today, user, cat, task, qty, hrs, now_iso),
    )

# 3) audit_log CRUD actions
audit_entries = [
    (acme_id, "susan",   "create",  "Claim",          "Created claim #5521"),
    (acme_id, "susan",   "update",  "Claim",          "Updated claim status to Submitted"),
    (acme_id, "susan",   "update",  "Claim",          "Updated claim status to Paid"),
    (bio_id,  "rcm",     "create",  "CredRecord",     "Recredentialing for Dr Smith"),
    (acme_id, "jessica", "upload",  "ClientFile",     "Uploaded enrollment_packet.pdf"),
    (acme_id, "jessica", "create",  "EDIRecord",      "Added Availity 837 partner"),
]
for cid, user, action, ent, det in audit_entries:
    cur.execute(
        "INSERT INTO audit_log (client_id, username, action, entity_type, "
        "details, created_at) VALUES (?,?,?,?,?,?)",
        (cid, user, action, ent, det, now_iso),
    )

# 4) activity_events firehose
event_paths = [
    ("susan",   acme_id, "GET",  "/hub/api/claims"),
    ("susan",   acme_id, "GET",  "/hub/api/claims"),
    ("susan",   acme_id, "POST", "/hub/api/claims"),
    ("susan",   acme_id, "PUT",  "/hub/api/claims/5521"),
    ("susan",   acme_id, "GET",  "/hub/api/credentialing"),
    ("susan",   acme_id, "POST", "/hub/api/credentialing"),
    ("susan",   bio_id,  "GET",  "/hub/api/claims"),
    ("susan",   bio_id,  "POST", "/hub/api/payments"),
    ("jessica", acme_id, "GET",  "/hub/api/enrollment"),
    ("jessica", acme_id, "POST", "/hub/api/enrollment"),
    ("jessica", acme_id, "POST", "/hub/api/files/upload"),
    ("jessica", acme_id, "POST", "/hub/api/edi"),
    ("rcm",     bio_id,  "GET",  "/hub/api/claims"),
    ("rcm",     bio_id,  "PUT",  "/hub/api/claims/9981"),
    ("rcm",     bio_id,  "POST", "/hub/api/notes"),
    ("rcm",     bio_id,  "POST", "/hub/api/notes"),
]
for user, cid, method, path in event_paths:
    cur.execute(
        "INSERT INTO activity_events "
        "(occurred_at, username, client_id, event_type, method, path, status_code) "
        "VALUES (?,?,?,?,?,?,200)",
        (now_iso, user, cid, "request", method, path),
    )

# 5) user_presence rollups
for user, active_sec, idle_sec, actions in [
    ("susan",   6 * 3600 + 1800, 600,  85),   # 6.5h active
    ("jessica", 4 * 3600,        1200, 42),   # 4.0h active
    ("rcm",     3 * 3600 + 600,  900,  31),   # ~3.2h active
]:
    cur.execute(
        "INSERT INTO user_presence "
        "(username, work_date, first_seen_at, last_seen_at, "
        " active_seconds, idle_seconds, action_count) "
        "VALUES (?,?,?,?,?,?,?)",
        (user, today, morning, hour_ago, active_sec, idle_sec, actions),
    )

# 6) file uploads (client_files)
cur.execute(
    "INSERT INTO client_files (client_id, filename, original_name, "
    "category, status, row_count, uploaded_by, created_at) "
    "VALUES (?,?,?,?,?,?,?,?)",
    (acme_id, "x.pdf", "enrollment_packet.pdf", "Enrollment", "Uploaded", 0, "jessica", now_iso),
)
cur.execute(
    "INSERT INTO client_files (client_id, filename, original_name, "
    "category, status, row_count, uploaded_by, created_at) "
    "VALUES (?,?,?,?,?,?,?,?)",
    (bio_id, "y.xlsx", "denied_claims.xlsx", "Claims", "Imported", 412, "rcm", now_iso),
)

conn.commit()
conn.close()

# Now run the EOD aggregator
report = get_eod_team_report(today)
print(f"\n== Aggregator output for {today} ==")
print(f"Totals: {json.dumps(report['totals'], indent=2)}")
print(f"\nUsers ({len(report['users'])}):")
for u in report["users"]:
    t = u["totals"]
    print(f"\n  >> {u['username']} — {t['production_hours']}h logged, "
          f"{t['production_entries']} entries, {t['audit_actions']} CRUD, "
          f"tabs={t['tabs_touched']}")
    for c in u["by_client"]:
        print(f"     [{c['client_name']}] hrs={c['production_hours']} "
              f"entries={c['production_entries']} writes={sum(t['writes'] for t in c['tabs'].values())}")
        for cat in c["top_categories"]:
            print(f"        - {cat['name']}: {cat['entries']} entries, "
                  f"qty {cat['quantity']}, {cat['hours']}h")
        for tab in c["top_tabs"]:
            print(f"        ~ tab {tab['name']}: {tab['total']} hits "
                  f"(writes={tab['writes']}, gets={tab['gets']})")
        for tk in c["tasks"][:3]:
            print(f"        * task [{tk['category']}]: {tk['task']} "
                  f"(qty {tk['quantity']}, {tk['hours']}h)")
        if c["uploads"]:
            print(f"        + uploads: {c['uploads']} file(s), {c['upload_rows']} rows")

# Render HTML
text, html = _build_eod_report_html(report)
out_html = "/tmp/eod_smoke_preview.html"
with open(out_html, "w") as f:
    f.write(html)
print(f"\nHTML preview written to {out_html} ({len(html)} bytes)")
print(f"Plain text body ({len(text)} chars):")
print("─" * 60)
print(text)
print("─" * 60)

# Sanity checks
assert report["totals"]["users"] == 3, f"Expected 3 users, got {report['totals']['users']}"
assert report["totals"]["clients_touched"] == 2, f"Expected 2 clients touched, got {report['totals']['clients_touched']}"
assert report["totals"]["production_entries"] == 7, f"Expected 7 entries, got {report['totals']['production_entries']}"
assert abs(report["totals"]["production_hours"] - 12.5) < 0.01, \
    f"Expected 12.5 total hours, got {report['totals']['production_hours']}"
susan = next(u for u in report["users"] if u["username"] == "susan")
assert susan["totals"]["production_hours"] == 5.7, susan["totals"]["production_hours"]
assert len(susan["by_client"]) == 2, "Susan should be tagged to 2 clients"
assert any("Claims" in t["name"] for c in susan["by_client"] for t in c["top_tabs"]), \
    "Susan should have Claims tab activity"

print("\n✓ ALL ASSERTIONS PASS — real data flows end-to-end into the EOD report")
os.unlink(tmpdb)
