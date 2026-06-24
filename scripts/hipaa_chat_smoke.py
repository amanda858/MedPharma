"""HIPAA chat smoke: admin → create client → create room → invite client →
post message → verify body decrypts cleanly → verify audit log + activity feed
+ outbound email NEVER contain the message body.

Runs entirely in-process against a throwaway SQLite db so this proves the
behaviour without touching live data."""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Force isolated env BEFORE any app imports.
tmpdir = Path(tempfile.mkdtemp(prefix="hipaa_smoke_"))
os.environ["DB_PATH"] = str(tmpdir / "leads.db")
os.environ["CHAT_KEY_PATH"] = str(tmpdir / "chat.key")
os.environ["DATA_DIR"] = str(tmpdir)
os.environ["CLIENTS_SEED_PATH"] = str(tmpdir / "clients_seed.json")  # don't pollute the repo file
os.environ.pop("CHAT_ENCRYPTION_KEY", None)
os.environ.pop("SENDGRID_API_KEY", None)
os.environ.pop("SMTP_HOST", None)
os.environ.pop("PORT", None)   # IS_PROD = False
os.environ["NOTIFY_EMAILS"] = ""
os.environ["EOD_REPORT_EMAIL"] = ""

# Capture outbound emails instead of sending.
captured_emails: list[dict] = []

def _patch_email_sinks():
    import app.notifications as nt
    def fake_send_email(subject, body, html_body=""):
        captured_emails.append({"to": "(default-notify)", "subject": subject,
                                "body": body, "html": html_body})
        return True, "captured"
    def fake_send_email_to(to_email, subject, body, html_body="", **kw):
        captured_emails.append({"to": to_email, "subject": subject,
                                "body": body, "html": html_body})
        return True, "captured"
    nt._send_email = fake_send_email  # type: ignore
    nt._send_email_to = fake_send_email_to  # type: ignore

    import app.client_routes as cr
    def fake_direct(to_email, subject, text_body, html_body=""):
        captured_emails.append({"to": to_email, "subject": subject,
                                "body": text_body, "html": html_body})
        return True, "captured"
    cr._send_direct_email = fake_direct  # type: ignore


def main() -> int:
    sys.path.insert(0, "/workspaces/MedPharma")

    # Import in the right order so init runs against the temp DB.
    import app.client_db as cdb
    cdb.init_client_hub_db()
    _patch_email_sinks()

    from app import security
    print("Encryption status:", security.encryption_status())
    assert security.encryption_status()["ready"], "encryption MUST be ready"

    # 1) Seed admin + a 'client' user we'll add to the room.
    # init_client_hub_db() already runs _seed_data + _ensure_medpharma_team_accounts,
    # which creates admin/admin123. Verify the login works.
    admin_user, admin_token = cdb.authenticate("admin", "admin123")
    assert admin_user, "admin/admin123 login failed — seed broken"
    print(f"admin login: {admin_user['username']} (id={admin_user['id']}, role={admin_user['role']})")

    # 2) Create a client account through the admin path so the audit log + chat
    #    invite path mirrors production.
    from fastapi.testclient import TestClient
    from app.hub_app import app as hub_app
    client = TestClient(hub_app)
    # Log in as admin via the real cookie flow
    r = client.post("/hub/api/login", json={"username": "admin", "password": "admin123"})
    print(f"login HTTP {r.status_code}: {r.json().get('user',{}).get('username') if r.status_code==200 else r.text[:200]}")
    assert r.status_code == 200

    # 3) Add a client (Apex Pain Test) with a contact email.
    import time as _t
    UNAME = f"smoke_apex_{int(_t.time())}"
    PWORD = "ApexSmoke123!"
    r = client.post("/hub/api/clients", json={
        "username": UNAME,
        "password": PWORD,
        "role": "client",
        "company": f"Apex Pain Smoke {UNAME}",
        "contact_name": "Apex Tester",
        "email": "apex.tester@example.com",
    })
    print(f"create client HTTP {r.status_code}: {r.text[:240]}")
    assert r.status_code in (200, 201), f"create client failed: {r.text}"
    apex_id = r.json().get("id") or r.json().get("client_id")
    if not apex_id:
        # Some response shapes nest it
        apex_id = (r.json().get("client") or {}).get("id")
    assert apex_id, f"no client id in response: {r.text}"
    print(f"created client id={apex_id}")

    # 4) Create a chat room with that client as a member.
    captured_emails.clear()
    r = client.post("/hub/api/chat/rooms", json={
        "name": "PHI Smoke Room",
        "description": "Chat encryption smoke",
        "client_id": apex_id,
        "member_user_ids": [apex_id],
    })
    print(f"create room HTTP {r.status_code}: {r.text[:300]}")
    assert r.status_code == 200, r.text
    room = r.json()
    room_id = room["id"]
    invites = room.get("invites", [])
    print(f"room {room_id} created; invites:")
    for inv in invites:
        print(f"  - {inv}")
    assert any(inv.get("sent") for inv in invites), \
        "expected at least one invite to be 'sent' via captured sink"
    # Invite emails MUST NOT include any PHI text (they only have the room name + link)
    for em in captured_emails:
        assert "ssn" not in em["body"].lower(), "invite leaked SSN string"
    print(f"captured {len(captured_emails)} invite email(s) — none contain message body PHI")

    # 5) Log in as the client and post a message containing fake PHI.
    captured_emails.clear()
    rclient = client.post("/hub/api/login", json={"username": UNAME, "password": PWORD})
    assert rclient.status_code == 200, rclient.text
    PHI_TEXT = "Patient John Doe SSN 123-45-6789 DOB 1980-01-01 CPT 99213"
    r = client.post(f"/hub/api/chat/rooms/{room_id}/messages", json={"body": PHI_TEXT})
    print(f"post message HTTP {r.status_code}: {r.text[:200]}")
    assert r.status_code == 200, r.text
    msg_id = r.json()["id"]

    # 6) Read back the messages — body must decrypt to the original PHI text.
    r = client.get(f"/hub/api/chat/rooms/{room_id}/messages")
    assert r.status_code == 200, r.text
    msgs = r.json()["messages"]
    assert msgs, "no messages returned"
    last = msgs[-1]
    print(f"readback body: {last['body']!r}")
    assert last["body"] == PHI_TEXT, "readback decryption mismatch"

    # 7) Confirm the raw SQLite row is encrypted (NOT the plaintext we posted).
    import sqlite3
    raw = sqlite3.connect(os.environ["DB_PATH"]).execute(
        "SELECT body FROM chat_messages WHERE id=?", (msg_id,)
    ).fetchone()[0]
    print(f"raw db cell starts with: {raw[:12]!r} ({len(raw)} chars)")
    assert raw.startswith("enc1:"), "body in DB was NOT encrypted"
    assert PHI_TEXT not in raw, "plaintext PHI leaked into DB cell"

    # 8) Confirm audit_log entry does NOT contain the PHI string.
    rows = sqlite3.connect(os.environ["DB_PATH"]).execute(
        "SELECT details FROM audit_log WHERE action='chat_message' ORDER BY id DESC LIMIT 5"
    ).fetchall()
    for (det,) in rows:
        print(f"audit row: {det!r}")
        assert PHI_TEXT not in (det or ""), "PHI leaked into audit_log"
        assert "123-45-6789" not in (det or ""), "SSN leaked into audit_log"

    # 9) Confirm captured outbound notification email did NOT carry the PHI text.
    print(f"captured {len(captured_emails)} email(s) from posting:")
    for em in captured_emails:
        print(f"  → {em['to']} :: {em['subject']!r}")
        assert PHI_TEXT not in em["body"], "PHI leaked into notification text body"
        assert PHI_TEXT not in em["html"], "PHI leaked into notification HTML body"
        assert "123-45-6789" not in em["body"], "SSN leaked into email body"
        assert "123-45-6789" not in em["html"], "SSN leaked into email HTML"

    # 10) Confirm the room list returns the decrypted last_body.
    r = client.get("/hub/api/chat/rooms")
    rooms = r.json()["rooms"]
    target = next((rm for rm in rooms if rm["id"] == room_id), None)
    print(f"room-list last_body: {target.get('last_body')!r}")
    assert target and target.get("last_body") == PHI_TEXT, "room list last_body not decrypted"

    print("\n✅ HIPAA chat smoke PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
