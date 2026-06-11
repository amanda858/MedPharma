#!/usr/bin/env python3
"""Smoke-test the new email diag/test/status endpoints against an in-process
hub instance with an isolated tempfile DB. Asserts:
 - /hub/api/email-status returns ready=false when no SendGrid is set
 - admin-only endpoints (/hub/api/admin/diag/email{,/test}) require admin
 - /hub/api/admin/diag/email/test returns ok=false + a real failure reason
   instead of silently 'succeeding'
 - sending a chat-room invite with no email provider still creates the room
   and returns per-user invites with sent=false + a real reason
"""
from __future__ import annotations
import os
import secrets
import sys
import tempfile
from pathlib import Path

# Force a clean, isolated DB and STRIP every email env var so we exercise the
# "no provider configured" path.
_tmp = Path(tempfile.mkdtemp(prefix="medpharma_emaildiag_"))
os.environ["DB_PATH"] = str(_tmp / "leads.db")
os.environ["CHAT_KEY_PATH"] = str(_tmp / "chat.key")
os.environ["DATA_DIR"] = str(_tmp)
os.environ["CLIENTS_SEED_PATH"] = str(_tmp / "clients_seed.json")
for var in (
    "SENDGRID_API_KEY", "SENDGRID_KEY", "SENDGRID_TOKEN", "SG_API_KEY",
    "SENDGRID", "SENDGRID_FROM", "SENDGRID_FROM_EMAIL", "MAIL_FROM",
    "FROM_EMAIL", "SMTP_HOST", "SMTP_USER", "SMTP_PASS",
):
    os.environ.pop(var, None)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402
from app.hub_app import app  # noqa: E402

PASSES: list[str] = []
FAILS: list[str] = []


def expect(label: str, cond: bool, detail: str = ""):
    (PASSES if cond else FAILS).append(f"{label}{(' — ' + detail) if detail else ''}")
    icon = "[PASS]" if cond else "[FAIL]"
    print(f"  {icon} {label}{(' — ' + detail) if detail else ''}")


def login(client: TestClient, username: str, password: str) -> bool:
    r = client.post("/hub/api/login",
                    json={"username": username, "password": password})
    return r.status_code == 200


def main() -> int:
    print("\n=== Email diag/test/status smoke ===")
    print(f"  DB: {os.environ['DB_PATH']}\n")
    with TestClient(app) as client:
        # 1. unauth /email-status should be 401, not 500
        print("[1] Unauthenticated /hub/api/email-status returns 401")
        r = client.get("/hub/api/email-status")
        expect("returns 401 not 500", r.status_code == 401,
               f"got {r.status_code}")

        # 2. admin login
        print("\n[2] Admin login")
        ok = login(client, "admin", "admin123")
        expect("admin can log in", ok)

        # 3. /email-status as admin → ready false, via 'none'
        print("\n[3] /hub/api/email-status as admin (no provider configured)")
        r = client.get("/hub/api/email-status")
        expect("200", r.status_code == 200, f"got {r.status_code}")
        data = r.json() if r.status_code == 200 else {}
        expect("ready=false", data.get("ready") is False,
               f"got ready={data.get('ready')}")
        expect("via='none'", data.get("via") == "none",
               f"got via={data.get('via')}")

        # 4. /admin/diag/email as admin returns full structure with accepted aliases
        print("\n[4] /hub/api/admin/diag/email returns alias list")
        r = client.get("/hub/api/admin/diag/email")
        expect("200", r.status_code == 200, f"got {r.status_code}")
        diag = r.json() if r.status_code == 200 else {}
        email = diag.get("email", {})
        expect("ready=false", email.get("ready") is False)
        expect("accepted_key_env_vars includes SENDGRID_API_KEY",
               "SENDGRID_API_KEY" in (email.get("accepted_key_env_vars") or []))
        expect("accepted_key_env_vars includes SENDGRID_KEY",
               "SENDGRID_KEY" in (email.get("accepted_key_env_vars") or []))
        expect("guidance mentions Render",
               "Render" in (email.get("guidance") or ""))

        # 5. /admin/diag/email/test fails with real reason (not silently ok)
        print("\n[5] /hub/api/admin/diag/email/test → ok=false with reason")
        r = client.post("/hub/api/admin/diag/email/test",
                        json={"to": "lexi@medprosc.com"})
        expect("200", r.status_code == 200, f"got {r.status_code}")
        test_result = r.json() if r.status_code == 200 else {}
        expect("ok=false (no provider)", test_result.get("ok") is False,
               f"got ok={test_result.get('ok')}")
        expect("'via' contains 'not configured'",
               "not configured" in (test_result.get("via") or "").lower(),
               f"got via={test_result.get('via')}")
        expect("provider_config.sendgrid_key_set=false",
               (test_result.get("provider_config") or {}).get("sendgrid_key_set") is False)
        expect("guidance is actionable",
               "SENDGRID" in (test_result.get("guidance") or ""))

        # 6. /admin/diag/email/test rejects missing/bad address
        print("\n[6] /hub/api/admin/diag/email/test rejects bad input")
        r = client.post("/hub/api/admin/diag/email/test",
                        json={"to": "not-an-email"})
        expect("400", r.status_code == 400, f"got {r.status_code}")

        # 7. SENDGRID_KEY alias is honored (one of the typo names)
        print("\n[7] _resolved_email_config honors SENDGRID_KEY alias")
        os.environ["SENDGRID_KEY"] = "SG.TESTKEY_NOT_REAL"
        try:
            from app.client_routes import _resolved_email_config
            cfg = _resolved_email_config()
            expect("ready_sendgrid=true with SENDGRID_KEY alias",
                   cfg["ready_sendgrid"] is True,
                   f"got ready_sendgrid={cfg['ready_sendgrid']}")
            expect("sg_key_name=SENDGRID_KEY",
                   cfg["sg_key_name"] == "SENDGRID_KEY",
                   f"got sg_key_name={cfg['sg_key_name']}")
            expect("sg_key_prefix masked",
                   cfg["sg_key_prefix"].endswith("…")
                   and len(cfg["sg_key_prefix"]) < len(cfg["sg_key"]),
                   f"got prefix={cfg['sg_key_prefix']}")
        finally:
            os.environ.pop("SENDGRID_KEY", None)

        # 8. Non-admin (staff) gets /email-status (200) but is forbidden on admin diag
        print("\n[8] Non-admin permissions")
        # Create a staff user via admin
        staff_uname = f"staff_{secrets.token_hex(3)}"
        r = client.post("/hub/api/admin/users/invite", json={
            "username": staff_uname,
            "email": f"{staff_uname}@example.com",
            "role": "staff",
        })
        expect("staff invite created", r.status_code == 200,
               f"got {r.status_code} {r.text[:200]}")
        invite = r.json() if r.status_code == 200 else {}
        # Extract token from setup_link (format: ".../hub?setup_token=XXX")
        setup_link = invite.get("setup_link") or ""
        setup_token = ""
        if "setup_token=" in setup_link:
            setup_token = setup_link.split("setup_token=", 1)[1].strip()
        if setup_token:
            rr = client.post(f"/hub/api/auth/setup-password/{setup_token}",
                             json={"password": "Staff#12345"})
            expect("staff completes setup", rr.status_code == 200,
                   f"got {rr.status_code} {rr.text[:200]}")
        # Log out admin
        client.post("/hub/api/logout")
        client.cookies.clear()
        # Log in as staff and probe both endpoints
        ok = login(client, staff_uname, "Staff#12345")
        expect("staff login", ok)
        r = client.get("/hub/api/email-status")
        expect("staff CAN read /email-status", r.status_code == 200,
               f"got {r.status_code}")
        r = client.get("/hub/api/admin/diag/email")
        expect("staff CANNOT read /admin/diag/email", r.status_code == 403,
               f"got {r.status_code}")
        r = client.post("/hub/api/admin/diag/email/test", json={"to": "x@x.com"})
        expect("staff CANNOT POST /admin/diag/email/test", r.status_code == 403,
               f"got {r.status_code}")

        # 9. Chat room create with no email provider still creates room + reports
        # per-user invite failures with reason — not silently 'sent'.
        print("\n[9] Chat room create surfaces per-user invite failure reasons")
        client.post("/hub/api/logout")
        client.cookies.clear()
        ok = login(client, "admin", "admin123")
        expect("admin re-login", ok)
        # /admin/users returns a list directly, not {"users": [...]}
        r = client.get("/hub/api/admin/users")
        users = r.json() if r.status_code == 200 else []
        if isinstance(users, dict):
            users = users.get("users", [])
        staff_id = next((u["id"] for u in users
                         if u.get("username") == staff_uname), None)
        expect("staff user id resolved", staff_id is not None,
               f"users={[u.get('username') for u in users]}")
        if staff_id:
            r = client.post("/hub/api/chat/rooms", json={
                "name": "Email diag smoke room",
                "description": "smoke test",
                "client_id": None,
                "member_user_ids": [staff_id],
            })
            expect("room created", r.status_code == 200,
                   f"got {r.status_code} {r.text[:200]}")
            j = r.json() if r.status_code == 200 else {}
            invites = j.get("invites") or []
            expect("invites returned (>=1)", len(invites) >= 1)
            staff_invite = next(
                (i for i in invites if i.get("user_id") == staff_id), None)
            expect("staff invite present", staff_invite is not None)
            if staff_invite:
                expect("staff invite sent=false (no provider)",
                       staff_invite.get("sent") is False,
                       f"got sent={staff_invite.get('sent')}")
                expect("staff invite has real failure reason",
                       "not configured" in (staff_invite.get("via") or "").lower(),
                       f"got via={staff_invite.get('via')}")

    print(f"\n=== {len(PASSES)} passed, {len(FAILS)} failed ===")
    if FAILS:
        print("\nFAILURES:")
        for f in FAILS:
            print("  [FAIL]", f)
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
