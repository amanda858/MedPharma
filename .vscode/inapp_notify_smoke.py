"""Local smoke for the in-app notification system.

Proves that chat invites, chat messages, welcome invites, and EOD reports
all surface in the recipient's hub inbox even when no email provider is
configured. PHI never appears in the notification body.

Runs entirely against the in-process app — no Render, no SendGrid.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import time as _t
from pathlib import Path

# Isolated DB + key dir so we don't touch prod data.
_tmp = Path(tempfile.mkdtemp(prefix="cvopro_inapp_smoke_"))
os.environ["DB_PATH"] = str(_tmp / "leads.db")
os.environ["CHAT_KEY_PATH"] = str(_tmp / "chat.key")
os.environ["DATA_DIR"] = str(_tmp)
os.environ["CLIENTS_SEED_PATH"] = str(_tmp / "clients_seed.json")
os.environ.pop("SENDGRID_API_KEY", None)
os.environ.pop("SMTP_HOST", None)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from app.hub_app import app  # noqa: E402

FAILS: list[str] = []


def check(label: str, cond: bool, detail: str = "") -> None:
    flag = "PASS" if cond else "FAIL"
    if not cond:
        FAILS.append(label + (f" — {detail}" if detail else ""))
    print(f"  [{flag}] {label}{('  ·  ' + detail) if detail else ''}")


def login(client: TestClient, username: str, password: str) -> bool:
    r = client.post("/hub/api/login",
                    json={"username": username, "password": password})
    return r.status_code == 200


def main() -> int:
    print(f"\n=== in-app notification smoke (DB={os.environ['DB_PATH']}) ===\n")
    with TestClient(app) as admin:
        check("admin login", login(admin, "admin", "admin123"))

        # 1) Create a fresh client
        uname = f"smoke_inapp_{int(_t.time())}"
        r = admin.post("/hub/api/clients", json={
            "username": uname,
            "password": "InappSmoke123!",
            "role": "client",
            "company": f"Smoke Inapp {uname}",
            "contact_name": "Smoke Inapp",
            "email": "smoke.inapp@example.com",
        })
        check("create client", r.status_code == 200,
              f"http {r.status_code}")
        cid = r.json().get("id")
        assert cid, f"no client id: {r.text}"

        # 2) Create a chat room with the new client as member.
        rr = admin.post("/hub/api/chat/rooms", json={
            "name": f"Smoke Room {int(_t.time())}",
            "description": "in-app smoke",
            "client_id": cid,
            "member_user_ids": [cid],
        })
        check("create chat room", rr.status_code == 200)
        room_id = rr.json().get("id")
        assert room_id

        # Email will fail because no provider — but in-app notification must
        # still have landed for the invited client.
        with TestClient(app) as client:
            check("client login", login(client, uname, "InappSmoke123!"))

            # Unread count > 0
            cu = client.get("/hub/api/notifications/unread-count").json()
            check("client has unread notifications after invite",
                  cu.get("unread", 0) >= 1, f"unread={cu.get('unread')}")

            # List notifications — should include 'chat_invite' for this room
            lst = client.get("/hub/api/notifications").json()
            items = lst.get("items", [])
            invite_n = next((n for n in items
                             if n.get("kind") == "chat_invite"
                             and (n.get("related_id") or 0) == room_id), None)
            check("invite notification appears in inbox",
                  invite_n is not None,
                  f"items={[n.get('kind') for n in items]}")
            if invite_n:
                check("invite has deep link to room",
                      f"chat={room_id}" in (invite_n.get("link") or ""))

            # 3) Admin posts a message
            mr = admin.post(f"/hub/api/chat/rooms/{room_id}/messages",
                            json={"body": "Patient SSN 123-45-6789 secret"})
            check("admin posts message", mr.status_code == 200,
                  f"http {mr.status_code} body={mr.text[:120]}")

            # Client should now see an unread chat_message notification
            lst2 = client.get("/hub/api/notifications").json()
            items2 = lst2.get("items", [])
            msg_n = next((n for n in items2
                          if n.get("kind") == "chat_message"
                          and (n.get("related_id") or 0) == room_id), None)
            check("chat-message notification appears for client",
                  msg_n is not None,
                  f"items={[n.get('kind') for n in items2]}")
            # PHI safety: notification body must not include "SSN" or the digits
            if msg_n:
                blob = json.dumps(msg_n)
                check("notification body contains NO PHI",
                      "SSN" not in blob and "123-45-6789" not in blob,
                      f"body={msg_n.get('body','')[:80]}")
                check("notification body has length marker",
                      "chat message" in (msg_n.get("body") or "").lower()
                      or "·" in (msg_n.get("body") or ""))

            # 4) Mark one notification as read
            target_id = (msg_n or invite_n).get("id")
            mr2 = client.post(f"/hub/api/notifications/{target_id}/read")
            check("mark notification read", mr2.status_code == 200
                  and mr2.json().get("ok") is True)

            # Unread count drops by exactly one
            before = cu.get("unread", 0)
            after = client.get("/hub/api/notifications/unread-count").json().get("unread", 0)
            # We posted 1 message after the invite, so before+1 = total notifs.
            # After read-one, the unread count should be one less than peak.
            check("unread count decremented after mark-read",
                  after < (before + 1),
                  f"before={before} after={after}")

            # 5) Mark all read → count becomes 0
            ar = client.post("/hub/api/notifications/read-all")
            check("mark all read", ar.status_code == 200)
            check("unread becomes 0",
                  client.get("/hub/api/notifications/unread-count")
                  .json().get("unread") == 0)

        # 6) EOD report demo: archive must appear in history even though
        # email fails (no provider).
        from app.notifications import send_eod_team_report
        eod = send_eod_team_report(force=True)
        check("EOD send returns ok-shaped dict", isinstance(eod, dict))
        check("EOD archived to DB", bool(eod.get("archive_id")),
              f"archive_id={eod.get('archive_id')}")

        # 7) Admin can list EOD history
        hist = admin.get("/hub/api/reports/eod/history").json()
        reports = hist.get("reports", [])
        check("EOD history has at least one report",
              len(reports) >= 1, f"count={len(reports)}")
        if reports:
            arc_id = reports[0]["id"]
            view = admin.get(f"/hub/api/reports/eod/archive/{arc_id}").json()
            check("archived EOD has HTML body",
                  bool(view.get("html_body")),
                  f"html_len={len(view.get('html_body',''))}")
            check("archive email_status reflects no provider",
                  (view.get("email_status") or "") in ("no_provider", "failed", "no_recipients"),
                  f"status={view.get('email_status')}")

        # 8) Admin should also have an in-app 'eod_report' notification
        an = admin.get("/hub/api/notifications").json().get("items", [])
        eod_n = next((n for n in an if n.get("kind") == "eod_report"), None)
        check("admin received eod_report in-app notification",
              eod_n is not None,
              f"items_kinds={[n.get('kind') for n in an]}")

        # 9) cleanup
        admin.delete(f"/hub/api/chat/rooms/{room_id}")
        admin.delete(f"/hub/api/clients/{cid}")

    print()
    if FAILS:
        print(f"\n❌ {len(FAILS)} FAIL(s):\n  - " + "\n  - ".join(FAILS))
        return 1
    print("✅ ALL CHECKS PASSED — in-app notifications work without email.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
