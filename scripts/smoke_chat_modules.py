"""Local smoke test for chat + modules + access wiring.

Runs WITHOUT a server. Imports the modules, calls the helper functions
in-process against a temp DB, and confirms the new code paths don't
explode.

This is intentionally narrow: we only check that the new wiring is
syntactically and semantically intact. End-to-end testing happens
against the live deploy once we push.
"""
from __future__ import annotations

import os
import sys
import json
import tempfile

# Ensure repo root is on sys.path so `from app import ...` resolves.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Force a throwaway DB so we don't touch the dev DB.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["DATABASE_PATH"] = _tmp.name

# Reload app.config so DATABASE_PATH picks up the env override.
import importlib
from app import config as _config
importlib.reload(_config)
from app import client_db
importlib.reload(client_db)

print(f"[smoke] using temp DB: {_tmp.name}")

# Boot the schema.
client_db.init_client_hub_db()

# DEFAULT_ENABLED_MODULES should now include 'chat'.
mods = client_db.DEFAULT_ENABLED_MODULES
print(f"[smoke] DEFAULT_ENABLED_MODULES = {mods}")
assert "chat" in mods, "chat module missing from defaults"
assert "documents" in mods, "documents module missing from defaults"
assert "production" in mods, "production module missing from defaults"

# Create an admin user, a staff user, and a client.
admin_id = client_db.create_client({
    "username": "admin1",
    "company": "Admin Co",
    "contact_name": "Admin One",
    "email": "admin@example.com",
    "phone": "",
    "role": "admin",
    "password": "adminpass99",
})
staff_id = client_db.create_client({
    "username": "staff1",
    "company": "Admin Co",
    "contact_name": "Staff One",
    "email": "staff@example.com",
    "phone": "",
    "role": "staff",
    "password": "staffpass99",
})
client_id = client_db.create_client({
    "username": "client1",
    "company": "Demo Lab",
    "contact_name": "Client One",
    "email": "client@example.com",
    "phone": "",
    "role": "client",
    "password": "clientpass99",
})
print(f"[smoke] admin={admin_id} staff={staff_id} client={client_id}")

# Authenticate the client and confirm enabled_modules is in the dict.
out, token = client_db.authenticate("client1", "clientpass99")
assert out is not None, "client1 should authenticate"
assert "enabled_modules" in out, "enabled_modules missing from auth payload"
print(f"[smoke] client auth enabled_modules={out['enabled_modules']}")

# Validate the session and confirm enabled_modules is exposed there too.
sess = client_db.validate_session(token)
assert sess is not None, "session should validate"
assert "enabled_modules" in sess, "enabled_modules missing from session payload"
print("[smoke] validate_session exposes enabled_modules: OK")

# Persist a custom enabled_modules list for the client and re-validate.
client_db.update_profile(client_id, {"enabled_modules": json.dumps(["production", "chat"])})
sess2 = client_db.validate_session(token)
assert sess2["enabled_modules"] == ["production", "chat"], \
    f"expected ['production','chat'] got {sess2['enabled_modules']}"
print("[smoke] per-client module opt-out persisted + read back: OK")

# Per-client access: grant staff access, confirm round-trip.
client_db.set_client_access(client_id, [staff_id], granted_by="admin1")
granted = client_db.list_client_access(client_id)
granted_ids = {int(u["id"]) for u in granted}
assert staff_id in granted_ids, f"staff_id {staff_id} not in granted {granted_ids}"
visible = client_db.list_clients_for_user(staff_id)
assert client_id in visible, f"staff should see client {client_id}, sees {visible}"
print("[smoke] client_user_access set/list/visible: OK")

# Chat room flow: admin creates a room with staff + client as members.
room_id = client_db.create_room(
    name="ABC Lab — Billing",
    description="Daily standups",
    client_id=client_id,
    created_by="admin1",
    member_user_ids=[staff_id, client_id],
    creator_user_id=admin_id,
)
print(f"[smoke] created room id={room_id}")
members = client_db.list_room_members(room_id)
member_ids = {int(m["user_id"]) for m in members}
assert staff_id in member_ids and client_id in member_ids, \
    f"members missing: {member_ids}"
print(f"[smoke] room members include staff+client: OK ({member_ids})")

# Client posts a message, staff and admin both see it.
msg_id = client_db.add_room_message(
    room_id=room_id,
    sender_id=client_id,
    sender_name="client1",
    sender_role="client",
    body="Hi team, claim #12345 was denied — can someone look?",
)
print(f"[smoke] client posted message id={msg_id}")
msgs = client_db.list_room_messages(room_id, limit=10)
assert any(m["id"] == msg_id for m in msgs), "client message not in room history"

# Confirm room access checks
assert client_db.user_can_access_room(room_id, client_id, is_admin=False)
assert client_db.user_can_access_room(room_id, staff_id, is_admin=False)
# Admin always has access even if not in members.
random_other = client_db.create_client({
    "username": "outsider",
    "company": "Outsider",
    "contact_name": "x",
    "email": "x@x.com",
    "phone": "",
    "role": "client",
    "password": "outsiderpw9",
})
assert not client_db.user_can_access_room(room_id, random_other, is_admin=False), \
    "random client should NOT see foreign room"
assert client_db.user_can_access_room(room_id, random_other, is_admin=True), \
    "admin flag should bypass membership"
print("[smoke] room access checks: OK")

# Unread counter should be > 0 for staff who hasn't read yet.
unread = client_db.chat_unread_total(staff_id, is_admin=False)
assert unread >= 1, f"staff unread should be >=1, got {unread}"
print(f"[smoke] staff unread count: {unread}")

# Mark read clears it.
client_db.mark_room_read(room_id, staff_id)
unread2 = client_db.chat_unread_total(staff_id, is_admin=False)
assert unread2 == 0, f"after mark_read unread should be 0, got {unread2}"
print("[smoke] mark_read clears unread: OK")

# Confirm list_chat_eligible_users surfaces all active users.
elig = client_db.list_chat_eligible_users()
elig_ids = {int(u["id"]) for u in elig}
for uid in (admin_id, staff_id, client_id, random_other):
    assert uid in elig_ids, f"user {uid} missing from eligible users"
print(f"[smoke] eligible chat users include admin/staff/client: {len(elig)} total")

# Cleanup temp DB.
try:
    os.unlink(_tmp.name)
except OSError:
    pass

print("\n[smoke] ALL CHECKS PASSED ✓")
sys.exit(0)
