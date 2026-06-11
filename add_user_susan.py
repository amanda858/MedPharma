"""One-shot helper: ensure user `susan@medprosc.com` exists with password `susan123`.

Usage:
    python3 add_user_susan.py
"""
from __future__ import annotations

import os
import secrets
import sqlite3
import sys

# Make the `app` package importable regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.client_db import (  # type: ignore
    _ensure_auth_columns,
    _hash_pw,
    get_db,
    init_client_hub_db,
)

USERNAME = "susan@medprosc.com"
PASSWORD = "susan123"
EMAIL = "susan@medprosc.com"
ROLE = "staff"
COMPANY = "MedPharma SC"
CONTACT_NAME = "Susan"


def main() -> int:
    # Ensure schema exists.
    init_client_hub_db()

    conn = get_db()
    try:
        _ensure_auth_columns(conn)
        cur = conn.cursor()
        salt = secrets.token_hex(16)
        pw_hash = _hash_pw(PASSWORD, salt)

        row = cur.execute(
            "SELECT id FROM clients WHERE username=?", (USERNAME,)
        ).fetchone()

        if row:
            cur.execute(
                "UPDATE clients SET password=?, salt=?, role=?, company=?, "
                "contact_name=?, email=?, is_active=1, must_change_password=0 "
                "WHERE username=?",
                (pw_hash, salt, ROLE, COMPANY, CONTACT_NAME, EMAIL, USERNAME),
            )
            print(f"Updated existing user '{USERNAME}' (id={row['id']}).")
        else:
            cur.execute(
                "INSERT INTO clients (username, password, salt, company, "
                "contact_name, email, phone, role, is_active) "
                "VALUES (?,?,?,?,?,?,?,?,1)",
                (USERNAME, pw_hash, salt, COMPANY, CONTACT_NAME, EMAIL, "", ROLE),
            )
            print(f"Created user '{USERNAME}' (id={cur.lastrowid}).")

        conn.commit()
    finally:
        conn.close()

    print(f"Login: username='{USERNAME}'  password='{PASSWORD}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
