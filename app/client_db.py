"""Database — MedPharma Client Hub: claims_master, payments, notes_log,
credentialing, enrollment, edi_setup, providers, clients, sessions."""

import sqlite3
import os
import json
import hashlib
import secrets
import logging
from datetime import datetime, date, timedelta
from app.config import DATABASE_PATH, business_today, business_today_iso, business_now

log = logging.getLogger(__name__)

# Repo seed used for bootstrap/fallback.
_REPO_CLIENTS_SEED_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "clients_seed.json"
)
# Runtime seed path: prefer durable disk when present (Render /data).
_DEFAULT_CLIENTS_SEED_PATH = (
    "/data/clients_seed.json" if os.path.isdir("/data") else _REPO_CLIENTS_SEED_PATH
)
_CLIENTS_SEED_PATH = os.getenv("CLIENTS_SEED_PATH", _DEFAULT_CLIENTS_SEED_PATH)

# Accounts that exist only as a system/department login (no real person) and
# must never appear in any user-facing roster, picker, or production report.
# 'rcm' is a billing-department alias that was retired; it stays purged from
# every UI list (and its stale data rows are cleaned on startup) even though
# the row may transiently reappear from an old seed before cleanup runs.
_HIDDEN_ROSTER_USERS = {"rcm", "rcm@medprosc.com"}


def _sanitize_seed_entry(entry: dict) -> dict:
    return {
        "username": (entry.get("username") or "").strip().lower(),
        "company": entry.get("company", ""),
        "contact_name": entry.get("contact_name", ""),
        "email": entry.get("email", ""),
        "phone": entry.get("phone", ""),
        "role": entry.get("role", "client"),
        "service_type": entry.get("service_type", ""),
        "notes": entry.get("notes", ""),
    }


def _load_clients_seed() -> list[dict]:
    """Load clients_seed.json, return empty list on any error."""
    candidates = []
    if _CLIENTS_SEED_PATH:
        candidates.append(_CLIENTS_SEED_PATH)
    if _REPO_CLIENTS_SEED_PATH not in candidates:
        candidates.append(_REPO_CLIENTS_SEED_PATH)

    last_error = None
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                return []
            sanitized = [_sanitize_seed_entry(entry or {}) for entry in data]
            # If we loaded from fallback (repo), promote into runtime path.
            if path != _CLIENTS_SEED_PATH:
                try:
                    _save_clients_seed(sanitized)
                except Exception as copy_e:
                    log.warning("could not promote repo clients_seed to runtime path: %s", copy_e)
            elif sanitized != data:
                _save_clients_seed(sanitized)
            return sanitized
        except FileNotFoundError:
            continue
        except Exception as e:
            last_error = e

    if last_error is not None:
        log.error("could not read clients_seed.json: %s", last_error)
        raise last_error
    return []


def _save_clients_seed(clients: list[dict]):
    """Overwrite clients_seed.json atomically with the given list."""
    os.makedirs(os.path.dirname(_CLIENTS_SEED_PATH), exist_ok=True)
    tmp_path = f"{_CLIENTS_SEED_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(clients, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, _CLIENTS_SEED_PATH)


def _get_client_snapshot(cid: int) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT company, contact_name, email, phone, role, is_active, "
            "tax_id, group_npi, individual_npi, ptan_group, ptan_individual, "
            "address, specialty, notes, doc_tab_names, practice_type, "
            "report_tab_names, enabled_modules FROM clients WHERE id=?",
            [cid],
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def _upsert_client_from_seed(conn, entry: dict):
    """Insert or update a client from a seed entry (does NOT overwrite passwords)."""
    username = (entry.get("username") or "").strip().lower()
    if not username:
        return
    cur = conn.cursor()
    cur.execute("SELECT id FROM clients WHERE username=?", (username,))
    row = cur.fetchone()
    if row:
        # Client already exists — update non-sensitive fields only
        cur.execute(
            "UPDATE clients SET company=?, contact_name=?, email=?, phone=?, "
            "role=?, practice_type=?, notes=? WHERE username=?",
            (
                entry.get("company", ""), entry.get("contact_name", ""),
                entry.get("email", ""), entry.get("phone", ""),
                entry.get("role", "client"), entry.get("service_type", ""),
                entry.get("notes", ""),
                username,
            ),
        )
    else:
        # Insert new client with a random password; admin must invite/reset after reseed.
        salt = secrets.token_hex(16)
        raw_pw = secrets.token_urlsafe(24)
        cur.execute(
            "INSERT INTO clients (username,password,salt,company,contact_name,email,phone,role,practice_type,notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                username, _hash_pw(raw_pw, salt), salt,
                entry.get("company", ""), entry.get("contact_name", ""),
                entry.get("email", ""), entry.get("phone", ""),
                entry.get("role", "client"), entry.get("service_type", ""),
                entry.get("notes", ""),
            ),
        )
        log.info("startup: seeded client '%s' from clients_seed.json", username)


def get_db():
    db_dir = os.path.dirname(DATABASE_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _hash_pw(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode()).hexdigest()


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _ensure_password_setup_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS password_setup_tokens (
               id            INTEGER PRIMARY KEY AUTOINCREMENT,
               client_id     INTEGER NOT NULL,
               token_hash    TEXT UNIQUE NOT NULL,
               created_by    TEXT DEFAULT '',
               created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
               expires_at    TEXT NOT NULL,
               used_at       TEXT,
               FOREIGN KEY (client_id) REFERENCES clients(id)
           )"""
    )


def _ensure_auth_columns(conn):
    """Lazy-migrate auth columns needed by login/password flows."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(clients)")
    cols = {row[1] for row in cur.fetchall()}
    if "must_change_password" not in cols:
        cur.execute("ALTER TABLE clients ADD COLUMN must_change_password INTEGER DEFAULT 0")
        conn.commit()


def _ensure_migration_table(conn):
    conn.execute(
        """CREATE TABLE IF NOT EXISTS app_migrations (
               key         TEXT PRIMARY KEY,
               applied_at  TEXT DEFAULT CURRENT_TIMESTAMP
           )"""
    )


def _run_migration_once(conn, key: str, fn):
    _ensure_migration_table(conn)
    row = conn.execute("SELECT 1 FROM app_migrations WHERE key=?", (key,)).fetchone()
    if row:
        return False
    fn()
    conn.execute("INSERT INTO app_migrations(key) VALUES (?)", (key,))
    conn.commit()
    return True


def _apply_startup_user_migrations(conn):
    # Defensive: ensure auth columns exist before any migration touches them.
    _ensure_auth_columns(conn)
    cur = conn.cursor()

    def _fix_legacy_profiles():
        cur.execute(
            "UPDATE clients SET contact_name='Luminary Practice', email='info@luminarypractice.com' "
            "WHERE username='eric' AND contact_name='Eric'"
        )

    def _migrate_jessica_staff():
        cur.execute(
            "UPDATE clients SET role='staff', company='MedPharma SC' "
            "WHERE username='jessica' AND role IN ('client','admin')"
        )
        cur.execute("SELECT COUNT(*) FROM clients WHERE username='jessica'")
        if cur.fetchone()[0] == 0:
            jsalt = secrets.token_hex(16)
            cur.execute(
                "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
                ("jessica", _hash_pw("jessica123", jsalt), jsalt, "MedPharma SC", "Jessica", "", "staff")
            )

    def _deactivate_placeholder_clients():
        for _uname, _company in (("eric", "Luminary (OMT/MHP)"), ("trupath", "TruPath")):
            cur.execute(
                "SELECT id FROM clients WHERE username=? AND company=? AND role='client'",
                (_uname, _company),
            )
            _row = cur.fetchone()
            if not _row:
                continue
            _cid = _row[0]
            cur.execute("SELECT COUNT(*) FROM claims_master WHERE client_id=?", (_cid,))
            if cur.fetchone()[0] == 0:
                cur.execute("UPDATE clients SET is_active=0 WHERE id=?", (_cid,))

    def _clear_luminary_profile_fields():
        cur.execute(
            """UPDATE clients SET tax_id='', group_npi='', individual_npi='',
                   ptan_group='', ptan_individual='', specialty=''
                   WHERE username='eric' AND practice_type='MHP+OMT'"""
        )

    def _provision_susan_melissa():
        """Ensure susan@medprosc.com and melissa@medprosc.com exist with known
        starter passwords. Also retires the old short-username seed rows
        ('susan' / 'melissa') if they exist with a random seed password.
        """
        accounts = [
            ("susan@medprosc.com",   "susan",   "Susan",   "susan123"),
            ("melissa@medprosc.com", "melissa", "Melissa", "melissa123"),
        ]
        for email, legacy_username, contact, pw in accounts:
            salt = secrets.token_hex(16)
            pw_hash = _hash_pw(pw, salt)
            # Upsert by email-style username
            row = cur.execute(
                "SELECT id FROM clients WHERE username=?", (email,)
            ).fetchone()
            if row:
                cur.execute(
                    "UPDATE clients SET password=?, salt=?, role='staff', "
                    "company='MedPharma SC', contact_name=?, email=?, "
                    "is_active=1, must_change_password=0 WHERE id=?",
                    (pw_hash, salt, contact, email, row[0]),
                )
            else:
                cur.execute(
                    "INSERT INTO clients "
                    "(username,password,salt,company,contact_name,email,role,is_active) "
                    "VALUES (?,?,?,?,?,?,?,1)",
                    (email, pw_hash, salt, "MedPharma SC", contact, email, "staff"),
                )
            # Deactivate the legacy short-username seed row (random password)
            cur.execute(
                "UPDATE clients SET is_active=0 WHERE username=? AND username<>?",
                (legacy_username, email),
            )

    def _provision_eric_medprosc():
        """Ensure eric@medprosc.com exists as a real MedPharma admin login.

        The short 'eric' username was a legacy Luminary placeholder and is
        purged by purge_legacy_placeholders_v1. This is the real Eric.
        """
        email = "eric@medprosc.com"
        pw = "eric123"
        salt = secrets.token_hex(16)
        pw_hash = _hash_pw(pw, salt)
        row = cur.execute("SELECT id FROM clients WHERE username=?", (email,)).fetchone()
        if row:
            cur.execute(
                "UPDATE clients SET password=?, salt=?, role='admin', "
                "company='MedPharma SC', contact_name=COALESCE(NULLIF(contact_name,''),'Eric'), "
                "email=?, is_active=1, must_change_password=0 WHERE id=?",
                (pw_hash, salt, email, row[0]),
            )
        else:
            cur.execute(
                "INSERT INTO clients "
                "(username,password,salt,company,contact_name,email,role,is_active) "
                "VALUES (?,?,?,?,?,?,?,1)",
                (email, pw_hash, salt, "MedPharma SC", "Eric", email, "admin"),
            )

    def _purge_legacy_placeholder_clients():
        """Hard-delete the legacy placeholder accounts (Luminary, TruPath) and
        the example/demo accounts that should never appear in production.

        Removes the client row, their practice sub-profiles, and any rows in
        tables that reference client_id so they cannot resurface in the
        Manage Clients list or the account selector. Real customer data is
        never touched — only the hardcoded placeholder usernames listed below.
        """
        placeholder_usernames = (
            "eric", "trupath", "luminary", "luminary_practice",
            "admin1", "staff1", "client1", "outsider",
            "admin@example.com", "staff@example.com", "client@example.com", "x@x.com",
        )
        placeholder_companies = (
            "Luminary (OMT/MHP)", "Luminary", "Luminary Practice",
            "TruPath", "Admin Co", "Demo Lab", "Outsider",
        )
        # Collect ids to remove
        cur.execute(
            "SELECT id FROM clients WHERE LOWER(username) IN ("
            + ",".join("?" * len(placeholder_usernames))
            + ") OR company IN ("
            + ",".join("?" * len(placeholder_companies))
            + ")",
            (*placeholder_usernames, *placeholder_companies),
        )
        ids = [r[0] for r in cur.fetchall()]
        if not ids:
            return
        qs = ",".join("?" * len(ids))
        # Best-effort cascade across tables that reference client_id. Wrapped
        # individually so a missing/legacy table does not abort the purge.
        for tbl in (
            "practice_profiles", "claims_master", "claim_notes", "claim_payments",
            "credentialing", "enrollments", "edi_setup", "providers",
            "documents", "production_log", "alerts", "audit_log",
            "client_access", "chat_room_members", "chat_messages", "chat_rooms",
            "notifications",
        ):
            try:
                cur.execute(f"DELETE FROM {tbl} WHERE client_id IN ({qs})", ids)
            except Exception:
                pass
        try:
            cur.execute(f"DELETE FROM clients WHERE id IN ({qs})", ids)
        except Exception as _e:
            log.warning("purge_legacy_placeholders: delete clients failed: %s", _e)

    def _purge_rcm_account():
        """One-shot: permanently delete the rcm / rcm@medprosc.com accounts
        from the live persistent-disk DB so they don't survive a redeploy.
        Best-effort cascade across every table that references client_id;
        individual failures are swallowed so startup cannot be broken.
        """
        try:
            cur.execute(
                "SELECT id FROM clients WHERE LOWER(username) IN ('rcm','rcm@medprosc.com')"
            )
            rcm_ids = [r[0] for r in cur.fetchall()]

            if rcm_ids:
                cur.execute("PRAGMA foreign_keys = OFF")
                qs = ",".join("?" * len(rcm_ids))
                for tbl in (
                    "practice_profiles", "claims_master", "payments", "notes_log",
                    "credentialing", "enrollment", "edi_setup", "providers",
                    "client_files", "sharefile_links", "report_notes",
                    "team_production", "audit_log", "activity_events",
                    "client_user_access", "chat_room_members", "chat_messages",
                    "chat_rooms", "notifications", "jobs",
                ):
                    try:
                        cur.execute(f"DELETE FROM {tbl} WHERE client_id IN ({qs})", rcm_ids)
                    except Exception:
                        pass
                try:
                    cur.execute(f"DELETE FROM clients WHERE id IN ({qs})", rcm_ids)
                except Exception as exc:
                    log.warning("purge_rcm_account: delete clients row failed: %s", exc)
                finally:
                    cur.execute("PRAGMA foreign_keys = ON")

            # Also remove team_production / user_presence / activity_events rows
            # keyed by username (not client_id) for either username form.
            for tbl in ("team_production", "user_presence", "activity_events"):
                try:
                    cur.execute(
                        f"DELETE FROM {tbl} WHERE LOWER(username) IN ('rcm','rcm@medprosc.com')"
                    )
                except Exception:
                    pass

            log.info("purge_rcm_account: RCM account purged from live DB (ids=%s)", rcm_ids)
        except Exception as exc:
            log.warning("purge_rcm_account: migration failed (non-fatal): %s", exc)

    _run_migration_once(conn, "legacy_profiles_v1", _fix_legacy_profiles)
    _run_migration_once(conn, "jessica_staff_v1", _migrate_jessica_staff)
    _run_migration_once(conn, "placeholder_clients_inactive_v1", _deactivate_placeholder_clients)
    _run_migration_once(conn, "luminary_profile_clear_v1", _clear_luminary_profile_fields)
    _run_migration_once(conn, "provision_susan_melissa_v1", _provision_susan_melissa)
    _run_migration_once(conn, "provision_eric_medprosc_v1", _provision_eric_medprosc)
    _run_migration_once(conn, "purge_legacy_placeholders_v1", _purge_legacy_placeholder_clients)
    _run_migration_once(conn, "purge_rcm_account_v1", _purge_rcm_account)

    # ── ALWAYS-ENSURE: real MedPharma team accounts ──────────────────────────
    # `_run_migration_once` records its key in `app_migrations` and never runs
    # again. If the row was later deleted by a different migration / manual
    # cleanup / fresh persistent-disk DB, we end up with the migration marked
    # "applied" but the user missing. For our core team accounts that is
    # unacceptable, so re-assert them on every startup. UPDATE-or-INSERT is
    # cheap and idempotent.
    _ensure_medpharma_team_accounts(cur)
    _ensure_bizdev_account(cur)
    conn.commit()


def _ensure_bizdev_account(cur):
    """Create / repair the Business Development (Victor) login.

    role='bizdev' drives the dedicated Leads sidebar + weekly reporting + chat.
    Idempotent: preserves an operator-changed password on existing rows, but
    always re-asserts role and the leads module set so the account can never
    silently lose its Business Development view (even on a fresh disk DB).
    """
    username = "victor@medprosc.com"
    mods = json.dumps([
        "leads_rcm", "leads_payor", "leads_workflow", "leads_compliance",
        "leads_combination", "leads_closed", "leadreport", "chat",
    ])
    row = cur.execute("SELECT id FROM clients WHERE username=?", (username,)).fetchone()
    if row:
        cur.execute(
            "UPDATE clients SET role='bizdev', company='MedPharma SC', "
            "contact_name=COALESCE(NULLIF(contact_name,''),'Victor'), "
            "email=?, enabled_modules=?, is_active=1 WHERE id=?",
            (username, mods, row[0]),
        )
    else:
        salt = secrets.token_hex(16)
        pw_hash = _hash_pw("victor123", salt)
        cur.execute(
            "INSERT INTO clients "
            "(username,password,salt,company,contact_name,email,role,enabled_modules,is_active) "
            "VALUES (?,?,?,?,?,?,?,?,1)",
            (username, pw_hash, salt, "MedPharma SC", "Victor", username, "bizdev", mods),
        )


def _ensure_medpharma_team_accounts(cur):
    """Upsert the canonical MedPharma SC team accounts on every startup.

    Runs after the one-shot migrations. Idempotent: only INSERTs missing
    rows, preserves any operator-changed password on existing rows.

    IMPORTANT: This function does NOT deactivate the legacy short-username
    duplicates ('admin', 'jessica', etc.). Doing so locked operators
    out (commit ee7faf6). Both username forms keep working as login
    aliases. De-duplication for the UI (chat picker, Manage Clients team
    list) happens at the API layer in list_chat_eligible_users() so the
    user only sees one entry per real person.

    Email addresses ARE force-updated to the canonical values on every
    startup — that's what the chat-invite emails, EOD report distribution
    and any future notifications depend on. Per operator: admin maps to
    lexi@medprosc.com because Lexi IS the admin.
    """
    team = [
        # (canonical username, role, contact, starter_password, notify_email)
        ("admin@medprosc.com",   "admin", "Lexi",    "admin123",   "lexi@medprosc.com"),
        ("eric@medprosc.com",    "admin", "Eric",    "eric123",    "eric@medprosc.com"),
        ("susan@medprosc.com",   "staff", "Susan",   "susan123",   "susan@medprosc.com"),
        ("melissa@medprosc.com", "staff", "Melissa", "melissa123", "melissa@medprosc.com"),
        ("jessica@medprosc.com", "staff", "Jessica", "jessica123", "jessica@medprosc.com"),
    ]
    for username, role, contact, pw, notify_email in team:
        row = cur.execute(
            "SELECT id, salt FROM clients WHERE username=?", (username,)
        ).fetchone()
        if row:
            cur.execute(
                "UPDATE clients SET role=?, company='MedPharma SC', "
                "contact_name=?, email=?, is_active=1 WHERE id=?",
                (role, contact, notify_email, row[0]),
            )
        else:
            salt = secrets.token_hex(16)
            pw_hash = _hash_pw(pw, salt)
            cur.execute(
                "INSERT INTO clients "
                "(username,password,salt,company,contact_name,email,role,is_active) "
                "VALUES (?,?,?,?,?,?,?,1)",
                (username, pw_hash, salt, "MedPharma SC", contact, notify_email, role),
            )

    # Self-heal: reactivate legacy short logins that an earlier buggy
    # version of this function (commit ee7faf6) deactivated. Operators
    # use 'admin' / etc. to sign in and must not be locked out.
    cur.execute(
        "UPDATE clients SET is_active=1 "
        "WHERE username IN ('admin','jessica','susan','melissa','eric') "
        "AND COALESCE(is_active,1)=0"
    )

    # Keep the email on the legacy short rows in sync too so reports/alerts
    # sent under the short username still reach the right inbox.
    legacy_email_map = {
        "admin":   "lexi@medprosc.com",
        "eric":    "eric@medprosc.com",
        "susan":   "susan@medprosc.com",
        "melissa": "melissa@medprosc.com",
        "jessica": "jessica@medprosc.com",
    }
    for uname, em in legacy_email_map.items():
        cur.execute(
            "UPDATE clients SET email=? WHERE username=?",
            (em, uname),
        )

    # Belt-and-suspenders: hard-delete any stray RCM rows that slipped
    # through (e.g. re-seeded from clients_seed.json before this guard ran).
    # Keeps RCM fully gone on every startup, not just after the one-time
    # purge_rcm_account_v1 migration.
    try:
        cur.execute(
            "DELETE FROM clients WHERE LOWER(username) IN ('rcm','rcm@medprosc.com')"
        )
    except Exception as _exc:
        log.warning("_ensure_medpharma_team_accounts: belt-and-suspenders RCM delete failed: %s", _exc)

    # Also purge RCM's stale data rows keyed by username on every startup.
    # The one-shot purge_rcm_account_v1 migration only runs once, so any
    # team_production / presence / activity rows that survived (or were
    # written before the purge) keep surfacing 'rcm' in the Team Production
    # report. Clearing them every startup removes RCM from production for good.
    for _tbl in ("team_production", "user_presence", "activity_events"):
        try:
            cur.execute(
                f"DELETE FROM {_tbl} WHERE LOWER(username) IN ('rcm','rcm@medprosc.com')"
            )
        except Exception:
            # Table may not exist on older schemas — non-fatal.
            pass



# ─── Schema ───────────────────────────────────────────────────────────────────

def init_client_hub_db():
    conn = get_db()
    cur = conn.cursor()
    cur.executescript("""
        -- ── users / auth ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS clients (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT UNIQUE NOT NULL,
            password         TEXT NOT NULL,
            salt             TEXT NOT NULL,
            must_change_password INTEGER DEFAULT 0,
            company          TEXT NOT NULL,
            contact_name     TEXT DEFAULT '',
            email            TEXT DEFAULT '',
            phone            TEXT DEFAULT '',
            role             TEXT DEFAULT 'client',
            is_active        INTEGER DEFAULT 1,
            created_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            last_login       TEXT,
            tax_id           TEXT DEFAULT '',
            group_npi        TEXT DEFAULT '',
            individual_npi   TEXT DEFAULT '',
            ptan_group       TEXT DEFAULT '',
            ptan_individual  TEXT DEFAULT '',
            address          TEXT DEFAULT '',
            specialty        TEXT DEFAULT '',
            notes            TEXT DEFAULT '',
            doc_tab_names    TEXT DEFAULT '',
            practice_type    TEXT DEFAULT '',
            report_tab_names TEXT DEFAULT '',
            enabled_modules  TEXT DEFAULT '',
            daily_report_optin INTEGER DEFAULT 1,
            report_recipients TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token         TEXT PRIMARY KEY,
            client_id     INTEGER NOT NULL,
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at    TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS password_setup_tokens (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id     INTEGER NOT NULL,
            token_hash    TEXT UNIQUE NOT NULL,
            created_by    TEXT DEFAULT '',
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at    TEXT NOT NULL,
            used_at       TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        -- ── claims main table (single source of truth) ────────────────
        CREATE TABLE IF NOT EXISTS claims_master (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id           INTEGER NOT NULL,
            ClaimKey            TEXT NOT NULL,          -- unique claim ID
            PatientID           TEXT DEFAULT '',
            PatientName         TEXT DEFAULT '',
            Payor               TEXT DEFAULT '',
            ProviderName        TEXT DEFAULT '',
            NPI                 TEXT DEFAULT '',
            DOS                 TEXT DEFAULT '',        -- date of service
            CPTCode             TEXT DEFAULT '',
            Description         TEXT DEFAULT '',
            ChargeAmount        REAL DEFAULT 0,
            AllowedAmount       REAL DEFAULT 0,
            AdjustmentAmount    REAL DEFAULT 0,
            PaidAmount          REAL DEFAULT 0,
            BalanceRemaining    REAL DEFAULT 0,
            ClaimStatus         TEXT DEFAULT 'Intake',
            -- status dates
            StatusStartDate     TEXT DEFAULT '',
            BillDate            TEXT DEFAULT '',
            DeniedDate          TEXT DEFAULT '',
            PaidDate            TEXT DEFAULT '',
            LastTouchedDate     TEXT DEFAULT '',
            -- workflow
            Owner               TEXT DEFAULT '',
            NextAction          TEXT DEFAULT '',
            NextActionDueDate   TEXT DEFAULT '',
            SLABreached         INTEGER DEFAULT 0,
            -- denial
            DenialCategory      TEXT DEFAULT '',
            DenialReason        TEXT DEFAULT '',
            AppealDate          TEXT DEFAULT '',
            AppealStatus        TEXT DEFAULT '',
            -- sub-profile (e.g. MHP or OMT for Luminary)
            sub_profile         TEXT DEFAULT '',
            -- who uploaded/imported this row (hub username)
            uploaded_by         TEXT DEFAULT '',
            -- meta
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, ClaimKey),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        -- ── payments: one claim can have multiple payments ─────────────
        CREATE TABLE IF NOT EXISTS payments (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       INTEGER NOT NULL,
            ClaimKey        TEXT NOT NULL,
            PostDate        TEXT DEFAULT '',
            PaymentAmount   REAL DEFAULT 0,
            AdjustmentAmount REAL DEFAULT 0,
            PayerType       TEXT DEFAULT '',   -- Primary, Secondary, Patient
            CheckNumber     TEXT DEFAULT '',
            ERA             TEXT DEFAULT '',
            Notes           TEXT DEFAULT '',
            PostedBy        TEXT DEFAULT '',   -- hub user who posted this payment
            sub_profile     TEXT DEFAULT '',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        -- ── notes log ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS notes_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id   INTEGER NOT NULL,
            ClaimKey    TEXT DEFAULT '',
            Module      TEXT DEFAULT 'Claim',  -- Claim, Credentialing, Enrollment, EDI
            RefID       INTEGER DEFAULT 0,
            Note        TEXT NOT NULL,
            Author      TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        -- ── providers ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS providers (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       INTEGER NOT NULL,
            ProviderName    TEXT NOT NULL,
            NPI             TEXT DEFAULT '',
            Specialty       TEXT DEFAULT '',
            TaxID           TEXT DEFAULT '',
            Email           TEXT DEFAULT '',
            Phone           TEXT DEFAULT '',
            Status          TEXT DEFAULT 'Active',
            StartDate       TEXT DEFAULT '',
            Notes           TEXT DEFAULT '',
            sub_profile     TEXT DEFAULT '',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        -- ── credentialing ─────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS credentialing (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id           INTEGER NOT NULL,
            provider_id         INTEGER,
            ProviderName        TEXT DEFAULT '',
            Payor               TEXT DEFAULT '',
            CredType            TEXT DEFAULT 'Initial',   -- Initial, Revalidation, Recredentialing
            Status              TEXT DEFAULT 'Not Started',
            SubmittedDate       TEXT DEFAULT '',
            FollowUpDate        TEXT DEFAULT '',
            ApprovedDate        TEXT DEFAULT '',
            ExpirationDate      TEXT DEFAULT '',
            Owner               TEXT DEFAULT '',
            Notes               TEXT DEFAULT '',
            sub_profile         TEXT DEFAULT '',
            uploaded_by         TEXT DEFAULT '',
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (provider_id) REFERENCES providers(id)
        );

        -- ── enrollment ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS enrollment (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id           INTEGER NOT NULL,
            provider_id         INTEGER,
            ProviderName        TEXT DEFAULT '',
            Payor               TEXT DEFAULT '',
            EnrollType          TEXT DEFAULT 'Enrollment', -- Enrollment, Disenrollment, Revalidation
            Status              TEXT DEFAULT 'Not Started',
            SubmittedDate       TEXT DEFAULT '',
            FollowUpDate        TEXT DEFAULT '',
            ApprovedDate        TEXT DEFAULT '',
            EffectiveDate       TEXT DEFAULT '',
            Owner               TEXT DEFAULT '',
            Notes               TEXT DEFAULT '',
            sub_profile         TEXT DEFAULT '',
            uploaded_by         TEXT DEFAULT '',
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (provider_id) REFERENCES providers(id)
        );

        -- ── EDI / ERA / EFT setup ──────────────────────────────────────
        CREATE TABLE IF NOT EXISTS edi_setup (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       INTEGER NOT NULL,
            provider_id     INTEGER,
            ProviderName    TEXT DEFAULT '',
            Payor           TEXT DEFAULT '',
            EDIStatus       TEXT DEFAULT 'Not Started',
            ERAStatus       TEXT DEFAULT 'Not Started',
            EFTStatus       TEXT DEFAULT 'Not Started',
            SubmittedDate   TEXT DEFAULT '',
            GoLiveDate      TEXT DEFAULT '',
            PayerID         TEXT DEFAULT '',
            Owner           TEXT DEFAULT '',
            Notes           TEXT DEFAULT '',
            sub_profile     TEXT DEFAULT '',
            uploaded_by     TEXT DEFAULT '',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (provider_id) REFERENCES providers(id)
        );

        -- ── indexes ───────────────────────────────────────────────────
        CREATE INDEX IF NOT EXISTS idx_claims_client   ON claims_master(client_id);
        CREATE INDEX IF NOT EXISTS idx_claims_status   ON claims_master(ClaimStatus);
        CREATE INDEX IF NOT EXISTS idx_claims_key      ON claims_master(ClaimKey);
        CREATE INDEX IF NOT EXISTS idx_payments_claim  ON payments(ClaimKey);
        CREATE INDEX IF NOT EXISTS idx_notes_claim     ON notes_log(ClaimKey);
        CREATE INDEX IF NOT EXISTS idx_cred_client     ON credentialing(client_id);
        CREATE INDEX IF NOT EXISTS idx_enroll_client   ON enrollment(client_id);
        CREATE INDEX IF NOT EXISTS idx_edi_client      ON edi_setup(client_id);
        CREATE INDEX IF NOT EXISTS idx_prov_client     ON providers(client_id);

        -- ── client file uploads ───────────────────────────────────────
        CREATE TABLE IF NOT EXISTS client_files (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id     INTEGER NOT NULL,
            filename      TEXT NOT NULL,
            original_name TEXT NOT NULL,
            file_type     TEXT DEFAULT 'other',
            file_size     INTEGER DEFAULT 0,
            category      TEXT DEFAULT 'General',
            description   TEXT DEFAULT '',
            status        TEXT DEFAULT 'Uploaded',
            row_count     INTEGER DEFAULT 0,
            uploaded_by   TEXT DEFAULT '',
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE INDEX IF NOT EXISTS idx_files_client    ON client_files(client_id);

        -- ── Sharefile / external document links ──────────────────────────
        CREATE TABLE IF NOT EXISTS sharefile_links (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id   INTEGER NOT NULL,
            label       TEXT NOT NULL,
            url         TEXT NOT NULL,
            category    TEXT DEFAULT 'General',
            added_by    TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE INDEX IF NOT EXISTS idx_sf_client ON sharefile_links(client_id);

        -- ── practice sub-profiles (e.g. MHP + OMT under Luminary) ────────────────────
        CREATE TABLE IF NOT EXISTS practice_profiles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       INTEGER NOT NULL,
            profile_name    TEXT NOT NULL,
            practice_type   TEXT DEFAULT '',
            specialty       TEXT DEFAULT '',
            tax_id          TEXT DEFAULT '',
            group_npi       TEXT DEFAULT '',
            individual_npi  TEXT DEFAULT '',
            ptan_group      TEXT DEFAULT '',
            ptan_individual TEXT DEFAULT '',
            address         TEXT DEFAULT '',
            contact_name    TEXT DEFAULT '',
            email           TEXT DEFAULT '',
            phone           TEXT DEFAULT '',
            notes           TEXT DEFAULT '',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, profile_name),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE INDEX IF NOT EXISTS idx_pp_client ON practice_profiles(client_id);

        -- ── Team production daily logs ─────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS team_production (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id     INTEGER NOT NULL,
            work_date     TEXT NOT NULL,
            username      TEXT NOT NULL,
            category      TEXT DEFAULT '',
            task_description TEXT DEFAULT '',
            quantity      INTEGER DEFAULT 0,
            time_spent    REAL DEFAULT 0,
            notes         TEXT DEFAULT '',
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE INDEX IF NOT EXISTS idx_tp_client ON team_production(client_id);
        CREATE INDEX IF NOT EXISTS idx_tp_date   ON team_production(work_date);

        -- ── Audit trail / activity log ────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id   INTEGER,
            username    TEXT DEFAULT '',
            action      TEXT NOT NULL,
            entity_type TEXT DEFAULT '',
            entity_id   INTEGER,
            details     TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_audit_client ON audit_log(client_id);
        CREATE INDEX IF NOT EXISTS idx_audit_time   ON audit_log(created_at);

        -- ── Business-development leads (sales pipeline) ───────────────────────
        CREATE TABLE IF NOT EXISTS leads (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            practice_name      TEXT DEFAULT '',
            contact_name       TEXT DEFAULT '',
            contact_email      TEXT DEFAULT '',
            contact_phone      TEXT DEFAULT '',
            service_rcm        INTEGER DEFAULT 0,
            service_payor      INTEGER DEFAULT 0,
            service_workflow   INTEGER DEFAULT 0,
            service_compliance INTEGER DEFAULT 0,
            status             TEXT DEFAULT 'New',
            est_value          REAL DEFAULT 0,
            owner              TEXT DEFAULT '',
            notes              TEXT DEFAULT '',
            is_closed          INTEGER DEFAULT 0,
            created_at         TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at         TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_leads_closed ON leads(is_closed);
        CREATE INDEX IF NOT EXISTS idx_leads_owner  ON leads(owner);

        -- ── Report notes (custom report tab content) ──────────────────────────
        CREATE TABLE IF NOT EXISTS report_notes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id   INTEGER NOT NULL,
            tab_name    TEXT NOT NULL,
            content     TEXT DEFAULT '',
            updated_by  TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, tab_name),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE INDEX IF NOT EXISTS idx_rn_client ON report_notes(client_id);

        -- ── Chat rooms (admin-managed) ───────────────────────────────────────
        CREATE TABLE IF NOT EXISTS chat_rooms (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            description TEXT DEFAULT '',
            client_id   INTEGER,                 -- optional: anchor room to a client account
            created_by  TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            archived    INTEGER DEFAULT 0,
            is_dm       INTEGER DEFAULT 0        -- 1 = private 1:1 direct-message room
        );
        CREATE INDEX IF NOT EXISTS idx_rooms_client ON chat_rooms(client_id);

        CREATE TABLE IF NOT EXISTS chat_room_members (
            room_id     INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            role        TEXT DEFAULT 'member',   -- 'admin' | 'member'
            added_by    TEXT DEFAULT '',
            added_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (room_id, user_id),
            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES clients(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_room_members_user ON chat_room_members(user_id);

        CREATE TABLE IF NOT EXISTS chat_messages (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id     INTEGER NOT NULL,
            sender_id   INTEGER,
            sender_name TEXT DEFAULT '',
            sender_role TEXT DEFAULT 'member',
            body        TEXT NOT NULL,
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_chatmsg_room ON chat_messages(room_id, created_at);

        CREATE TABLE IF NOT EXISTS chat_reads (
            room_id              INTEGER NOT NULL,
            user_id              INTEGER NOT NULL,
            last_read_message_id INTEGER DEFAULT 0,
            updated_at           TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (room_id, user_id),
            FOREIGN KEY (room_id) REFERENCES chat_rooms(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES clients(id) ON DELETE CASCADE
        );

        -- One row per (message × mentioned user) once a 2-hour "you were
        -- mentioned and haven't read it" reminder email has been sent, so we
        -- never email the same person twice for the same message.
        CREATE TABLE IF NOT EXISTS chat_reminders (
            message_id INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            sent_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (message_id, user_id)
        );

        -- One row per user tracking the highest message id we've already sent
        -- a "you have unread team chat" catch-up email about. Lets the 15-min
        -- nudge fire once per wave of unread messages (never per-message spam),
        -- and never re-nudge once the person has caught up.
        CREATE TABLE IF NOT EXISTS chat_catchup_state (
            user_id                  INTEGER PRIMARY KEY,
            last_reminded_message_id INTEGER DEFAULT 0,
            sent_at                  TEXT DEFAULT CURRENT_TIMESTAMP
        );

        -- ── In-app notifications (decoupled from email) ─────────────────────
        -- One row per (recipient × event). Lets the hub show "you've been
        -- invited / you have a new message / your EOD report is ready" even
        -- when SendGrid/SMTP isn't configured. PHI-safe: body is just a
        -- short marker (e.g. "[chat message • 47 chars]") — never the real
        -- chat text.
        CREATE TABLE IF NOT EXISTS notifications (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            kind         TEXT NOT NULL,
            title        TEXT NOT NULL,
            body         TEXT DEFAULT '',
            link         TEXT DEFAULT '',
            related_type TEXT DEFAULT '',
            related_id   INTEGER,
            is_read      INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            read_at      TEXT,
            FOREIGN KEY (user_id) REFERENCES clients(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_notif_user_unread
            ON notifications(user_id, is_read, created_at);
        CREATE INDEX IF NOT EXISTS idx_notif_user_time
            ON notifications(user_id, created_at);

        -- ── EOD report archive (persists even if email delivery fails) ──────
        CREATE TABLE IF NOT EXISTS eod_reports (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date      TEXT NOT NULL,
            generated_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            generated_by     TEXT DEFAULT 'scheduled',
            headlines_json   TEXT DEFAULT '{}',
            summary_json     TEXT DEFAULT '{}',
            html_body        TEXT DEFAULT '',
            text_body        TEXT DEFAULT '',
            email_status     TEXT DEFAULT '',
            email_recipients TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_eod_reports_date
            ON eod_reports(report_date);

        -- ── App settings (encrypted secrets configurable from the hub UI) ──
        -- Lets the admin paste credentials (SendGrid API key, SMTP password,
        -- etc.) into the UI without ever touching Render env vars. Values
        -- are Fernet-encrypted at rest via app.security so they are never
        -- readable from the SQLite file alone.
        CREATE TABLE IF NOT EXISTS app_settings (
            key         TEXT PRIMARY KEY,
            value_enc   TEXT NOT NULL,
            updated_by  TEXT DEFAULT '',
            updated_at  TEXT DEFAULT CURRENT_TIMESTAMP
        );

        -- ── Client access grants (which staff/admin users can open a given client) ──
        -- Used by /accounts to filter the selector for staff users.
        -- Admins always see every client regardless of rows here.
        CREATE TABLE IF NOT EXISTS client_user_access (
            client_id   INTEGER NOT NULL,
            user_id     INTEGER NOT NULL,
            granted_by  TEXT DEFAULT '',
            granted_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (client_id, user_id),
            FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES clients(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_cua_user ON client_user_access(user_id);
        CREATE INDEX IF NOT EXISTS idx_cua_client ON client_user_access(client_id);

        -- ── Activity events (timestamped team-tracking firehose) ─────────────
        CREATE TABLE IF NOT EXISTS activity_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            username      TEXT NOT NULL,
            client_id     INTEGER,
            event_type    TEXT NOT NULL,
            method        TEXT DEFAULT '',
            path          TEXT DEFAULT '',
            status_code   INTEGER,
            duration_ms   INTEGER,
            ip            TEXT DEFAULT '',
            user_agent    TEXT DEFAULT '',
            details       TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_ae_user_time ON activity_events(username, occurred_at);
        CREATE INDEX IF NOT EXISTS idx_ae_time      ON activity_events(occurred_at);
        CREATE INDEX IF NOT EXISTS idx_ae_type      ON activity_events(event_type);

        -- ── Per-user-per-day presence rollup (ActivTrak-style) ──────────────
        CREATE TABLE IF NOT EXISTS user_presence (
            username       TEXT NOT NULL,
            work_date      TEXT NOT NULL,
            first_seen_at  TEXT,
            last_seen_at   TEXT,
            active_seconds INTEGER DEFAULT 0,
            idle_seconds   INTEGER DEFAULT 0,
            action_count   INTEGER DEFAULT 0,
            PRIMARY KEY (username, work_date)
        );
        CREATE INDEX IF NOT EXISTS idx_up_date ON user_presence(work_date);

        -- ── async jobs ─────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS jobs (
            id            TEXT PRIMARY KEY,
            account_id    INTEGER,
            job_type      TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'queued',
            progress      INTEGER DEFAULT 0,
            eta_seconds   INTEGER,
            latest_error  TEXT DEFAULT '',
            payload_json  TEXT DEFAULT '{}',
            result_json   TEXT DEFAULT '{}',
            created_by    TEXT DEFAULT '',
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            started_at    TEXT,
            finished_at   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status      ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_account     ON jobs(account_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_created_at  ON jobs(created_at);

        CREATE TABLE IF NOT EXISTS job_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id      TEXT NOT NULL,
            stage       TEXT DEFAULT '',
            level       TEXT DEFAULT 'info',
            message     TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, created_at);
    """)
    conn.commit()

    # ── Migrate existing DBs: add profile columns if missing ──────────────
    profile_cols = [
        ("must_change_password", "INTEGER DEFAULT 0"),
        ("tax_id", "TEXT DEFAULT ''"),
        ("group_npi", "TEXT DEFAULT ''"),
        ("individual_npi", "TEXT DEFAULT ''"),
        ("ptan_group", "TEXT DEFAULT ''"),
        ("ptan_individual", "TEXT DEFAULT ''"),
        ("address", "TEXT DEFAULT ''"),
        ("specialty", "TEXT DEFAULT ''"),
        ("notes", "TEXT DEFAULT ''"),
        ("doc_tab_names", "TEXT DEFAULT ''"),
        ("practice_type", "TEXT DEFAULT ''"),
        ("report_tab_names", "TEXT DEFAULT ''"),
        ("enabled_modules", "TEXT DEFAULT ''"),
        ("daily_report_optin", "INTEGER DEFAULT 1"),
        ("report_recipients", "TEXT DEFAULT ''"),
    ]
    cur.execute("PRAGMA table_info(clients)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col, col_def in profile_cols:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE clients ADD COLUMN {col} {col_def}")
    conn.commit()

    # ── Migrate existing DBs: production-log attachments ─────────────────
    cur.execute("PRAGMA table_info(team_production)")
    tp_cols = {row[1] for row in cur.fetchall()}
    for col, col_def in (
        ("attachment_file_id", "INTEGER"),
        ("attachment_name", "TEXT DEFAULT ''"),
    ):
        if col not in tp_cols:
            cur.execute(f"ALTER TABLE team_production ADD COLUMN {col} {col_def}")
    conn.commit()

    # ── Migrate existing DBs: payment posting attribution ────────────────
    cur.execute("PRAGMA table_info(payments)")
    pay_cols = {row[1] for row in cur.fetchall()}
    if "PostedBy" not in pay_cols:
        cur.execute("ALTER TABLE payments ADD COLUMN PostedBy TEXT DEFAULT ''")
    conn.commit()

    # ── Migrate existing DBs: 1:1 direct-message chat rooms ──────────────
    cur.execute("PRAGMA table_info(chat_rooms)")
    cr_cols = {row[1] for row in cur.fetchall()}
    if "is_dm" not in cr_cols:
        cur.execute("ALTER TABLE chat_rooms ADD COLUMN is_dm INTEGER DEFAULT 0")
    conn.commit()

    # ── Migrate existing DBs: BizDev lead follow-up tracking ─────────────
    cur.execute("PRAGMA table_info(leads)")
    lead_cols = {row[1] for row in cur.fetchall()}
    for col, col_def in (
        ("last_follow_up_at", "TEXT DEFAULT ''"),
        ("last_reminder_at", "TEXT DEFAULT ''"),
        ("deleted_at", "TEXT DEFAULT ''"),
    ):
        if col not in lead_cols:
            cur.execute(f"ALTER TABLE leads ADD COLUMN {col} {col_def}")
    conn.commit()

    # ── Migrate existing DBs: add expires_at column to sessions ──────────
    cur.execute("PRAGMA table_info(sessions)")
    session_cols = {row[1] for row in cur.fetchall()}
    if "expires_at" not in session_cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN expires_at TEXT")
    conn.commit()

    # ── Migrate existing DBs: add sub_profile column to data tables ───────
    sub_profile_tables = ["claims_master", "payments", "providers",
                          "credentialing", "enrollment", "edi_setup"]
    for tbl in sub_profile_tables:
        cur.execute(f"PRAGMA table_info({tbl})")
        cols = {row[1] for row in cur.fetchall()}
        if "sub_profile" not in cols:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN sub_profile TEXT DEFAULT ''")
    conn.commit()

    # ── Migrate existing DBs: per-user upload attribution on data tables ──
    # Records which hub user uploaded each imported row so the compiled Team
    # Production report can break work down per user (not just per account).
    uploaded_by_tables = ["claims_master", "credentialing", "enrollment", "edi_setup"]
    for tbl in uploaded_by_tables:
        cur.execute(f"PRAGMA table_info({tbl})")
        cols = {row[1] for row in cur.fetchall()}
        if "uploaded_by" not in cols:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN uploaded_by TEXT DEFAULT ''")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM clients")
    total = cur.fetchone()[0]

    if total == 0:
        _seed_data(conn)
    else:
        _apply_startup_user_migrations(conn)

    # ── ALWAYS ensure the canonical MedPharma SC team is present ─────────────
    # Lives OUTSIDE the if/else so it runs even on a fresh DB (right after
    # _seed_data) and even if _apply_startup_user_migrations was bypassed
    # for any reason. Idempotent: only INSERTs missing rows, preserves any
    # existing passwords.
    try:
        cur2 = conn.cursor()
        _ensure_medpharma_team_accounts(cur2)
        _ensure_bizdev_account(cur2)
        conn.commit()
    except Exception as _team_e:
        log.error("ensure_medpharma_team_accounts at startup failed: %s", _team_e)

    # ── Apply any password resets from environment variables ─────────────────
    # Set RESET_PW_<username>=<newpassword> in Render env vars to force-reset
    # a password on next startup. Remove the env var after logging in.
    import os as _os_pw
    for key, val in _os_pw.environ.items():
        if key.startswith("RESET_PW_") and val.strip():
            uname = key[len("RESET_PW_"):].lower()
            new_salt = secrets.token_hex(16)
            new_hash = _hash_pw(val.strip(), new_salt)
            conn.execute("UPDATE clients SET password=?, salt=? WHERE username=?",
                         (new_hash, new_salt, uname))
            conn.commit()
            log.info("startup: reset password for user '%s' via RESET_PW_ env var", uname)

    # ── Re-seed clients from data/clients_seed.json ───────────────────────────
    # Any real client added via Manage Clients is written to this JSON file so
    # it survives Render deploys (which wipe the SQLite DB every time).
    for entry in _load_clients_seed():
        try:
            _upsert_client_from_seed(conn, entry)
        except Exception as _e:
            log.warning("clients_seed.json upsert failed for %s: %s", entry.get("username"), _e)
    conn.commit()

    conn.close()


# ─── Seed data ────────────────────────────────────────────────────────────────

def _seed_data(conn):
    cur = conn.cursor()

    # Admin
    asalt = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("admin", _hash_pw("admin123", asalt), asalt, "MedPharma SC", "Admin", "admin@medpharmasc.com", "admin")
    )

    # Jessica — MedPharma operations staff (kept as a working staff login).
    jsalt = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("jessica", _hash_pw("jessica123", jsalt), jsalt, "MedPharma SC", "Jessica", "", "staff")
    )

    # NOTE: Real client accounts (Luminary, TruPath, etc.) are no longer
    # hardcoded here. Use Manage Clients in the UI — anything created there
    # is persisted to data/clients_seed.json and survives Render deploys.

    conn.commit()
    # No fake claims, providers, credentialing, or payments seeded.
    # All data is imported via Excel/CSV file uploads.


# ─── Auth ─────────────────────────────────────────────────────────────────────

def force_set_password(username: str, new_password: str) -> dict:
    """Admin-only: overwrite a user's password + salt, clear must_change_password.
    Returns a status dict describing what happened. Does NOT require knowing
    the old password.
    """
    uname = (username or "").strip().lower()
    pw = (new_password or "").strip()
    if not uname or not pw:
        return {"ok": False, "error": "username and new_password required"}
    conn = get_db()
    try:
        _ensure_auth_columns(conn)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, username, is_active FROM clients WHERE username=?", (uname,)
        ).fetchone()
        if not row:
            return {"ok": False, "error": f"no user with username '{uname}'"}
        salt = secrets.token_hex(16)
        pw_hash = _hash_pw(pw, salt)
        cur.execute(
            "UPDATE clients SET password=?, salt=?, is_active=1, "
            "must_change_password=0 WHERE id=?",
            (pw_hash, salt, row["id"]),
        )
        conn.commit()
        # Verify roundtrip
        check = cur.execute(
            "SELECT password, salt FROM clients WHERE id=?", (row["id"],)
        ).fetchone()
        ok = (_hash_pw(pw, check["salt"]) == check["password"])
        return {
            "ok": ok,
            "user_id": row["id"],
            "username": uname,
            "previous_is_active": row["is_active"],
            "hash_roundtrip_ok": ok,
        }
    finally:
        conn.close()


def authenticate(username: str, password: str):
    conn = get_db()
    try:
        _ensure_auth_columns(conn)
        cur = conn.cursor()
        uname = (username or "").strip()
        # Primary match: exact username.
        cur.execute("SELECT * FROM clients WHERE username=? AND is_active=1", (uname,))
        row = cur.fetchone()
        if not row:
            # Fallback: let users sign in with the email on file too
            # (case-insensitive), so e.g. "admin@medprosc.com" works as well
            # as the "admin" username.
            cur.execute(
                "SELECT * FROM clients WHERE LOWER(email)=LOWER(?) AND is_active=1 "
                "ORDER BY id LIMIT 1", (uname,))
            row = cur.fetchone()
        if not row:
            return None, None
        c = dict(row)
        if _hash_pw(password, c["salt"]) != c["password"]:
            return None, None
        token = secrets.token_urlsafe(32)
        expires = (datetime.now() + timedelta(days=30)).isoformat()
        cur.execute("INSERT INTO sessions (token,client_id,expires_at) VALUES (?,?,?)",
                    (token, c["id"], expires))
        cur.execute("UPDATE clients SET last_login=? WHERE id=?",
                    (datetime.now().isoformat(), c["id"]))
        conn.commit()
        out = {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role", "practice_type")}
        out["must_change_password"] = bool(c.get("must_change_password", 0))
        raw_mods = (c.get("enabled_modules") or "").strip()
        if raw_mods:
            try:
                out["enabled_modules"] = json.loads(raw_mods)
            except Exception:
                out["enabled_modules"] = DEFAULT_ENABLED_MODULES[:]
        else:
            out["enabled_modules"] = DEFAULT_ENABLED_MODULES[:]
        return out, token
    finally:
        conn.close()


def validate_session(token: str):
    if not token:
        return None
    conn = get_db()
    try:
        _ensure_auth_columns(conn)
        cur = conn.cursor()
        cur.execute("""SELECT c.* FROM sessions s
                       JOIN clients c ON c.id=s.client_id
                       WHERE s.token=? AND c.is_active=1
                       AND (s.expires_at IS NULL OR s.expires_at > ?)""",
                    (token, datetime.now().isoformat()))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    c = dict(row)
    out = {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role", "practice_type")}
    out["must_change_password"] = bool(c.get("must_change_password", 0))
    raw_mods = (c.get("enabled_modules") or "").strip()
    if raw_mods:
        try:
            out["enabled_modules"] = json.loads(raw_mods)
        except Exception:
            out["enabled_modules"] = DEFAULT_ENABLED_MODULES[:]
    else:
        out["enabled_modules"] = DEFAULT_ENABLED_MODULES[:]
    return out


def logout_session(token: str):
    conn = get_db()
    try:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))
        conn.commit()
    finally:
        conn.close()


# ─── Clients (admin) ──────────────────────────────────────────────────────────

def list_clients():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id,username,company,contact_name,email,phone,role,is_active,created_at,last_login,practice_type,enabled_modules FROM clients ORDER BY company")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def _auto_username(company: str, conn) -> str:
    """Derive a unique username slug from company name."""
    import re
    slug = re.sub(r"[^a-z0-9]", "", company.lower())[:16] or "client"
    base = slug
    suffix = 1
    while True:
        candidate = f"{base}{suffix:04d}" if suffix > 1 else base
        row = conn.execute("SELECT 1 FROM clients WHERE username=?", (candidate,)).fetchone()
        if not row:
            return candidate
        suffix += 1


def create_client(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        # Auto-generate credentials if not supplied
        username = (data.get("username") or "").strip() or _auto_username(data.get("company", "client"), conn)
        raw_password = (data.get("password") or "").strip() or secrets.token_urlsafe(12)
        salt = secrets.token_hex(16)
        service_type = (data.get("service_type") or "").strip()
        cur.execute(
            """INSERT INTO clients
                    (username, password, salt, company, contact_name, email, phone, role, practice_type, notes)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (username, _hash_pw(raw_password, salt), salt,
             data.get("company", ""), data.get("contact_name", ""),
             data.get("email", ""), data.get("phone", ""),
                 data.get("role", "client"), service_type, data.get("notes", "")),
        )
        conn.commit()
        cid = cur.lastrowid
    finally:
        conn.close()

    try:
        # Persist to clients_seed.json so client survives Render deploys
        _persist_client_to_seed({
            "username": username,
            "company": data.get("company", ""),
            "contact_name": data.get("contact_name", ""),
            "email": data.get("email", ""),
            "phone": data.get("phone", ""),
            "role": data.get("role", "client"),
            "service_type": service_type,
            "notes": data.get("notes", ""),
        })
    except Exception:
        rollback = get_db()
        try:
            rollback.execute("DELETE FROM clients WHERE id=?", [cid])
            rollback.commit()
        finally:
            rollback.close()
        raise
    # Stash the credentials the route just used so the API layer can show them
    # once to the admin (so a real client login can be handed off).
    data["_created_username"] = username
    data["_created_password"] = raw_password
    return cid


def create_user_invite(data: dict, invited_by: str, ttl_hours: int = 72) -> dict:
    """Create a user account and issue a one-time password setup token."""
    conn = get_db()
    try:
        cur = conn.cursor()
        _ensure_password_setup_table(conn)
        company = (data.get("company") or "").strip()
        contact_name = (data.get("contact_name") or "").strip()
        email = (data.get("email") or "").strip().lower()
        phone = (data.get("phone") or "").strip()
        role = (data.get("role") or "client").strip().lower()
        if role not in ("client", "staff", "admin"):
            role = "client"

        username = (data.get("username") or "").strip().lower()
        if not username:
            base = contact_name or company or (email.split("@", 1)[0] if "@" in email else "user")
            username = _auto_username(base, conn)

        # Random placeholder password; user must set via token.
        salt = secrets.token_hex(16)
        temp_pw = secrets.token_urlsafe(24)
        cur.execute(
            """INSERT INTO clients
               (username, password, salt, company, contact_name, email, phone, role, practice_type)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                username,
                _hash_pw(temp_pw, salt),
                salt,
                company or (contact_name or username),
                contact_name,
                email,
                phone,
                role,
                (data.get("service_type") or "").strip(),
            ),
        )
        client_id = cur.lastrowid

        # Invalidate any outstanding setup tokens for this user.
        cur.execute(
            "UPDATE password_setup_tokens SET used_at=? WHERE client_id=? AND used_at IS NULL",
            (datetime.now().isoformat(), client_id),
        )

        raw_token = secrets.token_urlsafe(48)
        token_hash = _hash_token(raw_token)
        expires_at = (datetime.now() + timedelta(hours=max(1, int(ttl_hours)))).isoformat()
        cur.execute(
            """INSERT INTO password_setup_tokens (client_id, token_hash, created_by, expires_at)
               VALUES (?,?,?,?)""",
            (client_id, token_hash, invited_by or "", expires_at),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "client_id": client_id,
        "username": username,
        "email": email,
        "contact_name": contact_name,
        "company": company,
        "role": role,
        "token": raw_token,
        "expires_at": expires_at,
    }


def get_password_setup_token_info(token: str) -> dict | None:
    if not token:
        return None
    conn = get_db()
    try:
        cur = conn.cursor()
        _ensure_password_setup_table(conn)
        cur.execute(
            """SELECT t.id, t.client_id, t.expires_at, t.used_at,
                      c.username, c.contact_name, c.email, c.company, c.role
               FROM password_setup_tokens t
               JOIN clients c ON c.id = t.client_id
               WHERE t.token_hash = ?""",
            (_hash_token(token),),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return None
    info = dict(row)
    if info.get("used_at"):
        return None
    if info.get("expires_at") and info["expires_at"] <= datetime.now().isoformat():
        return None
    return info


def consume_password_setup_token(token: str, new_password: str) -> dict | None:
    """Set password for token's user and consume all outstanding tokens."""
    if not token or not new_password:
        return None

    conn = get_db()
    try:
        cur = conn.cursor()
        _ensure_auth_columns(conn)
        _ensure_password_setup_table(conn)
        cur.execute(
            """SELECT t.id, t.client_id, t.expires_at, t.used_at, c.username
               FROM password_setup_tokens t
               JOIN clients c ON c.id=t.client_id
               WHERE t.token_hash=?""",
            (_hash_token(token),),
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("used_at"):
            return None
        if d.get("expires_at") and d["expires_at"] <= datetime.now().isoformat():
            return None

        salt = secrets.token_hex(16)
        pw_hash = _hash_pw(new_password, salt)
        now_iso = datetime.now().isoformat()
        cur.execute("UPDATE clients SET password=?, salt=?, must_change_password=0 WHERE id=?", (pw_hash, salt, d["client_id"]))
        cur.execute("UPDATE sessions SET expires_at=? WHERE client_id=?", (now_iso, d["client_id"]))
        cur.execute(
            "UPDATE password_setup_tokens SET used_at=? WHERE client_id=? AND used_at IS NULL",
            (now_iso, d["client_id"]),
        )
        conn.commit()
        return {"client_id": d["client_id"], "username": d["username"]}
    finally:
        conn.close()


def set_must_change_password(client_id: int, required: bool = True):
    conn = get_db()
    try:
        _ensure_auth_columns(conn)
        conn.execute(
            "UPDATE clients SET must_change_password=? WHERE id=?",
            (1 if required else 0, client_id),
        )
        conn.commit()
    finally:
        conn.close()


def change_password_with_current(client_id: int, current_password: str, new_password: str) -> bool:
    if not client_id or not current_password or not new_password:
        return False
    conn = get_db()
    try:
        _ensure_auth_columns(conn)
        cur = conn.cursor()
        cur.execute("SELECT password, salt FROM clients WHERE id=?", (client_id,))
        row = cur.fetchone()
        if not row:
            return False
        d = dict(row)
        if _hash_pw(current_password, d.get("salt", "")) != d.get("password", ""):
            return False
        salt = secrets.token_hex(16)
        cur.execute(
            "UPDATE clients SET password=?, salt=?, must_change_password=0 WHERE id=?",
            (_hash_pw(new_password, salt), salt, client_id),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def _persist_client_to_seed(entry: dict):
    """Append or update a client entry in clients_seed.json."""
    existing = _load_clients_seed()
    sanitized_entry = _sanitize_seed_entry(entry)
    username = sanitized_entry["username"]
    updated = False
    for i, e in enumerate(existing):
        if (e.get("username") or "").strip().lower() == username:
            existing[i] = sanitized_entry
            updated = True
            break
    if not updated:
        existing.append(sanitized_entry)
    _save_clients_seed(existing)


def _remove_client_from_seed(username: str):
    """Remove a client entry from clients_seed.json by username."""
    normalized = (username or "").strip().lower()
    if not normalized:
        return
    existing = _load_clients_seed()
    filtered = [
        entry for entry in existing
        if (entry.get("username") or "").strip().lower() != normalized
    ]
    if len(filtered) != len(existing):
        _save_clients_seed(filtered)


def _sync_client_to_seed(cid: int):
    """Update an existing seeded client entry after DB edits.

    Only touches usernames that already exist in clients_seed.json so internal
    bootstrap users do not get added implicitly.
    """
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT username, company, contact_name, email, phone, role, practice_type, notes "
            "FROM clients WHERE id=?",
            [cid],
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return

    current = dict(row)
    username = (current.get("username") or "").strip().lower()
    if not username:
        return

    existing = _load_clients_seed()
    for index, entry in enumerate(existing):
        if (entry.get("username") or "").strip().lower() != username:
            continue
        updated = {
            "username": current.get("username", ""),
            "company": current.get("company", ""),
            "contact_name": current.get("contact_name", ""),
            "email": current.get("email", ""),
            "phone": current.get("phone", ""),
            "role": current.get("role", "client"),
            "service_type": current.get("practice_type", ""),
            "notes": current.get("notes", ""),
        }
        existing[index] = updated
        _save_clients_seed(existing)
        break



def update_client(cid: int, data: dict):
    previous = _get_client_snapshot(cid)
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["company", "contact_name", "email", "phone", "role", "is_active",
                   "tax_id", "group_npi", "individual_npi", "ptan_group", "ptan_individual",
                   "address", "specialty", "notes", "doc_tab_names", "practice_type",
                   "report_tab_names", "enabled_modules",
                   "daily_report_optin", "report_recipients"]
        parts, params = [], []
        for f in allowed:
            if f in data and data[f] is not None:
                parts.append(f"{f}=?")
                params.append(data[f])
        if "password" in data and data["password"]:
            salt = secrets.token_hex(16)
            parts += ["password=?", "salt=?"]
            params += [_hash_pw(data["password"], salt), salt]
        if parts:
            params.append(cid)
            cur.execute(f"UPDATE clients SET {','.join(parts)} WHERE id=?", params)
            conn.commit()
    finally:
        conn.close()
    try:
        _sync_client_to_seed(cid)
    except Exception:
        if previous is not None:
            restore = dict(previous)
            if "password" in data and data["password"]:
                restore.pop("password", None)
            rollback = get_db()
            try:
                cols = []
                vals = []
                for key, value in restore.items():
                    cols.append(f"{key}=?")
                    vals.append(value)
                vals.append(cid)
                rollback.execute(f"UPDATE clients SET {','.join(cols)} WHERE id=?", vals)
                rollback.commit()
            finally:
                rollback.close()
        raise


def delete_client(cid: int):
    """Hard-delete a client and every dependent row across the schema.

    Dynamically discovers every user-table that references the client (via a
    ``client_id`` or ``account_id`` column) so newly added tables don't silently
    block deletion with a FOREIGN KEY error.
    """
    conn = get_db()
    try:
        row = conn.execute("SELECT username FROM clients WHERE id=?", [cid]).fetchone()
        if not row:
            # Nothing to delete; treat as success so the UI clears the row.
            return
        username = row["username"] or ""

        # Discover all tables and the columns that point at clients.
        tables = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        candidate_cols = ("client_id", "account_id")
        cleanup: list[tuple[str, str]] = []
        for table in tables:
            if table == "clients":
                continue
            try:
                cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            except sqlite3.OperationalError:
                continue
            for col in candidate_cols:
                if col in cols:
                    cleanup.append((table, col))

        # Disable FK enforcement for the duration of the delete so the cascade
        # order doesn't matter even if future tables reference clients(id).
        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            for table, col in cleanup:
                try:
                    conn.execute(f"DELETE FROM {table} WHERE {col}=?", [cid])
                except sqlite3.OperationalError as exc:
                    log.warning("delete_client: skipping %s.%s — %s", table, col, exc)
            conn.execute("DELETE FROM clients WHERE id=?", [cid])
            conn.commit()
        finally:
            try:
                conn.execute("PRAGMA foreign_keys = ON")
            except Exception:
                pass
    finally:
        conn.close()

    # Seed-file maintenance must never block account removal — log and move on.
    try:
        _remove_client_from_seed(username)
    except Exception as exc:
        log.error("delete_client: failed to update clients_seed.json for %s: %s", username, exc)


DEFAULT_DOC_TABS = ["Payor Letters", "Company Documents", "Credentialing Docs", "Reports", "General"]
DEFAULT_REPORT_TABS = ["Claims"]
DEFAULT_ENABLED_MODULES = [
    "claims",
    "credentialing",
    "enrollment",
    "edi",
    "providers",
    "reporting",
    "production",
    "documents",
    "chat",
]


def get_profile(client_id: int) -> dict:
    import json as _json
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT company, contact_name, email, phone,
                   tax_id, group_npi, individual_npi, ptan_group, ptan_individual,
                 address, specialty, notes, doc_tab_names, practice_type, report_tab_names, enabled_modules,
                 daily_report_optin, report_recipients
            FROM clients WHERE id=?""", [client_id])
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    cols = ["company", "contact_name", "email", "phone", "tax_id", "group_npi",
            "individual_npi", "ptan_group", "ptan_individual", "address", "specialty", "notes",
            "doc_tab_names", "practice_type", "report_tab_names", "enabled_modules",
            "daily_report_optin", "report_recipients"]
    d = {c: (row[i] if row[i] is not None else "") for i, c in enumerate(cols)}
    try:
        d["doc_tabs"] = _json.loads(d["doc_tab_names"]) if d["doc_tab_names"] else DEFAULT_DOC_TABS[:]
    except Exception:
        d["doc_tabs"] = DEFAULT_DOC_TABS[:]
    try:
        d["report_tabs"] = _json.loads(d["report_tab_names"]) if d["report_tab_names"] else DEFAULT_REPORT_TABS[:]
    except Exception:
        d["report_tabs"] = DEFAULT_REPORT_TABS[:]
    try:
        d["enabled_modules"] = _json.loads(d["enabled_modules"]) if d["enabled_modules"] else DEFAULT_ENABLED_MODULES[:]
    except Exception:
        d["enabled_modules"] = DEFAULT_ENABLED_MODULES[:]
    # Daily-report opt-in defaults ON when the client has an email on file.
    try:
        d["daily_report_optin"] = int(d["daily_report_optin"] if d["daily_report_optin"] != "" else 1)
    except Exception:
        d["daily_report_optin"] = 1
    try:
        d["report_recipients"] = (
            _json.loads(d["report_recipients"]) if d["report_recipients"] else []
        )
    except Exception:
        d["report_recipients"] = []
    return d


def update_profile(client_id: int, data: dict):
    import json as _json
    allowed = ["company", "contact_name", "email", "phone", "tax_id", "group_npi",
               "individual_npi", "ptan_group", "ptan_individual", "address", "specialty", "notes",
               "doc_tab_names", "practice_type", "report_tab_names", "enabled_modules",
               "daily_report_optin", "report_recipients"]
    payload = {}
    for k, v in (data or {}).items():
        if k not in allowed:
            continue
        if k == "report_recipients" and isinstance(v, (list, tuple)):
            payload[k] = _json.dumps([str(x).strip() for x in v if str(x).strip()])
        elif k == "daily_report_optin":
            payload[k] = 1 if (str(v).lower() in ("1", "true", "yes", "on") or v is True) else 0
        else:
            payload[k] = v
    update_client(client_id, payload)


def get_practice_profiles(client_id: int) -> list:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM practice_profiles WHERE client_id=? ORDER BY profile_name", [client_id])
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def upsert_practice_profile(client_id: int, profile_name: str, data: dict):
    allowed = ["practice_type", "specialty", "tax_id", "group_npi", "individual_npi",
               "ptan_group", "ptan_individual", "address", "contact_name", "email", "phone", "notes"]
    fields = [f for f in allowed if f in data and data[f] is not None]
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM practice_profiles WHERE client_id=? AND profile_name=?",
                    [client_id, profile_name])
        row = cur.fetchone()
        if row:
            if fields:
                sets = ", ".join(f + "=?" for f in fields) + ", updated_at=?"
                vals = [data[f] for f in fields] + [datetime.now().isoformat(), row[0]]
                cur.execute(f"UPDATE practice_profiles SET {sets} WHERE id=?", vals)
        else:
            cols = "client_id, profile_name" + (", " + ", ".join(fields) if fields else "")
            placeholders = "?,?" + (", " + ",".join("?" * len(fields)) if fields else "")
            vals = [client_id, profile_name] + [data[f] for f in fields]
            cur.execute(f"INSERT INTO practice_profiles ({cols}) VALUES ({placeholders})", vals)
        conn.commit()
    finally:
        conn.close()


def delete_practice_profile(pp_id: int, client_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM practice_profiles WHERE id=? AND client_id=?", [pp_id, client_id])
        conn.commit()
    finally:
        conn.close()


# ─── Providers ────────────────────────────────────────────────────────────────

def list_providers(client_id: int = None, sub_profile: str = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM providers WHERE 1=1"
        params = []
        if client_id is not None:
            q += " AND client_id=?"
            params.append(client_id)
        if sub_profile:
            q += " AND sub_profile=?"
            params.append(sub_profile)
        q += " ORDER BY ProviderName"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def create_provider(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO providers (client_id,ProviderName,NPI,Specialty,TaxID,Email,Phone,Status,StartDate,Notes,sub_profile)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (data["client_id"], data["ProviderName"], data.get("NPI", ""),
                     data.get("Specialty", ""), data.get("TaxID", ""), data.get("Email", ""),
                     data.get("Phone", ""), data.get("Status", "Active"),
                     data.get("StartDate", ""), data.get("Notes", ""), data.get("sub_profile", "")))
        conn.commit()
        pid = cur.lastrowid
    finally:
        conn.close()
    return pid


def update_provider(pid: int, data: dict):
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["ProviderName", "NPI", "Specialty", "TaxID", "Email", "Phone", "Status", "StartDate", "Notes", "sub_profile"]
        parts, params = [], []
        for f in allowed:
            if f in data:
                parts.append(f"{f}=?")
                params.append(data[f])
        if parts:
            parts.append("updated_at=?")
            params += [datetime.now().isoformat(), pid]
            cur.execute(f"UPDATE providers SET {','.join(parts)} WHERE id=?", params)
            conn.commit()
    finally:
        conn.close()


def delete_provider(pid: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM providers WHERE id=?", (pid,))
        conn.commit()
    finally:
        conn.close()


# ─── Claims ───────────────────────────────────────────────────────────────────

CLAIM_STATUSES = ["Intake", "Verification", "Coding", "Billed/Submitted",
                   "Rejected", "Denied", "A/R Follow-Up", "Appeals", "Paid", "Closed"]


# ─── Status normalization migration ──────────────────────────────────────────

_STATUS_NORMALIZE_MAP = {
    # Intake
    "new": "Intake", "received": "Intake", "open": "Intake",
    "entered": "Intake", "created": "Intake", "registered": "Intake",
    # Verification
    "verify": "Verification", "verifying": "Verification",
    "eligibility": "Verification", "elig check": "Verification", "auth": "Verification",
    "authorization": "Verification", "pre-auth": "Verification", "precert": "Verification",
    # Coding
    "coded": "Coding", "code review": "Coding",
    "charge entry": "Coding", "charge review": "Coding",
    # Billed/Submitted
    "billed": "Billed/Submitted", "submitted": "Billed/Submitted",
    "filed": "Billed/Submitted", "sent": "Billed/Submitted",
    "pending": "Billed/Submitted", "in process": "Billed/Submitted",
    "in-process": "Billed/Submitted", "processing": "Billed/Submitted",
    "pending payment": "Billed/Submitted", "awaiting payment": "Billed/Submitted",
    "claim submitted": "Billed/Submitted", "billed to insurance": "Billed/Submitted",
    # Rejected
    "reject": "Rejected", "returned": "Rejected",
    "kicked back": "Rejected", "not accepted": "Rejected", "error": "Rejected",
    "failed": "Rejected", "invalid": "Rejected",
    # Denied
    "deny": "Denied", "denial": "Denied",
    "not covered": "Denied", "non-covered": "Denied",
    "denied - initial": "Denied", "initial denial": "Denied",
    # A/R Follow-Up
    "a/r follow up": "A/R Follow-Up", "ar follow up": "A/R Follow-Up",
    "ar follow-up": "A/R Follow-Up", "ar followup": "A/R Follow-Up",
    "follow up": "A/R Follow-Up", "follow-up": "A/R Follow-Up", "followup": "A/R Follow-Up",
    "a/r follow-up": "A/R Follow-Up",
    "in review": "A/R Follow-Up", "under review": "A/R Follow-Up",
    "pending review": "A/R Follow-Up", "working": "A/R Follow-Up",
    "in progress": "A/R Follow-Up",
    # Appeals
    "appeal": "Appeals", "appealed": "Appeals",
    "appeal filed": "Appeals", "reconsideration": "Appeals",
    "corrected claim": "Appeals", "resubmitted": "Appeals",
    # Paid
    "approved": "Paid", "finalized": "Paid",
    "payment received": "Paid", "closed - paid": "Paid",
    "settled": "Paid", "remitted": "Paid", "collected": "Paid",
    # Closed
    "write off": "Closed", "write-off": "Closed",
    "written off": "Closed", "adjusted": "Closed", "void": "Closed",
    "voided": "Closed", "zero balance": "Closed", "closed - adjusted": "Closed",
}
# Add exact lowercase matches for the standard statuses themselves
for _s in CLAIM_STATUSES:
    _STATUS_NORMALIZE_MAP[_s.lower()] = _s


def normalize_claim_statuses():
    """One-time migration: update all claims_master rows with non-standard ClaimStatus values."""
    conn = get_db()
    try:
        cur = conn.cursor()
        # Get distinct statuses currently in the DB
        rows = cur.execute("SELECT DISTINCT ClaimStatus FROM claims_master WHERE ClaimStatus IS NOT NULL").fetchall()
        updates = 0
        for row in rows:
            raw = row[0]
            if not raw:
                continue
            key = raw.strip().lower()
            normalized = _STATUS_NORMALIZE_MAP.get(key)
            if not normalized:
                # Partial / substring match
                for map_key, map_val in _STATUS_NORMALIZE_MAP.items():
                    if len(map_key) >= 4 and map_key in key:
                        normalized = map_val
                        break
            if normalized and normalized != raw:
                cur.execute("UPDATE claims_master SET ClaimStatus=? WHERE ClaimStatus=?", (normalized, raw))
                updates += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    if updates:
        print(f"[migration] Normalized {updates} claim status values")
    return updates


def get_claims(client_id: int = None, status: str = None, sub_profile: str = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        q = """SELECT cm.*, c.company as client_company
               FROM claims_master cm
               JOIN clients c ON c.id = cm.client_id
               WHERE 1=1"""
        params = []
        if client_id is not None:
            q += " AND cm.client_id=?"
            params.append(client_id)
        if status:
            q += " AND cm.ClaimStatus=?"
            params.append(status)
        if sub_profile:
            q += " AND cm.sub_profile=?"
            params.append(sub_profile)
        q += " ORDER BY cm.updated_at DESC"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


# ── A/R aging + worklist priority weighting ──────────────────────────────────
# Statuses that still have collectible money in play (open A/R). Paid/Closed are
# done. Intake/Verification/Coding aren't billed yet, so they carry less A/R
# urgency than something already submitted/denied.
_AR_OPEN_STATUSES = ("Intake", "Verification", "Coding", "Billed/Submitted",
                     "Rejected", "Denied", "A/R Follow-Up", "Appeals")
_AR_STATUS_WEIGHT = {
    "Denied": 1.6, "Rejected": 1.5, "Appeals": 1.4, "A/R Follow-Up": 1.3,
    "Billed/Submitted": 1.0, "Coding": 0.7, "Verification": 0.6, "Intake": 0.5,
}


def _parse_any_date(value):
    """Parse the loose date strings claims carry ('YYYY-MM-DD', 'MM/DD/YYYY',
    ISO timestamps) into a date, or None."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("T", " ").split(" ")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def _ar_bucket(days):
    if days <= 30:
        return "0-30"
    if days <= 60:
        return "31-60"
    if days <= 90:
        return "61-90"
    if days <= 120:
        return "91-120"
    return "120+"


def get_ar_worklist(client_id: int = None, owner: str = None,
                    bucket: str = None, sub_profile: str = None,
                    limit: int = 300) -> dict:
    """Build a prioritized Accounts-Receivable worklist.

    Returns open claims (balance > 0, not Paid/Closed) scored by
    `balance × age_weight × status_weight`, plus aging-bucket rollups, so a
    biller can work the highest-recovery claims first instead of guessing.
    """
    today = business_today()
    conn = get_db()
    try:
        cur = conn.cursor()
        q = """SELECT cm.*, c.company AS client_company
               FROM claims_master cm
               JOIN clients c ON c.id = cm.client_id
               WHERE cm.BalanceRemaining > 0
                 AND cm.ClaimStatus NOT IN ('Paid', 'Closed')"""
        params = []
        if client_id is not None:
            q += " AND cm.client_id=?"
            params.append(client_id)
        if owner:
            q += " AND lower(cm.Owner)=?"
            params.append(owner.strip().lower())
        if sub_profile:
            q += " AND cm.sub_profile=?"
            params.append(sub_profile)
        rows = [dict(r) for r in cur.execute(q, params).fetchall()]
    finally:
        conn.close()

    buckets = {b: {"count": 0, "balance": 0.0} for b in ("0-30", "31-60", "61-90", "91-120", "120+")}
    total_balance = 0.0
    items = []
    for r in rows:
        bal = float(r.get("BalanceRemaining") or 0)
        # Age from date of service, falling back to bill date, then created_at.
        ref = (_parse_any_date(r.get("DOS")) or _parse_any_date(r.get("BillDate"))
               or _parse_any_date(r.get("created_at")))
        age = (today - ref).days if ref else 0
        if age < 0:
            age = 0
        bk = _ar_bucket(age)
        # Age weight ramps up the older the money gets.
        if age <= 30:
            age_w = 1.0
        elif age <= 60:
            age_w = 1.5
        elif age <= 90:
            age_w = 2.2
        elif age <= 120:
            age_w = 3.2
        else:
            age_w = 4.5
        status_w = _AR_STATUS_WEIGHT.get(r.get("ClaimStatus") or "", 1.0)
        # Overdue next action gives an extra nudge.
        due = _parse_any_date(r.get("NextActionDueDate"))
        overdue = bool(due and due < today)
        score = bal * age_w * status_w * (1.25 if overdue else 1.0)

        buckets[bk]["count"] += 1
        buckets[bk]["balance"] += bal
        total_balance += bal

        items.append({
            "id": r.get("id"),
            "client_id": r.get("client_id"),
            "client_company": r.get("client_company") or "",
            "ClaimKey": r.get("ClaimKey") or "",
            "PatientName": r.get("PatientName") or "",
            "Payor": r.get("Payor") or "",
            "ProviderName": r.get("ProviderName") or "",
            "DOS": r.get("DOS") or "",
            "ClaimStatus": r.get("ClaimStatus") or "",
            "BalanceRemaining": round(bal, 2),
            "Owner": r.get("Owner") or "",
            "NextAction": r.get("NextAction") or "",
            "NextActionDueDate": r.get("NextActionDueDate") or "",
            "DenialReason": r.get("DenialReason") or "",
            "aging_days": age,
            "aging_bucket": bk,
            "overdue": overdue,
            "priority_score": round(score, 2),
        })

    items.sort(key=lambda it: it["priority_score"], reverse=True)
    if bucket:
        items = [it for it in items if it["aging_bucket"] == bucket]
    items = items[: max(1, int(limit or 300))]

    for b in buckets.values():
        b["balance"] = round(b["balance"], 2)

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_open_count": len(rows),
        "total_open_balance": round(total_balance, 2),
        "buckets": buckets,
        "items": items,
    }


def get_claim(claim_id: int):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM claims_master WHERE id=?", (claim_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def create_claim(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        now = datetime.now().isoformat()
        cur.execute("""
            INSERT INTO claims_master
            (client_id,ClaimKey,PatientID,PatientName,Payor,ProviderName,NPI,DOS,CPTCode,Description,
             ChargeAmount,AllowedAmount,AdjustmentAmount,PaidAmount,BalanceRemaining,
             ClaimStatus,StatusStartDate,BillDate,DeniedDate,PaidDate,LastTouchedDate,
             Owner,NextAction,NextActionDueDate,SLABreached,DenialCategory,DenialReason,AppealDate,AppealStatus,
             sub_profile)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(client_id, ClaimKey) DO UPDATE SET
                PatientID=excluded.PatientID, PatientName=excluded.PatientName,
                Payor=excluded.Payor, ProviderName=excluded.ProviderName, NPI=excluded.NPI,
                DOS=excluded.DOS, CPTCode=excluded.CPTCode, Description=excluded.Description,
                ChargeAmount=excluded.ChargeAmount, AllowedAmount=excluded.AllowedAmount,
                AdjustmentAmount=excluded.AdjustmentAmount, PaidAmount=excluded.PaidAmount,
                BalanceRemaining=excluded.BalanceRemaining, ClaimStatus=excluded.ClaimStatus,
                StatusStartDate=excluded.StatusStartDate, BillDate=excluded.BillDate,
                DeniedDate=excluded.DeniedDate, PaidDate=excluded.PaidDate,
                LastTouchedDate=excluded.LastTouchedDate, Owner=excluded.Owner,
                NextAction=excluded.NextAction, NextActionDueDate=excluded.NextActionDueDate,
                SLABreached=excluded.SLABreached, DenialCategory=excluded.DenialCategory,
                DenialReason=excluded.DenialReason, AppealDate=excluded.AppealDate,
                AppealStatus=excluded.AppealStatus, sub_profile=excluded.sub_profile,
                updated_at=CURRENT_TIMESTAMP
        """, (
            data["client_id"], data["ClaimKey"], data.get("PatientID", ""), data.get("PatientName", ""),
            data.get("Payor", ""), data.get("ProviderName", ""), data.get("NPI", ""),
            data.get("DOS", ""), data.get("CPTCode", ""), data.get("Description", ""),
            data.get("ChargeAmount", 0), data.get("AllowedAmount", 0),
            data.get("AdjustmentAmount", 0), data.get("PaidAmount", 0), data.get("BalanceRemaining", 0),
            data.get("ClaimStatus", "Intake"), now, data.get("BillDate", ""),
            data.get("DeniedDate", ""), data.get("PaidDate", ""), now,
            data.get("Owner", ""), data.get("NextAction", ""), data.get("NextActionDueDate", ""),
            data.get("SLABreached", 0), data.get("DenialCategory", ""), data.get("DenialReason", ""),
            data.get("AppealDate", ""), data.get("AppealStatus", ""),
            data.get("sub_profile", "")
        ))
        conn.commit()
        cid = cur.lastrowid
    finally:
        conn.close()
    return cid


def update_claim(claim_id: int, data: dict):
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["ClaimKey", "PatientID", "PatientName", "Payor", "ProviderName", "NPI", "DOS",
                   "CPTCode", "Description", "ChargeAmount", "AllowedAmount", "AdjustmentAmount",
                   "PaidAmount", "BalanceRemaining", "ClaimStatus", "StatusStartDate", "BillDate",
                   "DeniedDate", "PaidDate", "Owner", "NextAction", "NextActionDueDate",
                   "SLABreached", "DenialCategory", "DenialReason", "AppealDate", "AppealStatus",
                   "sub_profile"]
        now = datetime.now().isoformat()
        parts, params = ["LastTouchedDate=?", "updated_at=?"], [now, now]
        for f in allowed:
            if f in data:
                parts.append(f"{f}=?")
                params.append(data[f])
        # Keep BalanceRemaining in sync when a money field is edited but the
        # caller didn't explicitly set a balance. Without this, changing a
        # claim's Charge/Adjustment/Paid leaves the old AR balance behind, so
        # the dashboard, AR worklist and productivity reports keep showing a
        # stale outstanding amount. Mirror the import formula:
        # Balance = Charge - Adjustment - Paid (never negative; zero once
        # Paid/Closed).
        # A status change matters too: flipping a claim to Paid/Closed must zero
        # its AR, and re-opening it must restore the balance. Without this, a
        # biller who only changes the status (no money field) leaves the old
        # outstanding amount behind, so Outstanding AR never comes down.
        money_edited = any(k in data for k in ("ChargeAmount", "AdjustmentAmount", "PaidAmount"))
        status_changed = "ClaimStatus" in data
        if (money_edited or status_changed) and "BalanceRemaining" not in data:
            cur.execute("SELECT ChargeAmount, AdjustmentAmount, PaidAmount, ClaimStatus "
                        "FROM claims_master WHERE id=?", (claim_id,))
            cur_row = cur.fetchone()
            if cur_row:
                def _num(v):
                    try:
                        return float(v or 0)
                    except (TypeError, ValueError):
                        return 0.0
                charge = _num(data.get("ChargeAmount", cur_row["ChargeAmount"]))
                adjust = _num(data.get("AdjustmentAmount", cur_row["AdjustmentAmount"]))
                paid = _num(data.get("PaidAmount", cur_row["PaidAmount"]))
                status = data.get("ClaimStatus", cur_row["ClaimStatus"])
                new_balance = 0.0 if status in ("Paid", "Closed") else max(charge - adjust - paid, 0.0)
                parts.append("BalanceRemaining=?")
                params.append(round(new_balance, 2))
        # Stamp BillDate the day a claim is first marked Billed/Submitted so the
        # report can reflect *when* billing happened. Only fill it if empty and the
        # caller didn't explicitly provide a BillDate.
        if data.get("ClaimStatus") == "Billed/Submitted" and "BillDate" not in data:
            parts.append("BillDate=CASE WHEN COALESCE(BillDate,'')='' THEN ? ELSE BillDate END")
            params.append(now[:10])
        params.append(claim_id)
        cur.execute(f"UPDATE claims_master SET {','.join(parts)} WHERE id=?", params)
        conn.commit()
    finally:
        conn.close()


def delete_claim(claim_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM claims_master WHERE id=?", (claim_id,))
        conn.commit()
    finally:
        conn.close()


# ─── Payments ─────────────────────────────────────────────────────────────────

def get_payments(client_id: int, claim_key: str):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM payments WHERE client_id=? AND ClaimKey=? ORDER BY PostDate DESC",
                    (client_id, claim_key))
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def create_payment(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO payments
            (client_id,ClaimKey,PostDate,PaymentAmount,AdjustmentAmount,PayerType,CheckNumber,ERA,Notes,PostedBy,sub_profile)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (data["client_id"], data["ClaimKey"], data.get("PostDate", ""),
             data.get("PaymentAmount", 0), data.get("AdjustmentAmount", 0),
             data.get("PayerType", "Primary"), data.get("CheckNumber", ""),
             data.get("ERA", ""), data.get("Notes", ""), data.get("PostedBy", ""),
             data.get("sub_profile", "")))
        # Recalculate PaidAmount + AR on the claim. Posted contractual
        # adjustments must reduce the balance too, otherwise a fully resolved
        # claim (payer pays part, writes off the rest) keeps showing phantom AR
        # and the outstanding totals never come down. Canonical formula matches
        # the import path: Balance = Charge - Adjustment - Paid (never negative),
        # where Adjustment combines the claim-level adjustment and every posted
        # payment adjustment.
        cur.execute("""SELECT COALESCE(SUM(PaymentAmount),0), COALESCE(SUM(AdjustmentAmount),0)
                       FROM payments WHERE client_id=? AND ClaimKey=?""",
                    (data["client_id"], data["ClaimKey"]))
        total_paid, posted_adj = cur.fetchone()
        cur.execute("SELECT COALESCE(ChargeAmount,0), COALESCE(AdjustmentAmount,0) "
                    "FROM claims_master WHERE client_id=? AND ClaimKey=?",
                    (data["client_id"], data["ClaimKey"]))
        crow = cur.fetchone()
        charge, claim_adj = (crow[0], crow[1]) if crow else (0.0, 0.0)
        new_balance = max(charge - claim_adj - posted_adj - total_paid, 0.0)
        cur.execute("""UPDATE claims_master SET PaidAmount=?,
                       BalanceRemaining=?,
                       updated_at=? WHERE client_id=? AND ClaimKey=?""",
                    (total_paid, round(new_balance, 2), datetime.now().isoformat(),
                     data["client_id"], data["ClaimKey"]))
        conn.commit()
        pid = cur.lastrowid
    finally:
        conn.close()
    return pid


def delete_payment(payment_id: int):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT client_id, ClaimKey FROM payments WHERE id=?", (payment_id,))
        row = cur.fetchone()
        conn.execute("DELETE FROM payments WHERE id=?", (payment_id,))
        if row:
            client_id, claim_key = row
            cur.execute("""SELECT COALESCE(SUM(PaymentAmount),0), COALESCE(SUM(AdjustmentAmount),0)
                           FROM payments WHERE client_id=? AND ClaimKey=?""",
                        (client_id, claim_key))
            total_paid, posted_adj = cur.fetchone()
            cur.execute("SELECT COALESCE(ChargeAmount,0), COALESCE(AdjustmentAmount,0) "
                        "FROM claims_master WHERE client_id=? AND ClaimKey=?",
                        (client_id, claim_key))
            crow = cur.fetchone()
            charge, claim_adj = (crow[0], crow[1]) if crow else (0.0, 0.0)
            new_balance = max(charge - claim_adj - posted_adj - total_paid, 0.0)
            cur.execute("""UPDATE claims_master SET PaidAmount=?,
                           BalanceRemaining=?,
                           updated_at=? WHERE client_id=? AND ClaimKey=?""",
                        (total_paid, round(new_balance, 2), datetime.now().isoformat(),
                         client_id, claim_key))
        conn.commit()
    finally:
        conn.close()


# ─── Notes ────────────────────────────────────────────────────────────────────

def get_notes(client_id: int, claim_key: str = None, module: str = None, ref_id: int = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM notes_log WHERE client_id=?"
        params = [client_id]
        if claim_key:
            q += " AND ClaimKey=?"
            params.append(claim_key)
        if module:
            q += " AND Module=?"
            params.append(module)
        if ref_id is not None:
            q += " AND RefID=?"
            params.append(ref_id)
        q += " ORDER BY created_at ASC"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def add_note(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO notes_log (client_id,ClaimKey,Module,RefID,Note,Author)
                       VALUES (?,?,?,?,?,?)""",
                    (data["client_id"], data.get("ClaimKey", ""), data.get("Module", "Claim"),
                     data.get("RefID", 0), data["Note"], data.get("Author", "")))
        conn.commit()
        nid = cur.lastrowid
    finally:
        conn.close()
    return nid


# ─── Credentialing ────────────────────────────────────────────────────────────

def get_credentialing(client_id: int = None, status: str = None, sub_profile: str = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM credentialing WHERE 1=1"
        params = []
        if client_id is not None:
            q += " AND client_id=?"; params.append(client_id)
        if status:
            q += " AND Status=?"; params.append(status)
        if sub_profile:
            q += " AND sub_profile=?"; params.append(sub_profile)
        q += " ORDER BY updated_at DESC"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def create_credentialing(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO credentialing
            (client_id,provider_id,ProviderName,Payor,CredType,Status,SubmittedDate,FollowUpDate,ApprovedDate,ExpirationDate,Owner,Notes,sub_profile)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data["client_id"], data.get("provider_id"), data.get("ProviderName", ""),
             data.get("Payor", ""), data.get("CredType", "Initial"), data.get("Status", "Not Started"),
             data.get("SubmittedDate", ""), data.get("FollowUpDate", ""),
             data.get("ApprovedDate", ""), data.get("ExpirationDate", ""),
             data.get("Owner", ""), data.get("Notes", ""), data.get("sub_profile", "")))
        conn.commit()
        cid = cur.lastrowid
    finally:
        conn.close()
    return cid


def update_credentialing(rec_id: int, data: dict):
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["ProviderName", "Payor", "CredType", "Status", "SubmittedDate",
                   "FollowUpDate", "ApprovedDate", "ExpirationDate", "Owner", "Notes", "sub_profile"]
        parts, params = ["updated_at=?"], [datetime.now().isoformat()]
        for f in allowed:
            if f in data:
                parts.append(f"{f}=?")
                params.append(data[f])
        params.append(rec_id)
        cur.execute(f"UPDATE credentialing SET {','.join(parts)} WHERE id=?", params)
        conn.commit()
    finally:
        conn.close()


def delete_credentialing(rec_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM credentialing WHERE id=?", (rec_id,))
        conn.commit()
    finally:
        conn.close()


# ─── Enrollment ───────────────────────────────────────────────────────────────

def get_enrollment(client_id: int = None, status: str = None, sub_profile: str = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM enrollment WHERE 1=1"
        params = []
        if client_id is not None:
            q += " AND client_id=?"; params.append(client_id)
        if status:
            q += " AND Status=?"; params.append(status)
        if sub_profile:
            q += " AND sub_profile=?"; params.append(sub_profile)
        q += " ORDER BY updated_at DESC"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def create_enrollment(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO enrollment
            (client_id,provider_id,ProviderName,Payor,EnrollType,Status,SubmittedDate,FollowUpDate,ApprovedDate,EffectiveDate,Owner,Notes,sub_profile)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data["client_id"], data.get("provider_id"), data.get("ProviderName", ""),
             data.get("Payor", ""), data.get("EnrollType", "Enrollment"), data.get("Status", "Not Started"),
             data.get("SubmittedDate", ""), data.get("FollowUpDate", ""),
             data.get("ApprovedDate", ""), data.get("EffectiveDate", ""),
             data.get("Owner", ""), data.get("Notes", ""), data.get("sub_profile", "")))
        conn.commit()
        eid = cur.lastrowid
    finally:
        conn.close()
    return eid


def update_enrollment(rec_id: int, data: dict):
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["ProviderName", "Payor", "EnrollType", "Status", "SubmittedDate",
                   "FollowUpDate", "ApprovedDate", "EffectiveDate", "Owner", "Notes", "sub_profile"]
        parts, params = ["updated_at=?"], [datetime.now().isoformat()]
        for f in allowed:
            if f in data:
                parts.append(f"{f}=?")
                params.append(data[f])
        params.append(rec_id)
        cur.execute(f"UPDATE enrollment SET {','.join(parts)} WHERE id=?", params)
        conn.commit()
    finally:
        conn.close()


def delete_enrollment(rec_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM enrollment WHERE id=?", (rec_id,))
        conn.commit()
    finally:
        conn.close()


# ─── EDI Setup ────────────────────────────────────────────────────────────────

def get_edi(client_id: int = None, sub_profile: str = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        q = "SELECT * FROM edi_setup WHERE 1=1"
        params = []
        if client_id is not None:
            q += " AND client_id=?"; params.append(client_id)
        if sub_profile:
            q += " AND sub_profile=?"; params.append(sub_profile)
        q += " ORDER BY updated_at DESC"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def create_edi(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO edi_setup
            (client_id,provider_id,ProviderName,Payor,EDIStatus,ERAStatus,EFTStatus,SubmittedDate,GoLiveDate,PayerID,Owner,Notes,sub_profile)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data["client_id"], data.get("provider_id"), data.get("ProviderName", ""),
             data.get("Payor", ""), data.get("EDIStatus", "Not Started"),
             data.get("ERAStatus", "Not Started"), data.get("EFTStatus", "Not Started"),
             data.get("SubmittedDate", ""), data.get("GoLiveDate", ""),
             data.get("PayerID", ""), data.get("Owner", ""), data.get("Notes", ""), data.get("sub_profile", "")))
        conn.commit()
        eid = cur.lastrowid
    finally:
        conn.close()
    return eid


def update_edi(rec_id: int, data: dict):
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["ProviderName", "Payor", "EDIStatus", "ERAStatus", "EFTStatus",
                   "SubmittedDate", "GoLiveDate", "PayerID", "Owner", "Notes", "sub_profile"]
        parts, params = ["updated_at=?"], [datetime.now().isoformat()]
        for f in allowed:
            if f in data:
                parts.append(f"{f}=?")
                params.append(data[f])
        params.append(rec_id)
        cur.execute(f"UPDATE edi_setup SET {','.join(parts)} WHERE id=?", params)
        conn.commit()
    finally:
        conn.close()


def delete_edi(rec_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM edi_setup WHERE id=?", (rec_id,))
        conn.commit()
    finally:
        conn.close()


# ─── Dashboard ────────────────────────────────────────────────────────────────

def get_dashboard(client_id: int = None, sub_profile: str = None,
                  date_from: str = None, date_to: str = None):
    """Full KPI dashboard — pass client_id=None for admin (all clients).
       Pass sub_profile='MHP' or 'OMT' to filter by sub-profile.
       Pass date_from / date_to (YYYY-MM-DD) for date range filtering on DOS."""
    conn = get_db()
    try:
        cur = conn.cursor()

        # Base conditions (apply to all tables)
        base_conditions = []
        base_p = []
        if client_id is not None:
            base_conditions.append("client_id=?")
            base_p.append(client_id)
        if sub_profile:
            base_conditions.append("sub_profile=?")
            base_p.append(sub_profile)

        # Claims-specific conditions (include DOS date filter)
        claims_conditions = list(base_conditions)
        claims_p = list(base_p)
        if date_from:
            claims_conditions.append("DOS >= ?")
            claims_p.append(date_from)
        if date_to:
            claims_conditions.append("DOS <= ?")
            claims_p.append(date_to)

        cond = ("WHERE " + " AND ".join(claims_conditions)) if claims_conditions else ""
        p = claims_p
        # Base cond for non-claims tables (payments, credentialing, etc.)
        base_cond = ("WHERE " + " AND ".join(base_conditions)) if base_conditions else ""

        today = business_today()
        mtd_start = today.replace(day=1).isoformat()
        ytd_start = today.replace(month=1, day=1).isoformat()

        def q1(sql, params=None):
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return row[0] if row else 0

        # Total AR
        total_ar = q1(f"SELECT COALESCE(SUM(BalanceRemaining),0) FROM claims_master {cond}", p)
        # Active claims (not Paid, not Closed)
        active_p = p + ["Paid", "Closed"]
        active = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus NOT IN (?,?)", active_p)

        # Submitted MTD
        submitted_mtd = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} BillDate >= ?",
                           p + [mtd_start])
        submitted_ytd = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} BillDate >= ?",
                           p + [ytd_start])

        # Denials MTD
        denied_mtd = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} DeniedDate >= ?",
                        p + [mtd_start])
        denied_all = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus IN ('Denied','Appeals')", p)

        # Payments MTD (payments table has no DOS column — use base_cond)
        pay_mtd = q1(f"SELECT COALESCE(SUM(PaymentAmount),0) FROM payments {base_cond} {'AND' if base_cond else 'WHERE'} PostDate >= ?",
                     base_p + [mtd_start])
        pay_ytd = q1(f"SELECT COALESCE(SUM(PaymentAmount),0) FROM payments {base_cond} {'AND' if base_cond else 'WHERE'} PostDate >= ?",
                     base_p + [ytd_start])

        # Totals for rates
        total_submitted = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} BillDate != ''", p)
        total_denied = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} DeniedDate != ''", p)
        total_paid_count = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus='Paid'", p)
        # Clean claim = paid without denial
        clean_claims = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus='Paid' AND DenialReason=''", p)

        clean_rate = round(clean_claims / max(total_submitted, 1) * 100, 1)
        denial_rate = round(total_denied / max(total_submitted, 1) * 100, 1)

        # Avg days to payment
        cur.execute(f"""SELECT AVG(CAST(julianday(PaidDate) - julianday(DOS) AS REAL))
                        FROM claims_master {cond} {'AND' if cond else 'WHERE'} PaidDate != '' AND DOS != ''""", p)
        row = cur.fetchone()
        avg_days_to_pay = round(row[0] or 0, 1)

        # SLA breaches
        sla_breaches = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} SLABreached=1", p)

        # Net collection rate
        total_charge = q1(f"SELECT COALESCE(SUM(ChargeAmount),0) FROM claims_master {cond}", p)
        total_paid = q1(f"SELECT COALESCE(SUM(PaidAmount),0) FROM claims_master {cond}", p)
        net_coll_rate = round(total_paid / max(total_charge, 1) * 100, 1)

        # AR Aging buckets (by BillDate proxy for age)
        aging = {"current": 0, "days_31_60": 0, "days_61_90": 0, "days_90_plus": 0}
        ar_p = p + ["Paid", "Closed"]
        cur.execute(f"""SELECT BalanceRemaining,
                        CAST(julianday('now') - julianday(COALESCE(NULLIF(BillDate,''), DOS, updated_at)) AS INTEGER) as age
                        FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus NOT IN (?,?) AND BalanceRemaining > 0""",
                    ar_p)
        for row in cur.fetchall():
            bal, age = row
            age = age or 0
            if age <= 30:   aging["current"] += bal
            elif age <= 60: aging["days_31_60"] += bal
            elif age <= 90: aging["days_61_90"] += bal
            else:           aging["days_90_plus"] += bal
        aging = {k: round(v, 2) for k, v in aging.items()}

        # Billed Activity — count of claim lines billed and their charge value,
        # grouped by BillDate window (today / yesterday / last 7 days / this month).
        # Scoped to the same client_id + sub_profile as the rest of the dashboard so
        # every user monitors their own billing activity. Computed independently of
        # the DOS date filter (base_cond) so recent billing always shows.
        billing_activity = {
            "today": {"count": 0, "charged": 0.0},
            "yesterday": {"count": 0, "charged": 0.0},
            "this_week": {"count": 0, "charged": 0.0},
            "this_month": {"count": 0, "charged": 0.0},
            "all_time": {"count": 0, "charged": 0.0},
        }
        _ba_today = today
        _ba_yesterday = today.fromordinal(today.toordinal() - 1)
        _ba_week_start = today.fromordinal(today.toordinal() - 6)  # inclusive last 7 days
        _ba_month_start = today.replace(day=1)
        cur.execute(f"""SELECT BillDate, ChargeAmount FROM claims_master
                        {base_cond} {'AND' if base_cond else 'WHERE'} COALESCE(BillDate,'') != ''""", base_p)
        for _bd_raw, _amt_raw in cur.fetchall():
            _bd = str(_bd_raw or "").strip()[:10]
            try:
                _d = date.fromisoformat(_bd)
            except (ValueError, TypeError):
                continue
            _amt = float(_amt_raw or 0)
            # All-time billed since inception — every claim line with a Bill Date.
            billing_activity["all_time"]["count"] += 1
            billing_activity["all_time"]["charged"] += _amt
            if _d == _ba_today:
                billing_activity["today"]["count"] += 1
                billing_activity["today"]["charged"] += _amt
            if _d == _ba_yesterday:
                billing_activity["yesterday"]["count"] += 1
                billing_activity["yesterday"]["charged"] += _amt
            if _d >= _ba_week_start:
                billing_activity["this_week"]["count"] += 1
                billing_activity["this_week"]["charged"] += _amt
            if _d >= _ba_month_start:
                billing_activity["this_month"]["count"] += 1
                billing_activity["this_month"]["charged"] += _amt
        for _k in billing_activity:
            billing_activity[_k]["charged"] = round(billing_activity[_k]["charged"], 2)

        # Status distribution (flat: status → count, for frontend bar chart)
        cur.execute(f"SELECT ClaimStatus, COUNT(*) FROM claims_master {cond} GROUP BY ClaimStatus", p)
        status_dist = {r[0]: r[1] for r in cur.fetchall()}

        # Payor mix (flat: payor → count, for frontend bar chart)
        cur.execute(f"""SELECT Payor, COUNT(*)
                        FROM claims_master {cond} GROUP BY Payor ORDER BY COUNT(*) DESC LIMIT 8""", p)
        payor_mix = {r[0]: r[1] for r in cur.fetchall()}

        # Denial categories (flat: category → count, for frontend bar chart)
        cur.execute(f"""SELECT DenialCategory, COUNT(*) FROM claims_master
                        {cond} {'AND' if cond else 'WHERE'} DenialCategory != '' GROUP BY DenialCategory ORDER BY COUNT(*) DESC""", p)
        denial_cats = {r[0]: r[1] for r in cur.fetchall()}

        # Payment trend (last 6 months — payments table, use base_cond)
        cur.execute(f"""SELECT strftime('%Y-%m', PostDate) as mo, COALESCE(SUM(PaymentAmount),0)
                        FROM payments {base_cond} {'AND' if base_cond else 'WHERE'} PostDate != '' GROUP BY mo ORDER BY mo DESC LIMIT 6""", base_p)
        pay_trend = [{"month": r[0], "amount": round(r[1], 2)} for r in reversed(cur.fetchall())]

        # Credentialing stats (no DOS column — use base_cond)
        cur.execute(f"SELECT Status, COUNT(*) FROM credentialing {base_cond} GROUP BY Status", base_p)
        cred_stats = {r[0]: r[1] for r in cur.fetchall()}

        # Enrollment stats (no DOS column — use base_cond)
        cur.execute(f"SELECT Status, COUNT(*) FROM enrollment {base_cond} GROUP BY Status", base_p)
        enroll_stats = {r[0]: r[1] for r in cur.fetchall()}

        # Client profile
        profile = {}
        if client_id:
            cur.execute("""SELECT company, contact_name, email, phone,
                                  tax_id, group_npi, individual_npi,
                                  ptan_group, ptan_individual, address, specialty, notes
                           FROM clients WHERE id=?""", [client_id])
            row = cur.fetchone()
            if row:
                cols = ["company","contact_name","email","phone",
                        "tax_id","group_npi","individual_npi",
                        "ptan_group","ptan_individual","address","specialty","notes"]
                profile = {c: (row[i] or "") for i, c in enumerate(cols)}

        return {
            "total_ar": round(total_ar, 2),
            "active_claims": active,
            "submitted_mtd": submitted_mtd,
            "submitted_ytd": submitted_ytd,
            "denied_mtd": denied_mtd,
            "denied_all": denied_all,
            "payments_mtd": round(pay_mtd, 2),
            "payments_ytd": round(pay_ytd, 2),
            "clean_claim_rate": clean_rate,
            "denial_rate": denial_rate,
            "avg_days_to_pay": avg_days_to_pay,
            "sla_breaches": sla_breaches,
            "net_collection_rate": net_coll_rate,
            "total_charge": round(total_charge, 2),
            "total_paid": round(total_paid, 2),
            "ar_aging": aging,
            "billing_activity": billing_activity,
            "status_distribution": status_dist,
            "payor_mix": payor_mix,
            "denial_categories": denial_cats,
            "payment_trend": pay_trend,
            "credentialing_stats": cred_stats,
            "enrollment_stats": enroll_stats,
            "profile": profile,
        }
    finally:
        conn.close()


# ─── Daily Account Summary (for 6 PM scheduled report) ────────────────────

def get_daily_account_summary():
    """
    Aggregate snapshot across ALL clients for the overall daily account summary.
    Returns high-level KPIs suitable for the Team Lead / Manager report.
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        today = business_today()
        today_str = today.isoformat()
        mtd_start = today.replace(day=1).isoformat()
        ytd_start = today.replace(month=1, day=1).isoformat()

        def q1(sql, params=None):
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return row[0] if row else 0

        # ── Claims KPIs ──
        total_claims       = q1("SELECT COUNT(*) FROM claims_master")
        total_ar           = q1("SELECT COALESCE(SUM(BalanceRemaining),0) FROM claims_master")
        active_claims      = q1("SELECT COUNT(*) FROM claims_master WHERE ClaimStatus NOT IN ('Paid','Closed')")
        claims_paid        = q1("SELECT COUNT(*) FROM claims_master WHERE ClaimStatus='Paid'")
        claims_denied      = q1("SELECT COUNT(*) FROM claims_master WHERE ClaimStatus IN ('Denied','Appeals')")
        claims_submitted   = q1("SELECT COUNT(*) FROM claims_master WHERE BillDate != ''")
        submitted_today    = q1("SELECT COUNT(*) FROM claims_master WHERE BillDate=?", [today_str])
        paid_today         = q1("SELECT COUNT(*) FROM claims_master WHERE PaidDate=?", [today_str])
        denied_today       = q1("SELECT COUNT(*) FROM claims_master WHERE DeniedDate=?", [today_str])
        submitted_mtd      = q1("SELECT COUNT(*) FROM claims_master WHERE BillDate>=?", [mtd_start])
        paid_mtd           = q1("SELECT COUNT(*) FROM claims_master WHERE PaidDate>=?", [mtd_start])
        denied_mtd         = q1("SELECT COUNT(*) FROM claims_master WHERE DeniedDate>=?", [mtd_start])

        total_charge       = q1("SELECT COALESCE(SUM(ChargeAmount),0) FROM claims_master")
        total_paid_amt     = q1("SELECT COALESCE(SUM(PaidAmount),0) FROM claims_master")
        net_coll_rate      = round(total_paid_amt / max(total_charge, 1) * 100, 1)

        clean_claims       = q1("SELECT COUNT(*) FROM claims_master WHERE ClaimStatus='Paid' AND DenialReason=''")
        clean_rate         = round(clean_claims / max(claims_submitted, 1) * 100, 1)
        denial_rate        = round(claims_denied / max(claims_submitted, 1) * 100, 1)

        sla_breaches       = q1("SELECT COUNT(*) FROM claims_master WHERE SLABreached=1")

        # Avg days to pay
        cur.execute("SELECT AVG(CAST(julianday(PaidDate) - julianday(DOS) AS REAL)) FROM claims_master WHERE PaidDate != '' AND DOS != ''")
        row = cur.fetchone()
        avg_days_to_pay = round(row[0] or 0, 1)

        # Payments
        payments_today     = q1("SELECT COALESCE(SUM(PaymentAmount),0) FROM payments WHERE PostDate=?", [today_str])
        payments_mtd       = q1("SELECT COALESCE(SUM(PaymentAmount),0) FROM payments WHERE PostDate>=?", [mtd_start])
        payments_ytd       = q1("SELECT COALESCE(SUM(PaymentAmount),0) FROM payments WHERE PostDate>=?", [ytd_start])

        # AR Aging
        aging = {"current": 0, "31_60": 0, "61_90": 0, "90_plus": 0}
        cur.execute("""SELECT BalanceRemaining,
                       CAST(julianday('now') - julianday(COALESCE(NULLIF(BillDate,''), DOS, updated_at)) AS INTEGER) as age
                       FROM claims_master WHERE ClaimStatus NOT IN ('Paid','Closed') AND BalanceRemaining > 0""")
        for row in cur.fetchall():
            bal, age_days = row
            age_days = age_days or 0
            if age_days <= 30:   aging["current"] += bal
            elif age_days <= 60: aging["31_60"] += bal
            elif age_days <= 90: aging["61_90"] += bal
            else:                aging["90_plus"] += bal
        aging = {k: round(v, 2) for k, v in aging.items()}

        # Status distribution
        cur.execute("SELECT ClaimStatus, COUNT(*) FROM claims_master GROUP BY ClaimStatus ORDER BY COUNT(*) DESC")
        status_dist = {r[0]: r[1] for r in cur.fetchall()}

        # Top payors
        cur.execute("SELECT Payor, COUNT(*), COALESCE(SUM(ChargeAmount),0) FROM claims_master WHERE Payor != '' GROUP BY Payor ORDER BY COUNT(*) DESC LIMIT 10")
        top_payors = [{"payor": r[0], "count": r[1], "charges": round(r[2], 2)} for r in cur.fetchall()]

        # ── Credentialing KPIs ──
        cur.execute("SELECT Status, COUNT(*) FROM credentialing GROUP BY Status")
        cred_stats = {r[0]: r[1] for r in cur.fetchall()}
        cred_total          = sum(cred_stats.values())
        cred_approved       = cred_stats.get("Approved", 0) + cred_stats.get("Active", 0)
        cred_pending        = cred_stats.get("Pending", 0) + cred_stats.get("In Progress", 0) + cred_stats.get("Submitted", 0)
        cred_not_started    = cred_stats.get("Not Started", 0)

        # ── Enrollment KPIs ──
        cur.execute("SELECT Status, COUNT(*) FROM enrollment GROUP BY Status")
        enroll_stats = {r[0]: r[1] for r in cur.fetchall()}
        enroll_total     = sum(enroll_stats.values())
        enroll_approved  = enroll_stats.get("Approved", 0) + enroll_stats.get("Active", 0) + enroll_stats.get("Enrolled", 0)
        enroll_pending   = enroll_stats.get("Pending", 0) + enroll_stats.get("In Progress", 0) + enroll_stats.get("Submitted", 0)

        # ── EDI KPIs ──
        cur.execute("SELECT EDIStatus, COUNT(*) FROM edi_setup GROUP BY EDIStatus")
        edi_stats = {r[0]: r[1] for r in cur.fetchall()}
        edi_total = sum(edi_stats.values())
        edi_live  = edi_stats.get("Live", 0) + edi_stats.get("Active", 0) + edi_stats.get("Complete", 0)

        # ── Clients ──
        total_clients = q1("SELECT COUNT(*) FROM clients WHERE role='client'")

        # ── Today's audit activity ──
        today_actions = q1("SELECT COUNT(*) FROM audit_log WHERE created_at >= ?", [today_str])

        return {
            # Claims
            "total_claims": total_claims,
            "total_ar": round(total_ar, 2),
            "active_claims": active_claims,
            "claims_paid": claims_paid,
            "claims_denied": claims_denied,
            "claims_submitted": claims_submitted,
            "submitted_today": submitted_today,
            "paid_today": paid_today,
            "denied_today": denied_today,
            "submitted_mtd": submitted_mtd,
            "paid_mtd": paid_mtd,
            "denied_mtd": denied_mtd,
            "net_collection_rate": net_coll_rate,
            "clean_claim_rate": clean_rate,
            "denial_rate": denial_rate,
            "avg_days_to_pay": avg_days_to_pay,
            "sla_breaches": sla_breaches,
            "total_charge": round(total_charge, 2),
            "total_paid_amt": round(total_paid_amt, 2),
            # Payments
            "payments_today": round(payments_today, 2),
            "payments_mtd": round(payments_mtd, 2),
            "payments_ytd": round(payments_ytd, 2),
            # Aging
            "ar_aging": aging,
            # Distribution
            "status_distribution": status_dist,
            "top_payors": top_payors,
            # Credentialing
            "cred_total": cred_total,
            "cred_approved": cred_approved,
            "cred_pending": cred_pending,
            "cred_not_started": cred_not_started,
            "cred_stats": cred_stats,
            # Enrollment
            "enroll_total": enroll_total,
            "enroll_approved": enroll_approved,
            "enroll_pending": enroll_pending,
            "enroll_stats": enroll_stats,
            # EDI
            "edi_total": edi_total,
            "edi_live": edi_live,
            "edi_stats": edi_stats,
            # General
            "total_clients": total_clients,
            "today_actions": today_actions,
        }
    finally:
        conn.close()


# ─── Audit Trail ──────────────────────────────────────────────────────────

def log_audit(client_id: int, username: str, action: str,
              entity_type: str = "", entity_id: int = None, details: str = ""):
    """Record an action in the audit log."""
    conn = None
    try:
        conn = get_db()
        conn.execute("""INSERT INTO audit_log (client_id, username, action, entity_type, entity_id, details)
                        VALUES (?,?,?,?,?,?)""",
                     (client_id, username, action, entity_type, entity_id, details))
        conn.commit()
    except Exception:
        pass  # Don't let audit logging break main operations
    finally:
        if conn:
            conn.close()


def get_audit_log(client_id: int = None, limit: int = 100):
    conn = get_db()
    try:
        cur = conn.cursor()
        if client_id is not None:
            cur.execute("SELECT * FROM audit_log WHERE client_id=? ORDER BY created_at DESC LIMIT ?", (client_id, limit))
        else:
            cur.execute("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT ?", (limit,))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


# ─── In-App Notifications (HIPAA-safe) ──────────────────────────────────
#
# A persistent inbox per user. Anything that *would* send an email
# (chat invite, chat message, EOD report ready, welcome) also drops a
# row here so the recipient sees it in the hub even when the email
# provider is down or unconfigured. Bodies are always PHI-safe markers
# (e.g. "[chat message · 47 chars]") — the real PHI lives only in
# the encrypted chat_messages table.

def create_notification(user_id: int, kind: str, title: str,
                        body: str = "", link: str = "",
                        related_type: str = "",
                        related_id: int | None = None) -> int:
    """Insert a single notification row. Returns the new id (or 0 on failure
    — safe to ignore, we never want notification writes to break the
    triggering action)."""
    if not user_id or not kind or not title:
        return 0
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO notifications "
            "(user_id, kind, title, body, link, related_type, related_id) "
            "VALUES (?,?,?,?,?,?,?)",
            (int(user_id), kind, title, body or "", link or "",
             related_type or "", int(related_id) if related_id else None),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    except Exception:
        log.exception("create_notification failed for user_id=%s kind=%s",
                      user_id, kind)
        return 0
    finally:
        if conn:
            conn.close()


def fanout_notification(user_ids: list[int], kind: str, title: str,
                        body: str = "", link: str = "",
                        related_type: str = "",
                        related_id: int | None = None,
                        skip_user_id: int | None = None) -> int:
    """Insert the same notification for many recipients in one transaction."""
    valid_ids = [int(u) for u in (user_ids or [])
                 if u and int(u) != int(skip_user_id or 0)]
    if not valid_ids:
        return 0
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        rows = [
            (uid, kind, title, body or "", link or "", related_type or "",
             int(related_id) if related_id else None)
            for uid in valid_ids
        ]
        cur.executemany(
            "INSERT INTO notifications "
            "(user_id, kind, title, body, link, related_type, related_id) "
            "VALUES (?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        return cur.rowcount or 0
    except Exception:
        log.exception("fanout_notification failed kind=%s n=%s",
                      kind, len(valid_ids))
        return 0
    finally:
        if conn:
            conn.close()


def list_notifications(user_id: int, unread_only: bool = False,
                       limit: int = 50) -> list[dict]:
    """Most recent notifications for a single user (newest first)."""
    if not user_id:
        return []
    conn = get_db()
    try:
        cur = conn.cursor()
        if unread_only:
            cur.execute(
                "SELECT id, kind, title, body, link, related_type, related_id, "
                "       is_read, created_at, read_at FROM notifications "
                "WHERE user_id=? AND is_read=0 "
                "ORDER BY datetime(created_at) DESC LIMIT ?",
                (int(user_id), int(limit)),
            )
        else:
            cur.execute(
                "SELECT id, kind, title, body, link, related_type, related_id, "
                "       is_read, created_at, read_at FROM notifications "
                "WHERE user_id=? "
                "ORDER BY datetime(created_at) DESC LIMIT ?",
                (int(user_id), int(limit)),
            )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def count_unread_notifications(user_id: int) -> int:
    if not user_id:
        return 0
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id=? AND is_read=0",
            (int(user_id),),
        )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        conn.close()


def mark_notification_read(user_id: int, notification_id: int) -> bool:
    if not user_id or not notification_id:
        return False
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE notifications SET is_read=1, "
            "read_at=CURRENT_TIMESTAMP "
            "WHERE id=? AND user_id=? AND is_read=0",
            (int(notification_id), int(user_id)),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    finally:
        conn.close()


def mark_all_notifications_read(user_id: int) -> int:
    if not user_id:
        return 0
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE notifications SET is_read=1, "
            "read_at=CURRENT_TIMESTAMP "
            "WHERE user_id=? AND is_read=0",
            (int(user_id),),
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


def delete_notification(user_id: int, notification_id: int) -> bool:
    """Permanently remove a single notification belonging to this user."""
    if not user_id or not notification_id:
        return False
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM notifications WHERE id=? AND user_id=?",
            (int(notification_id), int(user_id)),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    finally:
        conn.close()


def delete_notifications(user_id: int, kind: str | None = None,
                         read_only: bool = False) -> int:
    """Bulk-delete this user's notifications.

    ``kind``      — if given, only delete notifications of that kind
                    (e.g. 'chat_message').
    ``read_only`` — if True, only delete ones already marked read.
    Returns the number of rows removed.
    """
    if not user_id:
        return 0
    sql = "DELETE FROM notifications WHERE user_id=?"
    params: list = [int(user_id)]
    if kind:
        sql += " AND kind=?"
        params.append(kind)
    if read_only:
        sql += " AND is_read=1"
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


# ─── EOD Report Archive ─────────────────────────────────────────────────

def save_eod_report(report_date: str, headlines: dict, summary: dict,
                    html_body: str = "", text_body: str = "",
                    generated_by: str = "scheduled",
                    email_status: str = "",
                    email_recipients: list[str] | None = None) -> int:
    """Persist a generated EOD report so it can be viewed later even if
    email delivery fails."""
    import json as _json
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO eod_reports "
            "(report_date, generated_by, headlines_json, summary_json, "
            " html_body, text_body, email_status, email_recipients) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                report_date,
                generated_by or "scheduled",
                _json.dumps(headlines or {}, default=str),
                _json.dumps(summary or {}, default=str),
                html_body or "",
                text_body or "",
                email_status or "",
                ",".join(email_recipients or []),
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)
    except Exception:
        log.exception("save_eod_report failed for date=%s", report_date)
        return 0
    finally:
        if conn:
            conn.close()


def update_eod_report_email_status(report_id: int, status: str,
                                   recipients: list[str] | None = None) -> bool:
    if not report_id:
        return False
    conn = get_db()
    try:
        cur = conn.cursor()
        if recipients is not None:
            cur.execute(
                "UPDATE eod_reports SET email_status=?, email_recipients=? "
                "WHERE id=?",
                (status or "", ",".join(recipients or []), int(report_id)),
            )
        else:
            cur.execute(
                "UPDATE eod_reports SET email_status=? WHERE id=?",
                (status or "", int(report_id)),
            )
        conn.commit()
        return (cur.rowcount or 0) > 0
    finally:
        conn.close()


def list_eod_reports(limit: int = 30) -> list[dict]:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, report_date, generated_at, generated_by, "
            "       headlines_json, email_status, email_recipients "
            "FROM eod_reports ORDER BY datetime(generated_at) DESC LIMIT ?",
            (int(limit),),
        )
        out: list[dict] = []
        import json as _json
        for r in cur.fetchall():
            d = dict(r)
            try:
                d["headlines"] = _json.loads(d.pop("headlines_json") or "{}")
            except Exception:
                d["headlines"] = {}
            out.append(d)
        return out
    finally:
        conn.close()


def get_eod_report(report_id: int) -> dict | None:
    if not report_id:
        return None
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM eod_reports WHERE id=?",
            (int(report_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        import json as _json
        try:
            d["headlines"] = _json.loads(d.pop("headlines_json", "") or "{}")
        except Exception:
            d["headlines"] = {}
        try:
            d["summary"] = _json.loads(d.pop("summary_json", "") or "{}")
        except Exception:
            d["summary"] = {}
        return d
    finally:
        conn.close()


# ─── Team activity rollup (per day / per week / per month) ──────────────────
#
# A time-series view of the SAME work the end-of-day report summarizes, but
# bucketed across many days so admins can see trends per day, per week and per
# month instead of a single day at a time. Metrics mirror the EOD headlines so
# the numbers reconcile with the nightly report.

_ROLLUP_METRIC_KEYS = (
    "claims_new", "claims_touched", "payments_posted", "payments_amount",
    "billed_count", "billed_amount", "cred_new", "enroll_new", "edi_new",
    "production_rows", "production_hours", "notes_new", "files_uploaded",
    "active_users",
)


def _rollup_buckets(bucket: str, count: int):
    """Return a list of (label, start_date, end_date) tuples — the most recent
    `count` buckets ending today, oldest first. Dates are business-timezone
    `date` objects. The current (partial) bucket is capped at today."""
    today = business_today()
    out: list[tuple[str, date, date]] = []
    if bucket == "week":
        # ISO weeks, Monday→Sunday. Current week ends today.
        this_monday = today - timedelta(days=today.weekday())
        for i in range(count - 1, -1, -1):
            wk_start = this_monday - timedelta(weeks=i)
            wk_end = min(wk_start + timedelta(days=6), today)
            label = f"Week of {wk_start.isoformat()}"
            out.append((label, wk_start, wk_end))
    elif bucket == "month":
        # Calendar months. Walk back `count` months from the 1st of this month.
        y, m = today.year, today.month
        starts: list[date] = []
        for _ in range(count):
            starts.append(date(y, m, 1))
            m -= 1
            if m == 0:
                m = 12
                y -= 1
        starts.reverse()
        for mstart in starts:
            if mstart.month == 12:
                nxt = date(mstart.year + 1, 1, 1)
            else:
                nxt = date(mstart.year, mstart.month + 1, 1)
            mend = min(nxt - timedelta(days=1), today)
            label = mstart.strftime("%b %Y")
            out.append((label, mstart, mend))
    else:  # day
        for i in range(count - 1, -1, -1):
            d = today - timedelta(days=i)
            out.append((d.isoformat(), d, d))
    return out


def get_team_activity_rollup(bucket: str = "day", count: int = 14,
                             client_id: int = None) -> dict:
    """Aggregate team-wide work into per-day / per-week / per-month buckets.

    `bucket` is one of 'day', 'week', 'month'. `count` is the number of most
    recent buckets to return (oldest first). When `client_id` is given the
    rollup is scoped to that account; otherwise it spans every client (the same
    team-wide scope as the nightly EOD report).

    Each bucket carries the EOD-style headline metrics summed over its date
    range so the figures reconcile with the end-of-day report.
    """
    bucket = (bucket or "day").lower().strip()
    if bucket not in ("day", "week", "month"):
        bucket = "day"
    try:
        count = int(count)
    except (TypeError, ValueError):
        count = 14
    count = max(1, min(count, 366))

    buckets = _rollup_buckets(bucket, count)
    conn = get_db()
    try:
        cur = conn.cursor()

        def _scalar(sql: str, params: tuple):
            cur.execute(sql, params)
            row = cur.fetchone()
            return (row[0] if row and row[0] is not None else 0)

        # client_id filter fragment shared by the per-table queries.
        cfilt = " AND client_id=?" if client_id is not None else ""
        cargs: tuple = (int(client_id),) if client_id is not None else ()

        # created_at/updated_at are stored as UTC timestamps (SQLite
        # CURRENT_TIMESTAMP). Shift them into the business timezone before
        # taking the date so a payment posted at 11pm Eastern counts toward the
        # correct business day (not the next UTC day). The offset is an integer
        # we compute here, so inlining it into the SQL is injection-safe.
        _off = business_now().utcoffset() or timedelta(0)
        _tz_mod = f"{int(_off.total_seconds())} seconds"
        cc = f"date(created_at, '{_tz_mod}')"   # created_at, business-local date
        uc = f"date(updated_at, '{_tz_mod}')"   # updated_at, business-local date

        rows_out: list[dict] = []
        for label, d_start, d_end in buckets:
            s, e = d_start.isoformat(), d_end.isoformat()

            claims_new = _scalar(
                f"SELECT COUNT(*) FROM claims_master "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            claims_touched = _scalar(
                f"SELECT COUNT(*) FROM claims_master "
                f"WHERE {uc} BETWEEN ? AND ? "
                f"AND {cc} NOT BETWEEN ? AND ?{cfilt}",
                (s, e, s, e, *cargs))
            payments_posted = _scalar(
                f"SELECT COUNT(*) FROM payments "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            payments_amount = _scalar(
                f"SELECT ROUND(COALESCE(SUM(PaymentAmount),0),2) FROM payments "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            cred_new = _scalar(
                f"SELECT COUNT(*) FROM credentialing "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            enroll_new = _scalar(
                f"SELECT COUNT(*) FROM enrollment "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            edi_new = _scalar(
                f"SELECT COUNT(*) FROM edi_setup "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            production_rows = _scalar(
                f"SELECT COUNT(*) FROM team_production "
                f"WHERE (work_date BETWEEN ? AND ? "
                f"OR {cc} BETWEEN ? AND ?){cfilt}",
                (s, e, s, e, *cargs))
            production_hours = _scalar(
                f"SELECT ROUND(COALESCE(SUM(time_spent),0),2) FROM team_production "
                f"WHERE (work_date BETWEEN ? AND ? "
                f"OR {cc} BETWEEN ? AND ?){cfilt}",
                (s, e, s, e, *cargs))
            notes_new = _scalar(
                f"SELECT COUNT(*) FROM notes_log "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))
            files_uploaded = _scalar(
                f"SELECT COUNT(*) FROM client_files "
                f"WHERE {cc} BETWEEN ? AND ?{cfilt}",
                (s, e, *cargs))

            # Billed = claim lines whose Bill Date falls in the window. Bill Date
            # is free-text, so normalize to the first 10 chars (ISO date prefix),
            # matching the dashboard's billing-activity computation.
            cur.execute(
                f"SELECT substr(COALESCE(BillDate,''),1,10), ChargeAmount "
                f"FROM claims_master "
                f"WHERE COALESCE(BillDate,'')!=''{cfilt}",
                cargs)
            billed_count = 0
            billed_amount = 0.0
            for _bd_raw, _amt_raw in cur.fetchall():
                try:
                    _bd = date.fromisoformat(str(_bd_raw or "").strip())
                except (ValueError, TypeError):
                    continue
                if d_start <= _bd <= d_end:
                    billed_count += 1
                    billed_amount += float(_amt_raw or 0)
            billed_amount = round(billed_amount, 2)

            # Distinct contributors who logged any work in the window.
            active_users = _scalar(
                "SELECT COUNT(*) FROM ("
                f"  SELECT PostedBy AS u FROM payments "
                f"   WHERE {cc} BETWEEN ? AND ? AND COALESCE(PostedBy,'')!=''{cfilt} "
                "  UNION "
                f"  SELECT username AS u FROM team_production "
                f"   WHERE (work_date BETWEEN ? AND ? OR {cc} BETWEEN ? AND ?) "
                f"     AND COALESCE(username,'')!=''{cfilt} "
                "  UNION "
                f"  SELECT uploaded_by AS u FROM client_files "
                f"   WHERE {cc} BETWEEN ? AND ? AND COALESCE(uploaded_by,'')!=''{cfilt} "
                "  UNION "
                f"  SELECT Author AS u FROM notes_log "
                f"   WHERE {cc} BETWEEN ? AND ? AND COALESCE(Author,'')!=''{cfilt} "
                ")",
                (s, e, *cargs,
                 s, e, s, e, *cargs,
                 s, e, *cargs,
                 s, e, *cargs))

            rows_out.append({
                "label": label,
                "start": s,
                "end": e,
                "claims_new": int(claims_new),
                "claims_touched": int(claims_touched),
                "payments_posted": int(payments_posted),
                "payments_amount": float(payments_amount),
                "billed_count": int(billed_count),
                "billed_amount": billed_amount,
                "cred_new": int(cred_new),
                "enroll_new": int(enroll_new),
                "edi_new": int(edi_new),
                "production_rows": int(production_rows),
                "production_hours": float(production_hours),
                "notes_new": int(notes_new),
                "files_uploaded": int(files_uploaded),
                "active_users": int(active_users),
            })

        # Column totals across all returned buckets.
        totals = {k: 0 for k in _ROLLUP_METRIC_KEYS}
        for r in rows_out:
            for k in _ROLLUP_METRIC_KEYS:
                totals[k] += r.get(k, 0)
        for k in ("payments_amount", "billed_amount", "production_hours"):
            totals[k] = round(totals[k], 2)

        return {
            "bucket": bucket,
            "count": len(rows_out),
            "client_id": int(client_id) if client_id is not None else None,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "metric_keys": list(_ROLLUP_METRIC_KEYS),
            "buckets": rows_out,
            "totals": totals,
        }
    finally:
        conn.close()


# ─── App Settings (in-DB encrypted secrets) ─────────────────────────────
#
# Lets admins paste credentials (SendGrid API key, SMTP password, etc.)
# into the hub UI instead of fighting Render env vars. Stored Fernet-
# encrypted via app.security so the SQLite file alone reveals nothing.

# Whitelisted settings keys — anything outside this set is rejected so
# the endpoint can never be used as a generic shell for arbitrary writes.
ALLOWED_SETTING_KEYS = {
    "SENDGRID_API_KEY", "SENDGRID_FROM",
    "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS",
    "NOTIFY_EMAILS", "EOD_REPORT_EMAIL",
}


def set_app_setting(key: str, value: str, updated_by: str = "") -> bool:
    """Store an encrypted secret. Empty value deletes the row."""
    k = (key or "").strip().upper()
    if k not in ALLOWED_SETTING_KEYS:
        return False
    val = (value or "").strip()
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        if not val:
            cur.execute("DELETE FROM app_settings WHERE key=?", (k,))
            conn.commit()
            return True
        try:
            from app.security import encrypt_message
            enc = encrypt_message(val)
        except Exception:
            enc = val  # last-resort plaintext; security.py will have logged
        cur.execute(
            "INSERT INTO app_settings (key, value_enc, updated_by, updated_at) "
            "VALUES (?,?,?,CURRENT_TIMESTAMP) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value_enc=excluded.value_enc, updated_by=excluded.updated_by, "
            "updated_at=CURRENT_TIMESTAMP",
            (k, enc, updated_by or ""),
        )
        conn.commit()
        return True
    except Exception:
        log.exception("set_app_setting failed for key=%s", key)
        return False
    finally:
        if conn:
            conn.close()


def get_app_setting(key: str) -> str:
    """Return the decrypted value (or empty string)."""
    k = (key or "").strip().upper()
    if k not in ALLOWED_SETTING_KEYS:
        return ""
    conn = get_db()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT value_enc FROM app_settings WHERE key=?", (k,)
        ).fetchone()
        if not row or not row[0]:
            return ""
        try:
            from app.security import decrypt_message
            return decrypt_message(row[0]) or ""
        except Exception:
            return ""
    finally:
        conn.close()


def list_app_settings() -> dict:
    """Return a dict of {key: {"set": bool, "preview": str, "updated_*": ...}}
    for every whitelisted key. Never returns full secret values."""
    out = {k: {"set": False, "preview": "", "updated_by": "",
               "updated_at": ""} for k in ALLOWED_SETTING_KEYS}
    conn = get_db()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT key, value_enc, updated_by, updated_at FROM app_settings"
        ).fetchall()
        try:
            from app.security import decrypt_message
        except Exception:
            decrypt_message = lambda x: ""  # noqa: E731
        for r in rows:
            k = r["key"]
            if k not in out:
                continue
            try:
                v = decrypt_message(r["value_enc"]) or ""
            except Exception:
                v = ""
            preview = ""
            if v:
                if k in ("SENDGRID_API_KEY", "SMTP_PASS"):
                    preview = v[:6] + "…" + v[-2:] if len(v) > 10 else "***"
                else:
                    preview = v
            out[k] = {
                "set": bool(v),
                "preview": preview,
                "updated_by": r["updated_by"] or "",
                "updated_at": r["updated_at"] or "",
            }
    finally:
        conn.close()
    return out


# ─── Team Tracking / Productivity (ActivTrak-style) ───────────────────────
#
# We log every authenticated HTTP request + login/logout + frontend heartbeats
# into `activity_events`, and maintain a per-user-per-day rollup in
# `user_presence`. "Active seconds" is computed as the gap between consecutive
# events for the same user, capped at IDLE_THRESHOLD_SECONDS (default 5 min).
# Gaps longer than the threshold are considered idle and not counted.

IDLE_THRESHOLD_SECONDS = 5 * 60   # gap > 5 min = idle session
HEARTBEAT_INTERVAL_SEC = 60       # frontend pings this often when tab is focused
PRODUCTIVITY_TARGET_HOURS = 7.0   # used for the 0-100 productivity score


def log_activity(username: str,
                 event_type: str,
                 *,
                 client_id: int = None,
                 method: str = "",
                 path: str = "",
                 status_code: int = None,
                 duration_ms: int = None,
                 ip: str = "",
                 user_agent: str = "",
                 details: str = "") -> None:
    """Record one timestamped activity event and update the user's daily
    presence rollup. Safe — swallows any errors so tracking never breaks the
    main request flow.
    """
    if not username:
        return
    username = username.strip().lower()
    if not username:
        return
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        now = datetime.now()
        now_iso = now.isoformat(timespec="seconds")
        today = now.strftime("%Y-%m-%d")

        cur.execute(
            "INSERT INTO activity_events "
            "(occurred_at, username, client_id, event_type, method, path, "
            " status_code, duration_ms, ip, user_agent, details) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (now_iso, username, client_id, event_type, method or "",
             path or "", status_code, duration_ms, ip or "",
             (user_agent or "")[:255], details or ""),
        )

        cur.execute(
            "SELECT last_seen_at, active_seconds, idle_seconds, action_count "
            "FROM user_presence WHERE username=? AND work_date=?",
            (username, today),
        )
        row = cur.fetchone()
        counts_as_action = event_type not in ("heartbeat",)
        action_inc = 1 if counts_as_action else 0

        if row is None:
            cur.execute(
                "INSERT INTO user_presence "
                "(username, work_date, first_seen_at, last_seen_at, "
                " active_seconds, idle_seconds, action_count) "
                "VALUES (?,?,?,?,?,?,?)",
                (username, today, now_iso, now_iso, 0, 0, action_inc),
            )
        else:
            try:
                prev = datetime.fromisoformat(row["last_seen_at"]) if row["last_seen_at"] else now
            except Exception:
                prev = now
            gap = max(0, int((now - prev).total_seconds()))
            if gap <= IDLE_THRESHOLD_SECONDS:
                active_add, idle_add = gap, 0
            else:
                active_add, idle_add = 0, gap
            cur.execute(
                "UPDATE user_presence SET "
                "  last_seen_at=?, "
                "  active_seconds=active_seconds+?, "
                "  idle_seconds=idle_seconds+?, "
                "  action_count=action_count+? "
                "WHERE username=? AND work_date=?",
                (now_iso, active_add, idle_add, action_inc, username, today),
            )
        conn.commit()
    except Exception:
        pass
    finally:
        if conn:
            conn.close()


def list_activity_events(username: str = None,
                         start: str = None,
                         end: str = None,
                         event_type: str = None,
                         limit: int = 500) -> list[dict]:
    """List recent activity events with optional filters."""
    conn = get_db()
    try:
        cur = conn.cursor()
        conds, params = [], []
        if username:
            conds.append("lower(username)=lower(?)")
            params.append(username)
        if start:
            conds.append("occurred_at >= ?")
            params.append(start)
        if end:
            conds.append("occurred_at <= ?")
            params.append(end)
        if event_type:
            conds.append("event_type=?")
            params.append(event_type)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        params.append(int(limit))
        cur.execute(
            f"SELECT * FROM activity_events {where} "
            f"ORDER BY occurred_at DESC LIMIT ?", params,
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_live_users(within_seconds: int = 300) -> list[dict]:
    """Users seen within the last N seconds — 'who is online right now'."""
    conn = get_db()
    try:
        cutoff = (datetime.now() - timedelta(seconds=within_seconds)).isoformat(timespec="seconds")
        cur = conn.cursor()
        cur.execute(
            "SELECT username, MAX(occurred_at) AS last_seen, COUNT(*) AS recent_events "
            "FROM activity_events WHERE occurred_at >= ? "
            "GROUP BY username ORDER BY last_seen DESC",
            (cutoff,),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_productivity_report(start_date: str = None,
                            end_date: str = None,
                            username: str = None) -> dict:
    """ActivTrak-style productivity report aggregated from `user_presence`.

    Returns one row per user per day with active/idle minutes, action count
    and a 0-100 productivity score relative to PRODUCTIVITY_TARGET_HOURS.
    Also returns per-user totals and a top-paths breakdown.
    """
    conn = get_db()
    try:
        cur = conn.cursor()
        conds, params = [], []
        if start_date:
            conds.append("work_date >= ?")
            params.append(start_date)
        if end_date:
            conds.append("work_date <= ?")
            params.append(end_date)
        if username:
            conds.append("lower(username)=lower(?)")
            params.append(username)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""

        target_sec = PRODUCTIVITY_TARGET_HOURS * 3600.0
        cur.execute(
            f"SELECT username, work_date, first_seen_at, last_seen_at, "
            f"       active_seconds, idle_seconds, action_count "
            f"FROM user_presence {where} "
            f"ORDER BY work_date DESC, username", params,
        )
        daily = []
        for r in cur.fetchall():
            d = dict(r)
            score = round(min(100.0, (d["active_seconds"] / target_sec) * 100.0), 1) if target_sec else 0.0
            d["active_minutes"] = round(d["active_seconds"] / 60.0, 1)
            d["idle_minutes"]   = round(d["idle_seconds"]   / 60.0, 1)
            d["productivity_score"] = score
            daily.append(d)

        cur.execute(
            f"SELECT username, "
            f"       SUM(active_seconds) AS active_seconds, "
            f"       SUM(idle_seconds)   AS idle_seconds, "
            f"       SUM(action_count)   AS action_count, "
            f"       COUNT(*)            AS days_active "
            f"FROM user_presence {where} "
            f"GROUP BY username ORDER BY active_seconds DESC", params,
        )
        by_user = []
        for r in cur.fetchall():
            d = dict(r)
            d["active_seconds"] = int(d["active_seconds"] or 0)
            d["idle_seconds"]   = int(d["idle_seconds"]   or 0)
            d["action_count"]   = int(d["action_count"]   or 0)
            d["active_hours"]   = round(d["active_seconds"] / 3600.0, 2)
            d["idle_hours"]     = round(d["idle_seconds"]   / 3600.0, 2)
            avg_daily_sec = (d["active_seconds"] / d["days_active"]) if d["days_active"] else 0
            d["avg_active_hours_per_day"] = round(avg_daily_sec / 3600.0, 2)
            d["productivity_score"] = round(min(100.0, (avg_daily_sec / target_sec) * 100.0), 1) if target_sec else 0.0
            by_user.append(d)

        ev_conds, ev_params = ["event_type='request'"], []
        if start_date:
            ev_conds.append("date(occurred_at) >= ?")
            ev_params.append(start_date)
        if end_date:
            ev_conds.append("date(occurred_at) <= ?")
            ev_params.append(end_date)
        if username:
            ev_conds.append("lower(username)=lower(?)")
            ev_params.append(username)
        ev_where = "WHERE " + " AND ".join(ev_conds)
        cur.execute(
            f"SELECT path, COUNT(*) AS hits FROM activity_events {ev_where} "
            f"GROUP BY path ORDER BY hits DESC LIMIT 20", ev_params,
        )
        top_paths = [dict(r) for r in cur.fetchall()]

        return {
            "ok": True,
            "target_hours_per_day": PRODUCTIVITY_TARGET_HOURS,
            "idle_threshold_seconds": IDLE_THRESHOLD_SECONDS,
            "daily": daily,
            "by_user": by_user,
            "top_paths": top_paths,
        }
    finally:
        conn.close()


# ─── End-of-Day Team Report ──────────────────────────────────────────────

# ─── Business-development leads (sales pipeline) ──────────────────────────────

LEAD_CLOSED_STATUSES = {"won", "lost", "closed"}
# BizDev follow-up cadence: a lead with no contact in this many days is "due".
LEAD_FOLLOWUP_DAYS = 2
_LEAD_EDITABLE_FIELDS = (
    "practice_name", "contact_name", "contact_email", "contact_phone",
    "service_rcm", "service_payor", "service_workflow", "service_compliance",
    "status", "est_value", "owner", "notes",
)


def _parse_dt(value):
    """Best-effort parse of the various timestamp formats stored in leads."""
    if not value:
        return None
    s = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt) + 2] if len(s) >= len(fmt) else s, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _lead_row_to_dict(row) -> dict:
    d = dict(row)
    lines = [label for label, col in (
        ("RCM", "service_rcm"), ("Payor", "service_payor"),
        ("Workflow", "service_workflow"), ("Compliance", "service_compliance"),
    ) if d.get(col)]
    d["service_lines"] = lines
    d["is_combination"] = len(lines) >= 2
    # Follow-up status: time since last contact (a logged follow-up, or creation).
    last_contact = _parse_dt(d.get("last_follow_up_at")) or _parse_dt(d.get("created_at"))
    if last_contact:
        days = (datetime.now() - last_contact).total_seconds() / 86400.0
        d["days_since_contact"] = int(days)
    else:
        d["days_since_contact"] = None
    d["followup_due"] = bool(
        not d.get("is_closed")
        and d["days_since_contact"] is not None
        and d["days_since_contact"] >= LEAD_FOLLOWUP_DAYS
    )
    return d


def list_leads(category: str = None) -> list:
    cat = (category or "all").strip().lower()
    where = ["COALESCE(deleted_at,'')=''"]
    if cat == "closed":
        where.append("is_closed=1")
    else:
        where.append("is_closed=0")
        if cat == "rcm":
            where.append("service_rcm=1")
        elif cat == "payor":
            where.append("service_payor=1")
        elif cat == "workflow":
            where.append("service_workflow=1")
        elif cat == "compliance":
            where.append("service_compliance=1")
        elif cat == "combination":
            where.append("(service_rcm+service_payor+service_workflow+service_compliance) >= 2")
    sql = "SELECT * FROM leads WHERE " + " AND ".join(where) + " ORDER BY updated_at DESC, id DESC"
    conn = get_db()
    try:
        return [_lead_row_to_dict(r) for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()


def create_lead(data: dict) -> int:
    status = (data.get("status") or "New").strip() or "New"
    is_closed = 1 if status.lower() in LEAD_CLOSED_STATUSES else 0
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO leads
               (practice_name, contact_name, contact_email, contact_phone,
                service_rcm, service_payor, service_workflow, service_compliance,
                status, est_value, owner, notes, is_closed)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                (data.get("practice_name") or "").strip(),
                (data.get("contact_name") or "").strip(),
                (data.get("contact_email") or "").strip(),
                (data.get("contact_phone") or "").strip(),
                1 if data.get("service_rcm") else 0,
                1 if data.get("service_payor") else 0,
                1 if data.get("service_workflow") else 0,
                1 if data.get("service_compliance") else 0,
                status,
                float(data.get("est_value") or 0),
                (data.get("owner") or "").strip(),
                (data.get("notes") or "").strip(),
                is_closed,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_lead(lead_id: int, changes: dict) -> bool:
    sets = {}
    for k, v in (changes or {}).items():
        if k not in _LEAD_EDITABLE_FIELDS:
            continue
        if k.startswith("service_"):
            sets[k] = 1 if v else 0
        elif k == "est_value":
            sets[k] = float(v or 0)
        else:
            sets[k] = (str(v).strip() if v is not None else "")
    if "status" in sets:
        sets["is_closed"] = 1 if sets["status"].lower() in LEAD_CLOSED_STATUSES else 0
    if not sets:
        return False
    sets["updated_at"] = datetime.now().isoformat(sep=" ", timespec="seconds")
    cols = ", ".join(f"{k}=?" for k in sets)
    vals = list(sets.values()) + [lead_id]
    conn = get_db()
    try:
        conn.execute(f"UPDATE leads SET {cols} WHERE id=?", vals)
        conn.commit()
        return True
    finally:
        conn.close()


def delete_lead(lead_id: int) -> bool:
    """Soft-delete: archive the lead (set deleted_at) instead of erasing it,
    so it can be recovered from the 'Deleted leads' view. Real data is never
    lost to a stray click."""
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE leads SET deleted_at=? WHERE id=? "
            "AND COALESCE(deleted_at,'')=''",
            (datetime.now().isoformat(), lead_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def restore_lead(lead_id: int) -> bool:
    """Bring a soft-deleted lead back into the active pipeline."""
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE leads SET deleted_at='' WHERE id=? "
            "AND COALESCE(deleted_at,'')<>''", (lead_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def list_deleted_leads() -> list:
    """Return archived (soft-deleted) leads, most-recently-deleted first."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM leads WHERE COALESCE(deleted_at,'')<>'' "
            "ORDER BY deleted_at DESC, id DESC").fetchall()
        return [_lead_row_to_dict(r) for r in rows]
    finally:
        conn.close()


def mark_lead_followed_up(lead_id: int) -> bool:
    """Record that a BizDev follow-up just happened — resets the 2-day clock."""
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE leads SET last_follow_up_at=?, updated_at=? WHERE id=?",
            (now, now, lead_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def list_leads_due_followup() -> list:
    """Open leads with no contact in the last LEAD_FOLLOWUP_DAYS days.

    Used by the BizDev UI banner so Victor always sees who needs a touch."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM leads WHERE is_closed=0 AND COALESCE(deleted_at,'')='' "
            "ORDER BY COALESCE(last_follow_up_at, created_at) ASC, id ASC"
        ).fetchall()
    finally:
        conn.close()
    due = [_lead_row_to_dict(r) for r in rows]
    return [d for d in due if d.get("followup_due")]


def claim_leads_for_reminder() -> list:
    """Return open, overdue leads that haven't been reminded in the last
    LEAD_FOLLOWUP_DAYS days, and stamp last_reminder_at=now on them.

    This drives the scheduled "every 2 days" email so a single overdue lead
    is not re-emailed daily — only once per cadence window."""
    now = datetime.now()
    now_s = now.isoformat(sep=" ", timespec="seconds")
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM leads WHERE is_closed=0 AND COALESCE(deleted_at,'')=''"
        ).fetchall()
        to_remind = []
        for r in rows:
            d = _lead_row_to_dict(r)
            if not d.get("followup_due"):
                continue
            last_rem = _parse_dt(d.get("last_reminder_at"))
            if last_rem and (now - last_rem).total_seconds() / 86400.0 < LEAD_FOLLOWUP_DAYS:
                continue
            to_remind.append(d)
        if to_remind:
            ids = [d["id"] for d in to_remind]
            conn.executemany(
                "UPDATE leads SET last_reminder_at=? WHERE id=?",
                [(now_s, i) for i in ids],
            )
            conn.commit()
        return to_remind
    finally:
        conn.close()


def get_leads_weekly_report(week_start: str = None) -> dict:
    """Weekly (Mon–Sun) business-development snapshot for the Victor / BizDev view."""
    if week_start:
        try:
            anchor = datetime.strptime(week_start, "%Y-%m-%d")
        except Exception:
            anchor = datetime.now()
    else:
        anchor = datetime.now()
    monday = (anchor - timedelta(days=anchor.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0)
    next_monday = monday + timedelta(days=7)
    ws = monday.strftime("%Y-%m-%d %H:%M:%S")
    we = next_monday.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    try:
        cur = conn.cursor()

        def _count(where, args=()):
            return cur.execute(
                f"SELECT COUNT(*) FROM leads WHERE COALESCE(deleted_at,'')='' AND ({where})",
                args).fetchone()[0]

        categories = {
            "rcm": _count("is_closed=0 AND service_rcm=1"),
            "payor": _count("is_closed=0 AND service_payor=1"),
            "workflow": _count("is_closed=0 AND service_workflow=1"),
            "compliance": _count("is_closed=0 AND service_compliance=1"),
            "combination": _count("is_closed=0 AND (service_rcm+service_payor+service_workflow+service_compliance)>=2"),
            "closed": _count("is_closed=1"),
            "open_total": _count("is_closed=0"),
        }
        new_this_week = _count("created_at>=? AND created_at<?", (ws, we))
        closed_this_week = _count("is_closed=1 AND updated_at>=? AND updated_at<?", (ws, we))
        pipeline_value = cur.execute(
            "SELECT COALESCE(SUM(est_value),0) FROM leads WHERE is_closed=0 AND COALESCE(deleted_at,'')=''").fetchone()[0]
        won_value = cur.execute(
            "SELECT COALESCE(SUM(est_value),0) FROM leads WHERE LOWER(status)='won' "
            "AND COALESCE(deleted_at,'')='' AND updated_at>=? AND updated_at<?", (ws, we)).fetchone()[0]
        rows = cur.execute(
            "SELECT * FROM leads WHERE COALESCE(deleted_at,'')='' AND ((created_at>=? AND created_at<?) "
            "OR (updated_at>=? AND updated_at<?)) ORDER BY updated_at DESC LIMIT 100",
            (ws, we, ws, we)).fetchall()
        return {
            "week_start": monday.strftime("%Y-%m-%d"),
            "week_end": (next_monday - timedelta(days=1)).strftime("%Y-%m-%d"),
            "categories": categories,
            "new_this_week": new_this_week,
            "closed_this_week": closed_this_week,
            "pipeline_value": round(float(pipeline_value), 2),
            "won_value_this_week": round(float(won_value), 2),
            "rows": [_lead_row_to_dict(r) for r in rows],
        }
    finally:
        conn.close()


# ── Business-development pipeline forecast ────────────────────────────────────
# Ordered sales stages and the probability each will close. Weighted forecast =
# Σ(est_value × stage probability) across open leads — a realistic revenue
# expectation, not just a raw pipeline total.
LEAD_PIPELINE_STAGES = ["New", "Contacted", "Qualified", "Proposal", "Negotiation"]
LEAD_STAGE_PROBABILITY = {
    "new": 0.10, "contacted": 0.20, "qualified": 0.40,
    "proposal": 0.60, "negotiation": 0.80, "won": 1.0, "lost": 0.0,
}


def get_leads_pipeline() -> dict:
    """Weighted sales pipeline for the Business Development view.

    Returns per-stage counts/value/weighted forecast, headline totals (open
    value, weighted forecast, won value), conversion rate, and the leads that
    have gone stale (no contact past the follow-up window) so nothing rots.
    """
    conn = get_db()
    try:
        rows = [_lead_row_to_dict(r) for r in conn.execute(
            "SELECT * FROM leads WHERE COALESCE(deleted_at,'')=''"
        ).fetchall()]
    finally:
        conn.close()

    stages = {s: {"stage": s, "count": 0, "value": 0.0, "weighted": 0.0}
              for s in LEAD_PIPELINE_STAGES}
    total_open_value = 0.0
    total_weighted = 0.0
    open_count = 0
    won_count = 0
    won_value = 0.0
    lost_count = 0
    stalled = []

    for r in rows:
        status = (r.get("status") or "New").strip()
        skey = status.lower()
        val = float(r.get("est_value") or 0)
        prob = LEAD_STAGE_PROBABILITY.get(skey, 0.10)
        if skey == "won":
            won_count += 1
            won_value += val
            continue
        if skey == "lost" or r.get("is_closed"):
            lost_count += 1
            continue
        # Open lead.
        open_count += 1
        total_open_value += val
        total_weighted += val * prob
        if status not in stages:
            stages[status] = {"stage": status, "count": 0, "value": 0.0, "weighted": 0.0}
        stages[status]["count"] += 1
        stages[status]["value"] += val
        stages[status]["weighted"] += val * prob
        if r.get("followup_due"):
            stalled.append({
                "id": r.get("id"),
                "practice_name": r.get("practice_name") or "—",
                "status": status,
                "owner": r.get("owner") or "",
                "est_value": round(val, 2),
                "days_since_contact": r.get("days_since_contact"),
            })

    stage_list = []
    for s in LEAD_PIPELINE_STAGES:
        b = stages.get(s, {"stage": s, "count": 0, "value": 0.0, "weighted": 0.0})
        stage_list.append({
            "stage": s,
            "probability": int(LEAD_STAGE_PROBABILITY.get(s.lower(), 0.1) * 100),
            "count": b["count"],
            "value": round(b["value"], 2),
            "weighted": round(b["weighted"], 2),
        })
    stalled.sort(key=lambda x: (x["days_since_contact"] or 0), reverse=True)
    closed_total = won_count + lost_count
    conversion_rate = round((won_count / closed_total) * 100, 1) if closed_total else 0.0

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "stages": stage_list,
        "open_count": open_count,
        "total_open_value": round(total_open_value, 2),
        "weighted_forecast": round(total_weighted, 2),
        "won_count": won_count,
        "won_value": round(won_value, 2),
        "lost_count": lost_count,
        "conversion_rate": conversion_rate,
        "stalled": stalled,
    }


def get_eod_team_report(report_date: str = None) -> dict:
    """Build the full end-of-day report for the team.

    Pulls every per-tab data store the hub has (claims, credentialing,
    enrollment, EDI, production logs, notes, audit log, file uploads,
    chat messages, activity firehose, presence rollup) and groups by
    user, then by client, so the daily email shows exactly:
      - who was active
      - what client they worked
      - what tab they touched (Claims / Credentialing / Enrollment /
        EDI / Documents / Production / Chat / Reporting)
      - how many rows they created/updated
      - how many notes / files / messages they added
      - hours active + idle on the platform

    Returns a structured dict the emailer can render as HTML.
    """
    from collections import defaultdict
    if not report_date:
        report_date = business_today_iso()
    day_start = f"{report_date} 00:00:00"
    day_end   = f"{report_date} 23:59:59"

    conn = get_db()
    try:
        cur = conn.cursor()

        # Map client_id -> ("Company Name", enabled_modules:list) for display
        # and per-account module filtering on the report.
        client_lookup: dict[int, str] = {}
        client_modules: dict[int, list[str]] = {}
        for row in cur.execute(
            "SELECT id, COALESCE(NULLIF(company,''), username) AS name, "
            "       enabled_modules FROM clients WHERE COALESCE(role,'client')='client' "
            "OR id IN (SELECT DISTINCT client_id FROM claims_master)"
        ).fetchall():
            cid_i = int(row["id"])
            client_lookup[cid_i] = row["name"]
            raw_mods = (row["enabled_modules"] or "").strip()
            mods = DEFAULT_ENABLED_MODULES[:]
            if raw_mods:
                try:
                    import json as _json
                    parsed = _json.loads(raw_mods)
                    if isinstance(parsed, list) and parsed:
                        mods = [str(m).lower() for m in parsed]
                except Exception:
                    pass
            client_modules[cid_i] = mods

        # Map username -> {contact_name, email, role} for the team roster.
        # The canonical email-style row wins so legacy short usernames
        # ('eric', 'jessica', ...) inherit the real contact name + the
        # email we want notifications to land in.
        _LEGACY_TO_CANONICAL = {
            "admin":   "admin@medprosc.com",
            "eric":    "eric@medprosc.com",
            "susan":   "susan@medprosc.com",
            "melissa": "melissa@medprosc.com",
            "jessica": "jessica@medprosc.com",
        }
        team_lookup: dict[str, dict] = {}
        canonical_meta: dict[str, dict] = {}
        for row in cur.execute(
            "SELECT lower(username) AS u, COALESCE(NULLIF(contact_name,''), username) AS name, "
            "       email, role FROM clients "
            "WHERE COALESCE(is_active,1)=1"
        ).fetchall():
            d = dict(row)
            team_lookup[d["u"]] = d
            canonical_meta[d["u"]] = d
        # Overlay: each legacy short username inherits its canonical row's
        # contact_name + email if the canonical row exists.
        for short, canonical in _LEGACY_TO_CANONICAL.items():
            if canonical in canonical_meta:
                meta = dict(canonical_meta[canonical])
                meta["u"] = short
                team_lookup[short] = meta

        def _client_name(cid):
            try:
                cid_i = int(cid) if cid is not None else 0
            except (TypeError, ValueError):
                cid_i = 0
            if not cid_i:
                return "— No client tagged —"
            return client_lookup.get(cid_i, f"Client #{cid_i}")

        # Skeleton: per-user, per-client, per-tab counts.
        TAB_KEYS = (
            "Claims", "Payments", "Credentialing", "Enrollment", "EDI",
            "Production", "Leads", "Documents", "Notes", "Chat", "Audit", "Pageviews",
        )

        def _new_tab_bucket():
            return {k: 0 for k in TAB_KEYS}

        # users[user_key] = {
        #   "username": ..., "contact_name": ..., "email": ..., "role": ...,
        #   "active_hours": float, "idle_hours": float, "actions": int,
        #   "first_seen": str, "last_seen": str,
        #   "totals": {tab: int}, "highlights": list[str],
        #   "clients": { client_name: {totals: {tab:int}, items: list[dict]} }
        # }
        users: dict[str, dict] = defaultdict(lambda: {
            "username": "",
            "contact_name": "",
            "email": "",
            "role": "",
            "is_admin": False,
            "active_hours": 0.0,
            "idle_hours": 0.0,
            "actions": 0,
            "first_seen": "",
            "last_seen": "",
            "totals": _new_tab_bucket(),
            "billed": {
                "today": {"count": 0, "amount": 0.0},
                "wtd":   {"count": 0, "amount": 0.0},
            },
            "highlights": [],
            "clients": defaultdict(lambda: {
                "client_id": 0,
                "enabled_modules": list(DEFAULT_ENABLED_MODULES),
                "totals": _new_tab_bucket(),
                "items": [],
            }),
        })

        def _u(username: str) -> dict:
            key = (username or "").strip().lower() or "unknown"
            slot = users[key]
            slot["username"] = key
            meta = team_lookup.get(key, {})
            slot["contact_name"] = meta.get("name") or key.title()
            slot["email"] = meta.get("email") or ""
            role = (meta.get("role") or "").lower()
            slot["role"] = role
            slot["is_admin"] = role in ("admin", "owner", "superadmin")
            return slot

        def _bump(username: str, client_id, tab: str, item: dict = None):
            slot = _u(username)
            slot["totals"][tab] = slot["totals"].get(tab, 0) + 1
            cname = _client_name(client_id)
            cb = slot["clients"][cname]
            cb["totals"][tab] = cb["totals"].get(tab, 0) + 1
            # Populate client_id + enabled_modules once per (user, client) bucket.
            try:
                cid_i = int(client_id) if client_id is not None else 0
            except (TypeError, ValueError):
                cid_i = 0
            if cid_i and not cb.get("client_id"):
                cb["client_id"] = cid_i
                cb["enabled_modules"] = client_modules.get(cid_i, list(DEFAULT_ENABLED_MODULES))
            if item is not None and len(cb["items"]) < 25:
                cb["items"].append({"tab": tab, **item})

        # ── 1) Presence rollup → active/idle hours ──
        for row in cur.execute(
            "SELECT username, active_seconds, idle_seconds, action_count, "
            "       first_seen_at, last_seen_at "
            "FROM user_presence WHERE work_date=?",
            (report_date,),
        ).fetchall():
            slot = _u(row["username"])
            slot["active_hours"] = round((row["active_seconds"] or 0) / 3600.0, 2)
            slot["idle_hours"]   = round((row["idle_seconds"]   or 0) / 3600.0, 2)
            slot["actions"]      = int(row["action_count"] or 0)
            slot["first_seen"]   = row["first_seen_at"] or ""
            slot["last_seen"]    = row["last_seen_at"]  or ""

        # ── 2) Activity firehose → pageviews per client ──
        for row in cur.execute(
            "SELECT username, client_id, path, COUNT(*) AS hits "
            "FROM activity_events "
            "WHERE date(occurred_at)=? AND event_type IN ('request','pageview') "
            "GROUP BY username, client_id, path "
            "ORDER BY hits DESC",
            (report_date,),
        ).fetchall():
            if not row["username"]:
                continue
            slot = _u(row["username"])
            slot["totals"]["Pageviews"] = slot["totals"].get("Pageviews", 0) + int(row["hits"])
            cname = _client_name(row["client_id"])
            cb = slot["clients"][cname]
            cb["totals"]["Pageviews"] = cb["totals"].get("Pageviews", 0) + int(row["hits"])

        # ── 3) Claims created/updated today, attributed to Owner ──
        for row in cur.execute(
            "SELECT client_id, ClaimKey, ClaimStatus, Owner, "
            "       created_at, updated_at, "
            "       date(created_at) AS cd, date(updated_at) AS ud "
            "FROM claims_master "
            "WHERE date(created_at)=? OR date(updated_at)=?",
            (report_date, report_date),
        ).fetchall():
            owner = (row["Owner"] or "").strip().lower()
            if not owner:
                continue
            action = "created" if row["cd"] == report_date else "updated"
            ts = row["created_at"] if action == "created" else row["updated_at"]
            _bump(owner, row["client_id"], "Claims", {
                "action": action,
                "title": f"{row['ClaimKey']} ({row['ClaimStatus']})",
                "ts": ts or "",
            })

        # ── 3b) Payments posted today, attributed to the poster ──
        # Payment posting is part of how the team is paid, so credit each
        # payment to the hub user who posted it (PostedBy). Older rows with no
        # PostedBy are skipped (same as ownerless claims) since there is no
        # reliable way to attribute them.
        for row in cur.execute(
            "SELECT client_id, ClaimKey, PaymentAmount, PayerType, PostDate, "
            "       PostedBy, created_at "
            "FROM payments WHERE date(created_at)=?",
            (report_date,),
        ).fetchall():
            poster = (row["PostedBy"] or "").strip().lower()
            if not poster:
                continue
            try:
                amt = float(row["PaymentAmount"] or 0)
            except (TypeError, ValueError):
                amt = 0.0
            amt_str = f" · ${amt:,.2f}" if amt else ""
            _bump(poster, row["client_id"], "Payments", {
                "action": "posted",
                "title": f"{row['ClaimKey']} ({row['PayerType'] or 'Payment'}){amt_str}",
                "ts": row["created_at"] or "",
            })

        # ── 3c) Billed $ attributed to the claim Owner, week-to-date ──
        # Eric/Amanda want the daily report to surface how much was *billed*
        # (sum of ChargeAmount on claims that carry a BillDate) attributed to
        # whoever owns the claim — not just today, but cumulatively "since last
        # Friday". Compute the most recent Friday on/before report_date as the
        # window start so the figure resets each Friday alongside the work week.
        try:
            _rd = datetime.strptime(report_date, "%Y-%m-%d").date()
            _days_since_fri = (_rd.weekday() - 4) % 7   # Mon=0 … Fri=4 … Sun=6
            week_start = _rd.fromordinal(_rd.toordinal() - _days_since_fri).isoformat()
        except Exception:
            week_start = report_date
        for row in cur.execute(
            "SELECT Owner, ChargeAmount, BillDate FROM claims_master "
            "WHERE COALESCE(BillDate,'') != '' AND BillDate >= ? AND BillDate <= ?",
            (week_start, report_date),
        ).fetchall():
            owner = (row["Owner"] or "").strip().lower()
            if not owner:
                continue
            try:
                charge = float(row["ChargeAmount"] or 0)
            except (TypeError, ValueError):
                charge = 0.0
            slot = _u(owner)
            slot["billed"]["wtd"]["count"] += 1
            slot["billed"]["wtd"]["amount"] += charge
            if row["BillDate"] == report_date:
                slot["billed"]["today"]["count"] += 1
                slot["billed"]["today"]["amount"] += charge

        # ── 4) Credentialing / Enrollment / EDI created/updated today ──
        for table, tab, status_col in (
            ("credentialing", "Credentialing", "Status"),
            ("enrollment",    "Enrollment",    "Status"),
            ("edi_setup",     "EDI",           "EDIStatus"),
        ):
            for row in cur.execute(
                f"SELECT client_id, ProviderName, Payor, {status_col} AS Status, Owner, "
                f"       created_at, updated_at, "
                f"       date(created_at) AS cd, date(updated_at) AS ud "
                f"FROM {table} "
                f"WHERE date(created_at)=? OR date(updated_at)=?",
                (report_date, report_date),
            ).fetchall():
                owner = (row["Owner"] or "").strip().lower()
                if not owner:
                    continue
                action = "created" if row["cd"] == report_date else "updated"
                ts = row["created_at"] if action == "created" else row["updated_at"]
                _bump(owner, row["client_id"], tab, {
                    "action": action,
                    "title": f"{row['ProviderName'] or '—'} · {row['Payor'] or '—'} ({row['Status'] or 'Not Started'})",
                    "ts": ts or "",
                })

        # ── 5) Production entries (Team Production tab) ──
        try:
            for row in cur.execute(
                "SELECT client_id, username, category, task_description, "
                "       quantity, time_spent, created_at FROM team_production "
                "WHERE date(created_at)=? OR work_date=?",
                (report_date, report_date),
            ).fetchall():
                if not row["username"]:
                    continue
                _bump(row["username"], row["client_id"], "Production", {
                    "action": "logged",
                    "title": f"{row['category'] or '—'}: {(row['task_description'] or '')[:80]} "
                             f"({row['quantity'] or 0} · {row['time_spent'] or 0}h)",
                    "ts": row["created_at"] or "",
                })
        except Exception:
            pass

        # ── 6) Notes log ──
        try:
            for row in cur.execute(
                "SELECT client_id, ClaimKey, Module, Author, Note, created_at "
                "FROM notes_log WHERE date(created_at)=?",
                (report_date,),
            ).fetchall():
                author = (row["Author"] or "").strip().lower()
                if not author:
                    continue
                _bump(author, row["client_id"], "Notes", {
                    "action": "noted",
                    "title": f"{row['Module'] or 'Claim'} {row['ClaimKey'] or ''} — {(row['Note'] or '')[:80]}",
                    "ts": row["created_at"] or "",
                })
        except Exception:
            pass

        # ── 7) Audit log (catch-all of explicit operator actions) ──
        try:
            for row in cur.execute(
                "SELECT client_id, username, action, entity_type, entity_id, details "
                "FROM audit_log WHERE date(created_at)=?",
                (report_date,),
            ).fetchall():
                user_key = (row["username"] or "").strip().lower()
                if not user_key:
                    continue
                slot = _u(user_key)
                slot["totals"]["Audit"] = slot["totals"].get("Audit", 0) + 1
                cname = _client_name(row["client_id"])
                cb = slot["clients"][cname]
                cb["totals"]["Audit"] = cb["totals"].get("Audit", 0) + 1
                # Audit detail lines get put in the "highlights" pool so the
                # email shows operator-meaningful events without flooding.
                if len(slot["highlights"]) < 8:
                    label = row["action"] or "action"
                    where = row["entity_type"] or ""
                    extra = (row["details"] or "")[:120]
                    slot["highlights"].append(
                        f"{label} {where} — {extra}" if extra else f"{label} {where}"
                    )
        except Exception:
            pass

        # ── 8) File uploads ──
        try:
            for row in cur.execute(
                "SELECT client_id, original_name, uploaded_by, category, created_at "
                "FROM client_files WHERE date(created_at)=?",
                (report_date,),
            ).fetchall():
                user_key = (row["uploaded_by"] or "").strip().lower()
                if not user_key:
                    continue
                _bump(user_key, row["client_id"], "Documents", {
                    "action": "uploaded",
                    "title": f"{row['original_name']} · {row['category'] or 'General'}",
                    "ts": row["created_at"] or "",
                })
        except Exception:
            pass

        # ── 9) Chat messages sent ──
        try:
            for row in cur.execute(
                "SELECT m.room_id, m.sender_name, r.client_id, r.name AS room_name, m.created_at "
                "FROM chat_messages m LEFT JOIN chat_rooms r ON r.id = m.room_id "
                "WHERE date(m.created_at)=?",
                (report_date,),
            ).fetchall():
                sender = (row["sender_name"] or "").strip().lower()
                if not sender:
                    continue
                _bump(sender, row["client_id"], "Chat", {
                    "action": "messaged",
                    "title": f"room '{row['room_name'] or row['room_id']}'",
                    "ts": row["created_at"] or "",
                })
        except Exception:
            pass

        # ── 10) Business Development leads worked today (so the bizdev/Victor
        #        appears in the daily report right alongside everyone else) ──
        try:
            for row in cur.execute(
                "SELECT practice_name, status, est_value, owner, "
                "       created_at, updated_at, "
                "       date(created_at) AS cd, date(updated_at) AS ud "
                "FROM leads "
                "WHERE date(created_at)=? OR date(updated_at)=?",
                (report_date, report_date),
            ).fetchall():
                owner = (row["owner"] or "").strip().lower()
                if not owner:
                    continue
                action = "added" if row["cd"] == report_date else "updated"
                ts = row["created_at"] if action == "added" else row["updated_at"]
                try:
                    val = float(row["est_value"] or 0)
                except (TypeError, ValueError):
                    val = 0
                val_str = f" · ${val:,.0f}" if val else ""
                _bump(owner, None, "Leads", {
                    "action": action,
                    "title": f"{row['practice_name'] or '—'} ({row['status'] or 'New'}){val_str}",
                    "ts": ts or "",
                })
        except Exception:
            pass

        # ── Finalize: convert defaultdicts to dicts and sort ──
        ordered = []
        for key in sorted(users.keys()):
            u = users[key]
            # Drop completely empty rows (no activity AND no presence AND no
            # billed work in the week-to-date window).
            total_actions = sum(u["totals"].values())
            billed_wtd_count = u["billed"]["wtd"]["count"]
            if (total_actions == 0 and u["active_hours"] == 0
                    and u["actions"] == 0 and billed_wtd_count == 0):
                continue
            for _scope in ("today", "wtd"):
                u["billed"][_scope]["amount"] = round(u["billed"][_scope]["amount"], 2)
            u["clients"] = {
                cname: {
                    "client_id":       cb.get("client_id", 0),
                    "enabled_modules": cb.get("enabled_modules", list(DEFAULT_ENABLED_MODULES)),
                    "totals":          dict(cb["totals"]),
                    "items":           cb["items"],
                }
                for cname, cb in sorted(u["clients"].items())
            }
            u["total_actions"] = total_actions
            ordered.append(u)

        # ── Team-wide rollup ──
        team_totals = _new_tab_bucket()
        team_billed = {
            "today": {"count": 0, "amount": 0.0},
            "wtd":   {"count": 0, "amount": 0.0},
        }
        for u in ordered:
            for k, v in u["totals"].items():
                team_totals[k] = team_totals.get(k, 0) + v
            for _scope in ("today", "wtd"):
                team_billed[_scope]["count"] += u["billed"][_scope]["count"]
                team_billed[_scope]["amount"] += u["billed"][_scope]["amount"]
        for _scope in ("today", "wtd"):
            team_billed[_scope]["amount"] = round(team_billed[_scope]["amount"], 2)

        # New rows added across the org today (handy headline numbers).
        def _scalar(sql, params=()):
            try:
                return cur.execute(sql, params).fetchone()[0] or 0
            except Exception:
                return 0

        headlines = {
            "claims_new":       _scalar("SELECT COUNT(*) FROM claims_master WHERE date(created_at)=?", (report_date,)),
            "claims_touched":   _scalar("SELECT COUNT(*) FROM claims_master WHERE date(updated_at)=? AND date(created_at)<>?", (report_date, report_date)),
            "payments_posted":  _scalar("SELECT COUNT(*) FROM payments WHERE date(created_at)=?", (report_date,)),
            "payments_amount":  _scalar("SELECT ROUND(COALESCE(SUM(PaymentAmount),0),2) FROM payments WHERE date(created_at)=?", (report_date,)),
            "cred_new":         _scalar("SELECT COUNT(*) FROM credentialing WHERE date(created_at)=?", (report_date,)),
            "enroll_new":       _scalar("SELECT COUNT(*) FROM enrollment   WHERE date(created_at)=?", (report_date,)),
            "edi_new":          _scalar("SELECT COUNT(*) FROM edi_setup    WHERE date(created_at)=?", (report_date,)),
            "production_rows":  _scalar("SELECT COUNT(*) FROM team_production WHERE date(created_at)=? OR work_date=?", (report_date, report_date)),
            "production_hours": _scalar("SELECT ROUND(COALESCE(SUM(time_spent),0),2) FROM team_production WHERE date(created_at)=? OR work_date=?", (report_date, report_date)),
            "leads_new":        _scalar("SELECT COUNT(*) FROM leads WHERE date(created_at)=?", (report_date,)),
            "leads_touched":    _scalar("SELECT COUNT(*) FROM leads WHERE date(updated_at)=? AND date(created_at)<>?", (report_date, report_date)),
            "notes_new":        _scalar("SELECT COUNT(*) FROM notes_log    WHERE date(created_at)=?", (report_date,)),
            "files_uploaded":   _scalar("SELECT COUNT(*) FROM client_files WHERE date(created_at)=?", (report_date,)),
            "chat_messages":    _scalar("SELECT COUNT(*) FROM chat_messages WHERE date(created_at)=?", (report_date,)),
            "audit_events":     _scalar("SELECT COUNT(*) FROM audit_log    WHERE date(created_at)=?", (report_date,)),
            "active_users":     len(ordered),
            "billed_today_amount": team_billed["today"]["amount"],
            "billed_today_count":  team_billed["today"]["count"],
            "billed_wtd_amount":   team_billed["wtd"]["amount"],
            "billed_wtd_count":    team_billed["wtd"]["count"],
        }

        return {
            "report_date": report_date,
            "week_start": week_start,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "tab_keys": list(TAB_KEYS),
            "users": ordered,
            "team_totals": team_totals,
            "team_billed": team_billed,
            "headlines": headlines,
            "client_count": len(client_lookup),
        }
    finally:
        conn.close()


def get_client_daily_report(client_id: int, report_date: str = None) -> dict:
    """Build a per-CLIENT production-focused daily report.

    Aggregates only the work touching a single client_id, organised by
    production area (Claims movements / Credentialing / Enrollment / EDI /
    Production hours / Notes / Documents) so the practice owner sees what
    MedPharma did for them today. Each itemised row carries a timestamp.

    Returns:
        {
            "client_id":  int,
            "company":    str,
            "contact_name": str,
            "email":      str,
            "report_date": "YYYY-MM-DD",
            "generated_at": iso,
            "enabled_modules": [str, ...],
            "headlines": {
                "claims_new":      int,
                "claims_touched":  int,
                "claims_paid":     int,
                "claims_denied":   int,
                "cred_new":        int,
                "enroll_new":      int,
                "edi_new":         int,
                "production_hours": float,
                "notes_new":       int,
                "files_uploaded":  int,
                "operators":       int,
            },
            "sections": {
                "claims":        [ {ts, ClaimKey, ClaimStatus, Owner, action}, ... ],
                "credentialing": [ ... ],
                "enrollment":    [ ... ],
                "edi":           [ ... ],
                "production":    [ ... ],
                "notes":         [ ... ],
                "documents":     [ ... ],
            },
            "operators": [ {username, contact_name, hours, actions}, ... ],
        }
    """
    import json as _json
    from collections import defaultdict
    if not report_date:
        report_date = business_today_iso()

    conn = get_db()
    try:
        cur = conn.cursor()

        # ── Client info ──
        cur.execute(
            "SELECT id, COALESCE(NULLIF(company,''), username) AS company, "
            "       contact_name, email, enabled_modules, "
            "       daily_report_optin, report_recipients "
            "FROM clients WHERE id=?",
            (int(client_id),),
        )
        crow = cur.fetchone()
        if not crow:
            return {"ok": False, "error": "client not found", "client_id": client_id}

        enabled = DEFAULT_ENABLED_MODULES[:]
        raw_mods = (crow["enabled_modules"] or "").strip()
        if raw_mods:
            try:
                parsed = _json.loads(raw_mods)
                if isinstance(parsed, list) and parsed:
                    enabled = [str(m).lower() for m in parsed]
            except Exception:
                pass

        extra_recipients = []
        raw_recip = (crow["report_recipients"] or "").strip()
        if raw_recip:
            try:
                parsed = _json.loads(raw_recip)
                if isinstance(parsed, list):
                    extra_recipients = [str(x).strip() for x in parsed if str(x).strip()]
            except Exception:
                pass

        cid = int(crow["id"])

        # ── Pull each tab's rows scoped to THIS client + date ──
        sections: dict[str, list[dict]] = {
            "claims":        [],
            "credentialing": [],
            "enrollment":    [],
            "edi":           [],
            "production":    [],
            "notes":         [],
            "documents":     [],
        }

        # Claims (always include — claims is the core of every RCM client)
        for row in cur.execute(
            "SELECT ClaimKey, ClaimStatus, Owner, created_at, updated_at, "
            "       date(created_at) AS cd, date(updated_at) AS ud "
            "FROM claims_master "
            "WHERE client_id=? AND (date(created_at)=? OR date(updated_at)=?)",
            (cid, report_date, report_date),
        ).fetchall():
            action = "created" if row["cd"] == report_date else "updated"
            ts = row["created_at"] if action == "created" else row["updated_at"]
            sections["claims"].append({
                "ts": ts or "",
                "ClaimKey": row["ClaimKey"] or "",
                "ClaimStatus": row["ClaimStatus"] or "",
                "Owner": row["Owner"] or "",
                "action": action,
            })

        # Credentialing / Enrollment / EDI (module-gated)
        for table, key, status_col, mod in (
            ("credentialing", "credentialing", "Status",    "credentialing"),
            ("enrollment",    "enrollment",   "Status",    "enrollment"),
            ("edi_setup",     "edi",          "EDIStatus", "edi"),
        ):
            if mod not in enabled:
                continue
            for row in cur.execute(
                f"SELECT ProviderName, Payor, {status_col} AS Status, Owner, "
                f"       created_at, updated_at, "
                f"       date(created_at) AS cd, date(updated_at) AS ud "
                f"FROM {table} "
                f"WHERE client_id=? AND (date(created_at)=? OR date(updated_at)=?)",
                (cid, report_date, report_date),
            ).fetchall():
                action = "created" if row["cd"] == report_date else "updated"
                ts = row["created_at"] if action == "created" else row["updated_at"]
                sections[key].append({
                    "ts": ts or "",
                    "ProviderName": row["ProviderName"] or "",
                    "Payor": row["Payor"] or "",
                    "Status": row["Status"] or "",
                    "Owner": row["Owner"] or "",
                    "action": action,
                })

        # Production hours (module-gated)
        if "production" in enabled:
            try:
                for row in cur.execute(
                    "SELECT username, category, task_description, quantity, "
                    "       time_spent, created_at FROM team_production "
                    "WHERE client_id=? AND (date(created_at)=? OR work_date=?)",
                    (cid, report_date, report_date),
                ).fetchall():
                    sections["production"].append({
                        "ts": row["created_at"] or "",
                        "Owner": row["username"] or "",
                        "Category": row["category"] or "",
                        "Task": (row["task_description"] or "")[:200],
                        "Qty": row["quantity"] or 0,
                        "Hours": row["time_spent"] or 0,
                    })
            except Exception:
                pass

        # Notes
        try:
            for row in cur.execute(
                "SELECT Author, ClaimKey, Module, Note, created_at "
                "FROM notes_log WHERE client_id=? AND date(created_at)=?",
                (cid, report_date),
            ).fetchall():
                sections["notes"].append({
                    "ts": row["created_at"] or "",
                    "Author": row["Author"] or "",
                    "Subject": f"{row['Module'] or 'Claim'} {row['ClaimKey'] or ''}".strip(),
                    "Note": (row["Note"] or "")[:300],
                })
        except Exception:
            pass

        # Documents (module-gated)
        if "documents" in enabled:
            try:
                for row in cur.execute(
                    "SELECT original_name, uploaded_by, category, created_at "
                    "FROM client_files WHERE client_id=? AND date(created_at)=?",
                    (cid, report_date),
                ).fetchall():
                    sections["documents"].append({
                        "ts": row["created_at"] or "",
                        "Filename": row["original_name"] or "",
                        "UploadedBy": row["uploaded_by"] or "",
                        "Category": row["category"] or "General",
                    })
            except Exception:
                pass

        # ── Operator roll-up: which MedPharma users worked this client today ──
        op_lookup: dict[str, dict] = defaultdict(lambda: {"actions": 0, "hours": 0.0})
        for row in sections["claims"]:
            if row["Owner"]:
                op_lookup[row["Owner"].lower()]["actions"] += 1
        for k in ("credentialing", "enrollment", "edi"):
            for row in sections[k]:
                if row["Owner"]:
                    op_lookup[row["Owner"].lower()]["actions"] += 1
        for row in sections["production"]:
            if row["Owner"]:
                key = row["Owner"].lower()
                op_lookup[key]["actions"] += 1
                try:
                    op_lookup[key]["hours"] += float(row["Hours"] or 0)
                except Exception:
                    pass
        for row in sections["notes"]:
            if row["Author"]:
                op_lookup[row["Author"].lower()]["actions"] += 1
        for row in sections["documents"]:
            if row["UploadedBy"]:
                op_lookup[row["UploadedBy"].lower()]["actions"] += 1

        # Resolve operator contact names
        operators = []
        if op_lookup:
            usernames = list(op_lookup.keys())
            placeholders = ",".join("?" * len(usernames))
            name_map = {}
            try:
                for row in cur.execute(
                    f"SELECT lower(username) AS u, "
                    f"  COALESCE(NULLIF(contact_name,''), username) AS name, role "
                    f"FROM clients WHERE lower(username) IN ({placeholders})",
                    usernames,
                ).fetchall():
                    name_map[row["u"]] = {"name": row["name"], "role": row["role"] or ""}
            except Exception:
                pass
            for uname, agg in sorted(op_lookup.items()):
                meta = name_map.get(uname, {})
                operators.append({
                    "username": uname,
                    "contact_name": meta.get("name") or uname.title(),
                    "role": meta.get("role", ""),
                    "actions": agg["actions"],
                    "hours": round(agg["hours"], 2),
                })

        # ── Headlines ──
        def _count(items, predicate=lambda r: True):
            return sum(1 for r in items if predicate(r))

        paid_set = {"Paid", "Posted", "Closed"}
        denied_set = {"Denied", "Rejected", "Appeals"}

        headlines = {
            "claims_new":       _count(sections["claims"], lambda r: r["action"] == "created"),
            "claims_touched":   _count(sections["claims"], lambda r: r["action"] == "updated"),
            "claims_paid":      _count(sections["claims"], lambda r: r["ClaimStatus"] in paid_set),
            "claims_denied":    _count(sections["claims"], lambda r: r["ClaimStatus"] in denied_set),
            "cred_new":         _count(sections["credentialing"], lambda r: r["action"] == "created"),
            "enroll_new":       _count(sections["enrollment"],    lambda r: r["action"] == "created"),
            "edi_new":          _count(sections["edi"],           lambda r: r["action"] == "created"),
            "production_hours": round(sum(float(r["Hours"] or 0) for r in sections["production"]), 2),
            "notes_new":        len(sections["notes"]),
            "files_uploaded":   len(sections["documents"]),
            "operators":        len(operators),
        }

        return {
            "ok": True,
            "client_id":     cid,
            "company":       crow["company"] or "",
            "contact_name":  crow["contact_name"] or "",
            "email":         crow["email"] or "",
            "report_date":   report_date,
            "generated_at":  datetime.now().isoformat(timespec="seconds"),
            "enabled_modules": enabled,
            "report_recipients": extra_recipients,
            "headlines":     headlines,
            "sections":      sections,
            "operators":     operators,
            "daily_report_optin": int(crow["daily_report_optin"] if crow["daily_report_optin"] is not None else 1),
        }
    finally:
        conn.close()


def list_clients_optin_for_daily_report() -> list[dict]:
    """Return every client row that has opted in to receive a daily
    production report and has a deliverable email on file.

    Used by the scheduler to fan out reports each evening without the
    admin having to push a button per client.
    """
    import json as _json
    conn = get_db()
    out: list[dict] = []
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, COALESCE(NULLIF(company,''), username) AS company, "
            "       contact_name, email, daily_report_optin, report_recipients, "
            "       enabled_modules "
            "FROM clients "
            "WHERE COALESCE(role,'client')='client' "
            "  AND COALESCE(is_active,1)=1 "
            "  AND COALESCE(daily_report_optin,1)=1 "
            "  AND TRIM(COALESCE(email,'')) <> ''"
        )
        for row in cur.fetchall():
            recipients = []
            raw = (row["report_recipients"] or "").strip()
            if raw:
                try:
                    parsed = _json.loads(raw)
                    if isinstance(parsed, list):
                        recipients = [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    pass
            out.append({
                "client_id":    int(row["id"]),
                "company":      row["company"] or "",
                "contact_name": row["contact_name"] or "",
                "email":        row["email"] or "",
                "extra_recipients": recipients,
            })
    finally:
        conn.close()
    return out


# ─── SLA Auto-Flagging ───────────────────────────────────────────────────

SLA_THRESHOLDS = {
    "Intake": 3, "Verification": 5, "Coding": 5,
    "Billed/Submitted": 14, "A/R Follow-Up": 14,
    "Appeals": 30, "Denied": 7, "Rejected": 3,
}


def auto_flag_sla(client_id: int = None):
    """Auto-flag claims that have exceeded SLA thresholds. Returns count of newly flagged."""
    conn = get_db()
    try:
        cur = conn.cursor()
        flagged = 0
        for status, days in SLA_THRESHOLDS.items():
            cond = "WHERE ClaimStatus=? AND SLABreached=0"
            p = [status]
            if client_id:
                cond += " AND client_id=?"
                p.append(client_id)
            cur.execute(f"""UPDATE claims_master SET SLABreached=1
                            {cond}
                            AND CAST(julianday('now') - julianday(
                                COALESCE(NULLIF(StatusStartDate,''), NULLIF(LastTouchedDate,''), updated_at)
                            ) AS INTEGER) > ?""",
                        p + [days])
            flagged += cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return flagged


# ─── Alerts / Notifications ──────────────────────────────────────────────

def get_alerts(client_id: int = None):
    """Generate real-time alerts for the client."""
    conn = get_db()
    try:
        cur = conn.cursor()
        alerts = []
        cond = "WHERE client_id=?" if client_id else ""
        p = [client_id] if client_id else []

        # 1. SLA Breaches
        sla_count = cur.execute(
            f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} SLABreached=1 AND ClaimStatus NOT IN ('Paid','Closed')", p
        ).fetchone()[0]
        if sla_count:
            alerts.append({"type": "danger", "icon": "🚨", "title": f"{sla_count} SLA Breach(es)",
                            "detail": "Claims exceeded time thresholds — review immediately"})

        # 2. Credentialing expirations (next 90 days)
        for window, level in [(30, "danger"), (60, "warning"), (90, "info")]:
            exp = cur.execute(
                f"""SELECT COUNT(*) FROM credentialing {cond}
                    {'AND' if cond else 'WHERE'} ExpirationDate != ''
                    AND ExpirationDate <= date('now', '+{window} days')
                    AND ExpirationDate >= date('now')
                    AND Status NOT IN ('Expired','Denied')""", p
            ).fetchone()[0]
            if exp:
                alerts.append({"type": level, "icon": "📋" if window > 30 else "⚠️",
                               "title": f"{exp} Credentialing(s) expiring within {window} days",
                               "detail": f"Review and initiate revalidation"})
                break  # Show most urgent only

        # 3. Overdue follow-ups (credentialing & enrollment)
        for tbl, label in [("credentialing", "Credentialing"), ("enrollment", "Enrollment")]:
            overdue = cur.execute(
                f"""SELECT COUNT(*) FROM {tbl} {cond}
                    {'AND' if cond else 'WHERE'} FollowUpDate != '' AND FollowUpDate < date('now')
                    AND Status NOT IN ('Approved','Active','Completed','Denied','Expired','Terminated')""", p
            ).fetchone()[0]
            if overdue:
                alerts.append({"type": "warning", "icon": "📅", "title": f"{overdue} Overdue {label} Follow-ups",
                               "detail": "Past follow-up dates need attention"})

        # 4. High denial rate warning
        total_sub = cur.execute(
            f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} BillDate != ''", p
        ).fetchone()[0]
        total_denied = cur.execute(
            f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} DeniedDate != ''", p
        ).fetchone()[0]
        if total_sub > 10:
            rate = round(total_denied / total_sub * 100, 1)
            if rate > 15:
                alerts.append({"type": "danger", "icon": "❌", "title": f"High Denial Rate: {rate}%",
                               "detail": "Denial rate exceeds 15% — review denial patterns"})
            elif rate > 10:
                alerts.append({"type": "warning", "icon": "⚠️", "title": f"Elevated Denial Rate: {rate}%",
                               "detail": "Denial rate above 10% — monitor closely"})

        # 5. Unpaid claims > 90 days
        old_ar = cur.execute(
            f"""SELECT COUNT(*), COALESCE(SUM(BalanceRemaining),0) FROM claims_master
                {cond} {'AND' if cond else 'WHERE'} ClaimStatus NOT IN ('Paid','Closed')
                AND BalanceRemaining > 0
                AND CAST(julianday('now') - julianday(COALESCE(NULLIF(BillDate,''), DOS, updated_at)) AS INTEGER) > 90""", p
        ).fetchone()
        if old_ar[0] > 0:
            alerts.append({"type": "danger", "icon": "💰", "title": f"{old_ar[0]} Claims in 90+ Day AR (${old_ar[1]:,.0f})",
                            "detail": "These claims need escalated collection efforts"})
    finally:
        conn.close()
    return alerts


# ─── Global Search ────────────────────────────────────────────────────────

def global_search(query: str, client_id: int = None, limit: int = 30):
    """Search across claims, providers, credentialing, enrollment, EDI."""
    if not query or not query.strip():
        return []
    conn = get_db()
    try:
        cur = conn.cursor()
        results = []
        q = f"%{query}%"
        cond = " AND client_id=?" if client_id is not None else ""
        p_base = [client_id] if client_id is not None else []

        # Claims
        cur.execute(f"""SELECT id, 'claim' as type, ClaimKey as title,
                        PatientName || ' — ' || Payor || ' — $' || ChargeAmount as subtitle,
                        ClaimStatus as status
                        FROM claims_master WHERE (
                            ClaimKey LIKE ? OR PatientName LIKE ? OR Payor LIKE ? OR
                            ProviderName LIKE ? OR PatientID LIKE ? OR DenialReason LIKE ?
                        ) {cond} ORDER BY updated_at DESC LIMIT ?""",
                    [q]*6 + p_base + [limit])
        results += [dict(r) for r in cur.fetchall()]

        # Providers
        cur.execute(f"""SELECT id, 'provider' as type, ProviderName as title,
                        NPI || ' — ' || Specialty as subtitle, Status as status
                        FROM providers WHERE (
                            ProviderName LIKE ? OR NPI LIKE ? OR Specialty LIKE ? OR Email LIKE ?
                        ) {cond} ORDER BY ProviderName LIMIT ?""",
                    [q]*4 + p_base + [limit])
        results += [dict(r) for r in cur.fetchall()]

        # Credentialing
        cur.execute(f"""SELECT id, 'credentialing' as type,
                        ProviderName || ' → ' || Payor as title,
                        CredType || ' — ' || Owner as subtitle, Status as status
                        FROM credentialing WHERE (
                            ProviderName LIKE ? OR Payor LIKE ? OR Owner LIKE ?
                        ) {cond} ORDER BY updated_at DESC LIMIT ?""",
                    [q]*3 + p_base + [limit])
        results += [dict(r) for r in cur.fetchall()]

        # Enrollment
        cur.execute(f"""SELECT id, 'enrollment' as type,
                        ProviderName || ' → ' || Payor as title,
                        EnrollType || ' — ' || Owner as subtitle, Status as status
                        FROM enrollment WHERE (
                            ProviderName LIKE ? OR Payor LIKE ? OR Owner LIKE ?
                        ) {cond} ORDER BY updated_at DESC LIMIT ?""",
                    [q]*3 + p_base + [limit])
        results += [dict(r) for r in cur.fetchall()]

        # EDI
        cur.execute(f"""SELECT id, 'edi' as type,
                        ProviderName || ' → ' || Payor as title,
                        'EDI: ' || EDIStatus || ' | ERA: ' || ERAStatus as subtitle,
                        EDIStatus as status
                        FROM edi_setup WHERE (
                            ProviderName LIKE ? OR Payor LIKE ? OR PayerID LIKE ?
                        ) {cond} ORDER BY updated_at DESC LIMIT ?""",
                    [q]*3 + p_base + [limit])
        results += [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return results[:limit]


# ─── Bulk Claim Updates ──────────────────────────────────────────────────

def bulk_update_claims(claim_ids: list, data: dict, client_id: int = None):
    """Update multiple claims at once. Returns count of updated rows."""
    if not claim_ids or not data:
        return 0
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["ClaimStatus", "Owner", "NextAction", "NextActionDueDate", "SLABreached",
                   "StatusStartDate", "LastTouchedDate"]
        parts, params = [], []
        for f in allowed:
            if f in data and data[f] is not None:
                parts.append(f"{f}=?")
                params.append(data[f])
        if not parts:
            return 0
        # Always update LastTouchedDate
        if "LastTouchedDate" not in data:
            parts.append("LastTouchedDate=?")
            params.append(datetime.now().isoformat()[:10])
        # Stamp BillDate the day claims are first marked Billed/Submitted (only if
        # empty) so recent-billing activity is captured for the report.
        if data.get("ClaimStatus") == "Billed/Submitted" and "BillDate" not in data:
            parts.append("BillDate=CASE WHEN COALESCE(BillDate,'')='' THEN ? ELSE BillDate END")
            params.append(datetime.now().isoformat()[:10])
        placeholders = ",".join("?" for _ in claim_ids)
        sql = f"UPDATE claims_master SET {', '.join(parts)} WHERE id IN ({placeholders})"
        if client_id is not None:
            sql += " AND client_id=?"
            params += list(claim_ids) + [client_id]
        else:
            params += list(claim_ids)
        cur.execute(sql, params)
        updated = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return updated


# ─── Export Data ──────────────────────────────────────────────────────────

def export_claims(client_id: int = None, sub_profile: str = None):
    """Return all claims as list of dicts for CSV/Excel export."""
    conn = get_db()
    try:
        cond, p = "", []
        if client_id is not None:
            cond = "WHERE client_id=?"
            p.append(client_id)
        if sub_profile:
            cond += (" AND " if cond else "WHERE ") + "sub_profile=?"
            p.append(sub_profile)
        raw_rows = conn.execute(
            f"SELECT * FROM claims_master {cond} ORDER BY updated_at DESC", p).fetchall()
        rows = [dict(r) for r in raw_rows]
    finally:
        conn.close()
    return rows


def export_table(table: str, client_id: int = None):
    """Generic export for credentialing, enrollment, edi_setup, providers."""
    allowed_tables = {"credentialing", "enrollment", "edi_setup", "providers"}
    if table not in allowed_tables:
        return []
    conn = get_db()
    try:
        if client_id is not None:
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM {table} WHERE client_id=? ORDER BY updated_at DESC", (client_id,)).fetchall()]
        else:
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM {table} ORDER BY updated_at DESC").fetchall()]
    finally:
        conn.close()
    return rows


# ─── Async Jobs ───────────────────────────────────────────────────────────

def _job_row_to_dict(row) -> dict:
    if not row:
        return {}
    item = dict(row)
    try:
        item["payload"] = json.loads(item.get("payload_json") or "{}")
    except Exception:
        item["payload"] = {}
    try:
        item["result"] = json.loads(item.get("result_json") or "{}")
    except Exception:
        item["result"] = {}
    item.pop("payload_json", None)
    item.pop("result_json", None)
    return item


def _ensure_jobs_tables(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id            TEXT PRIMARY KEY,
            account_id    INTEGER,
            job_type      TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'queued',
            progress      INTEGER DEFAULT 0,
            eta_seconds   INTEGER,
            latest_error  TEXT DEFAULT '',
            payload_json  TEXT DEFAULT '{}',
            result_json   TEXT DEFAULT '{}',
            created_by    TEXT DEFAULT '',
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            started_at    TEXT,
            finished_at   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_status      ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_account     ON jobs(account_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_created_at  ON jobs(created_at);

        CREATE TABLE IF NOT EXISTS job_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id      TEXT NOT NULL,
            stage       TEXT DEFAULT '',
            level       TEXT DEFAULT 'info',
            message     TEXT DEFAULT '',
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, created_at);
        """
    )
    conn.commit()


def create_job(account_id: int = None, job_type: str = "", created_by: str = "", payload: dict = None) -> dict:
    if not job_type:
        raise ValueError("job_type is required")
    payload = payload or {}
    job_id = secrets.token_hex(16)
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        conn.execute(
            """
            INSERT INTO jobs (id, account_id, job_type, status, progress, payload_json, created_by)
            VALUES (?, ?, ?, 'queued', 0, ?, ?)
            """,
            (job_id, account_id, job_type, json.dumps(payload), created_by),
        )
        conn.commit()
    finally:
        conn.close()
    return get_job(job_id)


def append_job_event(job_id: str, stage: str, message: str, level: str = "info"):
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        conn.execute(
            "INSERT INTO job_events (job_id, stage, level, message) VALUES (?, ?, ?, ?)",
            (job_id, stage or "", level or "info", (message or "")[:500]),
        )
        conn.commit()
    finally:
        conn.close()


def set_job_running(job_id: str, progress: int = 0, eta_seconds: int = None):
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        conn.execute(
            """
            UPDATE jobs
            SET status='running', progress=?, eta_seconds=?, started_at=COALESCE(started_at, CURRENT_TIMESTAMP), latest_error=''
            WHERE id=?
            """,
            (max(0, min(100, int(progress))), eta_seconds, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_job_progress(job_id: str, progress: int, eta_seconds: int = None):
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        conn.execute(
            "UPDATE jobs SET progress=?, eta_seconds=? WHERE id=?",
            (max(0, min(100, int(progress))), eta_seconds, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def complete_job(job_id: str, result: dict = None):
    result = result or {}
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        conn.execute(
            """
            UPDATE jobs
            SET status='done', progress=100, eta_seconds=NULL, latest_error='',
                result_json=?, finished_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (json.dumps(result), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def fail_job(job_id: str, error: str):
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        conn.execute(
            """
            UPDATE jobs
            SET status='error', latest_error=?, eta_seconds=NULL, finished_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            ((error or "Job failed")[:500], job_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_job_events(job_id: str, limit: int = 200) -> list:
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        rows = conn.execute(
            "SELECT * FROM job_events WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, max(1, min(int(limit), 1000))),
        ).fetchall()
    finally:
        conn.close()
    events = [dict(r) for r in rows]
    events.reverse()
    return events


def get_job(job_id: str, include_events: bool = False) -> dict | None:
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    item = _job_row_to_dict(row)
    if include_events:
        item["events"] = get_job_events(job_id)
    return item


def list_jobs(account_id: int = None, status: str = "", job_type: str = "", limit: int = 50) -> list:
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        cond = []
        params = []
        if account_id is not None:
            cond.append("account_id=?")
            params.append(account_id)
        if status:
            cond.append("status=?")
            params.append(status)
        if job_type:
            cond.append("job_type=?")
            params.append(job_type)
        where = f"WHERE {' AND '.join(cond)}" if cond else ""
        params.append(max(1, min(int(limit), 200)))
        rows = conn.execute(
            f"SELECT * FROM jobs {where} ORDER BY created_at DESC LIMIT ?",
            params,
        ).fetchall()
    finally:
        conn.close()
    return [_job_row_to_dict(r) for r in rows]


def reset_job_for_retry(job_id: str) -> dict | None:
    conn = get_db()
    try:
        _ensure_jobs_tables(conn)
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return None
        if row["status"] != "error":
            return _job_row_to_dict(row)
        conn.execute(
            """
            UPDATE jobs
            SET status='queued', progress=0, eta_seconds=NULL, latest_error='',
                started_at=NULL, finished_at=NULL
            WHERE id=?
            """,
            (job_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return get_job(job_id)


# ─── Team Production ──────────────────────────────────────────────────────

def list_production_logs(client_id: int = None, start_date: str = None, end_date: str = None, username: str = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        conditions, p = [], []
        if client_id:
            conditions.append("client_id=?")
            p.append(client_id)
        if start_date:
            conditions.append("work_date>=?")
            p.append(start_date)
        if end_date:
            conditions.append("work_date<=?")
            p.append(end_date)
        if username:
            conditions.append("username=?")
            p.append(username)
        cond = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        cur.execute(f"SELECT * FROM team_production {cond} ORDER BY work_date DESC, created_at DESC", p)
        rows = [dict(r) for r in cur.fetchall()]
        # Hide retired system/department logins (e.g. 'rcm') from the log view.
        rows = [r for r in rows
                if str(r.get("username") or "").strip().lower() not in _HIDDEN_ROSTER_USERS]
    finally:
        conn.close()
    return rows


def add_production_log(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO team_production (client_id, work_date, username, category, task_description, quantity, time_spent, notes, attachment_file_id, attachment_name)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            data["client_id"], data["work_date"], data["username"],
            data.get("category", ""), data.get("task_description", ""),
            data.get("quantity", 0), data.get("time_spent", 0),
            data.get("notes", ""),
            data.get("attachment_file_id"), data.get("attachment_name", "")
        ))
        conn.commit()
        new_id = cur.lastrowid
    finally:
        conn.close()
    return new_id


def delete_production_log(log_id: int, client_id: int = None, username: str = None) -> bool:
    conn = get_db()
    try:
        conditions = ["id=?"]
        params: list[object] = [log_id]
        if client_id:
            conditions.append("client_id=?")
            params.append(client_id)
        if username:
            conditions.append("LOWER(username)=LOWER(?)")
            params.append(username)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM team_production WHERE {' AND '.join(conditions)}", params)
        conn.commit()
        deleted = cur.rowcount > 0
    finally:
        conn.close()
    return deleted


def get_user_production_snapshot(work_date: str = None):
    """Return per-user production activity for a given date (defaults to today)."""
    target_date = (work_date or business_today_iso()).strip()
    conn = get_db()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, client_id, work_date, username, category, task_description, quantity, time_spent, notes, created_at
            FROM team_production
            WHERE work_date=?
            ORDER BY created_at DESC, id DESC
            """,
            (target_date,),
        )
        entries = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT username,
                   COUNT(*) AS entry_count,
                   ROUND(COALESCE(SUM(time_spent),0),1) AS total_hours,
                   COALESCE(SUM(quantity),0) AS total_qty,
                   COALESCE(GROUP_CONCAT(DISTINCT category), '') AS categories
            FROM team_production
            WHERE work_date=?
            GROUP BY username
            ORDER BY total_hours DESC, entry_count DESC, username ASC
            """,
            (target_date,),
        )
        user_stats = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT uploaded_by AS username, COUNT(*) AS file_count
            FROM client_files
            WHERE DATE(created_at)=? AND TRIM(COALESCE(uploaded_by, '')) != ''
            GROUP BY uploaded_by
            """,
            (target_date,),
        )
        file_uploads = {str(r["username"]): int(r["file_count"] or 0) for r in cur.fetchall()}

        # Payments posted on this date, attributed to the poster (PostedBy).
        cur.execute(
            """
            SELECT TRIM(PostedBy) AS username,
                   COUNT(*) AS payments_posted,
                   ROUND(COALESCE(SUM(PaymentAmount),0),2) AS payments_amount
            FROM payments
            WHERE COALESCE(NULLIF(PostDate,''), date(created_at))=?
              AND TRIM(COALESCE(PostedBy,'')) != ''
            GROUP BY TRIM(PostedBy)
            """,
            (target_date,),
        )
        payments_by_user = {}
        payments_total_count = 0
        payments_total_amount = 0.0
        for r in cur.fetchall():
            uname = str(r["username"] or "").strip()
            if not uname or uname.lower() in _HIDDEN_ROSTER_USERS:
                continue
            cnt = int(r["payments_posted"] or 0)
            amt = float(r["payments_amount"] or 0)
            payments_by_user[uname] = {"payments_posted": cnt, "payments_amount": amt}
            payments_total_count += cnt
            payments_total_amount += amt

        total_entries = len(entries)
        total_hours = round(sum(float(e.get("time_spent") or 0) for e in entries), 1)
        total_quantity = int(sum(int(e.get("quantity") or 0) for e in entries))
        return {
            "work_date": target_date,
            "total_entries": total_entries,
            "total_hours": total_hours,
            "total_quantity": total_quantity,
            "user_stats": user_stats,
            "file_uploads": file_uploads,
            "payments_by_user": payments_by_user,
            "payments_total_count": payments_total_count,
            "payments_total_amount": round(payments_total_amount, 2),
            "entries": entries,
        }
    finally:
        conn.close()


def get_production_report(client_id: int = None, start_date: str = None, end_date: str = None):
    """Weekly production report — aggregated by user and category."""
    conn = get_db()
    try:
        cur = conn.cursor()
        conditions, p = [], []
        if client_id:
            conditions.append("client_id=?")
            p.append(client_id)
        if start_date:
            conditions.append("work_date>=?")
            p.append(start_date)
        if end_date:
            conditions.append("work_date<=?")
            p.append(end_date)
        cond = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # Summary by user
        cur.execute(f"""
            SELECT username,
                   COUNT(*) as total_entries,
                   SUM(quantity) as total_quantity,
                   ROUND(SUM(time_spent),1) as total_hours,
                   COUNT(DISTINCT work_date) as days_worked
            FROM team_production {cond}
            GROUP BY username ORDER BY username
        """, p)
        by_user = [dict(r) for r in cur.fetchall()]
        # Never surface system/department logins (e.g. retired 'rcm') in the
        # Team Production report, even if stale rows linger in team_production.
        by_user = [u for u in by_user
                   if str(u.get("username") or "").strip().lower() not in _HIDDEN_ROSTER_USERS]

        # Include all active users in scope, even if they have zero rows in this period.
        if client_id:
            cur.execute("SELECT company FROM clients WHERE id=?", (client_id,))
            scope_row = cur.fetchone()
            if scope_row and (scope_row["company"] or "").strip():
                cur.execute(
                    "SELECT username FROM clients WHERE is_active=1 AND company=? ORDER BY username",
                    (scope_row["company"],),
                )
            else:
                cur.execute("SELECT username FROM clients WHERE is_active=1 AND id=?", (client_id,))
        else:
            cur.execute("SELECT username FROM clients WHERE is_active=1 ORDER BY username")
        scoped_usernames = [str(r["username"]).strip() for r in cur.fetchall()
                            if (r["username"] or "").strip()
                            and str(r["username"]).strip().lower() not in _HIDDEN_ROSTER_USERS]

        by_user_map = {str(u.get("username") or "").strip(): u for u in by_user if str(u.get("username") or "").strip()}
        for username in scoped_usernames:
            if username not in by_user_map:
                by_user_map[username] = {
                    "username": username,
                    "total_entries": 0,
                    "total_quantity": 0,
                    "total_hours": 0,
                    "days_worked": 0,
                }

        # ── Payments posted (part of how the team is paid) ────────────────
        # Attribute each payment to the hub user who posted it (PostedBy),
        # filtered to the same date window. Payments carry no work_date, so the
        # effective posting date is PostDate when present, else the row's
        # created_at date.
        pay_conditions, pay_p = [], []
        if client_id:
            pay_conditions.append("client_id=?")
            pay_p.append(client_id)
        if start_date:
            pay_conditions.append("COALESCE(NULLIF(PostDate,''), date(created_at)) >= ?")
            pay_p.append(start_date)
        if end_date:
            pay_conditions.append("COALESCE(NULLIF(PostDate,''), date(created_at)) <= ?")
            pay_p.append(end_date)
        pay_conditions.append("TRIM(COALESCE(PostedBy,'')) != ''")
        pay_cond = "WHERE " + " AND ".join(pay_conditions)
        cur.execute(f"""
            SELECT TRIM(PostedBy) AS username,
                   COUNT(*) AS payments_posted,
                   ROUND(COALESCE(SUM(PaymentAmount),0),2) AS payments_amount
            FROM payments {pay_cond}
            GROUP BY TRIM(PostedBy)
        """, pay_p)
        payments_by_user = {}
        for r in cur.fetchall():
            uname = str(r["username"] or "").strip()
            if not uname or uname.lower() in _HIDDEN_ROSTER_USERS:
                continue
            payments_by_user[uname] = {
                "payments_posted": int(r["payments_posted"] or 0),
                "payments_amount": float(r["payments_amount"] or 0),
            }

        # Detailed payment log for the period (for the printable report).
        cur.execute(f"""
            SELECT COALESCE(NULLIF(PostDate,''), date(created_at)) AS post_date,
                   TRIM(PostedBy) AS username, ClaimKey, PayerType,
                   PaymentAmount, CheckNumber
            FROM payments {pay_cond}
            ORDER BY post_date DESC, username
        """, pay_p)
        payment_details = []
        for r in cur.fetchall():
            uname = str(r["username"] or "").strip()
            if uname.lower() in _HIDDEN_ROSTER_USERS:
                continue
            payment_details.append(dict(r))

        # Merge payment stats onto every user row (zero-fill), and make sure a
        # poster who logged no production work still shows up in the report.
        for uname, stats in payments_by_user.items():
            if uname not in by_user_map:
                by_user_map[uname] = {
                    "username": uname,
                    "total_entries": 0,
                    "total_quantity": 0,
                    "total_hours": 0,
                    "days_worked": 0,
                }
        for uname, urow in by_user_map.items():
            stats = payments_by_user.get(uname, {})
            urow["payments_posted"] = int(stats.get("payments_posted", 0))
            urow["payments_amount"] = float(stats.get("payments_amount", 0))

        # ── Imported data attributed to each uploader ─────────────────────
        # When a user uploads a claims / credentialing / enrollment / EDI
        # spreadsheet, each imported row is stamped with their hub username in
        # the uploaded_by column. Roll those counts up per user so the compiled
        # Team Production report reflects upload-based work (not just manually
        # logged production and posted payments). Rows are scoped to the same
        # date window using the row's created_at (upload) date.
        uploads_by_user = {}
        upload_specs = [
            ("claims_master", "claims_uploaded", "ChargeAmount"),
            ("credentialing", "credentialing_uploaded", None),
            ("enrollment",    "enrollment_uploaded",    None),
            ("edi_setup",     "edi_uploaded",           None),
        ]
        for table, field, amount_col in upload_specs:
            up_conditions = ["TRIM(COALESCE(uploaded_by,'')) != ''"]
            up_p = []
            if client_id:
                up_conditions.append("client_id=?")
                up_p.append(client_id)
            if start_date:
                up_conditions.append("date(created_at) >= ?")
                up_p.append(start_date)
            if end_date:
                up_conditions.append("date(created_at) <= ?")
                up_p.append(end_date)
            up_cond = "WHERE " + " AND ".join(up_conditions)
            amount_select = (f", ROUND(COALESCE(SUM({amount_col}),0),2) AS amount"
                             if amount_col else ", 0 AS amount")
            try:
                cur.execute(f"""
                    SELECT TRIM(uploaded_by) AS username,
                           COUNT(*) AS cnt{amount_select}
                    FROM {table} {up_cond}
                    GROUP BY TRIM(uploaded_by)
                """, up_p)
            except Exception:
                # Older databases that predate the uploaded_by migration.
                continue
            for r in cur.fetchall():
                uname = str(r["username"] or "").strip()
                if not uname or uname.lower() in _HIDDEN_ROSTER_USERS:
                    continue
                bucket = uploads_by_user.setdefault(uname, {})
                bucket[field] = int(r["cnt"] or 0)
                if amount_col:
                    bucket["claims_uploaded_amount"] = float(r["amount"] or 0)

        # Make sure an uploader who logged no production/payment work still
        # appears in the report, then zero-fill upload stats on every row.
        for uname in uploads_by_user:
            if uname not in by_user_map:
                by_user_map[uname] = {
                    "username": uname,
                    "total_entries": 0,
                    "total_quantity": 0,
                    "total_hours": 0,
                    "days_worked": 0,
                    "payments_posted": 0,
                    "payments_amount": 0,
                }
        for uname, urow in by_user_map.items():
            stats = uploads_by_user.get(uname, {})
            urow["claims_uploaded"]         = int(stats.get("claims_uploaded", 0))
            urow["claims_uploaded_amount"]  = float(stats.get("claims_uploaded_amount", 0))
            urow["credentialing_uploaded"]  = int(stats.get("credentialing_uploaded", 0))
            urow["enrollment_uploaded"]     = int(stats.get("enrollment_uploaded", 0))
            urow["edi_uploaded"]            = int(stats.get("edi_uploaded", 0))
            urow["total_uploaded"] = (urow["claims_uploaded"] + urow["credentialing_uploaded"]
                                      + urow["enrollment_uploaded"] + urow["edi_uploaded"])

        by_user = [by_user_map[k] for k in sorted(by_user_map.keys(), key=lambda x: x.lower())]

        # Summary by category
        cur.execute(f"""
            SELECT category,
                   COUNT(*) as total_entries,
                   SUM(quantity) as total_quantity,
                   ROUND(SUM(time_spent),1) as total_hours
            FROM team_production {cond}
            GROUP BY category ORDER BY total_hours DESC
        """, p)
        by_category = [dict(r) for r in cur.fetchall()]

        # Daily breakdown
        cur.execute(f"""
            SELECT work_date, username, category, task_description, quantity, time_spent, notes
            FROM team_production {cond}
            ORDER BY work_date DESC, username, category
        """, p)
        details = [dict(r) for r in cur.fetchall()]
        details = [d for d in details
                   if str(d.get("username") or "").strip().lower() not in _HIDDEN_ROSTER_USERS]

        # Time management flags — users averaging < 6 hrs/day worked
        flags = []
        for u in by_user:
            u["total_entries"] = int(u.get("total_entries") or 0)
            u["total_quantity"] = int(u.get("total_quantity") or 0)
            u["total_hours"] = float(u.get("total_hours") or 0)
            u["days_worked"] = int(u.get("days_worked") or 0)
            u["payments_posted"] = int(u.get("payments_posted") or 0)
            u["payments_amount"] = float(u.get("payments_amount") or 0)
            u["claims_uploaded"] = int(u.get("claims_uploaded") or 0)
            u["claims_uploaded_amount"] = float(u.get("claims_uploaded_amount") or 0)
            u["credentialing_uploaded"] = int(u.get("credentialing_uploaded") or 0)
            u["enrollment_uploaded"] = int(u.get("enrollment_uploaded") or 0)
            u["edi_uploaded"] = int(u.get("edi_uploaded") or 0)
            u["total_uploaded"] = int(u.get("total_uploaded") or 0)
            if u["days_worked"] > 0:
                avg_hrs = round(u["total_hours"] / u["days_worked"], 1)
            else:
                avg_hrs = 0
            u["avg_hours_per_day"] = avg_hrs
            if u["days_worked"] > 0 and avg_hrs < 6:
                flags.append({"username": u["username"], "avg_hours_per_day": avg_hrs,
                              "days_worked": u["days_worked"],
                              "recommendation": "Below 6hr/day average — review time management"})
    finally:
        conn.close()
    return {
        "by_user": by_user,
        "by_category": by_category,
        "details": details,
        "payment_details": payment_details,
        "payments_total_count": sum(int(u.get("payments_posted") or 0) for u in by_user),
        "payments_total_amount": round(sum(float(u.get("payments_amount") or 0) for u in by_user), 2),
        "uploads_total_count": sum(int(u.get("total_uploaded") or 0) for u in by_user),
        "uploads_total_amount": round(sum(float(u.get("claims_uploaded_amount") or 0) for u in by_user), 2),
        "time_management_flags": flags,
    }


# ─── File uploads ──────────────────────────────────────────────────────────

def list_files(client_id=None):
    conn = get_db()
    try:
        cur = conn.cursor()
        if isinstance(client_id, (list, tuple, set)):
            ids = [int(x) for x in client_id if x]
            if not ids:
                return []
            placeholders = ",".join("?" * len(ids))
            cur.execute(
                f"SELECT * FROM client_files WHERE client_id IN ({placeholders}) ORDER BY created_at DESC",
                ids,
            )
        elif client_id:
            cur.execute("SELECT * FROM client_files WHERE client_id=? ORDER BY created_at DESC", (client_id,))
        else:
            cur.execute("SELECT * FROM client_files ORDER BY created_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def add_file(client_id: int, filename: str, original_name: str, file_type: str,
             file_size: int, category: str, description: str, row_count: int, uploaded_by: str):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO client_files
            (client_id,filename,original_name,file_type,file_size,category,description,row_count,uploaded_by,status)
            VALUES (?,?,?,?,?,?,?,?,?,'Uploaded')
        """, (client_id, filename, original_name, file_type, file_size, category, description, row_count, uploaded_by))
        conn.commit()
        new_id = cur.lastrowid
    finally:
        conn.close()
    return new_id


def get_file_record(file_id: int, client_id: int = None):
    """Fetch a single file record by ID."""
    conn = get_db()
    try:
        cur = conn.cursor()
        if client_id:
            cur.execute("SELECT * FROM client_files WHERE id=? AND client_id=?", (file_id, client_id))
        else:
            cur.execute("SELECT * FROM client_files WHERE id=?", (file_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def update_file_record(file_id: int, data: dict, client_id: int = None):
    """Update a file record (filename, original_name, file_size, row_count, description, status, etc.)."""
    conn = get_db()
    try:
        allowed = {"filename", "original_name", "file_type", "file_size", "category",
                   "description", "row_count", "status", "uploaded_by"}
        sets = []
        vals = []
        for k, v in data.items():
            if k in allowed:
                sets.append(f"{k}=?")
                vals.append(v)
        if not sets:
            return
        vals.append(file_id)
        cond = "id=?"
        if client_id:
            cond += " AND client_id=?"
            vals.append(client_id)
        conn.execute(f"UPDATE client_files SET {', '.join(sets)} WHERE {cond}", vals)
        conn.commit()
    finally:
        conn.close()


def delete_file_record(file_id: int, client_id: int = None):
    conn = get_db()
    try:
        if client_id:
            conn.execute("DELETE FROM client_files WHERE id=? AND client_id=?", (file_id, client_id))
        else:
            conn.execute("DELETE FROM client_files WHERE id=?", (file_id,))
        conn.commit()
    finally:
        conn.close()


# ─── Report Notes (custom report tab content) ──────────────────────────────────
def get_report_notes(client_id: int, tab_name: str = None) -> list:
    conn = get_db()
    try:
        cur = conn.cursor()
        if tab_name:
            cur.execute("SELECT * FROM report_notes WHERE client_id=? AND tab_name=?",
                        [client_id, tab_name])
        else:
            cur.execute("SELECT * FROM report_notes WHERE client_id=? ORDER BY tab_name",
                        [client_id])
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def upsert_report_note(client_id: int, tab_name: str, content: str, username: str = ""):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM report_notes WHERE client_id=? AND tab_name=?",
                    [client_id, tab_name])
        row = cur.fetchone()
        now = datetime.now().isoformat()
        if row:
            cur.execute("""UPDATE report_notes SET content=?, updated_by=?, updated_at=?
                           WHERE id=?""", [content, username, now, row[0]])
        else:
            cur.execute("""INSERT INTO report_notes (client_id, tab_name, content, updated_by, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [client_id, tab_name, content, username, now, now])
        conn.commit()
    finally:
        conn.close()


def delete_report_note(client_id: int, tab_name: str):
    conn = get_db()
    try:
        conn.execute("DELETE FROM report_notes WHERE client_id=? AND tab_name=?",
                     [client_id, tab_name])
        conn.commit()
    finally:
        conn.close()


def rename_report_note(client_id: int, old_name: str, new_name: str):
    conn = get_db()
    try:
        conn.execute("UPDATE report_notes SET tab_name=?, updated_at=? WHERE client_id=? AND tab_name=?",
                     [new_name, datetime.now().isoformat(), client_id, old_name])
        conn.commit()
    finally:
        conn.close()

# ── Sharefile links ────────────────────────────────────────────────────────────

def list_sharefile_links(client_id) -> list:
    conn = get_db()
    try:
        if isinstance(client_id, (list, tuple, set)):
            ids = [int(x) for x in client_id if x]
            if not ids:
                return []
            placeholders = ",".join("?" * len(ids))
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM sharefile_links WHERE client_id IN ({placeholders}) ORDER BY created_at DESC",
                ids,
            ).fetchall()]
        else:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM sharefile_links WHERE client_id=? ORDER BY created_at DESC",
                (client_id,)
            ).fetchall()]
    finally:
        conn.close()
    return rows


def add_sharefile_link(client_id: int, label: str, url: str, category: str, added_by: str) -> int:
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO sharefile_links (client_id,label,url,category,added_by) VALUES (?,?,?,?,?)",
            (client_id, label, url, category, added_by)
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def delete_sharefile_link(link_id: int, client_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM sharefile_links WHERE id=? AND client_id=?", (link_id, client_id))
        conn.commit()
    finally:
        conn.close()


# ── Chat rooms (admin-managed) ────────────────────────────────────────────────

def _row_to_room(row) -> dict:
    if not row:
        return {}
    d = dict(row)
    d["archived"] = bool(d.get("archived"))
    d["is_dm"] = bool(d.get("is_dm"))
    # For 1:1 DMs the stored room name is internal; show the other person's name.
    if d.get("is_dm") and d.get("dm_other_name"):
        d["name"] = d["dm_other_name"]
    # Decrypt the last-message preview so the room list shows readable text.
    if "last_body" in d and d["last_body"]:
        try:
            from app.security import decrypt_message
            d["last_body"] = decrypt_message(d["last_body"])
        except Exception:
            pass
    return d


def list_rooms_for_user(user_id: int, is_admin: bool = False,
                        include_archived: bool = False) -> list[dict]:
    """Return rooms visible to this user, with unread count + last message preview.
    Admins see every room; members see only rooms they belong to."""
    conn = get_db()
    try:
        where = []
        params: list = []
        if not is_admin:
            where.append("r.id IN (SELECT room_id FROM chat_room_members WHERE user_id=?)")
            params.append(user_id)
        else:
            # Admins see every group room, but private 1:1 DMs stay private —
            # only the two participants can ever see a DM, never an admin snooping.
            where.append(
                "(COALESCE(r.is_dm,0)=0 OR r.id IN "
                "(SELECT room_id FROM chat_room_members WHERE user_id=?))"
            )
            params.append(user_id)
        if not include_archived:
            where.append("COALESCE(r.archived,0)=0")
        sql = """
            SELECT r.id, r.name, r.description, r.client_id, r.created_by,
                   r.created_at, r.archived,
                   COALESCE(r.is_dm,0) AS is_dm,
                   (SELECT COALESCE(NULLIF(c2.contact_name,''), c2.username)
                      FROM chat_room_members m2 JOIN clients c2 ON c2.id=m2.user_id
                      WHERE m2.room_id=r.id AND m2.user_id<>? LIMIT 1) AS dm_other_name,
                   c.company AS client_company,
                   (SELECT COUNT(*) FROM chat_room_members rm WHERE rm.room_id=r.id) AS member_count,
                   (SELECT body FROM chat_messages m
                      WHERE m.room_id=r.id
                      ORDER BY datetime(m.created_at) DESC, m.id DESC LIMIT 1) AS last_body,
                   (SELECT created_at FROM chat_messages m
                      WHERE m.room_id=r.id
                      ORDER BY datetime(m.created_at) DESC, m.id DESC LIMIT 1) AS last_at,
                   (SELECT sender_name FROM chat_messages m
                      WHERE m.room_id=r.id
                      ORDER BY datetime(m.created_at) DESC, m.id DESC LIMIT 1) AS last_sender,
                   (SELECT COUNT(*) FROM chat_messages m
                      WHERE m.room_id=r.id
                        AND m.id > COALESCE(
                            (SELECT last_read_message_id FROM chat_reads
                              WHERE room_id=r.id AND user_id=?), 0)
                        AND COALESCE(m.sender_id,0) <> ?) AS unread
            FROM chat_rooms r
            LEFT JOIN clients c ON c.id = r.client_id
        """
        params = [user_id, user_id, user_id] + params
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY (last_at IS NULL), datetime(last_at) DESC, r.created_at DESC"
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_room(r) for r in rows]
    finally:
        conn.close()


def get_room(room_id: int) -> dict | None:
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT r.*, c.company AS client_company
               FROM chat_rooms r LEFT JOIN clients c ON c.id=r.client_id
               WHERE r.id=?""",
            (room_id,),
        ).fetchone()
        return _row_to_room(row) if row else None
    finally:
        conn.close()


def create_room(name: str, description: str = "", client_id: int | None = None,
                created_by: str = "", member_user_ids: list[int] | None = None,
                creator_user_id: int | None = None) -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Room name is required")
    conn = get_db()
    try:
        cur = conn.execute(
            """INSERT INTO chat_rooms (name, description, client_id, created_by)
               VALUES (?,?,?,?)""",
            (name, (description or "").strip(), client_id, created_by),
        )
        room_id = cur.lastrowid
        seen: set[int] = set()
        # Creator becomes an admin member (so admins see their own rooms too).
        if creator_user_id:
            conn.execute(
                """INSERT OR IGNORE INTO chat_room_members
                   (room_id, user_id, role, added_by) VALUES (?,?,?,?)""",
                (room_id, int(creator_user_id), "admin", created_by),
            )
            seen.add(int(creator_user_id))
        for uid in (member_user_ids or []):
            try:
                uid_i = int(uid)
            except Exception:
                continue
            if uid_i in seen:
                continue
            conn.execute(
                """INSERT OR IGNORE INTO chat_room_members
                   (room_id, user_id, role, added_by) VALUES (?,?,?,?)""",
                (room_id, uid_i, "member", created_by),
            )
            seen.add(uid_i)
        conn.commit()
        return room_id
    finally:
        conn.close()


def get_or_create_dm_room(user_a_id: int, user_b_id: int,
                          created_by: str = "") -> int:
    """Return the existing 1:1 DM room between two users, creating it if needed.

    A DM room is a chat room flagged ``is_dm=1`` whose only two members are
    ``user_a_id`` and ``user_b_id``. Look-up is order-independent so opening a
    DM from either side resolves to the same room."""
    a, b = int(user_a_id), int(user_b_id)
    if a == b:
        raise ValueError("Cannot start a direct message with yourself")
    conn = get_db()
    try:
        row = conn.execute(
            """SELECT r.id FROM chat_rooms r
               WHERE COALESCE(r.is_dm,0)=1
                 AND (SELECT COUNT(*) FROM chat_room_members m WHERE m.room_id=r.id)=2
                 AND EXISTS(SELECT 1 FROM chat_room_members WHERE room_id=r.id AND user_id=?)
                 AND EXISTS(SELECT 1 FROM chat_room_members WHERE room_id=r.id AND user_id=?)
               LIMIT 1""",
            (a, b),
        ).fetchone()
        if row:
            return int(row["id"])
        names = {}
        for uid in (a, b):
            r = conn.execute(
                "SELECT COALESCE(NULLIF(contact_name,''), username) AS n FROM clients WHERE id=?",
                (uid,),
            ).fetchone()
            names[uid] = (r["n"] if r else str(uid))
        cur = conn.execute(
            """INSERT INTO chat_rooms (name, description, client_id, created_by, is_dm)
               VALUES (?,?,?,?,1)""",
            (f"DM: {names[a]} / {names[b]}", "", None, created_by),
        )
        room_id = cur.lastrowid
        for uid in (a, b):
            conn.execute(
                """INSERT OR IGNORE INTO chat_room_members
                   (room_id, user_id, role, added_by) VALUES (?,?,?,?)""",
                (room_id, uid, "member", created_by),
            )
        conn.commit()
        return int(room_id)
    finally:
        conn.close()


def update_room(room_id: int, data: dict) -> bool:
    allowed = {"name", "description", "client_id", "archived"}
    fields = {k: v for k, v in (data or {}).items() if k in allowed}
    if not fields:
        return False
    sets = ", ".join(f"{k}=?" for k in fields)
    params = list(fields.values()) + [room_id]
    conn = get_db()
    try:
        conn.execute(f"UPDATE chat_rooms SET {sets} WHERE id=?", params)
        conn.commit()
        return True
    finally:
        conn.close()


def delete_room(room_id: int):
    conn = get_db()
    try:
        # ON DELETE CASCADE handles members/messages/reads
        conn.execute("DELETE FROM chat_rooms WHERE id=?", (room_id,))
        conn.commit()
    finally:
        conn.close()


def list_room_members(room_id: int) -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT rm.user_id, rm.role AS member_role, rm.added_by, rm.added_at,
                      c.username, c.company, c.contact_name, c.email, c.role AS user_role
               FROM chat_room_members rm
               JOIN clients c ON c.id=rm.user_id
               WHERE rm.room_id=?
               ORDER BY c.company, c.username""",
            (room_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_room_member(room_id: int, user_id: int, role: str = "member",
                    added_by: str = "") -> bool:
    role = (role or "member").lower()
    if role not in ("admin", "member"):
        role = "member"
    conn = get_db()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO chat_room_members
               (room_id, user_id, role, added_by) VALUES (?,?,?,?)""",
            (room_id, user_id, role, added_by),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def remove_room_member(room_id: int, user_id: int) -> bool:
    conn = get_db()
    try:
        cur = conn.execute(
            "DELETE FROM chat_room_members WHERE room_id=? AND user_id=?",
            (room_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def user_can_access_room(room_id: int, user_id: int, is_admin: bool = False) -> bool:
    conn = get_db()
    try:
        is_member = bool(conn.execute(
            "SELECT 1 FROM chat_room_members WHERE room_id=? AND user_id=?",
            (room_id, user_id),
        ).fetchone())
        if is_member:
            return True
        # Admins can access any group room — but never a private 1:1 DM they
        # aren't part of.
        if is_admin:
            row = conn.execute(
                "SELECT COALESCE(is_dm,0) AS is_dm FROM chat_rooms WHERE id=?",
                (room_id,),
            ).fetchone()
            if row and int(row["is_dm"]):
                return False
            return True
        return False
    finally:
        conn.close()


def add_room_message(room_id: int, sender_id: int, sender_name: str,
                     sender_role: str, body: str) -> int:
    body = (body or "").strip()
    if not body:
        raise ValueError("Message body is required")
    role = (sender_role or "member").lower()
    # HIPAA: encrypt the body at rest. Schema is unchanged — we store the
    # ciphertext (or legacy plaintext) in the same TEXT column.
    try:
        from app.security import encrypt_message
        stored_body = encrypt_message(body)
    except Exception:
        log.exception("chat encryption failed; falling back to plaintext")
        stored_body = body
    conn = get_db()
    try:
        cur = conn.execute(
            """INSERT INTO chat_messages
               (room_id, sender_id, sender_name, sender_role, body)
               VALUES (?,?,?,?,?)""",
            (room_id, sender_id, sender_name, role, stored_body),
        )
        # Sender has implicitly read their own message
        conn.execute(
            """INSERT INTO chat_reads (room_id, user_id, last_read_message_id, updated_at)
               VALUES (?,?,?,CURRENT_TIMESTAMP)
               ON CONFLICT(room_id, user_id) DO UPDATE SET
                 last_read_message_id=excluded.last_read_message_id,
                 updated_at=excluded.updated_at""",
            (room_id, sender_id, cur.lastrowid),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_room_messages(room_id: int, limit: int = 200,
                       before_id: int | None = None) -> list[dict]:
    conn = get_db()
    try:
        if before_id:
            rows = conn.execute(
                """SELECT id, room_id, sender_id, sender_name, sender_role, body, created_at
                   FROM chat_messages
                   WHERE room_id=? AND id<?
                   ORDER BY id DESC LIMIT ?""",
                (room_id, before_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, room_id, sender_id, sender_name, sender_role, body, created_at
                   FROM chat_messages
                   WHERE room_id=?
                   ORDER BY id DESC LIMIT ?""",
                (room_id, limit),
            ).fetchall()
        # Return oldest → newest, decrypting each body on the way out.
        try:
            from app.security import decrypt_message
        except Exception:
            decrypt_message = lambda v: v  # noqa: E731
        out = []
        for r in reversed(rows):
            d = dict(r)
            d["body"] = decrypt_message(d.get("body"))
            out.append(d)
        return out
    finally:
        conn.close()


def mark_room_read(room_id: int, user_id: int) -> int:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT MAX(id) FROM chat_messages WHERE room_id=?",
            (room_id,),
        ).fetchone()
        last_id = int((row[0] if row else 0) or 0)
        conn.execute(
            """INSERT INTO chat_reads (room_id, user_id, last_read_message_id, updated_at)
               VALUES (?,?,?,CURRENT_TIMESTAMP)
               ON CONFLICT(room_id, user_id) DO UPDATE SET
                 last_read_message_id=MAX(last_read_message_id, excluded.last_read_message_id),
                 updated_at=excluded.updated_at""",
            (room_id, user_id, last_id),
        )
        conn.commit()
        return last_id
    finally:
        conn.close()


def _extract_mentions(body: str) -> set[str]:
    """Return the lowercased @tokens found in a message body, e.g.
    "hey @victor @susan" -> {"victor", "susan"}. Tokens are word-ish runs
    after an @ (letters, digits, dot, underscore, hyphen), min length 2."""
    import re
    if not body:
        return set()
    return {m.lower() for m in re.findall(r"@([A-Za-z0-9._-]{2,})", body)}


def _user_mention_aliases(member: dict) -> set[str]:
    """All lowercased handles a person could be @mentioned by: their
    username, the local-part of their email, and the first word of their
    contact name."""
    aliases: set[str] = set()
    uname = (member.get("username") or "").strip().lower()
    if uname:
        aliases.add(uname)
        # username may itself be an email — also add its local part
        if "@" in uname:
            aliases.add(uname.split("@", 1)[0])
    email = (member.get("email") or "").strip().lower()
    if email and "@" in email:
        aliases.add(email.split("@", 1)[0])
    contact = (member.get("contact_name") or "").strip().lower()
    if contact:
        first = contact.split()[0] if contact.split() else ""
        if len(first) >= 2:
            aliases.add(first)
    return {a for a in aliases if len(a) >= 2}


def _recently_active_user_ids(conn, active_within_minutes: int = 10) -> set[int]:
    """Users with recent hub activity (heartbeat or any authenticated request).

    Chat reminder emails should not fire while someone is actively in the app.
    """
    mins = max(1, int(active_within_minutes or 10))
    rows = conn.execute(
        """SELECT DISTINCT client_id
           FROM activity_events
           WHERE client_id IS NOT NULL
             AND occurred_at >= datetime('now', 'localtime', ?)
        """,
        (f"-{mins} minutes",),
    ).fetchall()
    return {int(r[0]) for r in rows if r and r[0] is not None}


def list_unread_mention_reminders(min_age_minutes: int = 120,
                                  max_age_minutes: int = 10080,
                                  active_within_minutes: int = 10) -> list[dict]:
    """Find chat messages that @mention a room member who still hasn't read
    them after ``min_age_minutes`` (default 2 hours) and for whom no reminder
    has been sent yet.

    Only messages between ``min_age_minutes`` and ``max_age_minutes`` old (so
    we never spam about ancient backlog) in non-archived rooms are considered.

    Returns one dict per (message × recipient):
        {message_id, room_id, room_name, sender_name, created_at,
         user_id, username, contact_name, email}
    """
    try:
        from app.security import decrypt_message
    except Exception:
        decrypt_message = lambda v: v  # noqa: E731
    conn = get_db()
    out: list[dict] = []
    try:
        active_ids = _recently_active_user_ids(
            conn, active_within_minutes=active_within_minutes
        )
        rows = conn.execute(
            """SELECT m.id, m.room_id, m.sender_id, m.sender_name, m.body,
                      m.created_at, r.name AS room_name
               FROM chat_messages m
               JOIN chat_rooms r ON r.id = m.room_id
               WHERE COALESCE(r.archived,0)=0
                 AND m.created_at <= datetime('now', ?)
                 AND m.created_at >= datetime('now', ?)
               ORDER BY m.id""",
            (f"-{int(min_age_minutes)} minutes",
             f"-{int(max_age_minutes)} minutes"),
        ).fetchall()
        for row in rows:
            msg = dict(row)
            body = decrypt_message(msg.get("body"))
            mentions = _extract_mentions(body)
            if not mentions:
                continue
            members = conn.execute(
                """SELECT rm.user_id, c.username, c.contact_name, c.email
                   FROM chat_room_members rm
                   JOIN clients c ON c.id = rm.user_id
                   WHERE rm.room_id=?""",
                (msg["room_id"],),
            ).fetchall()
            for mrow in members:
                member = dict(mrow)
                uid = int(member.get("user_id") or 0)
                if uid <= 0 or uid == int(msg.get("sender_id") or 0):
                    continue
                if uid in active_ids:
                    continue
                if not member.get("email"):
                    continue
                if not (_user_mention_aliases(member) & mentions):
                    continue
                # already read?
                read = conn.execute(
                    """SELECT last_read_message_id FROM chat_reads
                       WHERE room_id=? AND user_id=?""",
                    (msg["room_id"], uid),
                ).fetchone()
                last_read = int((read[0] if read else 0) or 0)
                if last_read >= int(msg["id"]):
                    continue
                # already reminded?
                done = conn.execute(
                    "SELECT 1 FROM chat_reminders WHERE message_id=? AND user_id=?",
                    (msg["id"], uid),
                ).fetchone()
                if done:
                    continue
                out.append({
                    "message_id": int(msg["id"]),
                    "room_id": int(msg["room_id"]),
                    "room_name": msg.get("room_name") or "",
                    "sender_name": msg.get("sender_name") or "",
                    "created_at": msg.get("created_at") or "",
                    "user_id": uid,
                    "username": member.get("username") or "",
                    "contact_name": member.get("contact_name") or "",
                    "email": member.get("email") or "",
                })
        return out
    finally:
        conn.close()


def mark_chat_reminder_sent(message_id: int, user_id: int) -> None:
    """Record that a 2-hour mention reminder was emailed for this message so
    we never send it again."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO chat_reminders (message_id, user_id) VALUES (?,?)",
            (int(message_id), int(user_id)),
        )
        conn.commit()
    finally:
        conn.close()


def list_room_read_state(room_id: int) -> list[dict]:
    """Per-member read position for a room, used to render read receipts.

    Returns one row per room member:
        {user_id, name, last_read_message_id}
    where ``name`` prefers the contact name, falling back to username.
    """
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT rm.user_id,
                      c.username, c.contact_name,
                      COALESCE(cr.last_read_message_id, 0) AS last_read
               FROM chat_room_members rm
               JOIN clients c ON c.id = rm.user_id
               LEFT JOIN chat_reads cr
                      ON cr.room_id = rm.room_id AND cr.user_id = rm.user_id
               WHERE rm.room_id = ?""",
            (room_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            out.append({
                "user_id": int(d.get("user_id") or 0),
                "name": (d.get("contact_name") or d.get("username") or "User"),
                "last_read_message_id": int(d.get("last_read") or 0),
            })
        return out
    finally:
        conn.close()


def list_stale_unread_users(min_age_minutes: int = 15,
                            max_age_minutes: int = 10080,
                            active_within_minutes: int = 10) -> list[dict]:
    """Find every chat member who still has UNREAD messages older than
    ``min_age_minutes`` (default 15) that we haven't already nudged them about.

    Read messages never count (last_read advances past them), the sender is
    never reminded about their own message, and a per-user high-water mark
    (chat_catchup_state) means each person is emailed at most once per wave of
    new messages — not once per message.

    Returns one row per user:
        {user_id, username, contact_name, email, max_unread_id, unread_count}
    """
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT c.id            AS user_id,
                      c.username      AS username,
                      c.contact_name  AS contact_name,
                      c.email         AS email,
                      MAX(m.id)       AS max_unread_id,
                      COUNT(*)        AS unread_count
               FROM chat_messages m
               JOIN chat_rooms r        ON r.id = m.room_id AND COALESCE(r.archived,0)=0
               JOIN chat_room_members rm ON rm.room_id = m.room_id
               JOIN clients c           ON c.id = rm.user_id
               LEFT JOIN chat_reads cr  ON cr.room_id = m.room_id AND cr.user_id = rm.user_id
               LEFT JOIN chat_catchup_state cs ON cs.user_id = rm.user_id
               WHERE COALESCE(m.sender_id,0) <> rm.user_id
                 AND m.id > COALESCE(cr.last_read_message_id, 0)
                 AND m.id > COALESCE(cs.last_reminded_message_id, 0)
                 AND m.created_at <= datetime('now', ?)
                 AND m.created_at >= datetime('now', ?)
                                 AND NOT EXISTS (
                                         SELECT 1 FROM activity_events ae
                                         WHERE ae.client_id = c.id
                                             AND ae.occurred_at >= datetime('now', 'localtime', ?)
                                 )
                 AND c.email IS NOT NULL AND TRIM(c.email) <> ''
               GROUP BY c.id""",
            (f"-{int(min_age_minutes)} minutes",
                         f"-{int(max_age_minutes)} minutes",
                         f"-{max(1, int(active_within_minutes or 10))} minutes"),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_catchup_reminded(user_id: int, last_message_id: int) -> None:
    """Stamp the high-water mark after emailing a user the catch-up nudge, so
    we don't re-nudge them about the same backlog of unread messages."""
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO chat_catchup_state (user_id, last_reminded_message_id, sent_at)
               VALUES (?,?,CURRENT_TIMESTAMP)
               ON CONFLICT(user_id) DO UPDATE SET
                 last_reminded_message_id = MAX(last_reminded_message_id, excluded.last_reminded_message_id),
                 sent_at = excluded.sent_at""",
            (int(user_id), int(last_message_id or 0)),
        )
        conn.commit()
    finally:
        conn.close()


def chat_unread_total(user_id: int, is_admin: bool = False) -> int:
    """Total unread messages across rooms visible to this user."""
    conn = get_db()
    try:
        if is_admin:
            row = conn.execute(
                """SELECT COUNT(*) FROM chat_messages m
                   WHERE COALESCE(m.sender_id,0) <> ?
                     AND m.id > COALESCE(
                         (SELECT last_read_message_id FROM chat_reads
                            WHERE room_id=m.room_id AND user_id=?), 0)""",
                (user_id, user_id),
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT COUNT(*) FROM chat_messages m
                   JOIN chat_room_members rm ON rm.room_id=m.room_id
                   WHERE rm.user_id=?
                     AND COALESCE(m.sender_id,0) <> ?
                     AND m.id > COALESCE(
                         (SELECT last_read_message_id FROM chat_reads
                            WHERE room_id=m.room_id AND user_id=?), 0)""",
                (user_id, user_id, user_id),
            ).fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def list_chat_eligible_users() -> list[dict]:
    """All active users that can be added to a chat room.

    De-duplicates legacy short-username rows ('admin', 'jessica',
    'susan', 'melissa', 'eric') against their canonical email-style row
    ('admin@medprosc.com', etc.) so each real person only appears once in
    the New Room picker. Both rows still authenticate for login.
    """
    # legacy short username -> canonical email-style username
    _LEGACY_TO_CANONICAL = {
        "admin":   "admin@medprosc.com",
        "eric":    "eric@medprosc.com",
        "susan":   "susan@medprosc.com",
        "melissa": "melissa@medprosc.com",
        "jessica": "jessica@medprosc.com",
    }
    # Accounts that exist only as a system/department login and must never
    # appear in the user-facing roster (chat picker, Team Production user
    # filter, client-access list). See module-level _HIDDEN_ROSTER_USERS.
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT id, username, company, contact_name, email, role
               FROM clients
               WHERE COALESCE(is_active,1)=1
               ORDER BY role DESC, company, username"""
        ).fetchall()
        # Build the set of canonical usernames present so we can hide the
        # matching legacy short rows. If the canonical row doesn't exist,
        # keep the legacy row so the person isn't dropped from the picker.
        present = {r["username"] for r in rows}
        deduped = []
        for r in rows:
            uname = r["username"]
            if (uname or "").lower() in _HIDDEN_ROSTER_USERS:
                continue  # system/department login — never show in the roster
            canonical = _LEGACY_TO_CANONICAL.get(uname)
            if canonical and canonical in present and canonical != uname:
                continue  # hide legacy short row, canonical is in the list
            deduped.append(dict(r))
        return deduped
    finally:
        conn.close()


# ─── Client access (which users can open a given client) ─────────────────────

def list_client_access(client_id: int) -> list[dict]:
    """Return active users currently granted access to ``client_id``."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT c.id, c.username, c.company, c.contact_name, c.email, c.role,
                      cua.granted_by, cua.granted_at
               FROM client_user_access cua
               JOIN clients c ON c.id = cua.user_id
               WHERE cua.client_id=? AND COALESCE(c.is_active,1)=1
               ORDER BY c.role DESC, c.contact_name, c.username""",
            (int(client_id),),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def set_client_access(client_id: int, user_ids: list[int], granted_by: str = "") -> int:
    """Replace the access list for ``client_id`` with exactly ``user_ids``.
    Returns the number of users now granted access."""
    cid = int(client_id)
    cleaned: list[int] = []
    for raw in (user_ids or []):
        try:
            uid = int(raw)
        except (TypeError, ValueError):
            continue
        if uid > 0 and uid not in cleaned:
            cleaned.append(uid)

    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM client_user_access WHERE client_id=?", (cid,))
        for uid in cleaned:
            # Skip rows where the user_id doesn't exist (defensive)
            exists = cur.execute(
                "SELECT 1 FROM clients WHERE id=? AND COALESCE(is_active,1)=1",
                (uid,),
            ).fetchone()
            if not exists:
                continue
            cur.execute(
                "INSERT OR IGNORE INTO client_user_access (client_id, user_id, granted_by) "
                "VALUES (?,?,?)",
                (cid, uid, granted_by or ""),
            )
        conn.commit()
        return cur.execute(
            "SELECT COUNT(*) FROM client_user_access WHERE client_id=?", (cid,)
        ).fetchone()[0]
    finally:
        conn.close()


def accounts_assigned_to_user(user_id: int) -> list[int]:
    """Account client_ids this user has been EXPLICITLY granted access to.

    Unlike ``list_clients_for_user`` this does NOT apply the legacy
    default-open rule — it returns only accounts with a real
    ``client_user_access`` row for ``user_id``. Used so every user assigned to
    an account sees the same documents and attachments the admin uploads for
    that account.
    """
    uid = int(user_id or 0)
    if not uid:
        return []
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT DISTINCT client_id FROM client_user_access WHERE user_id=?",
            (uid,),
        ).fetchall()
        return [int(r[0]) for r in rows if r[0] is not None]
    finally:
        conn.close()


def list_clients_for_user(user_id: int) -> list[int]:
    """Client IDs visible to ``user_id``.

    Legacy/default-open behavior is preserved for existing clients that have no
    rows in ``client_user_access`` yet: those clients remain visible to all
    staff. Once a client has at least one explicit access row, only the listed
    staff users can see it.
    """
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT c.id
            FROM clients c
            WHERE COALESCE(c.role, 'client')='client'
              AND COALESCE(c.is_active,1)=1
              AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM client_user_access cua_any
                        WHERE cua_any.client_id = c.id
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM client_user_access cua_self
                        WHERE cua_self.client_id = c.id
                          AND cua_self.user_id = ?
                    )
              )
            ORDER BY c.id
            """,
            (int(user_id),),
        ).fetchall()
        return [int(r[0]) for r in rows]
    finally:
        conn.close()
