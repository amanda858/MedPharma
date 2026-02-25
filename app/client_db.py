"""Database — MedPharma Client Hub: claims_master, payments, notes_log,
credentialing, edi_setup, providers, clients, sessions."""

import sqlite3
import os
import hashlib
import secrets
from datetime import datetime, date, timedelta
from app.config import DATABASE_PATH


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
            report_tab_names TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token         TEXT PRIMARY KEY,
            client_id     INTEGER NOT NULL,
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at    TEXT,
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
            sub_profile     TEXT DEFAULT '',
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        -- ── notes log ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS notes_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id   INTEGER NOT NULL,
            ClaimKey    TEXT DEFAULT '',
            Module      TEXT DEFAULT 'Claim',  -- Claim, Credentialing, EDI
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
    """)
    conn.commit()

    # ── Migrate existing DBs: add profile columns if missing ──────────────
    profile_cols = [
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
    ]
    cur.execute("PRAGMA table_info(clients)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col, col_def in profile_cols:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE clients ADD COLUMN {col} {col_def}")
    conn.commit()

    # ── Migrate existing DBs: add expires_at column to sessions ──────────
    cur.execute("PRAGMA table_info(sessions)")
    session_cols = {row[1] for row in cur.fetchall()}
    if "expires_at" not in session_cols:
        cur.execute("ALTER TABLE sessions ADD COLUMN expires_at TEXT")
    conn.commit()

    # ── Migrate existing DBs: add sub_profile column to data tables ───────
    sub_profile_tables = ["claims_master", "payments", "providers",
                          "credentialing", "edi_setup"]
    for tbl in sub_profile_tables:
        cur.execute(f"PRAGMA table_info({tbl})")
        cols = {row[1] for row in cur.fetchall()}
        if "sub_profile" not in cols:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN sub_profile TEXT DEFAULT ''")
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM clients")
    total = cur.fetchone()[0]

    if total == 0:
        _seed_data(conn)
    else:
        # Auto-migrate: fix client profile data
        cur.execute("UPDATE clients SET contact_name='Luminary Practice', email='info@luminarypractice.com' WHERE username='eric' AND contact_name='Eric'")
        # Migrate jessica from client → admin (she is a MedPharma staff user, not a client)
        cur.execute("UPDATE clients SET role='admin', company='MedPharma SC' WHERE username='jessica' AND role='client'")
        # Ensure jessica account exists as admin
        cur.execute("SELECT COUNT(*) FROM clients WHERE username='jessica'")
        if cur.fetchone()[0] == 0:
            jsalt = secrets.token_hex(16)
            cur.execute(
                "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
                ("jessica", _hash_pw("jessica123", jsalt), jsalt, "MedPharma SC", "Jessica", "", "admin")
            )
        # Migrate rcm from client → admin (MedPharma staff who sees all accounts)
        cur.execute("UPDATE clients SET role='admin', company='MedPharma SC' WHERE username='rcm' AND role='client'")
        # Reset rcm password so login works
        rcm_salt = secrets.token_hex(16)
        cur.execute("UPDATE clients SET password=?, salt=? WHERE username='rcm'",
                    (_hash_pw("rcm123", rcm_salt), rcm_salt))
        # Ensure rcm account exists as admin
        cur.execute("SELECT COUNT(*) FROM clients WHERE username='rcm'")
        if cur.fetchone()[0] == 0:
            rcm_salt2 = secrets.token_hex(16)
            cur.execute(
                "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
                ("rcm", _hash_pw("rcm123", rcm_salt2), rcm_salt2, "MedPharma SC", "RCM", "", "admin")
            )
        # Ensure TruPath client exists (separate from rcm user)
        cur.execute("SELECT COUNT(*) FROM clients WHERE company='TruPath' AND role='client'")
        if cur.fetchone()[0] == 0:
            tpsalt = secrets.token_hex(16)
            cur.execute(
                "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
                ("trupath", _hash_pw("trupath123", tpsalt), tpsalt, "TruPath", "TruPath", "", "client")
            )
        # Clear Luminary's own profile fields — only sub-profiles (MHP/OMT) hold profile data
        cur.execute("""UPDATE clients SET tax_id='', group_npi='', individual_npi='',
                       ptan_group='', ptan_individual='', specialty=''
                       WHERE username='eric' AND practice_type='MHP+OMT'""")
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

    # Client 1 — Luminary (Ancillary practice: OMT + MHP as sub-profiles)
    # NOTE: Luminary has NO own profile fields — all profile data lives in sub-profiles (MHP/OMT)
    s1 = secrets.token_hex(16)
    cur.execute(
        """INSERT INTO clients
           (username,password,salt,company,contact_name,email,role,practice_type)
           VALUES (?,?,?,?,?,?,?,?)""",
        ("eric", _hash_pw("eric123", s1), s1, "Luminary (OMT/MHP)", "Luminary Practice", "info@luminarypractice.com", "client",
         "MHP+OMT")
    )
    luminary_id = cur.lastrowid

    # Client 2 — TruPath
    s2 = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("trupath", _hash_pw("trupath123", s2), s2, "TruPath", "TruPath", "", "client")
    )

    # Jessica — MedPharma staff user (admin), NOT a client
    jsalt = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("jessica", _hash_pw("jessica123", jsalt), jsalt, "MedPharma SC", "Jessica", "", "admin")
    )

    # RCM — MedPharma staff user (admin), sees all client accounts
    rsalt = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("rcm", _hash_pw("rcm123", rsalt), rsalt, "MedPharma SC", "RCM", "", "admin")
    )

    # Sub-profiles for Luminary: MHP and OMT (both Ancillary)
    cur.execute(
        """INSERT INTO practice_profiles
           (client_id,profile_name,practice_type,specialty,tax_id,group_npi,individual_npi,ptan_group,ptan_individual)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (luminary_id, "MHP", "Ancillary", "Michigan Health Partners",
         "334707784", "1033901723", "1497174478", "MI120440", "MI20440001")
    )
    cur.execute(
        """INSERT INTO practice_profiles
           (client_id,profile_name,practice_type,specialty)
           VALUES (?,?,?,?)""",
        (luminary_id, "OMT", "Ancillary", "Occupational / Manual Therapy")
    )

    conn.commit()
    # No fake claims, providers, credentialing, or payments seeded.
    # All data is imported via Excel/CSV file uploads.


# ─── Auth ─────────────────────────────────────────────────────────────────────

def authenticate(username: str, password: str):
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM clients WHERE username=? AND is_active=1", (username,))
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
        return {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role", "practice_type")}, token
    finally:
        conn.close()


def validate_session(token: str):
    if not token:
        return None
    conn = get_db()
    try:
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
    return {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role", "practice_type")}


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
        cur.execute("SELECT id,username,company,contact_name,email,phone,role,is_active,created_at,last_login,practice_type FROM clients ORDER BY company")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return rows


def create_client(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        salt = secrets.token_hex(16)
        cur.execute("""INSERT INTO clients (username,password,salt,company,contact_name,email,phone,role)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (data["username"], _hash_pw(data["password"], salt), salt,
                     data.get("company", ""), data.get("contact_name", ""),
                     data.get("email", ""), data.get("phone", ""), data.get("role", "client")))
        conn.commit()
        cid = cur.lastrowid
    finally:
        conn.close()
    return cid


def update_client(cid: int, data: dict):
    conn = get_db()
    try:
        cur = conn.cursor()
        allowed = ["company", "contact_name", "email", "phone", "role", "is_active",
                   "tax_id", "group_npi", "individual_npi", "ptan_group", "ptan_individual",
                   "address", "specialty", "notes", "doc_tab_names", "practice_type"]
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


DEFAULT_DOC_TABS = ["Payor Letters", "Company Documents", "Credentialing Docs", "Reports", "General"]
DEFAULT_REPORT_TABS = ["Claims", "Credentialing", "EDI"]


def get_profile(client_id: int) -> dict:
    import json as _json
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT company, contact_name, email, phone,
                   tax_id, group_npi, individual_npi, ptan_group, ptan_individual,
                   address, specialty, notes, doc_tab_names, practice_type, report_tab_names
            FROM clients WHERE id=?""", [client_id])
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    cols = ["company", "contact_name", "email", "phone", "tax_id", "group_npi",
            "individual_npi", "ptan_group", "ptan_individual", "address", "specialty", "notes",
            "doc_tab_names", "practice_type", "report_tab_names"]
    d = {c: (row[i] or "") for i, c in enumerate(cols)}
    try:
        d["doc_tabs"] = _json.loads(d["doc_tab_names"]) if d["doc_tab_names"] else DEFAULT_DOC_TABS[:]
    except Exception:
        d["doc_tabs"] = DEFAULT_DOC_TABS[:]
    try:
        d["report_tabs"] = _json.loads(d["report_tab_names"]) if d["report_tab_names"] else DEFAULT_REPORT_TABS[:]
    except Exception:
        d["report_tabs"] = DEFAULT_REPORT_TABS[:]
    return d


def update_profile(client_id: int, data: dict):
    import json as _json
    allowed = ["company", "contact_name", "email", "phone", "tax_id", "group_npi",
               "individual_npi", "ptan_group", "ptan_individual", "address", "specialty", "notes",
               "doc_tab_names", "practice_type", "report_tab_names"]
    update_client(client_id, {k: v for k, v in data.items() if k in allowed})


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
            (client_id,ClaimKey,PostDate,PaymentAmount,AdjustmentAmount,PayerType,CheckNumber,ERA,Notes,sub_profile)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (data["client_id"], data["ClaimKey"], data.get("PostDate", ""),
             data.get("PaymentAmount", 0), data.get("AdjustmentAmount", 0),
             data.get("PayerType", "Primary"), data.get("CheckNumber", ""),
             data.get("ERA", ""), data.get("Notes", ""), data.get("sub_profile", "")))
        # Recalculate PaidAmount on claim
        cur.execute("SELECT COALESCE(SUM(PaymentAmount),0) FROM payments WHERE client_id=? AND ClaimKey=?",
                    (data["client_id"], data["ClaimKey"]))
        total_paid = cur.fetchone()[0]
        cur.execute("""UPDATE claims_master SET PaidAmount=?,
                       BalanceRemaining=MAX(0, ChargeAmount - ?),
                       updated_at=? WHERE client_id=? AND ClaimKey=?""",
                    (total_paid, total_paid, datetime.now().isoformat(),
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
            cur.execute("SELECT COALESCE(SUM(PaymentAmount),0) FROM payments WHERE client_id=? AND ClaimKey=?",
                        (client_id, claim_key))
            total_paid = cur.fetchone()[0]
            cur.execute("""UPDATE claims_master SET PaidAmount=?,
                           BalanceRemaining=MAX(0, ChargeAmount - ?),
                           updated_at=? WHERE client_id=? AND ClaimKey=?""",
                        (total_paid, total_paid, datetime.now().isoformat(), client_id, claim_key))
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

        today = date.today()
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
            "status_distribution": status_dist,
            "payor_mix": payor_mix,
            "denial_categories": denial_cats,
            "payment_trend": pay_trend,
            "credentialing_stats": cred_stats,
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
        today = date.today()
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

        # 3. Overdue follow-ups (credentialing)
        for tbl, label in [("credentialing", "Credentialing")]:
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
    """Search across claims, providers, credentialing, EDI."""
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
    """Generic export for credentialing, edi_setup, providers."""
    allowed_tables = {"credentialing", "edi_setup", "providers"}
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
    finally:
        conn.close()
    return rows


def add_production_log(data: dict) -> int:
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO team_production (client_id, work_date, username, category, task_description, quantity, time_spent, notes)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            data["client_id"], data["work_date"], data["username"],
            data.get("category", ""), data.get("task_description", ""),
            data.get("quantity", 0), data.get("time_spent", 0),
            data.get("notes", "")
        ))
        conn.commit()
        new_id = cur.lastrowid
    finally:
        conn.close()
    return new_id


def delete_production_log(log_id: int, client_id: int = None):
    conn = get_db()
    try:
        if client_id:
            conn.execute("DELETE FROM team_production WHERE id=? AND client_id=?", (log_id, client_id))
        else:
            conn.execute("DELETE FROM team_production WHERE id=?", (log_id,))
        conn.commit()
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

        # Time management flags — users averaging < 6 hrs/day worked
        flags = []
        for u in by_user:
            if u["days_worked"] > 0:
                avg_hrs = round(u["total_hours"] / u["days_worked"], 1)
                u["avg_hours_per_day"] = avg_hrs
                if avg_hrs < 6:
                    flags.append({"username": u["username"], "avg_hours_per_day": avg_hrs,
                                  "days_worked": u["days_worked"],
                                  "recommendation": "Below 6hr/day average — review time management"})
    finally:
        conn.close()
    return {
        "by_user": by_user,
        "by_category": by_category,
        "details": details,
        "time_management_flags": flags,
    }


# ─── File uploads ──────────────────────────────────────────────────────────

def list_files(client_id: int = None):
    conn = get_db()
    try:
        cur = conn.cursor()
        if client_id:
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