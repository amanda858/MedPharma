"""Database — MedPharma Client Hub: claims_master, payments, notes_log,
credentialing, enrollment, edi_setup, providers, clients, sessions."""

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
            practice_type    TEXT DEFAULT ''
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
    ]
    cur.execute("PRAGMA table_info(clients)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col, col_def in profile_cols:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE clients ADD COLUMN {col} {col_def}")
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

    cur.execute("SELECT COUNT(*) FROM clients")
    total = cur.fetchone()[0]

    if total == 0:
        _seed_data(conn)
    else:
        # Auto-fix: ensure BOTH expected client accounts exist.
        # If either is missing the DB has stale/wrong seed data — wipe and re-seed.
        cur.execute("SELECT COUNT(*) FROM clients WHERE username IN ('eric','rcm')")
        found = cur.fetchone()[0]
        if found < 2:
            for tbl in (
                "sessions", "claims_master", "payments", "notes_log",
                "credentialing", "enrollment", "edi_setup", "providers",
                "client_files", "clients"
            ):
                try:
                    cur.execute(f"DELETE FROM {tbl}")
                except Exception:
                    pass
            conn.commit()
            _seed_data(conn)

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
    s1 = secrets.token_hex(16)
    cur.execute(
        """INSERT INTO clients
           (username,password,salt,company,contact_name,email,role,
            tax_id,group_npi,individual_npi,ptan_group,ptan_individual,practice_type)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        ("eric", _hash_pw("eric123", s1), s1, "Luminary (OMT/MHP)", "Luminary Practice", "info@luminarypractice.com", "client",
         "334707784", "1033901723", "1497174478", "MI120440", "MI20440001", "MHP+OMT")
    )
    luminary_id = cur.lastrowid

    # Client 2 — TruPath (Laboratory — uses Group Contracting, not individual credentialing)
    s2 = secrets.token_hex(16)
    cur.execute(
        """INSERT INTO clients
           (username,password,salt,company,contact_name,email,role,specialty,practice_type)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        ("rcm", _hash_pw("rcm123", s2), s2, "TruPath", "RCM Team", "rcm@medprosc.com", "client",
         "Laboratory", "Laboratory")
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
    cur = conn.cursor()
    cur.execute("SELECT * FROM clients WHERE username=? AND is_active=1", (username,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None, None
    c = dict(row)
    if _hash_pw(password, c["salt"]) != c["password"]:
        conn.close()
        return None, None
    token = secrets.token_urlsafe(32)
    cur.execute("INSERT INTO sessions (token,client_id,expires_at) VALUES (?,?,?)",
                (token, c["id"], "2099-12-31"))
    cur.execute("UPDATE clients SET last_login=? WHERE id=?",
                (datetime.now().isoformat(), c["id"]))
    conn.commit()
    conn.close()
    return {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role", "practice_type")}, token


def validate_session(token: str):
    if not token:
        return None
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""SELECT c.* FROM sessions s
                   JOIN clients c ON c.id=s.client_id
                   WHERE s.token=? AND c.is_active=1""", (token,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    c = dict(row)
    return {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role", "practice_type")}


def logout_session(token: str):
    conn = get_db()
    conn.execute("DELETE FROM sessions WHERE token=?", (token,))
    conn.commit()
    conn.close()


# ─── Clients (admin) ──────────────────────────────────────────────────────────

def list_clients():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id,username,company,contact_name,email,phone,role,is_active,created_at,last_login,practice_type FROM clients ORDER BY company")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_client(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    salt = secrets.token_hex(16)
    cur.execute("""INSERT INTO clients (username,password,salt,company,contact_name,email,phone,role)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (data["username"], _hash_pw(data["password"], salt), salt,
                 data.get("company", ""), data.get("contact_name", ""),
                 data.get("email", ""), data.get("phone", ""), data.get("role", "client")))
    conn.commit()
    cid = cur.lastrowid
    conn.close()
    return cid


def update_client(cid: int, data: dict):
    conn = get_db()
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
    conn.close()


DEFAULT_DOC_TABS = ["Payor Letters", "Company Documents", "Credentialing Docs", "Reports", "General"]


def get_profile(client_id: int) -> dict:
    import json as _json
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT company, contact_name, email, phone,
               tax_id, group_npi, individual_npi, ptan_group, ptan_individual,
               address, specialty, notes, doc_tab_names, practice_type
        FROM clients WHERE id=?""", [client_id])
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    cols = ["company", "contact_name", "email", "phone", "tax_id", "group_npi",
            "individual_npi", "ptan_group", "ptan_individual", "address", "specialty", "notes",
            "doc_tab_names", "practice_type"]
    d = {c: (row[i] or "") for i, c in enumerate(cols)}
    try:
        d["doc_tabs"] = _json.loads(d["doc_tab_names"]) if d["doc_tab_names"] else DEFAULT_DOC_TABS[:]
    except Exception:
        d["doc_tabs"] = DEFAULT_DOC_TABS[:]
    return d


def update_profile(client_id: int, data: dict):
    import json as _json
    allowed = ["company", "contact_name", "email", "phone", "tax_id", "group_npi",
               "individual_npi", "ptan_group", "ptan_individual", "address", "specialty", "notes",
               "doc_tab_names", "practice_type"]
    update_client(client_id, {k: v for k, v in data.items() if k in allowed})


def get_practice_profiles(client_id: int) -> list:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM practice_profiles WHERE client_id=? ORDER BY profile_name", [client_id])
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def upsert_practice_profile(client_id: int, profile_name: str, data: dict):
    allowed = ["practice_type", "specialty", "tax_id", "group_npi", "individual_npi",
               "ptan_group", "ptan_individual", "address", "contact_name", "email", "phone", "notes"]
    fields = [f for f in allowed if f in data and data[f] is not None]
    conn = get_db()
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
    conn.close()


def delete_practice_profile(pp_id: int, client_id: int):
    conn = get_db()
    conn.execute("DELETE FROM practice_profiles WHERE id=? AND client_id=?", [pp_id, client_id])
    conn.commit()
    conn.close()


# ─── Providers ────────────────────────────────────────────────────────────────

def list_providers(client_id: int = None, sub_profile: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM providers WHERE 1=1"
    params = []
    if client_id:
        q += " AND client_id=?"
        params.append(client_id)
    if sub_profile:
        q += " AND sub_profile=?"
        params.append(sub_profile)
    q += " ORDER BY ProviderName"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_provider(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO providers (client_id,ProviderName,NPI,Specialty,TaxID,Email,Phone,Status,StartDate,Notes,sub_profile)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (data["client_id"], data["ProviderName"], data.get("NPI", ""),
                 data.get("Specialty", ""), data.get("TaxID", ""), data.get("Email", ""),
                 data.get("Phone", ""), data.get("Status", "Active"),
                 data.get("StartDate", ""), data.get("Notes", ""), data.get("sub_profile", "")))
    conn.commit()
    pid = cur.lastrowid
    conn.close()
    return pid


def update_provider(pid: int, data: dict):
    conn = get_db()
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
    conn.close()


def delete_provider(pid: int):
    conn = get_db()
    conn.execute("DELETE FROM providers WHERE id=?", (pid,))
    conn.commit()
    conn.close()


# ─── Claims ───────────────────────────────────────────────────────────────────

CLAIM_STATUSES = ["Intake", "Verification", "Coding", "Billed/Submitted",
                   "Rejected", "Denied", "A/R Follow-Up", "Appeals", "Paid", "Closed"]


def get_claims(client_id: int = None, status: str = None, sub_profile: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = """SELECT cm.*, c.company as client_company
           FROM claims_master cm
           JOIN clients c ON c.id = cm.client_id
           WHERE 1=1"""
    params = []
    if client_id:
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
    conn.close()
    return rows


def get_claim(claim_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM claims_master WHERE id=?", (claim_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def create_claim(data: dict) -> int:
    conn = get_db()
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
    conn.close()
    return cid


def update_claim(claim_id: int, data: dict):
    conn = get_db()
    cur = conn.cursor()
    allowed = ["ClaimKey", "PatientID", "PatientName", "Payor", "ProviderName", "NPI", "DOS",
               "CPTCode", "Description", "ChargeAmount", "AllowedAmount", "AdjustmentAmount",
               "PaidAmount", "BalanceRemaining", "ClaimStatus", "StatusStartDate", "BillDate",
               "DeniedDate", "PaidDate", "Owner", "NextAction", "NextActionDueDate",
               "SLABreached", "DenialCategory", "DenialReason", "AppealDate", "AppealStatus",
               "sub_profile"]
    parts, params = ["LastTouchedDate=?", "updated_at=?"], [datetime.now().isoformat(), datetime.now().isoformat()]
    for f in allowed:
        if f in data:
            parts.append(f"{f}=?")
            params.append(data[f])
    params.append(claim_id)
    cur.execute(f"UPDATE claims_master SET {','.join(parts)} WHERE id=?", params)
    conn.commit()
    conn.close()


def delete_claim(claim_id: int):
    conn = get_db()
    conn.execute("DELETE FROM claims_master WHERE id=?", (claim_id,))
    conn.commit()
    conn.close()


# ─── Payments ─────────────────────────────────────────────────────────────────

def get_payments(client_id: int, claim_key: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM payments WHERE client_id=? AND ClaimKey=? ORDER BY PostDate DESC",
                (client_id, claim_key))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_payment(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO payments
        (client_id,ClaimKey,PostDate,PaymentAmount,AdjustmentAmount,PayerType,CheckNumber,ERA,Notes)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (data["client_id"], data["ClaimKey"], data.get("PostDate", ""),
         data.get("PaymentAmount", 0), data.get("AdjustmentAmount", 0),
         data.get("PayerType", "Primary"), data.get("CheckNumber", ""),
         data.get("ERA", ""), data.get("Notes", "")))
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
    conn.close()
    return pid


def delete_payment(payment_id: int):
    conn = get_db()
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
    conn.close()


# ─── Notes ────────────────────────────────────────────────────────────────────

def get_notes(client_id: int, claim_key: str = None, module: str = None, ref_id: int = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM notes_log WHERE client_id=?"
    params = [client_id]
    if claim_key:
        q += " AND ClaimKey=?"
        params.append(claim_key)
    if module:
        q += " AND Module=?"
        params.append(module)
    if ref_id:
        q += " AND RefID=?"
        params.append(ref_id)
    q += " ORDER BY created_at ASC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def add_note(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO notes_log (client_id,ClaimKey,Module,RefID,Note,Author)
                   VALUES (?,?,?,?,?,?)""",
                (data["client_id"], data.get("ClaimKey", ""), data.get("Module", "Claim"),
                 data.get("RefID", 0), data["Note"], data.get("Author", "")))
    conn.commit()
    nid = cur.lastrowid
    conn.close()
    return nid


# ─── Credentialing ────────────────────────────────────────────────────────────

def get_credentialing(client_id: int = None, status: str = None, sub_profile: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM credentialing WHERE 1=1"
    params = []
    if client_id:
        q += " AND client_id=?"; params.append(client_id)
    if status:
        q += " AND Status=?"; params.append(status)
    if sub_profile:
        q += " AND sub_profile=?"; params.append(sub_profile)
    q += " ORDER BY updated_at DESC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_credentialing(data: dict) -> int:
    conn = get_db()
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
    conn.close()
    return cid


def update_credentialing(rec_id: int, data: dict):
    conn = get_db()
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
    conn.close()


def delete_credentialing(rec_id: int):
    conn = get_db()
    conn.execute("DELETE FROM credentialing WHERE id=?", (rec_id,)); conn.commit(); conn.close()


# ─── Enrollment ───────────────────────────────────────────────────────────────

def get_enrollment(client_id: int = None, status: str = None, sub_profile: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM enrollment WHERE 1=1"
    params = []
    if client_id:
        q += " AND client_id=?"; params.append(client_id)
    if status:
        q += " AND Status=?"; params.append(status)
    if sub_profile:
        q += " AND sub_profile=?"; params.append(sub_profile)
    q += " ORDER BY updated_at DESC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_enrollment(data: dict) -> int:
    conn = get_db()
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
    conn.close()
    return eid


def update_enrollment(rec_id: int, data: dict):
    conn = get_db()
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
    conn.close()


def delete_enrollment(rec_id: int):
    conn = get_db()
    conn.execute("DELETE FROM enrollment WHERE id=?", (rec_id,)); conn.commit(); conn.close()


# ─── EDI Setup ────────────────────────────────────────────────────────────────

def get_edi(client_id: int = None, sub_profile: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM edi_setup WHERE 1=1"
    params = []
    if client_id:
        q += " AND client_id=?"; params.append(client_id)
    if sub_profile:
        q += " AND sub_profile=?"; params.append(sub_profile)
    q += " ORDER BY updated_at DESC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_edi(data: dict) -> int:
    conn = get_db()
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
    conn.close()
    return eid


def update_edi(rec_id: int, data: dict):
    conn = get_db()
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
    conn.close()


def delete_edi(rec_id: int):
    conn = get_db()
    conn.execute("DELETE FROM edi_setup WHERE id=?", (rec_id,)); conn.commit(); conn.close()


# ─── Dashboard ────────────────────────────────────────────────────────────────

def get_dashboard(client_id: int = None, sub_profile: str = None):
    """Full KPI dashboard — pass client_id=None for admin (all clients).
       Pass sub_profile='MHP' or 'OMT' to filter by sub-profile."""
    conn = get_db()
    cur = conn.cursor()

    # Build condition fragments
    conditions = []
    p = []
    if client_id:
        conditions.append("client_id=?")
        p.append(client_id)
    if sub_profile:
        conditions.append("sub_profile=?")
        p.append(sub_profile)
    cond = ("WHERE " + " AND ".join(conditions)) if conditions else ""

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

    # Payments MTD
    pay_mtd = q1(f"SELECT COALESCE(SUM(PaymentAmount),0) FROM payments {cond} {'AND' if cond else 'WHERE'} PostDate >= ?",
                 p + [mtd_start])
    pay_ytd = q1(f"SELECT COALESCE(SUM(PaymentAmount),0) FROM payments {cond} {'AND' if cond else 'WHERE'} PostDate >= ?",
                 p + [ytd_start])

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
    aging = {"0_30": 0, "31_60": 0, "61_90": 0, "over_90": 0}
    ar_p = p + ["Paid", "Closed"]
    cur.execute(f"""SELECT BalanceRemaining,
                    CAST(julianday('now') - julianday(COALESCE(NULLIF(BillDate,''), DOS, updated_at)) AS INTEGER) as age
                    FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus NOT IN (?,?) AND BalanceRemaining > 0""",
                ar_p)
    for row in cur.fetchall():
        bal, age = row
        age = age or 0
        if age <= 30:   aging["0_30"] += bal
        elif age <= 60: aging["31_60"] += bal
        elif age <= 90: aging["61_90"] += bal
        else:           aging["over_90"] += bal
    aging = {k: round(v, 2) for k, v in aging.items()}

    # Status distribution
    cur.execute(f"SELECT ClaimStatus, COUNT(*), COALESCE(SUM(ChargeAmount),0) FROM claims_master {cond} GROUP BY ClaimStatus", p)
    status_dist = {r[0]: {"count": r[1], "charge": round(r[2], 2)} for r in cur.fetchall()}

    # Payor mix
    cur.execute(f"""SELECT Payor, COUNT(*), COALESCE(SUM(ChargeAmount),0), COALESCE(SUM(PaidAmount),0)
                    FROM claims_master {cond} GROUP BY Payor ORDER BY SUM(ChargeAmount) DESC LIMIT 8""", p)
    payor_mix = [{"payor": r[0], "count": r[1], "charge": round(r[2], 2), "paid": round(r[3], 2)}
                 for r in cur.fetchall()]

    # Denial categories
    cur.execute(f"""SELECT DenialCategory, COUNT(*) FROM claims_master
                    {cond} {'AND' if cond else 'WHERE'} DenialCategory != '' GROUP BY DenialCategory ORDER BY COUNT(*) DESC""", p)
    denial_cats = [{"category": r[0], "count": r[1]} for r in cur.fetchall()]

    # Payment trend (last 6 months)
    cur.execute(f"""SELECT strftime('%Y-%m', PostDate) as mo, COALESCE(SUM(PaymentAmount),0)
                    FROM payments {cond} {'AND' if cond else 'WHERE'} PostDate != '' GROUP BY mo ORDER BY mo DESC LIMIT 6""", p)
    pay_trend = [{"month": r[0], "amount": round(r[1], 2)} for r in reversed(cur.fetchall())]

    # Credentialing stats
    cur.execute(f"SELECT Status, COUNT(*) FROM credentialing {cond} GROUP BY Status", p)
    cred_stats = {r[0]: r[1] for r in cur.fetchall()}

    # Enrollment stats
    cur.execute(f"SELECT Status, COUNT(*) FROM enrollment {cond} GROUP BY Status", p)
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

    conn.close()
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
        "enrollment_stats": enroll_stats,
        "profile": profile,
    }


# ─── File uploads ──────────────────────────────────────────────────────────

def list_files(client_id: int = None):
    conn = get_db()
    cur = conn.cursor()
    if client_id:
        cur.execute("SELECT * FROM client_files WHERE client_id=? ORDER BY created_at DESC", (client_id,))
    else:
        cur.execute("SELECT * FROM client_files ORDER BY created_at DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def add_file(client_id: int, filename: str, original_name: str, file_type: str,
             file_size: int, category: str, description: str, row_count: int, uploaded_by: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO client_files
        (client_id,filename,original_name,file_type,file_size,category,description,row_count,uploaded_by,status)
        VALUES (?,?,?,?,?,?,?,?,'Uploaded')
    """, (client_id, filename, original_name, file_type, file_size, category, description, row_count, uploaded_by))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return new_id


def delete_file_record(file_id: int, client_id: int = None):
    conn = get_db()
    if client_id:
        conn.execute("DELETE FROM client_files WHERE id=? AND client_id=?", (file_id, client_id))
    else:
        conn.execute("DELETE FROM client_files WHERE id=?", (file_id,))
    conn.commit()
    conn.close()