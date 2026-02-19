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
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            password      TEXT NOT NULL,
            salt          TEXT NOT NULL,
            company       TEXT NOT NULL,
            contact_name  TEXT DEFAULT '',
            email         TEXT DEFAULT '',
            phone         TEXT DEFAULT '',
            role          TEXT DEFAULT 'client',
            is_active     INTEGER DEFAULT 1,
            created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            last_login    TEXT
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
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM clients")
    if cur.fetchone()[0] == 0:
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
    admin_id = cur.lastrowid

    # Client 1 — Luminary MHP
    s1 = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("luminary", _hash_pw("client123", s1), s1, "Luminary MHP", "Contact", "billing@luminarymhp.com", "client")
    )
    c1 = cur.lastrowid

    # Client 2 — TruPath
    s2 = secrets.token_hex(16)
    cur.execute(
        "INSERT INTO clients (username,password,salt,company,contact_name,email,role) VALUES (?,?,?,?,?,?,?)",
        ("trupath", _hash_pw("client123", s2), s2, "TruPath", "Contact", "billing@trupath.com", "client")
    )
    c2 = cur.lastrowid

    conn.commit()

    # Providers for c1
    providers_c1 = [
        (c1, "Dr. Maria Torres", "1234567890", "Internal Medicine", "83-1234567"),
        (c1, "Dr. Robert Lee", "0987654321", "Cardiology", "83-1234567"),
    ]
    for p in providers_c1:
        cur.execute(
            "INSERT INTO providers (client_id,ProviderName,NPI,Specialty,TaxID,Status) VALUES (?,?,?,?,?,'Active')",
            p
        )
    pid1 = cur.lastrowid - 1
    pid2 = cur.lastrowid

    conn.commit()

    # Credentialing records
    creds = [
        (c1, pid1, "Dr. Maria Torres", "Medicare", "Initial", "Approved", "2025-09-01", "2025-10-01", "2025-11-15", "2027-11-15", "Sarah K."),
        (c1, pid1, "Dr. Maria Torres", "BCBS", "Revalidation", "In Review", "2026-01-10", "2026-02-10", "", "", "Sarah K."),
        (c1, pid2, "Dr. Robert Lee", "Medicare", "Initial", "Submitted", "2026-01-20", "2026-02-20", "", "", "Mike R."),
        (c1, pid2, "Dr. Robert Lee", "Aetna", "Initial", "Not Started", "", "", "", "", "Mike R."),
    ]
    for r in creds:
        cur.execute("""INSERT INTO credentialing
            (client_id,provider_id,ProviderName,Payor,CredType,Status,SubmittedDate,FollowUpDate,ApprovedDate,ExpirationDate,Owner)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""", r)

    # Enrollment records
    enrolls = [
        (c1, pid1, "Dr. Maria Torres", "Medicare", "Enrollment", "Active", "2025-09-01", "", "2025-11-15", "2025-12-01", "Sarah K."),
        (c1, pid1, "Dr. Maria Torres", "BCBS", "Enrollment", "Submitted", "2026-01-10", "2026-02-10", "", "", "Sarah K."),
        (c1, pid2, "Dr. Robert Lee", "Medicare", "Enrollment", "In Progress", "2026-01-20", "2026-02-20", "", "", "Mike R."),
    ]
    for r in enrolls:
        cur.execute("""INSERT INTO enrollment
            (client_id,provider_id,ProviderName,Payor,EnrollType,Status,SubmittedDate,FollowUpDate,ApprovedDate,EffectiveDate,Owner)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""", r)

    # EDI setup
    edi_rows = [
        (c1, pid1, "Dr. Maria Torres", "Medicare", "Active", "Active", "Active", "2025-09-01", "2025-12-01", "MCR001", "Sarah K."),
        (c1, pid1, "Dr. Maria Torres", "BCBS", "In Progress", "Not Started", "Not Started", "2026-01-10", "", "BCBS002", "Sarah K."),
    ]
    for r in edi_rows:
        cur.execute("""INSERT INTO edi_setup
            (client_id,provider_id,ProviderName,Payor,EDIStatus,ERAStatus,EFTStatus,SubmittedDate,GoLiveDate,PayerID,Owner)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""", r)

    conn.commit()

    # Claims for c1
    today = date.today()
    claims_data = [
        # ClaimKey, Patient, Payor, Provider, DOS, Charge, Paid, Balance, Status, BillDate, DeniedDate, PaidDate, DenialCat, DenialReason, Owner
        ("CLM-001", "PT-101", "John Smith", "Medicare", "Dr. Maria Torres", "1234567890", (today - timedelta(days=60)).isoformat(), "99213", "Office Visit Level 3", 185.00, 130.00, 48.75, 6.25, 0.00, "Paid", (today - timedelta(days=55)).isoformat(), "", (today - timedelta(days=30)).isoformat(), "", "", "Sarah K.", "Closed", 0),
        ("CLM-002", "PT-102", "Linda Brown", "BCBS", "Dr. Maria Torres", "1234567890", (today - timedelta(days=50)).isoformat(), "80053", "Comp Metabolic Panel", 95.00, 76.00, 15.20, 3.80, 0.00, "Paid", (today - timedelta(days=45)).isoformat(), "", (today - timedelta(days=15)).isoformat(), "", "", "Sarah K.", "Paid", 0),
        ("CLM-003", "PT-103", "Carlos Diaz", "Aetna", "Dr. Robert Lee", "0987654321", (today - timedelta(days=45)).isoformat(), "93000", "Electrocardiogram", 125.00, 0.00, 0.00, 0.00, 125.00, "Denied", (today - timedelta(days=40)).isoformat(), (today - timedelta(days=20)).isoformat(), "", "Missing Auth", "Prior authorization not obtained", "Mike R.", "Denied", 0),
        ("CLM-004", "PT-104", "Anna White", "UHC", "Dr. Maria Torres", "1234567890", (today - timedelta(days=35)).isoformat(), "85025", "CBC with diff", 75.00, 60.00, 0.00, 0.00, 75.00, "Billed/Submitted", (today - timedelta(days=30)).isoformat(), "", "", "", "", "Sarah K.", "Billed/Submitted", 0),
        ("CLM-005", "PT-105", "David Kim", "Medicare", "Dr. Robert Lee", "0987654321", (today - timedelta(days=30)).isoformat(), "93306", "Echo w/Doppler", 850.00, 680.00, 0.00, 0.00, 850.00, "A/R Follow-Up", (today - timedelta(days=25)).isoformat(), "", "", "", "", "Mike R.", "A/R Follow-Up", 0),
        ("CLM-006", "PT-106", "Maria Lopez", "BCBS", "Dr. Maria Torres", "1234567890", (today - timedelta(days=20)).isoformat(), "99214", "Office Visit Level 4", 225.00, 0.00, 0.00, 0.00, 225.00, "Denied", (today - timedelta(days=15)).isoformat(), (today - timedelta(days=5)).isoformat(), "", "Coding Error", "Incorrect CPT code submitted", "Sarah K.", "Appeals", 0),
        ("CLM-007", "PT-107", "Robert Jones", "Cigna", "Dr. Robert Lee", "0987654321", (today - timedelta(days=10)).isoformat(), "99213", "Office Visit Level 3", 185.00, 0.00, 0.00, 0.00, 185.00, "Verification", "", "", "", "", "", "Mike R.", "Verification", 0),
        ("CLM-008", "PT-108", "Susan Clark", "Medicare", "Dr. Maria Torres", "1234567890", (today - timedelta(days=7)).isoformat(), "81001", "Urinalysis", 45.00, 0.00, 0.00, 0.00, 45.00, "Coding", "", "", "", "", "", "Sarah K.", "Coding", 0),
        ("CLM-009", "PT-109", "Thomas Moore", "Aetna", "Dr. Robert Lee", "0987654321", (today - timedelta(days=5)).isoformat(), "80061", "Lipid Panel", 95.00, 0.00, 0.00, 0.00, 95.00, "Intake", "", "", "", "", "", "", "Intake", 0),
        ("CLM-010", "PT-110", "Patricia Hall", "UHC", "Dr. Maria Torres", "1234567890", (today - timedelta(days=3)).isoformat(), "36415", "Venipuncture", 25.00, 0.00, 0.00, 0.00, 25.00, "Rejected", "", "", "", "Eligibility", "Patient not eligible on DOS", "Sarah K.", "Rejected", 0),
        ("CLM-011", "PT-101", "John Smith", "Medicare", "Dr. Maria Torres", "1234567890", (today - timedelta(days=65)).isoformat(), "99232", "Subsequent Hospital Care", 195.00, 140.00, 54.60, 30.40, 0.00, "Paid", (today - timedelta(days=60)).isoformat(), "", (today - timedelta(days=35)).isoformat(), "", "", "Sarah K.", "Paid", 0),
        ("CLM-012", "PT-111", "Helen Turner", "BCBS", "Dr. Robert Lee", "0987654321", (today - timedelta(days=55)).isoformat(), "93000", "ECG", 125.00, 100.00, 20.00, 5.00, 0.00, "Paid", (today - timedelta(days=50)).isoformat(), "", (today - timedelta(days=25)).isoformat(), "", "", "Mike R.", "Paid", 0),
    ]
    for row in claims_data:
        (ck, pid, pname, payor, provider, npi, dos, cpt, desc,
         charge, allowed, adj, paid, bal, cs, billdate, dendate, paiddate,
         denial_cat, denial_reason, owner, status, sla) = row
        cur.execute("""
            INSERT OR IGNORE INTO claims_master
            (client_id,ClaimKey,PatientID,PatientName,Payor,ProviderName,NPI,DOS,CPTCode,Description,
             ChargeAmount,AllowedAmount,AdjustmentAmount,PaidAmount,BalanceRemaining,
             ClaimStatus,BillDate,DeniedDate,PaidDate,DenialCategory,DenialReason,Owner,
             StatusStartDate,LastTouchedDate,SLABreached)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (c1, ck, pid, pname, payor, provider, npi, dos, cpt, desc,
              charge, allowed, adj, paid, bal,
              status, billdate, dendate, paiddate, denial_cat, denial_reason, owner,
              (today - timedelta(days=3)).isoformat(), today.isoformat(), sla))

    # Notes for some claims
    notes = [
        (c1, "CLM-003", "Claim", 0, "Submitted re-authorization request to Aetna.", "Sarah K."),
        (c1, "CLM-003", "Claim", 0, "Auth denied again — escalating to physician review.", "Mike R."),
        (c1, "CLM-005", "Claim", 0, "Called UHC — claim in review, follow up in 7 days.", "Mike R."),
        (c1, "CLM-006", "Claim", 0, "Appeal letter sent with corrected CPT documentation.", "Sarah K."),
    ]
    for n in notes:
        cur.execute(
            "INSERT INTO notes_log (client_id,ClaimKey,Module,RefID,Note,Author) VALUES (?,?,?,?,?,?)", n
        )

    # Payments for paid claims
    pay_rows = [
        (c1, "CLM-001", (today - timedelta(days=30)).isoformat(), 130.00, 48.75, "Primary", "CHK-11230", "ERA-001"),
        (c1, "CLM-002", (today - timedelta(days=15)).isoformat(), 76.00, 15.20, "Primary", "CHK-11231", "ERA-002"),
        (c1, "CLM-011", (today - timedelta(days=35)).isoformat(), 140.00, 54.60, "Primary", "CHK-11220", "ERA-003"),
        (c1, "CLM-012", (today - timedelta(days=25)).isoformat(), 100.00, 20.00, "Primary", "CHK-11225", "ERA-004"),
    ]
    for p in pay_rows:
        cur.execute("""INSERT INTO payments
            (client_id,ClaimKey,PostDate,PaymentAmount,AdjustmentAmount,PayerType,CheckNumber,ERA)
            VALUES (?,?,?,?,?,?,?,?)""", p)

    conn.commit()


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
    return {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role")}, token


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
    return {k: c[k] for k in ("id", "username", "company", "contact_name", "email", "phone", "role")}


def logout_session(token: str):
    conn = get_db()
    conn.execute("DELETE FROM sessions WHERE token=?", (token,))
    conn.commit()
    conn.close()


# ─── Clients (admin) ──────────────────────────────────────────────────────────

def list_clients():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id,username,company,contact_name,email,phone,role,is_active,created_at,last_login FROM clients ORDER BY company")
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
    allowed = ["company", "contact_name", "email", "phone", "role", "is_active"]
    parts, params = [], []
    for f in allowed:
        if f in data:
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


# ─── Providers ────────────────────────────────────────────────────────────────

def list_providers(client_id: int = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM providers"
    params = []
    if client_id:
        q += " WHERE client_id=?"
        params.append(client_id)
    q += " ORDER BY ProviderName"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_provider(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO providers (client_id,ProviderName,NPI,Specialty,TaxID,Email,Phone,Status,StartDate,Notes)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (data["client_id"], data["ProviderName"], data.get("NPI", ""),
                 data.get("Specialty", ""), data.get("TaxID", ""), data.get("Email", ""),
                 data.get("Phone", ""), data.get("Status", "Active"),
                 data.get("StartDate", ""), data.get("Notes", "")))
    conn.commit()
    pid = cur.lastrowid
    conn.close()
    return pid


def update_provider(pid: int, data: dict):
    conn = get_db()
    cur = conn.cursor()
    allowed = ["ProviderName", "NPI", "Specialty", "TaxID", "Email", "Phone", "Status", "StartDate", "Notes"]
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


def get_claims(client_id: int = None, status: str = None):
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
         Owner,NextAction,NextActionDueDate,SLABreached,DenialCategory,DenialReason,AppealDate,AppealStatus)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        data.get("AppealDate", ""), data.get("AppealStatus", "")
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
               "SLABreached", "DenialCategory", "DenialReason", "AppealDate", "AppealStatus"]
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

def get_credentialing(client_id: int = None, status: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM credentialing WHERE 1=1"
    params = []
    if client_id:
        q += " AND client_id=?"; params.append(client_id)
    if status:
        q += " AND Status=?"; params.append(status)
    q += " ORDER BY updated_at DESC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_credentialing(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO credentialing
        (client_id,provider_id,ProviderName,Payor,CredType,Status,SubmittedDate,FollowUpDate,ApprovedDate,ExpirationDate,Owner,Notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (data["client_id"], data.get("provider_id"), data.get("ProviderName", ""),
         data.get("Payor", ""), data.get("CredType", "Initial"), data.get("Status", "Not Started"),
         data.get("SubmittedDate", ""), data.get("FollowUpDate", ""),
         data.get("ApprovedDate", ""), data.get("ExpirationDate", ""),
         data.get("Owner", ""), data.get("Notes", "")))
    conn.commit()
    cid = cur.lastrowid
    conn.close()
    return cid


def update_credentialing(rec_id: int, data: dict):
    conn = get_db()
    cur = conn.cursor()
    allowed = ["ProviderName", "Payor", "CredType", "Status", "SubmittedDate",
               "FollowUpDate", "ApprovedDate", "ExpirationDate", "Owner", "Notes"]
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

def get_enrollment(client_id: int = None, status: str = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM enrollment WHERE 1=1"
    params = []
    if client_id:
        q += " AND client_id=?"; params.append(client_id)
    if status:
        q += " AND Status=?"; params.append(status)
    q += " ORDER BY updated_at DESC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_enrollment(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO enrollment
        (client_id,provider_id,ProviderName,Payor,EnrollType,Status,SubmittedDate,FollowUpDate,ApprovedDate,EffectiveDate,Owner,Notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (data["client_id"], data.get("provider_id"), data.get("ProviderName", ""),
         data.get("Payor", ""), data.get("EnrollType", "Enrollment"), data.get("Status", "Not Started"),
         data.get("SubmittedDate", ""), data.get("FollowUpDate", ""),
         data.get("ApprovedDate", ""), data.get("EffectiveDate", ""),
         data.get("Owner", ""), data.get("Notes", "")))
    conn.commit()
    eid = cur.lastrowid
    conn.close()
    return eid


def update_enrollment(rec_id: int, data: dict):
    conn = get_db()
    cur = conn.cursor()
    allowed = ["ProviderName", "Payor", "EnrollType", "Status", "SubmittedDate",
               "FollowUpDate", "ApprovedDate", "EffectiveDate", "Owner", "Notes"]
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

def get_edi(client_id: int = None):
    conn = get_db()
    cur = conn.cursor()
    q = "SELECT * FROM edi_setup"
    params = []
    if client_id:
        q += " WHERE client_id=?"; params.append(client_id)
    q += " ORDER BY updated_at DESC"
    cur.execute(q, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def create_edi(data: dict) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""INSERT INTO edi_setup
        (client_id,provider_id,ProviderName,Payor,EDIStatus,ERAStatus,EFTStatus,SubmittedDate,GoLiveDate,PayerID,Owner,Notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (data["client_id"], data.get("provider_id"), data.get("ProviderName", ""),
         data.get("Payor", ""), data.get("EDIStatus", "Not Started"),
         data.get("ERAStatus", "Not Started"), data.get("EFTStatus", "Not Started"),
         data.get("SubmittedDate", ""), data.get("GoLiveDate", ""),
         data.get("PayerID", ""), data.get("Owner", ""), data.get("Notes", "")))
    conn.commit()
    eid = cur.lastrowid
    conn.close()
    return eid


def update_edi(rec_id: int, data: dict):
    conn = get_db()
    cur = conn.cursor()
    allowed = ["ProviderName", "Payor", "EDIStatus", "ERAStatus", "EFTStatus",
               "SubmittedDate", "GoLiveDate", "PayerID", "Owner", "Notes"]
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

def get_dashboard(client_id: int = None):
    """Full KPI dashboard — pass client_id=None for admin (all clients)."""
    conn = get_db()
    cur = conn.cursor()
    cond = "WHERE client_id=?" if client_id else ""
    p = [client_id] if client_id else []
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
    denied_all = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus IN ('Denied','Appeals')",
                    p + ["Denied", "Appeals"] if cond else ["Denied", "Appeals"])

    # Payments MTD
    pay_mtd = q1(f"SELECT COALESCE(SUM(PaymentAmount),0) FROM payments {cond} {'AND' if cond else 'WHERE'} PostDate >= ?",
                 p + [mtd_start])
    pay_ytd = q1(f"SELECT COALESCE(SUM(PaymentAmount),0) FROM payments {cond} {'AND' if cond else 'WHERE'} PostDate >= ?",
                 p + [ytd_start])

    # Totals for rates
    total_submitted = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} BillDate != ''", p)
    total_denied = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} DeniedDate != ''", p)
    total_paid_count = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus='Paid'",
                          p + ["Paid"] if cond else ["Paid"])
    # Clean claim = paid without denial
    no_denial_p = p + ["Paid", ""]
    clean_claims = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} ClaimStatus='Paid' AND DenialReason=''",
                      no_denial_p)

    clean_rate = round(clean_claims / max(total_submitted, 1) * 100, 1)
    denial_rate = round(total_denied / max(total_submitted, 1) * 100, 1)

    # Avg days to payment
    cur.execute(f"""SELECT AVG(CAST(julianday(PaidDate) - julianday(DOS) AS REAL))
                    FROM claims_master {cond} {'AND' if cond else 'WHERE'} PaidDate != '' AND DOS != ''""", p)
    row = cur.fetchone()
    avg_days_to_pay = round(row[0] or 0, 1)

    # SLA breaches
    sla_breaches = q1(f"SELECT COUNT(*) FROM claims_master {cond} {'AND' if cond else 'WHERE'} SLABreached=1",
                      p + [1] if cond else [1])

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