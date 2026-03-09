"""Database models and persistence for saved leads."""

import json
import sqlite3
import os
from datetime import datetime
from app.config import DATABASE_PATH
from app.email_finder import _is_quality_email


def get_db():
    """Get database connection."""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database schema."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS saved_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT UNIQUE,
            organization_name TEXT,
            first_name TEXT,
            last_name TEXT,
            credential TEXT,
            taxonomy_code TEXT,
            taxonomy_desc TEXT,
            address_line1 TEXT,
            address_line2 TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            phone TEXT,
            fax TEXT,
            enumeration_date TEXT,
            last_updated TEXT,
            lead_score INTEGER DEFAULT 0,
            lead_status TEXT DEFAULT 'new',
            notes TEXT DEFAULT '',
            tags TEXT DEFAULT '',
            source TEXT DEFAULT 'scraped',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_type TEXT,
            search_params TEXT,
            results_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS lead_activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            activity_type TEXT,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (lead_id) REFERENCES saved_leads(id)
        );

        CREATE INDEX IF NOT EXISTS idx_leads_state ON saved_leads(state);
        CREATE INDEX IF NOT EXISTS idx_leads_status ON saved_leads(lead_status);
        CREATE INDEX IF NOT EXISTS idx_leads_score ON saved_leads(lead_score);

        CREATE TABLE IF NOT EXISTS lead_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT NOT NULL,
            email TEXT NOT NULL,
            first_name TEXT DEFAULT '',
            last_name TEXT DEFAULT '',
            position TEXT DEFAULT '',
            is_decision_maker INTEGER DEFAULT 0,
            confidence INTEGER DEFAULT 0,
            email_type TEXT DEFAULT 'pattern',
            source TEXT DEFAULT 'generated',
            domain TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(npi, email)
        );

        CREATE INDEX IF NOT EXISTS idx_emails_npi ON lead_emails(npi);

        CREATE TABLE IF NOT EXISTS lead_enrichment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT UNIQUE NOT NULL,
            organization_name TEXT DEFAULT '',
            enriched_at TEXT DEFAULT CURRENT_TIMESTAMP,
            -- Service-need scores (0-100)
            overall_score INTEGER DEFAULT 0,
            billing_score INTEGER DEFAULT 0,
            payor_score INTEGER DEFAULT 0,
            workflow_score INTEGER DEFAULT 0,
            -- Priority level
            priority TEXT DEFAULT 'low',
            services_needed TEXT DEFAULT '',
            recommendation TEXT DEFAULT '',
            -- Enrichment detail blobs (JSON)
            billing_reasons TEXT DEFAULT '[]',
            payor_reasons TEXT DEFAULT '[]',
            workflow_reasons TEXT DEFAULT '[]',
            -- Key intelligence fields
            clia_data TEXT DEFAULT '{}',
            medicare_data TEXT DEFAULT '{}',
            authorized_official TEXT DEFAULT '{}',
            location_count INTEGER DEFAULT 0,
            multi_state INTEGER DEFAULT 0,
            states_present TEXT DEFAULT '[]',
            taxonomy_count INTEGER DEFAULT 0,
            -- Timestamps
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_enrichment_npi ON lead_enrichment(npi);
        CREATE INDEX IF NOT EXISTS idx_enrichment_overall ON lead_enrichment(overall_score);
        CREATE INDEX IF NOT EXISTS idx_enrichment_billing ON lead_enrichment(billing_score);
        CREATE INDEX IF NOT EXISTS idx_enrichment_payor ON lead_enrichment(payor_score);
        CREATE INDEX IF NOT EXISTS idx_enrichment_workflow ON lead_enrichment(workflow_score);
    """)

    # Backward-compatible schema upgrades
    cursor.execute("PRAGMA table_info(lead_enrichment)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    if "urgency_score" not in existing_cols:
        cursor.execute("ALTER TABLE lead_enrichment ADD COLUMN urgency_score INTEGER DEFAULT 0")
    if "urgency_level" not in existing_cols:
        cursor.execute("ALTER TABLE lead_enrichment ADD COLUMN urgency_level TEXT DEFAULT 'low'")
    if "urgency_reason" not in existing_cols:
        cursor.execute("ALTER TABLE lead_enrichment ADD COLUMN urgency_reason TEXT DEFAULT ''")
    if "urgency_updated_at" not in existing_cols:
        cursor.execute("ALTER TABLE lead_enrichment ADD COLUMN urgency_updated_at TEXT DEFAULT ''")

    conn.commit()
    conn.close()


def seed_demo_leads():
    """Seed some demo leads for testing when database is empty."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if we already have leads
    cursor.execute("SELECT COUNT(*) FROM saved_leads")
    count = cursor.fetchone()[0]
    if count > 0:
        conn.close()
        return  # Already have data
    
    demo_leads = [
        {
            "npi": "1234567890",
            "organization_name": "Advanced Lab Services",
            "first_name": "Dr. Sarah",
            "last_name": "Johnson",
            "credential": "MD",
            "taxonomy_code": "207ZP0102X",
            "taxonomy_desc": "Anatomic Pathology & Clinical Pathology",
            "address_line1": "123 Medical Center Dr",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77030",
            "phone": "(713) 555-0123",
            "fax": "(713) 555-0124",
            "enumeration_date": "2020-01-15",
            "last_updated": "2024-01-15",
            "lead_score": 85,
            "lead_status": "qualified",
            "notes": "High-volume lab with billing issues",
            "tags": "laboratory,billing",
            "source": "demo"
        },
        {
            "npi": "1234567891",
            "organization_name": "City Urgent Care Center",
            "first_name": "Dr. Michael",
            "last_name": "Chen",
            "credential": "DO",
            "taxonomy_code": "207Q00000X",
            "taxonomy_desc": "Family Medicine",
            "address_line1": "456 Health Blvd",
            "city": "Austin",
            "state": "TX",
            "zip_code": "78701",
            "phone": "(512) 555-0456",
            "fax": "(512) 555-0457",
            "enumeration_date": "2019-06-20",
            "last_updated": "2024-01-15",
            "lead_score": 78,
            "lead_status": "contacted",
            "notes": "Urgent care with payor contracting needs",
            "tags": "urgent_care,payor",
            "source": "demo"
        },
        {
            "npi": "1234567892",
            "organization_name": "Primary Care Associates",
            "first_name": "Dr. Emily",
            "last_name": "Rodriguez",
            "credential": "MD",
            "taxonomy_code": "207Q00000X",
            "taxonomy_desc": "Family Medicine",
            "address_line1": "789 Wellness Way",
            "city": "Dallas",
            "state": "TX",
            "zip_code": "75201",
            "phone": "(214) 555-0789",
            "fax": "(214) 555-0790",
            "enumeration_date": "2018-03-10",
            "last_updated": "2024-01-15",
            "lead_score": 92,
            "lead_status": "qualified",
            "notes": "Primary care practice needing workflow support",
            "tags": "primary_care,workflow",
            "source": "demo"
        }
    ]
    
    for lead in demo_leads:
        cursor.execute("""
            INSERT OR IGNORE INTO saved_leads (
                npi, organization_name, first_name, last_name, credential,
                taxonomy_code, taxonomy_desc, address_line1, city, state, zip_code,
                phone, fax, enumeration_date, last_updated, lead_score, lead_status,
                notes, tags, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["npi"], lead["organization_name"], lead["first_name"], lead["last_name"], lead["credential"],
            lead["taxonomy_code"], lead["taxonomy_desc"], lead["address_line1"], lead["city"], lead["state"], lead["zip_code"],
            lead["phone"], lead["fax"], lead["enumeration_date"], lead["last_updated"], lead["lead_score"], lead["lead_status"],
            lead["notes"], lead["tags"], lead["source"]
        ))
        
        # Add demo emails
        cursor.execute("""
            INSERT OR IGNORE INTO lead_emails (npi, email, first_name, last_name, position, confidence, email_type, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["npi"],
            f"contact@{lead['organization_name'].lower().replace(' ', '')}.com",
            lead["first_name"],
            lead["last_name"],
            "Practice Manager",
            90,
            "verified",
            "demo"
        ))
        
        # Add enrichment data
        cursor.execute("""
            INSERT OR IGNORE INTO lead_enrichment (
                npi, organization_name, overall_score, billing_score, payor_score, workflow_score,
                priority, services_needed, authorized_official
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["npi"],
            lead["organization_name"],
            80,
            85 if "billing" in lead["tags"] else 70,
            90 if "payor" in lead["tags"] else 75,
            85 if "workflow" in lead["tags"] else 70,
            "high" if lead["lead_score"] > 80 else "medium",
            json.dumps(["Billing Services" if "billing" in lead["tags"] else "Payor Contracting"]),
            json.dumps({
                "first_name": lead["first_name"],
                "last_name": lead["last_name"],
                "title": "Medical Director",
                "phone": lead["phone"]
            })
        ))
    
    conn.commit()
    conn.close()


def save_lead(lead_data: dict) -> int:
    """Save a lead to the database. Returns the lead ID."""
    conn = get_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT OR REPLACE INTO saved_leads (
                npi, organization_name, first_name, last_name, credential,
                taxonomy_code, taxonomy_desc, address_line1, address_line2,
                city, state, zip_code, phone, fax, enumeration_date,
                last_updated, lead_score, lead_status, notes, tags, source, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead_data.get("npi"),
            lead_data.get("organization_name"),
            lead_data.get("first_name"),
            lead_data.get("last_name"),
            lead_data.get("credential"),
            lead_data.get("taxonomy_code"),
            lead_data.get("taxonomy_desc"),
            lead_data.get("address_line1"),
            lead_data.get("address_line2"),
            lead_data.get("city"),
            lead_data.get("state"),
            lead_data.get("zip_code"),
            lead_data.get("phone"),
            lead_data.get("fax"),
            lead_data.get("enumeration_date"),
            lead_data.get("last_updated"),
            lead_data.get("lead_score", 0),
            lead_data.get("lead_status", "new"),
            lead_data.get("notes", ""),
            lead_data.get("tags", ""),
            lead_data.get("source", "scraped"),
            datetime.now().isoformat()
        ))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_saved_leads(status=None, state=None, min_score=None):
    """Get saved leads with optional filters."""
    conn = get_db()
    cursor = conn.cursor()

    query = """
        SELECT sl.*,
               COALESCE(le.urgency_score, 0) as urgency_score,
               COALESCE(le.urgency_level, 'low') as urgency_level,
               COALESCE(le.urgency_reason, '') as urgency_reason,
               COALESCE(le.urgency_updated_at, '') as urgency_updated_at,
               COALESCE(le.services_needed, '[]') as services_wanted,
               GROUP_CONCAT(em.email, '; ') as emails,
               GROUP_CONCAT(em.position, '; ') as email_positions
        FROM saved_leads sl
        LEFT JOIN lead_enrichment le ON sl.npi = le.npi
        LEFT JOIN lead_emails em ON sl.npi = em.npi
        WHERE 1=1
    """
    params = []

    if status:
        query += " AND lead_status = ?"
        params.append(status)
    if state:
        query += " AND state = ?"
        params.append(state)
    if min_score is not None:
        query += " AND lead_score >= ?"
        params.append(min_score)

    query += " GROUP BY sl.id ORDER BY urgency_score DESC, lead_score DESC, sl.created_at DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    result = []
    for row in rows:
        item = dict(row)
        for json_field in ("services_wanted",):
            try:
                item[json_field] = json.loads(item.get(json_field, "[]"))
            except (json.JSONDecodeError, TypeError):
                item[json_field] = []
        result.append(item)
    return result


def update_lead(lead_id: int, updates: dict):
    """Update a saved lead."""
    conn = get_db()
    cursor = conn.cursor()

    allowed_fields = ["lead_status", "lead_score", "notes", "tags"]
    set_parts = []
    params = []

    for field in allowed_fields:
        if field in updates:
            set_parts.append(f"{field} = ?")
            params.append(updates[field])

    if set_parts:
        set_parts.append("updated_at = ?")
        params.append(datetime.now().isoformat())
        params.append(lead_id)

        query = f"UPDATE saved_leads SET {', '.join(set_parts)} WHERE id = ?"
        cursor.execute(query, params)
        conn.commit()

    conn.close()


def delete_lead(lead_id: int):
    """Delete a saved lead."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM saved_leads WHERE id = ?", (lead_id,))
    conn.commit()
    conn.close()


def get_lead_stats():
    """Get dashboard statistics."""
    conn = get_db()
    cursor = conn.cursor()

    stats = {}
    cursor.execute("SELECT COUNT(*) FROM saved_leads")
    stats["total_leads"] = cursor.fetchone()[0]

    cursor.execute("SELECT lead_status, COUNT(*) FROM saved_leads GROUP BY lead_status")
    stats["by_status"] = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT state, COUNT(*) FROM saved_leads GROUP BY state ORDER BY COUNT(*) DESC LIMIT 10")
    stats["top_states"] = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT AVG(lead_score) FROM saved_leads")
    avg = cursor.fetchone()[0]
    stats["avg_score"] = round(avg, 1) if avg else 0

    conn.close()
    return stats


def log_search(search_type: str, params: str, count: int):
    """Log a search to history."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO search_history (search_type, search_params, results_count) VALUES (?, ?, ?)",
        (search_type, params, count)
    )
    conn.commit()
    conn.close()


def save_lead_emails(npi: str, emails: list) -> int:
    """Save discovered emails for a lead. Returns count saved."""
    conn = get_db()
    cursor = conn.cursor()
    saved = 0
    for e in emails:
        email = e.get("email", "")
        source = str(e.get("source", "") or "").strip().lower()
        confidence = int(e.get("confidence", 0) or 0)
        verified = bool(e.get("verified", False))

        # Apply final quality check before saving
        if not _is_quality_email(email):
            print(f"WARNING: Blocked bad email from saving: {email}")
            continue

        # Block synthetic pattern emails unless explicitly verified.
        if ("pattern" in source or source in {"generated", "fallback"}) and not verified:
            print(f"WARNING: Blocked unverified pattern email: {email} ({source})")
            continue

        # Require stronger confidence for non-verified emails.
        if confidence < 70 and not verified:
            print(f"WARNING: Blocked low-confidence email: {email} ({confidence})")
            continue
            
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO lead_emails (
                    npi, email, first_name, last_name, position,
                    is_decision_maker, confidence, email_type, source, domain
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                npi,
                email,
                e.get("first_name", ""),
                e.get("last_name", ""),
                e.get("position", ""),
                1 if e.get("is_decision_maker") else 0,
                confidence,
                e.get("type", "pattern"),
                source or e.get("source", "generated"),
                e.get("domain", ""),
            ))
            saved += cursor.rowcount
        except Exception:
            pass
    conn.commit()
    conn.close()
    return saved


def get_lead_emails(npi: str) -> list:
    """Get all saved emails for a lead NPI."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM lead_emails WHERE npi = ? ORDER BY is_decision_maker DESC, confidence DESC",
        (npi,)
    )
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_leads_with_emails() -> list:
    """Get saved leads joined with their best email for CSV export."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
         SELECT sl.*,
             COALESCE(en.urgency_score, 0) AS urgency_score,
             COALESCE(en.urgency_level, 'low') AS urgency_level,
             COALESCE(en.services_needed, '[]') AS services_wanted,
               GROUP_CONCAT(le.email, '; ') AS emails,
               GROUP_CONCAT(le.position, '; ') AS email_positions
        FROM saved_leads sl
         LEFT JOIN lead_enrichment en ON sl.npi = en.npi
        LEFT JOIN lead_emails le ON sl.npi = le.npi
        GROUP BY sl.id
         ORDER BY urgency_score DESC, sl.lead_score DESC, sl.created_at DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    out = []
    for r in rows:
        item = dict(r)
        try:
            item["services_wanted"] = json.loads(item.get("services_wanted", "[]"))
        except (json.JSONDecodeError, TypeError):
            item["services_wanted"] = []
        out.append(item)
    return out


# ─── Lead Enrichment Persistence ────────────────────────────────────

def save_enrichment(npi: str, enrichment_data: dict) -> int:
    """Save or update enrichment data for a lead."""
    conn = get_db()
    cursor = conn.cursor()

    sn = enrichment_data.get("service_needs", {})
    billing = sn.get("billing", {})
    payor = sn.get("payor_contracting", {})
    workflow = sn.get("workflow", {})

    try:
        cursor.execute("""
            INSERT OR REPLACE INTO lead_enrichment (
                npi, organization_name, enriched_at,
                overall_score, billing_score, payor_score, workflow_score,
                priority, services_needed, recommendation,
                billing_reasons, payor_reasons, workflow_reasons,
                clia_data, medicare_data, authorized_official,
                location_count, multi_state, states_present, taxonomy_count,
                urgency_score, urgency_level, urgency_reason, urgency_updated_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            npi,
            enrichment_data.get("organization_name", ""),
            enrichment_data.get("enriched_at", datetime.now().isoformat()),
            sn.get("overall_score", 0),
            billing.get("score", 0),
            payor.get("score", 0),
            workflow.get("score", 0),
            sn.get("priority", "low"),
            json.dumps(sn.get("services_needed", [])),
            sn.get("recommendation", ""),
            json.dumps(billing.get("reasons", [])),
            json.dumps(payor.get("reasons", [])),
            json.dumps(workflow.get("reasons", [])),
            json.dumps(enrichment_data.get("data_sources", {}).get("clia", {})),
            json.dumps(enrichment_data.get("data_sources", {}).get("medicare", {})),
            json.dumps(enrichment_data.get("authorized_official", {})),
            enrichment_data.get("location_count", 0),
            1 if enrichment_data.get("multi_state") else 0,
            json.dumps(enrichment_data.get("states_present", [])),
            len(enrichment_data.get("data_sources", {}).get("npi", {}).get("taxonomies", [])),
            0,
            "low",
            "",
            "",
            datetime.now().isoformat(),
        ))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def update_enrichment_urgency(npi: str, urgency_score: int, urgency_level: str, urgency_reason: str):
    """Update urgency metadata for an enriched lead."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE lead_enrichment
        SET urgency_score = ?,
            urgency_level = ?,
            urgency_reason = ?,
            urgency_updated_at = ?,
            updated_at = ?
        WHERE npi = ?
        """,
        (
            int(max(0, min(100, urgency_score or 0))),
            (urgency_level or "low").lower(),
            urgency_reason or "",
            datetime.now().isoformat(),
            datetime.now().isoformat(),
            npi,
        ),
    )
    conn.commit()
    conn.close()


def get_enrichment(npi: str) -> dict:
    """Get cached enrichment data for a lead."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM lead_enrichment WHERE npi = ?", (npi,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return {}
    result = dict(row)
    # Parse JSON fields
    for field in ("services_needed", "billing_reasons", "payor_reasons",
                  "workflow_reasons", "clia_data", "medicare_data",
                  "authorized_official", "states_present"):
        try:
            result[field] = json.loads(result.get(field, "{}"))
        except (json.JSONDecodeError, TypeError):
            pass
    return result


def get_all_enrichments(min_overall: int = 0, service_filter: str = None) -> list:
    """Get all enriched leads, optionally filtered by score or service need."""
    conn = get_db()
    cursor = conn.cursor()

    query = "SELECT * FROM lead_enrichment WHERE overall_score >= ?"
    params = [min_overall]

    if service_filter:
        query += " AND services_needed LIKE ?"
        params.append(f"%{service_filter}%")

    query += " ORDER BY overall_score DESC, updated_at DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        r = dict(row)
        for field in ("services_needed", "billing_reasons", "payor_reasons",
                      "workflow_reasons", "clia_data", "medicare_data",
                      "authorized_official", "states_present"):
            try:
                r[field] = json.loads(r.get(field, "{}"))
            except (json.JSONDecodeError, TypeError):
                pass
        results.append(r)
    return results


def get_enrichment_stats() -> dict:
    """Get enrichment dashboard statistics."""
    conn = get_db()
    cursor = conn.cursor()

    stats = {}

    cursor.execute("SELECT COUNT(*) FROM lead_enrichment")
    stats["total_enriched"] = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(overall_score), AVG(billing_score), AVG(payor_score), AVG(workflow_score) FROM lead_enrichment")
    row = cursor.fetchone()
    stats["avg_overall"] = round(row[0] or 0, 1)
    stats["avg_billing"] = round(row[1] or 0, 1)
    stats["avg_payor"] = round(row[2] or 0, 1)
    stats["avg_workflow"] = round(row[3] or 0, 1)

    cursor.execute("SELECT priority, COUNT(*) FROM lead_enrichment GROUP BY priority")
    stats["by_priority"] = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT COUNT(*) FROM lead_enrichment WHERE billing_score >= 40")
    stats["need_billing"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM lead_enrichment WHERE payor_score >= 40")
    stats["need_payor"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM lead_enrichment WHERE workflow_score >= 40")
    stats["need_workflow"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM lead_enrichment WHERE overall_score >= 70")
    stats["high_priority"] = cursor.fetchone()[0]

    conn.close()
    return stats
