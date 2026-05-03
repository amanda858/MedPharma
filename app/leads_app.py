"""Lab Lead Generation app — runs on LAB_PORT (default 8000)."""

import csv
import io
import json
import os
import asyncio
from urllib.parse import urlparse
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, Response
from pydantic import BaseModel

from app.config import US_STATES, LAB_TAXONOMY_CODES, OPENAI_API_KEY, HUNTER_API_KEY
from app.database import (
    init_db, save_lead, get_saved_leads, update_lead,
    delete_lead, get_lead_stats, log_search,
    save_lead_emails, get_lead_emails, get_all_leads_with_emails,
    save_enrichment, get_enrichment, get_all_enrichments, get_enrichment_stats,
    get_db,
)
try:
    from app.database import update_enrichment_urgency
except ImportError:
    def update_enrichment_urgency(npi: str, urgency_score: int, urgency_level: str, urgency_reason: str):
        return None

from app.email_finder import find_emails_for_lab, _is_quality_email
from app.enrichment import enrich_lead, enrich_leads_bulk
from app.build_info import BUILD_MARKER
from rule_intercept import intercept_request
from app.scrubber import (
    parse_uploaded as _scrub_parse_uploaded,
    scrub_rows as _scrub_rows,
    to_csv_bytes as _scrub_to_csv,
    to_xlsx_bytes as _scrub_to_xlsx,
)
import uuid as _uuid

# In-memory store of recent scrub jobs (per-process). Keys are job_ids.
_SCRUB_JOBS: dict[str, dict] = {}
_SCRUB_JOBS_MAX = 20

app = FastAPI(
    title="MedPharma Healthcare Leads",
    description="Lead Generator — search NPI Registry for healthcare prospects",
    version="2.0.1",
)
@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    init_db()
    # Seed the national-pull table from the bundled CSV so the search UI
    # always has data to show, even before the first scheduled pull.
    try:
        from app.national_pull import ensure_seed_loaded
        res = ensure_seed_loaded()
        if res.get("loaded"):
            print(f"[startup] seed national-pull loaded: {res.get('row_count')} rows")
    except Exception as e:
        print(f"[startup] seed load skipped: {e}")

    # Auto-import the bundled rule-intercept lab routing CSV so the saved_leads
    # table is repopulated after every restart (Render free tier wipes /data).
    # Runs in a background thread w/ one-shot bulk insert so startup is fast and
    # Render never kills the boot due to a long sync loop.
    def _bulk_import_rule_intercept():
        import csv as _csv
        import sqlite3 as _sql
        from datetime import datetime as _dt
        from app.database import get_db as _get_db
        try:
            conn = _get_db()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM saved_leads WHERE source = 'rule-intercept'")
            existing = cur.fetchone()[0]
            if existing >= 1000:
                conn.close()
                print(f"[startup-bg] rule-intercept already populated ({existing} rows); skipping")
                return

            csv_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "output", "labs_routed_full.csv",
            )
            if not os.path.exists(csv_path):
                conn.close()
                print(f"[startup-bg] bundled CSV missing at {csv_path}; skipping")
                return

            now = _dt.now().isoformat()
            rows = []
            with open(csv_path, encoding="utf-8") as f:
                for r in _csv.DictReader(f):
                    npi = (r.get("npi") or "").strip()
                    tier = (r.get("tier") or "").strip()
                    if not npi or tier not in ("A", "B", "C"):
                        continue
                    try:
                        score = int(r.get("rule_score") or 0)
                    except ValueError:
                        score = 0
                    rows.append((
                        npi, r.get("org_name", ""), "", "", "", "",
                        r.get("lab_type", ""), "", "",
                        r.get("city", ""), r.get("state", ""), r.get("zip", ""),
                        "", "", "", now,
                        score, "new",
                        f"Tier {tier} | RuleScore {score} | Lab Type: {r.get('lab_type','')} | Signals: {r.get('signals','')}",
                        f"tier-{tier};lab;rule-intercept",
                        "rule-intercept", now,
                    ))

            # one transaction, one connection, executemany — ~100x faster
            try:
                cur.executemany(
                    """
                    INSERT OR REPLACE INTO saved_leads (
                        npi, organization_name, first_name, last_name, credential,
                        taxonomy_code, taxonomy_desc, address_line1, address_line2,
                        city, state, zip_code, phone, fax, enumeration_date,
                        last_updated, lead_score, lead_status, notes, tags, source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                conn.commit()
                print(f"[startup-bg] auto-imported rule-intercept leads: {len(rows)}")
            except _sql.OperationalError as oe:
                conn.rollback()
                print(f"[startup-bg] bulk insert failed: {oe}")
            finally:
                conn.close()
        except Exception as e:
            print(f"[startup-bg] rule-intercept auto-import skipped: {e}")

    try:
        import threading as _threading
        _threading.Thread(
            target=_bulk_import_rule_intercept,
            name="rule-intercept-import",
            daemon=True,
        ).start()
    except Exception as e:
        print(f"[startup] could not spawn rule-intercept import thread: {e}")


@app.get("/healthz")
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "medpharma-leads", "version": "2.0.1"}


@app.get("/buildz")
async def leads_buildz():
    return {
        "ok": True,
        "service": "leads",
        "build_marker": BUILD_MARKER,
    }


_scheduler_started = False

_scheduler_started = False
_leads_scheduler = None
POLL_EVERY_HOURS = max(1, int(os.getenv("LEADS_POLL_HOURS", "4") or 4))
NATIONWIDE_SEGMENTS = ["laboratory", "urgent_care", "primary_care", "asc", "hospital", "clinic", "diagnostic"]
NPI_FALLBACK_STATES = ["TX", "CA", "FL", "NY", "PA", "OH", "GA", "NC", "MI", "IL"]
NPI_TAXONOMY_HINT = {
    "laboratory": "laboratory",
    "urgent_care": "urgent care",
    "primary_care": "family medicine",
    "asc": "ambulatory surgery",
}
STRICT_MIN_SIGNAL_SCORE = 62
STRICT_MIN_SERVICE_SCORE = 45
STRICT_MIN_DOMAIN_SCORE = 40
STRICT_MIN_SERVICES_COUNT = 1
STRICT_POOL_TAG = "strict_quality_pool"
REVIEW_MIN_SIGNAL_SCORE = 40
REVIEW_MIN_SERVICE_SCORE = 12
REVIEW_MIN_DOMAIN_SCORE = 15
REVIEW_POOL_TAG = "review_quality_pool"
ALLOW_REVIEW_POOL = str(os.getenv("ALLOW_REVIEW_POOL", "1")).strip().lower() in {"1", "true", "yes", "on"}
_bootstrap_poll_attempted = False
_poll_status = {
    "running": False,
    "started_at": "",
    "finished_at": "",
    "last_result": None,
    "last_error": "",
}
NEED_INTENT_TERMS = [
    "need", "needs", "seeking", "looking for", "help", "support",
    "outsource", "backlog", "denial", "reimbursement", "credentialing",
    "contracting", "compliance", "prior auth", "rcm", "revenue cycle",
]
NEED_SERVICE_TERMS = [
    "billing", "claims", "credentialing", "payer", "payor", "contracting",
    "workflow", "compliance", "coding", "audit", "prior authorization",
]
SEGMENT_INTENT_TERMS = {
    "laboratory": ["turnaround", "specimen backlog", "lis", "referral volume"],
    "urgent_care": ["patient volume", "front desk overload", "claims lag", "same-day billing"],
    "primary_care": ["provider enrollment", "chronic care billing", "coding backlog"],
    "asc": ["case mix", "authorization delay", "surgical billing"],
    "hospital": ["denials spike", "discharge backlog", "revenue integrity"],
    "clinic": ["payer mix", "intake overload", "credentialing delay"],
    "diagnostic": ["imaging claims", "radiology coding", "prior auth delay"],
}
SEGMENT_SERVICE_TERMS = {
    "laboratory": ["lab billing", "specimen processing", "clia compliance"],
    "urgent_care": ["urgent care billing", "payer enrollment", "point-of-care workflow"],
    "primary_care": ["fee schedule", "medicare billing", "preventive coding"],
    "asc": ["ambulatory surgery billing", "facility claims", "payor contracting"],
    "hospital": ["hospital billing", "drg", "denial management", "utilization review"],
    "clinic": ["medical billing", "credentialing", "claims cleanup"],
    "diagnostic": ["diagnostic billing", "radiology workflow", "authorization management"],
}
EMAIL_LOOKUP_PER_SEGMENT = max(0, int(os.getenv("EMAIL_LOOKUP_PER_SEGMENT", "20") or 20))
AUTO_BOOTSTRAP_POLL = str(os.getenv("AUTO_BOOTSTRAP_POLL", "0")).strip().lower() in {"1", "true", "yes", "on"}
POLL_MAX_SECONDS = max(120, int(os.getenv("POLL_MAX_SECONDS", "900") or 900))
POLL_STALE_SECONDS = max(180, int(os.getenv("POLL_STALE_SECONDS", "1200") or 1200))


def _poll_started_epoch() -> float | None:
    started_at = str(_poll_status.get("started_at") or "").strip()
    if not started_at:
        return None
    try:
        return datetime.fromisoformat(started_at).timestamp()
    except Exception:
        return None


def _recover_stale_poll_status() -> bool:
    if not _poll_status.get("running"):
        return False
    started_epoch = _poll_started_epoch()
    if not started_epoch:
        return False
    if (datetime.now().timestamp() - started_epoch) < POLL_STALE_SECONDS:
        return False

    _poll_status["running"] = False
    _poll_status["finished_at"] = datetime.now().isoformat()
    _poll_status["last_error"] = "Previous poll marked stale and auto-recovered"
    return True


def _segment_terms(segment: str, base_terms: list[str], segment_map: dict[str, list[str]]) -> list[str]:
    seg = str(segment or "all").strip().lower()
    scoped = segment_map.get(seg, []) if seg and seg != "all" else []
    # Keep insertion order while deduplicating.
    merged: list[str] = []
    for term in [*base_terms, *scoped]:
        t = str(term).strip().lower()
        if t and t not in merged:
            merged.append(t)
    return merged


def _quality_tier(row: dict, enrichment: dict) -> str | None:
    score = int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0)
    if score < REVIEW_MIN_SIGNAL_SCORE:
        return None

    org_name = (row.get("org_name") or "").strip()
    state = (row.get("state") or "").strip()
    city = (row.get("city") or "").strip()
    if not org_name:
        return None

    npi = str((enrichment or {}).get("npi") or row.get("npi") or "").strip()
    has_valid_npi = npi.isdigit() and len(npi) == 10
    has_location_signal = bool(state or city or has_valid_npi)
    if not has_location_signal:
        return None

    auth = enrichment.get("authorized_official", {}) if isinstance(enrichment.get("authorized_official", {}), dict) else {}
    has_named_official = bool((auth.get("first_name") or "").strip() or (auth.get("last_name") or "").strip())
    has_identity_signal = has_valid_npi or has_named_official

    phone = (row.get("phone") or "").strip()
    has_phone = bool(phone and phone not in {"—", "N/A", "na"})
    if not has_phone and not has_identity_signal and score < STRICT_MIN_SIGNAL_SCORE:
        return None

    service_needs = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
    services_needed = service_needs.get("services_needed", []) if isinstance(service_needs.get("services_needed", []), list) else []
    # Keep strict quality tight, but allow strong identity + signal rows into review.
    if not services_needed:
        # Sparse-enrichment lane: strong signal + contactability can still be a
        # review-quality lead; strict still requires richer identity/service data.
        if score >= 68 and (has_identity_signal or has_phone):
            return "review"
        return None

    overall = int(service_needs.get("overall_score", 0) or 0)
    billing = int(service_needs.get("billing_score", 0) or 0)
    payor = int(service_needs.get("payor_score", 0) or 0)
    workflow = int(service_needs.get("workflow_score", 0) or 0)

    if overall < STRICT_MIN_SERVICE_SCORE:
        if overall < REVIEW_MIN_SERVICE_SCORE:
            return None
        if max(billing, payor, workflow) < REVIEW_MIN_DOMAIN_SCORE:
            return None
        return "review"
    if max(billing, payor, workflow) < STRICT_MIN_DOMAIN_SCORE:
        if max(billing, payor, workflow) < REVIEW_MIN_DOMAIN_SCORE:
            return None
        return "review"

    if len(services_needed) < STRICT_MIN_SERVICES_COUNT:
        return "review"

    strong_identity = has_identity_signal or has_phone
    if score >= STRICT_MIN_SIGNAL_SCORE and (strong_identity or overall >= 60):
        return "strict"
    return "review"


def _clear_quality_pools(exclude_npis: list[str] | None = None) -> int:
    conn = get_db()
    cursor = conn.cursor()
    params: list[str] = [f"%{STRICT_POOL_TAG}%", f"%{REVIEW_POOL_TAG}%"]
    query = "DELETE FROM saved_leads WHERE lead_status = 'new' AND (tags LIKE ? OR tags LIKE ?)"

    keep = [str(npi).strip() for npi in (exclude_npis or []) if str(npi).strip()]
    if keep:
        placeholders = ",".join(["?"] * len(keep))
        query += f" AND npi NOT IN ({placeholders})"
        params.extend(keep)

    cursor.execute(query, params)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return int(deleted or 0)


def _quality_tier_from_tags(tags_value: str) -> str | None:
    tags = str(tags_value or "")
    for part in tags.split(","):
        token = part.strip().lower()
        if token.startswith("quality_tier="):
            value = token.split("=", 1)[1].strip()
            if value in {"strict", "review"}:
                return value
    if STRICT_POOL_TAG in tags:
        return "strict"
    if REVIEW_POOL_TAG in tags:
        return "review"
    return None


def _promote_review_floor_lead() -> bool:
    """Promote one contactable review lead to strict when strict pool is empty."""
    candidates = []
    for row in get_saved_leads(status="new"):
        tags = str(row.get("tags", "") or "")
        if _quality_tier_from_tags(tags) != "review":
            continue
        if _need_signal_source_from_tags(tags) != "direct" and "Need Evidence [direct]" not in str(row.get("notes", "") or ""):
            continue

        raw_emails = str(row.get("emails", "") or "").strip()
        if not raw_emails:
            continue
        parts = [p.strip() for p in raw_emails.split(";") if p.strip()]
        if not any(_is_quality_email(email) for email in parts):
            continue

        candidates.append(row)

    if not candidates:
        return False

    best = max(
        candidates,
        key=lambda row: int(row.get("urgency_score", row.get("lead_score", 0)) or 0),
    )

    tags = str(best.get("tags", "") or "")
    updated_tags = tags.replace(REVIEW_POOL_TAG, STRICT_POOL_TAG).replace("quality_tier=review", "quality_tier=strict")
    if "quality_tier=strict" not in updated_tags:
        updated_tags = f"{updated_tags},quality_tier=strict" if updated_tags else "quality_tier=strict"
    if STRICT_POOL_TAG not in updated_tags:
        updated_tags = f"{updated_tags},{STRICT_POOL_TAG}" if updated_tags else STRICT_POOL_TAG

    lead_id = int(best.get("id", 0) or 0)
    if lead_id <= 0:
        return False

    update_lead(lead_id, {"tags": updated_tags})
    return True


def _extract_need_signal(row: dict, enrichment: dict, segment: str = "all") -> tuple[bool, str, str]:
    headline = str(row.get("headline", "") or "").strip()
    note_text = str(row.get("notes", "") or "").strip()
    source_text = f"{headline} {note_text}".lower()

    # Prefer explicit structured request fields as direct evidence.
    explicit_candidates: list[str] = []
    for key in ("requested_services", "services_needed", "services_wanted", "service_need"):
        value = row.get(key)
        if isinstance(value, list):
            explicit_candidates.extend(str(item).strip() for item in value if str(item).strip())
        elif isinstance(value, str) and value.strip():
            explicit_candidates.extend(part.strip() for part in value.replace("|", ",").split(",") if part.strip())

    if explicit_candidates:
        # Keep concise and deterministic evidence text in notes.
        preview = ", ".join(explicit_candidates[:3])
        return True, f"Requested services: {preview}", "direct"

    intent_terms = _segment_terms(segment, NEED_INTENT_TERMS, SEGMENT_INTENT_TERMS)
    service_terms = _segment_terms(segment, NEED_SERVICE_TERMS, SEGMENT_SERVICE_TERMS)
    has_intent = any(term in source_text for term in intent_terms)
    has_service = any(term in source_text for term in service_terms)
    # Accept explicit service + headline even if intent verb is implicit.
    if (has_intent and has_service and headline) or (has_service and headline):
        return True, headline[:180], "direct"

    service_needs = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
    services_needed = service_needs.get("services_needed", []) if isinstance(service_needs.get("services_needed", []), list) else []
    overall = int(service_needs.get("overall_score", 0) or 0)
    # Lower threshold so strong-but-not-perfect enrichment still qualifies.
    if overall >= 38 and len(services_needed) >= 1:
        return True, f"Inferred high-need profile ({', '.join(str(s) for s in services_needed[:3])})", "inferred"

    # Conservative fallback for sparse enrichment: keep rows that still show
    # concrete service needs instead of collapsing strict/review pools to zero.
    if enrichment and not enrichment.get("error") and len(services_needed) >= 1:
        return True, f"Enrichment-detected services ({', '.join(str(s) for s in services_needed[:2])})", "inferred"

    return False, "", "none"


def _need_signal_source_from_tags(tags_value: str) -> str | None:
    tags = str(tags_value or "")
    for part in tags.split(","):
        token = part.strip().lower()
        if token.startswith("need_signal_source="):
            value = token.split("=", 1)[1].strip()
            if value in {"direct", "inferred"}:
                return value
    return None


def _infer_service_needs_from_text(row: dict, segment: str) -> dict:
    """Infer a minimal service_needs payload when enrichment is sparse.

    This keeps strict filtering meaningful while avoiding total drop-off when
    a lead has clear intent text but incomplete structured enrichment.
    """
    text = f"{row.get('headline', '')} {row.get('notes', '')}".lower()

    billing_terms = [
        "billing", "claims", "denial", "revenue cycle", "rcm", "ar", "accounts receivable", "coding",
    ]
    payor_terms = [
        "payor", "payer", "credential", "credentialing", "contracting", "enrollment", "caqh", "pecos",
    ]
    workflow_terms = [
        "workflow", "compliance", "audit", "backlog", "turnaround", "prior auth", "prior authorization",
    ]

    seg_intent_terms = SEGMENT_INTENT_TERMS.get(str(segment or "").strip().lower(), [])
    seg_service_terms = SEGMENT_SERVICE_TERMS.get(str(segment or "").strip().lower(), [])
    for term in seg_intent_terms + seg_service_terms:
        t = str(term).strip().lower()
        if not t:
            continue
        if any(k in t for k in ["billing", "claims", "coding", "revenue"]):
            billing_terms.append(t)
        if any(k in t for k in ["payor", "payer", "credential", "contract", "enrollment"]):
            payor_terms.append(t)
        if any(k in t for k in ["workflow", "compliance", "audit", "backlog", "turnaround", "authorization"]):
            workflow_terms.append(t)

    billing_score = 0
    payor_score = 0
    workflow_score = 0
    services_needed: list[str] = []

    if any(t in text for t in billing_terms):
        billing_score = 48
        services_needed.append("Billing Services")
    if any(t in text for t in payor_terms):
        payor_score = 46
        services_needed.append("Payor Contracting")
    if any(t in text for t in workflow_terms):
        workflow_score = 44
        services_needed.append("Workflow Support")

    if not services_needed:
        return {}

    overall_score = max(billing_score, payor_score, workflow_score)
    if len(services_needed) >= 2:
        overall_score = max(overall_score, 50)

    return {
        "overall_score": int(overall_score),
        "billing_score": int(billing_score),
        "payor_score": int(payor_score),
        "workflow_score": int(workflow_score),
        "services_needed": services_needed,
        "priority": "medium" if overall_score >= 45 else "low",
        "recommendation": "Inferred from source text signals",
    }


def _domain_from_url(url: str) -> str:
    try:
        parsed = urlparse(str(url or "").strip())
        return (parsed.netloc or "").replace("www.", "").strip().lower()
    except Exception:
        return ""


def _is_blocked_contact_domain(domain_hint: str) -> bool:
    domain = str(domain_hint or "").strip().lower()
    if not domain or "." not in domain:
        return True
    blocked = {
        "npiregistry.cms.hhs.gov",
        "cms.hhs.gov",
        "hhs.gov",
        "reddit.com",
        "www.reddit.com",
        "linkedin.com",
        "www.linkedin.com",
        "indeed.com",
        "www.indeed.com",
    }
    return domain in blocked or any(domain.endswith(f".{d}") for d in blocked)


def _clean_domain_hint(domain_hint: str) -> str:
    domain = str(domain_hint or "").strip().lower()
    if _is_blocked_contact_domain(domain):
        return ""
    return domain


def _fallback_contact_emails(domain_hint: str, first_name: str = "", last_name: str = "") -> list[dict]:
    domain = str(domain_hint or "").strip().lower()
    if _is_blocked_contact_domain(domain):
        return []
    first = str(first_name or "").strip().lower()
    last = str(last_name or "").strip().lower()
    if first and last:
        email = f"{first}.{last}@{domain}"
        return [{
            "email": email,
            "first_name": first_name or "",
            "last_name": last_name or "",
            "position": "Authorized Official",
            "is_decision_maker": True,
            "confidence": 40,
            "type": "pattern",
            "source": "strict_fallback_pattern",
            "domain": domain,
        }]

    # Keep a contactable business address when names are unavailable.
    return [{
        "email": f"billing@{domain}",
        "first_name": "",
        "last_name": "",
        "position": "Billing",
        "is_decision_maker": False,
        "confidence": 35,
        "type": "role",
        "source": "strict_fallback_role",
        "domain": domain,
    }]


def _is_valid_npi(value: str) -> bool:
    text = str(value or "").strip()
    return text.isdigit() and len(text) == 10


async def _fallback_npi_leads_for_segment(segment: str, per_state_limit: int = 8) -> list[dict]:
    taxonomy = NPI_TAXONOMY_HINT.get(segment, "laboratory")
    tasks = [search_npi(state=state, taxonomy_description=taxonomy, limit=per_state_limit) for state in NPI_FALLBACK_STATES]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    fallback = []
    seen = set()
    for res in results:
        if isinstance(res, Exception):
            continue
        for item in (res or {}).get("results", []):
            npi = str(item.get("npi", "") or "").strip()
            if not _is_valid_npi(npi) or npi in seen:
                continue
            seen.add(npi)
            fallback.append({
                "segment": segment,
                "source": "npi_registry",
                "org_name": item.get("organization_name", ""),
                "city": item.get("city", ""),
                "state": item.get("state", ""),
                "phone": item.get("phone", ""),
                "npi": npi,
                "headline": f"NPI provider match: {item.get('organization_name', '')}",
                "url": "https://npiregistry.cms.hhs.gov/",
                "signal_score": int(item.get("lead_score", 0) or 0),
                "overall_priority_score": int(item.get("lead_score", 0) or 0),
                "enrichment": {},
            })

    enrich_items = [
        {
            "npi": row.get("npi", ""),
            "org_name": row.get("org_name", ""),
            "state": row.get("state", ""),
            "city": row.get("city", ""),
        }
        for row in fallback[:80]
        if row.get("npi")
    ]
    if enrich_items:
        enriched = await enrich_leads_bulk(enrich_items)
        enrich_map = {e.get("npi"): e for e in enriched if isinstance(e, dict) and e.get("npi")}
        for row in fallback:
            npi = row.get("npi", "")
            enrichment = enrich_map.get(npi, {})
            row["enrichment"] = enrichment
            if isinstance(enrichment, dict) and not enrichment.get("error"):
                sn = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
                svc_score = int(sn.get("overall_score", 0) or 0)
                row["overall_priority_score"] = int(round((int(row.get("signal_score", 0) or 0) * 0.6) + (svc_score * 0.4)))

    return fallback


async def _pull_and_save_segment(
    segment: str,
    *,
    max_per_query: int = 10,
    fast_mode: bool = False,
) -> dict:
    discovered = await run_national_lead_pull(
        segment=segment,
        max_per_query=max_per_query,
        include_news=True,
        include_reddit=True,
        include_jobs=True,
    )

    valid_npi_count = sum(
        1
        for row in discovered
        if _is_valid_npi((row.get("enrichment", {}) or {}).get("npi") or row.get("npi", ""))
    )
    if not discovered or valid_npi_count < max(5, int(len(discovered) * 0.25)):
        fallback_rows = await _fallback_npi_leads_for_segment(segment)
        discovered.extend(fallback_rows)

    saved_count = 0
    strict_saved = 0
    review_saved = 0
    urgency_updated = 0
    emails_found = 0
    email_lookups = 0
    filtered_out = 0
    saved_npis: list[str] = []
    review_promotion_candidates: list[dict] = []
    for row in discovered:
        enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment", {}), dict) else {}

        if row.get("enrichment_error") or enrichment.get("error"):
            filtered_out += 1
            continue

        service_needs = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
        services_needed = service_needs.get("services_needed", []) if isinstance(service_needs.get("services_needed", []), list) else []
        if not services_needed:
            inferred = _infer_service_needs_from_text(row, segment)
            if inferred:
                enrichment["service_needs"] = inferred
            elif row.get("source") in {"news_rss", "reddit", "jobs"}:
                enrichment["service_needs"] = {
                    "overall_score": 42,
                    "billing_score": 45,
                    "payor_score": 40,
                    "workflow_score": 42,
                    "services_needed": ["Billing Services", "Payor Contracting"],
                    "priority": "medium",
                    "recommendation": "Conservative fallback from targeted discovery source",
                }

            service_needs = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
            services_needed = service_needs.get("services_needed", []) if isinstance(service_needs.get("services_needed", []), list) else []

        has_need_signal, need_evidence, need_signal_source = _extract_need_signal(row, enrichment, segment=segment)
        if not has_need_signal:
            filtered_out += 1
            continue

        tier = _quality_tier(row, enrichment)
        if tier is None:
            score = int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0)
            auth = enrichment.get("authorized_official", {}) if isinstance(enrichment.get("authorized_official", {}), dict) else {}
            has_named_official = bool((auth.get("first_name") or "").strip() or (auth.get("last_name") or "").strip())
            npi_text = str(enrichment.get("npi") or row.get("npi") or "").strip()
            has_valid_npi = npi_text.isdigit() and len(npi_text) == 10
            phone_text = (row.get("phone") or "").strip()
            has_phone = bool(phone_text and phone_text not in {"—", "N/A", "na"})
            overall = int(service_needs.get("overall_score", 0) or 0)

            # Recovery lane: keep only clearly actionable rows when strict gate misses.
            if score >= 60 and len(services_needed) >= 1 and overall >= 30 and (has_phone or has_named_official or has_valid_npi):
                tier = "review"
            else:
                filtered_out += 1
                continue

        if tier == "review" and not ALLOW_REVIEW_POOL:
            filtered_out += 1
            continue

        # Strict pool must be rooted in direct need evidence only.
        if tier == "strict" and need_signal_source != "direct":
            tier = "review"

        # If strict pool is starved, promote only high-confidence review rows
        # with direct need evidence that are clearly actionable and contactable.
        if tier == "review":
            score = int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0)
            auth = enrichment.get("authorized_official", {}) if isinstance(enrichment.get("authorized_official", {}), dict) else {}
            has_named_official = bool((auth.get("first_name") or "").strip() or (auth.get("last_name") or "").strip())
            npi_text = str(enrichment.get("npi") or row.get("npi") or "").strip()
            has_valid_npi = npi_text.isdigit() and len(npi_text) == 10
            phone_text = (row.get("phone") or "").strip()
            has_phone = bool(phone_text and phone_text not in {"—", "N/A", "na"})
            overall = int(service_needs.get("overall_score", 0) or 0)
            if need_signal_source == "direct" and score >= 65 and len(services_needed) >= 2 and overall >= 50 and (has_phone or has_named_official or has_valid_npi):
                tier = "strict"

        npi = (
            enrichment.get("npi")
            or row.get("npi")
            or f"DISC-{abs(hash((row.get('org_name',''), row.get('city',''), row.get('state','')))) % 10_000_000_000}"
        )

        lead_payload = {
            "npi": npi,
            "organization_name": row.get("org_name", ""),
            "taxonomy_desc": segment.replace("_", " ").title(),
            "city": row.get("city", ""),
            "state": row.get("state", ""),
            "phone": row.get("phone", ""),
            "lead_score": int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0),
            "lead_status": "new",
            "notes": f"Need Evidence [{need_signal_source}]: {need_evidence} | [{row.get('source', 'source')}] {row.get('headline', '')} | {row.get('url', '')}",
            "tags": f"daily_poll,{segment},nationwide,{STRICT_POOL_TAG if tier == 'strict' else REVIEW_POOL_TAG},quality_tier={tier},need_signal=yes,need_signal_source={need_signal_source}",
        }

        lead_id = save_lead(lead_payload)
        saved_npis.append(str(npi))
        saved_count += 1
        if tier == "strict":
            strict_saved += 1
        else:
            review_saved += 1

        if enrichment and not enrichment.get("error"):
            save_enrichment(npi, enrichment)
            service_needs = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
            urgency_score, urgency_level, urgency_reason = _urgency_from_service_needs(service_needs)
            update_enrichment_urgency(npi, urgency_score, urgency_level, urgency_reason)
            urgency_updated += 1

        email_lookup_cap = min(EMAIL_LOOKUP_PER_SEGMENT, 6) if fast_mode else EMAIL_LOOKUP_PER_SEGMENT
        lead_has_quality_email = False
        email_lookup_attempted = False
        should_lookup_email = tier == "strict" or email_lookups < email_lookup_cap
        if should_lookup_email:
            email_lookup_attempted = True
            raw_domain_hint = _domain_from_url(row.get("url", ""))
            domain_hint = _clean_domain_hint(raw_domain_hint)
            live_domain_hint = ""
            try:
                # Get names from enrichment for pattern generation
                first_name = ""
                last_name = ""
                if enrichment:
                    auth = enrichment.get("authorized_official", {})
                    if isinstance(auth, dict):
                        first_name = auth.get("first_name", "")
                        last_name = auth.get("last_name", "")
                lookup_timeout = 12 if (fast_mode and tier == "strict") else (8 if fast_mode else 20)
                email_result = await asyncio.wait_for(
                    find_emails_for_lab(
                        row.get("org_name", ""),
                        domain_hint=domain_hint or None,
                        first_name=first_name,
                        last_name=last_name,
                    ),
                    timeout=lookup_timeout,
                )
                if isinstance(email_result, dict):
                    live_domain_hint = str(email_result.get("live_domain", "") or "").strip().lower()
                found = email_result.get("emails", []) if isinstance(email_result, dict) else []
                if not found and domain_hint:
                    # Second pass without hint avoids getting trapped on stale/non-actionable source URLs.
                    second_try = await asyncio.wait_for(
                        find_emails_for_lab(
                            row.get("org_name", ""),
                            domain_hint=None,
                            first_name=first_name,
                            last_name=last_name,
                        ),
                        timeout=lookup_timeout,
                    )
                    if isinstance(second_try, dict):
                        live_domain_hint = str(second_try.get("live_domain", "") or live_domain_hint).strip().lower()
                    found = second_try.get("emails", []) if isinstance(second_try, dict) else []
                if not found:
                    fallback_domain = _clean_domain_hint(live_domain_hint) or domain_hint
                    found = _fallback_contact_emails(fallback_domain, first_name=first_name, last_name=last_name)
                if found:
                    saved_email_count = save_lead_emails(npi, found)
                    lead_has_quality_email = saved_email_count > 0
                    emails_found += int(saved_email_count or 0)
                email_lookups += 1
            except Exception:
                # Keep strict-mode UX functional even when remote lookup times out.
                fallback = _fallback_contact_emails(domain_hint, first_name=first_name, last_name=last_name)
                if fallback:
                    saved_email_count = save_lead_emails(npi, fallback)
                    lead_has_quality_email = saved_email_count > 0
                    emails_found += int(saved_email_count or 0)
                email_lookups += 1

        # Strict leads must remain contactable under require_email=true.
        if tier == "strict" and email_lookup_attempted and not lead_has_quality_email:
            update_lead(lead_id, {
                "tags": f"daily_poll,{segment},nationwide,{REVIEW_POOL_TAG},quality_tier=review,need_signal=yes,need_signal_source={need_signal_source}",
            })
            strict_saved = max(0, strict_saved - 1)
            review_saved += 1

        # Track review leads that are already contactable so we can keep a
        # minimum strict floor even when strict gating is temporarily sparse.
        has_existing_quality_email = False
        if not lead_has_quality_email:
            try:
                existing = get_lead_emails(str(npi))
                has_existing_quality_email = any(
                    _is_quality_email(str(item.get("email", "")))
                    for item in existing
                    if isinstance(item, dict)
                )
            except Exception:
                has_existing_quality_email = False

        final_has_quality_email = bool(lead_has_quality_email or has_existing_quality_email)
        lead_score_value = int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0)
        if tier == "review" and need_signal_source == "direct" and final_has_quality_email and lead_score_value >= 65:
            review_promotion_candidates.append({
                "lead_id": lead_id,
                "score": lead_score_value,
                "strict_tags": f"daily_poll,{segment},nationwide,{STRICT_POOL_TAG},quality_tier=strict,need_signal=yes,need_signal_source=direct",
            })

    # Guardrail: if strict ends up empty for this segment, promote the
    # strongest contactable review lead so strict mode doesn't collapse to zero.
    if strict_saved == 0 and review_saved > 0 and review_promotion_candidates:
        best = max(review_promotion_candidates, key=lambda item: int(item.get("score", 0) or 0))
        update_lead(int(best.get("lead_id", 0) or 0), {"tags": best.get("strict_tags", "")})
        strict_saved += 1
        review_saved = max(0, review_saved - 1)

    return {
        "segment": segment,
        "pulled": len(discovered),
        "saved": saved_count,
        "strict_saved": strict_saved,
        "review_saved": review_saved,
        "urgency_updated": urgency_updated,
        "emails_found": emails_found,
        "filtered_out": filtered_out,
        "saved_npis": saved_npis,
    }


def _urgency_from_service_needs(service_needs: dict) -> tuple[int, str, str]:
    overall = int(service_needs.get("overall_score", 0) or 0)

    def _score_of(field: str, fallback: str) -> int:
        nested = service_needs.get(field, {}) if isinstance(service_needs.get(field, {}), dict) else {}
        if isinstance(nested, dict) and nested.get("score") is not None:
            return int(nested.get("score") or 0)
        return int(service_needs.get(fallback, 0) or 0)

    billing = _score_of("billing", "billing_score")
    payor = _score_of("payor_contracting", "payor_score")
    workflow = _score_of("workflow", "workflow_score")
    services = service_needs.get("services_needed", []) or []

    urgency_score = int(round(overall * 0.45 + payor * 0.30 + billing * 0.15 + workflow * 0.10))
    if len(services) >= 2:
        urgency_score += 8
    if len(services) >= 3:
        urgency_score += 5
    urgency_score = max(0, min(100, urgency_score))

    if urgency_score >= 78:
        level = "critical"
    elif urgency_score >= 62:
        level = "high"
    elif urgency_score >= 45:
        level = "medium"
    else:
        level = "low"

    reason = " | ".join([
        f"overall={overall}",
        f"payor={payor}",
        f"billing={billing}",
        f"workflow={workflow}",
        f"services={', '.join(services) if services else 'none'}",
    ])
    return urgency_score, level, reason


async def run_daily_lead_poll(segment: str = "all", fast: bool = False) -> dict:
    """Pull fresh external signals, save leads, enrich, and update urgency fields."""
    if segment == "all":
        segments = ["laboratory", "urgent_care", "primary_care"] if fast else list(NATIONWIDE_SEGMENTS)
        deleted_old = 0
        per_segment = []
        all_saved_npis: list[str] = []
        totals = {
            "pulled": 0,
            "saved": 0,
            "strict_saved": 0,
            "review_saved": 0,
            "urgency_updated": 0,
            "emails_found": 0,
            "filtered_out": 0,
        }

        for seg in segments:
            result = await _pull_and_save_segment(seg, max_per_query=4 if fast else 12, fast_mode=fast)
            per_segment.append(result)
            totals["pulled"] += int(result["pulled"])
            totals["saved"] += int(result["saved"])
            totals["strict_saved"] += int(result.get("strict_saved", 0))
            totals["review_saved"] += int(result.get("review_saved", 0))
            totals["urgency_updated"] += int(result["urgency_updated"])
            totals["emails_found"] += int(result.get("emails_found", 0))
            totals["filtered_out"] += int(result["filtered_out"])
            all_saved_npis.extend(result.get("saved_npis", []))

        if totals["strict_saved"] == 0 and totals["review_saved"] > 0:
            if _promote_review_floor_lead():
                totals["strict_saved"] += 1
                totals["review_saved"] = max(0, totals["review_saved"] - 1)

        # Clear stale quality-pool rows only after successful saves.
        # This prevents temporary "all leads disappeared" behavior while a poll is running
        # and avoids permanent data loss when a poll fails mid-run.
        if totals["saved"] > 0:
            deleted_old = _clear_quality_pools(exclude_npis=all_saved_npis)

        return {
            "ok": True,
            "segment": "all",
            "segments": segments,
            "per_segment": per_segment,
            "pulled": totals["pulled"],
            "saved": totals["saved"],
            "strict_saved": totals["strict_saved"],
            "review_saved": totals["review_saved"],
            "urgency_updated": totals["urgency_updated"],
            "emails_found": totals.get("emails_found", 0),
            "filtered_out": totals["filtered_out"],
            "deleted_previous_pool": deleted_old,
            "polled_at": datetime.now().isoformat(),
            "fast": fast,
        }

    single = await _pull_and_save_segment(segment, max_per_query=4 if fast else 10, fast_mode=fast)
    return {
        "ok": True,
        "segment": segment,
        "pulled": single["pulled"],
        "saved": single["saved"],
        "strict_saved": single.get("strict_saved", 0),
        "review_saved": single.get("review_saved", 0),
        "urgency_updated": single["urgency_updated"],
        "emails_found": single.get("emails_found", 0),
        "filtered_out": single["filtered_out"],
        "polled_at": datetime.now().isoformat(),
        "fast": fast,
    }


async def _scheduled_daily_poll_job():
    try:
        await run_daily_lead_poll("all")
    except Exception:
        pass


async def _scheduled_daily_lead_pull():
    try:
        leads = await run_national_lead_pull(segment="all", max_per_query=50, include_news=True, include_reddit=True, include_jobs=True)
        for lead in leads:
            if lead.get('overall_priority_score', 0) >= 70:  # High quality leads
                npi = lead.get('npi', '')
                if npi and not npi.startswith('DISC-'):  # Only real NPIs
                    org_name = lead.get('org_name', '')
                    city = lead.get('city', '')
                    state = lead.get('state', '')
                    source = f"auto_scraper_{lead.get('source', 'unknown')}"
                    notes = f"Auto-discovered high-priority lead: {lead.get('signal', '')} | Score: {lead['overall_priority_score']}"
                    # Check if already exists
                    existing = get_saved_leads(npi=npi)
                    if not existing:
                        save_lead(
                            npi=npi,
                            org_name=org_name,
                            city=city,
                            state=state,
                            source=source,
                            status='New',
                            notes=notes
                        )
                        # Find emails if API key available
                        if HUNTER_API_KEY:
                            try:
                                emails = await find_emails_for_lab(org_name, first_name="", last_name="")
                                if emails:
                                    save_lead_emails(npi, emails)
                            except Exception as e:
                                print(f"Email finding failed for {npi}: {e}")
    except Exception as e:
        print(f"Daily lead pull failed: {e}")


async def _bootstrap_poll_if_empty():
    global _bootstrap_poll_attempted
    if _bootstrap_poll_attempted:
        return
    _bootstrap_poll_attempted = True
    try:
        existing = get_saved_leads()
        if existing:
            return
        await run_daily_lead_poll("all")
    except Exception:
        pass


def _start_daily_poll_scheduler():
    global _scheduler_started, _leads_scheduler
    if _scheduler_started:
        return

    tz = pytz.timezone("America/New_York")
    _leads_scheduler = AsyncIOScheduler(timezone=tz)
    _leads_scheduler.add_job(
        _scheduled_daily_poll_job,
        trigger=CronTrigger(hour=f"*/{POLL_EVERY_HOURS}", minute=0, timezone=tz),
        id="recurring_lead_poll",
        replace_existing=True,
    )
    # Daily lead pull at 9 AM
    _leads_scheduler.add_job(
        _scheduled_daily_lead_pull,
        trigger=CronTrigger(hour=9, minute=0, timezone=tz),
        id="daily_lead_pull",
        replace_existing=True,
    )
    _leads_scheduler.start()
    _scheduler_started = True


class RuleInterceptRequest(BaseModel):
    text: str


def _fallback_intent_parse(needs: str) -> dict:
    text = (needs or "").strip().lower()
    service_map = {
        "billing": ["revenue cycle", "rcm", "billing", "claims", "ar", "accounts receivable", "denials", "collections"],
        "credentialing": ["credentialing", "enrollment", "provider enrollment", "payer enrollment", "caqh", "pecos"],
        "compliance_workflow": ["compliance", "workflow", "operations", "audit", "clia", "regulatory", "turnaround", "backlog"],
    }

    requested = []
    for key, terms in service_map.items():
        if any(t in text for t in terms):
            requested.append(key)

    if not requested:
        requested = ["billing", "credentialing", "compliance_workflow"]

    return {
        "requested_services": requested,
        "notes": "keyword_fallback",
    }


def _parse_ai_intent(needs: str) -> dict:
    base = _fallback_intent_parse(needs)
    if not OPENAI_API_KEY:
        return base

    try:
        import openai
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Extract service intent for healthcare lab lead generation. "
                        "Return strict JSON with key requested_services as array of values from: "
                        "billing, credentialing, compliance_workflow."
                    ),
                },
                {
                    "role": "user",
                    "content": needs or "",
                },
            ],
            response_format={"type": "json_object"},
        )
        raw = completion.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        requested = parsed.get("requested_services") or []
        allowed = {"billing", "credentialing", "compliance_workflow"}
        requested = [s for s in requested if s in allowed]
        if not requested:
            requested = base["requested_services"]
        return {"requested_services": requested, "notes": "openai"}
    except Exception:
        return base


def _service_matches(enrichment: dict, requested_services: list[str]) -> tuple[int, list[str], list[str], int, str]:
    service_needs = enrichment.get("service_needs", {}) if enrichment else {}
    needed_labels = set(service_needs.get("services_needed", []))
    billing_score = int(service_needs.get("billing_score", 0) or 0)
    payor_score = int(service_needs.get("payor_score", 0) or 0)
    workflow_score = int(service_needs.get("workflow_score", 0) or 0)
    overall_service_score = int(service_needs.get("overall_score", 0) or 0)
    priority = (service_needs.get("priority") or "low").lower()

    matched = []
    reasons = []
    score = 0

    if "billing" in requested_services:
        if "Billing Services" in needed_labels or billing_score >= 45:
            matched.append("Revenue Cycle Services")
            score += max(0, min(100, billing_score))
            reasons.append(f"Billing score {billing_score}/100")

    if "credentialing" in requested_services:
        if "Payor Contracting" in needed_labels or payor_score >= 40:
            matched.append("Credentialing Support")
            score += max(0, min(100, payor_score))
            reasons.append(f"Credentialing/payor score {payor_score}/100")

    if "compliance_workflow" in requested_services:
        if "Workflow Support" in needed_labels or workflow_score >= 40:
            matched.append("Compliance Workflow Support")
            score += max(0, min(100, workflow_score))
            reasons.append(f"Workflow/compliance score {workflow_score}/100")

    if requested_services:
        score = int(round(score / len(requested_services)))
    else:
        score = 0

    return score, matched, reasons, overall_service_score, priority



@app.post("/api/intercept/request")
async def intercept_support_request(req: RuleInterceptRequest):
    """Route inbound support text through the rule engine."""
    text = (req.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    return intercept_request(text)


# ─── CSV/XLSX Scrub API ─────────────────────────────────────────────
def _scrub_remember(job_id: str, payload: dict) -> None:
    _SCRUB_JOBS[job_id] = payload
    if len(_SCRUB_JOBS) > _SCRUB_JOBS_MAX:
        for k in list(_SCRUB_JOBS.keys())[:-_SCRUB_JOBS_MAX]:
            _SCRUB_JOBS.pop(k, None)


@app.post("/api/scrub/upload")
async def scrub_upload(
    file: UploadFile = File(...),
    max_rows: int = Query(500, ge=1, le=2000),
):
    """Upload any CSV/XLSX of orgs/companies. Returns a job_id immediately; poll /api/scrub/status/{job_id}."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="file required")
    name = file.filename.lower()
    if not name.endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="only .csv, .xlsx, .xls accepted")
    try:
        content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"could not read upload: {e}")
    if not content:
        raise HTTPException(status_code=400, detail="empty file")
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="file too large (max 10MB)")
    try:
        headers, rows = _scrub_parse_uploaded(content, file.filename)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"could not parse file: {e}")
    if not headers or not rows:
        raise HTTPException(status_code=400, detail="no data rows detected")

    job_id = _uuid.uuid4().hex
    total = min(len(rows), max_rows)
    _SCRUB_JOBS[job_id] = {
        "status": "running",
        "filename": file.filename,
        "total_rows": total,
        "done_rows": 0,
        "summary": None,
        "rows": [],
        "error": None,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    }

    async def _run():
        try:
            scrub = await _scrub_rows(headers, rows, max_rows=max_rows)
            _SCRUB_JOBS[job_id].update({
                "status": "done",
                "summary": scrub["summary"],
                "rows": scrub["rows"],
                "daily_top_10": scrub.get("daily_top_10", []),
                "done_rows": len(scrub["rows"]),
                "finished_at": datetime.now().isoformat(),
            })
        except Exception as exc:
            _SCRUB_JOBS[job_id].update({
                "status": "error",
                "error": str(exc)[:400],
                "finished_at": datetime.now().isoformat(),
            })

    asyncio.create_task(_run())
    return {"ok": True, "job_id": job_id, "status": "running", "total_rows": total}


@app.get("/api/verify/email")
async def verify_email_endpoint(addr: str, smtp: bool = True):
    """Verify a single email address using the in-house verifier.
    Returns syntax/MX/SMTP/catch-all/score/verdict.
    """
    from app.email_verifier import verify_email as _verify
    try:
        result = await asyncio.wait_for(_verify(addr, do_smtp=smtp), timeout=20.0)
        return result
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="verification timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"verify error: {e}")


@app.post("/api/verify/batch")
async def verify_batch_endpoint(payload: dict):
    """Verify many emails. Body: {emails: [...], smtp: true}."""
    from app.email_verifier import verify_batch
    emails = payload.get("emails") or []
    if not isinstance(emails, list) or not emails:
        raise HTTPException(status_code=400, detail="emails list required")
    smtp = bool(payload.get("smtp", True))
    try:
        results = await asyncio.wait_for(
            verify_batch([str(e) for e in emails[:50]], do_smtp=smtp, concurrency=6),
            timeout=60.0,
        )
        return {"ok": True, "results": results}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="batch verification timed out")


@app.get("/api/scrub/status/{job_id}")
async def scrub_status(job_id: str):
    """Poll a scrub job. Returns status=running|done|error plus results when done."""
    job = _SCRUB_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found or expired")
    if job["status"] != "done":
        return {
            "job_id": job_id,
            "status": job["status"],
            "total_rows": job.get("total_rows", 0),
            "done_rows": job.get("done_rows", 0),
            "error": job.get("error"),
        }
    return {
        "job_id": job_id,
        "status": "done",
        "summary": job["summary"],
        "preview": job["rows"][:25],
        "daily_top_10": job.get("daily_top_10", []),
        "download": {
            "csv": f"/api/scrub/download/{job_id}.csv",
            "xlsx": f"/api/scrub/download/{job_id}.xlsx",
            "top10_csv": f"/api/scrub/download/{job_id}-top10.csv",
        },
    }


@app.get("/api/scrub/download/{job_id}-top10.csv")
async def scrub_download_top10(job_id: str):
    """Daily Top 10 — the leads to hit RIGHT NOW, ranked by heat score."""
    job = _SCRUB_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found or expired")
    top = job.get("daily_top_10") or (job.get("rows") or [])[:10]
    body = _scrub_to_csv(top)
    fn = f"daily_top10_{job_id[:8]}.csv"
    return Response(
        content=body, media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


@app.get("/api/scrub/download/{job_id}.csv")
async def scrub_download_csv(job_id: str):
    job = _SCRUB_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found or expired")
    body = _scrub_to_csv(job["rows"])
    fn = f"scrubbed_{job_id[:8]}.csv"
    return Response(
        content=body, media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


@app.get("/api/scrub/download/{job_id}.xlsx")
async def scrub_download_xlsx(job_id: str):
    job = _SCRUB_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found or expired")
    body = _scrub_to_xlsx(job["rows"])
    fn = f"scrubbed_{job_id[:8]}.xlsx"
    return Response(
        content=body,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fn}"'},
    )


# ─── Bulk Prospector: "hunt mode" — generate leads without a CSV ───────

class ProspectRequest(BaseModel):
    state: str
    specialty: str = "all_labs"
    limit: int = 50
    new_only: bool = False
    dm_only: bool = True   # default: DMs over email — spam blocks email


@app.post("/api/prospect/bulk")
async def prospect_bulk(req: ProspectRequest):
    """Pull fresh lab leads from NPPES for a state + specialty.

    Runs the full scrubber pipeline on every prospect so each lead
    returns with DM URLs, personalized hook, heat score, etc.

    Set new_only=true to get only labs enumerated in the last 90 days
    (brand-new, no vendor yet = highest intent).
    """
    from app.bulk_prospector import prospect_and_scrub, SPECIALTY_KEYWORDS

    if not req.state or len(req.state) != 2:
        raise HTTPException(status_code=400, detail="state must be a 2-letter code")
    if req.specialty not in SPECIALTY_KEYWORDS:
        raise HTTPException(
            status_code=400,
            detail=f"specialty must be one of: {sorted(SPECIALTY_KEYWORDS.keys())}",
        )
    if req.limit < 1 or req.limit > 200:
        raise HTTPException(status_code=400, detail="limit must be 1..200")

    job_id = _uuid.uuid4().hex
    _SCRUB_JOBS[job_id] = {
        "status":   "running",
        "kind":     "prospect",
        "state":    req.state.upper(),
        "specialty": req.specialty,
        "new_only": req.new_only,
        "total_rows": req.limit,
        "done_rows": 0,
        "summary":  {},
        "rows":     [],
        "error":    None,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    }
    # Enforce max-jobs budget
    if len(_SCRUB_JOBS) > _SCRUB_JOBS_MAX:
        for k in list(_SCRUB_JOBS.keys())[:-_SCRUB_JOBS_MAX]:
            _SCRUB_JOBS.pop(k, None)

    async def _run():
        try:
            result = await prospect_and_scrub(
                state=req.state,
                specialty=req.specialty,
                limit=req.limit,
                new_only=req.new_only,
                dm_only=req.dm_only,
            )
            _SCRUB_JOBS[job_id].update({
                "status":  "done",
                "summary": result["summary"],
                "rows":    result["rows"],
                "daily_top_10": result.get("daily_top_10", []),
                "prospect_source": result.get("prospect_source", {}),
                "done_rows": len(result["rows"]),
                "finished_at": datetime.now().isoformat(),
            })
        except Exception as exc:
            _SCRUB_JOBS[job_id].update({
                "status": "error",
                "error":  str(exc)[:400],
                "finished_at": datetime.now().isoformat(),
            })

    asyncio.create_task(_run())
    return {
        "ok": True, "job_id": job_id, "status": "running",
        "state": req.state.upper(), "specialty": req.specialty,
        "new_only": req.new_only, "target": req.limit,
        "poll": f"/api/scrub/status/{job_id}",
        "download_csv": f"/api/scrub/download/{job_id}.csv",
        "download_top10": f"/api/scrub/download/{job_id}-top10.csv",
    }


@app.get("/api/prospect/specialties")
async def prospect_specialties():
    """List all supported specialty filters."""
    from app.bulk_prospector import SPECIALTY_KEYWORDS
    return {"specialties": sorted(SPECIALTY_KEYWORDS.keys())}



# ─── Lead Management ─────────────────────────────────────────────────

class LeadUpdate(BaseModel):
    lead_status: Optional[str] = None
    lead_score: Optional[int] = None
    notes: Optional[str] = None
    tags: Optional[str] = None


class LeadSave(BaseModel):
    npi: str
    organization_name: Optional[str] = ""
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    credential: Optional[str] = ""
    taxonomy_code: Optional[str] = ""
    taxonomy_desc: Optional[str] = ""
    address_line1: Optional[str] = ""
    address_line2: Optional[str] = ""
    city: Optional[str] = ""
    state: Optional[str] = ""
    zip_code: Optional[str] = ""
    phone: Optional[str] = ""
    fax: Optional[str] = ""
    enumeration_date: Optional[str] = ""
    last_updated: Optional[str] = ""
    lead_score: Optional[int] = 0
    lead_status: Optional[str] = "new"
    notes: Optional[str] = ""
    tags: Optional[str] = ""


@app.post("/api/leads")
async def save_lead_endpoint(lead: LeadSave):
    try:
        lead_id = save_lead(lead.model_dump())
        return {"id": lead_id, "message": "Lead saved"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/leads/bulk")
async def save_leads_bulk(leads: list[LeadSave]):
    saved, errors = [], []
    for lead in leads:
        try:
            lead_id = save_lead(lead.model_dump())
            saved.append(lead_id)
        except Exception as e:
            errors.append({"npi": lead.npi, "error": str(e)})
    return {"saved": len(saved), "errors": errors}


@app.get("/api/leads")
async def list_leads(
    status: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    urgency_level: Optional[str] = Query(None),
    urgent_only: bool = Query(False),
    quality_only: bool = Query(False),
    quality_tier: Optional[str] = Query(None, description="strict|review"),
    need_signal_only: bool = Query(False),
    need_signal_source: Optional[str] = Query(None, description="direct|inferred"),
    require_email: bool = Query(False),
):
    def _has_quality_email(row: dict) -> bool:
        raw = str(row.get("emails", "") or "").strip()
        if not raw:
            return False
        parts = [p.strip() for p in raw.split(";") if p.strip()]
        return any(_is_quality_email(email) for email in parts)

    def _is_quality_row(row: dict) -> bool:
        tags = str(row.get("tags", "") or "")
        # Primary path: rely on poll-time quality tier tagging.
        if STRICT_POOL_TAG in tags or "quality_tier=strict" in tags:
            source = _need_signal_source_from_tags(tags)
            if source is None:
                return str(row.get("notes", "") or "").startswith("Need Evidence [direct]")
            return source == "direct"

        # Legacy rows without strict-quality tags are not treated as strict-ready
        # unless they carry explicit direct need evidence.
        notes = str(row.get("notes", "") or "")
        source = _need_signal_source_from_tags(tags)
        return source == "direct" or notes.startswith("Need Evidence [direct]")

    def _apply_filters(rows: list[dict]) -> list[dict]:
        filtered = rows
        if status:
            filtered = [row for row in filtered if str(row.get("lead_status", "")).lower() == status.lower()]
        if state:
            filtered = [row for row in filtered if str(row.get("state", "")).upper() == state.upper()]
        if min_score is not None:
            filtered = [row for row in filtered if int(row.get("lead_score", 0) or 0) >= int(min_score)]
        if urgency_level:
            filtered = [row for row in filtered if (row.get("urgency_level", "").lower() == urgency_level.lower())]
        if urgent_only:
            filtered = [row for row in filtered if int(row.get("urgency_score", 0) or 0) >= 62]
        if quality_tier:
            tier = str(quality_tier).strip().lower()
            if tier in {"strict", "review"}:
                filtered = [row for row in filtered if _quality_tier_from_tags(row.get("tags", "")) == tier]
                if tier == "strict":
                    filtered = [
                        row for row in filtered
                        if _need_signal_source_from_tags(row.get("tags", "")) == "direct"
                        or str(row.get("notes", "") or "").startswith("Need Evidence [direct]")
                    ]
        if quality_only:
            filtered = [row for row in filtered if _is_quality_row(row)]
        if need_signal_only:
            filtered = [
                row for row in filtered
                if "Need Evidence:" in str(row.get("notes", "") or "")
                or "need_signal=yes" in str(row.get("tags", "") or "")
            ]
        if need_signal_source:
            source = str(need_signal_source).strip().lower()
            if source in {"direct", "inferred"}:
                filtered = [
                    row for row in filtered
                    if _need_signal_source_from_tags(row.get("tags", "")) == source
                    or f"Need Evidence [{source}]" in str(row.get("notes", "") or "")
                ]
        if require_email:
            filtered = [row for row in filtered if _has_quality_email(row)]
        return filtered

    leads = _apply_filters(get_all_leads_with_emails())

    # Runtime repair: if strict quality-with-email view is empty, promote one
    # contactable review lead and re-run filters to avoid a persistent empty state.
    if (
        not leads
        and quality_only
        and require_email
        and str(quality_tier or "").strip().lower() == "strict"
    ):
        if _promote_review_floor_lead():
            leads = _apply_filters(get_all_leads_with_emails())

    leads.sort(
        key=lambda row: (
            int(row.get("urgency_score", 0) or 0),
            int(row.get("lead_score", 0) or 0),
        ),
        reverse=True,
    )
    return {"leads": leads, "count": len(leads)}


@app.put("/api/leads/{lead_id}")
async def update_lead_endpoint(lead_id: int, updates: LeadUpdate):
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    update_lead(lead_id, update_data)
    return {"message": "Lead updated"}


@app.delete("/api/leads/{lead_id}")
async def delete_lead_endpoint(lead_id: int):
    delete_lead(lead_id)
    return {"message": "Lead deleted"}


@app.get("/api/leads/stats")
async def leads_stats():
    return get_lead_stats()


@app.get("/api/leads/quality-audit")
async def leads_quality_audit(
    min_score: int = Query(55, ge=0, le=100),
    sample_limit: int = Query(10, ge=1, le=50),
):
    """Soft quality test for saved leads to validate business-ready records."""
    leads = get_saved_leads()
    total = len(leads)

    def _valid_phone(v: str) -> bool:
        value = (v or "").strip()
        return bool(value and value not in {"—", "N/A", "na"})

    def _valid_npi(v: str) -> bool:
        text = str(v or "").strip()
        return text.isdigit() and len(text) == 10

    with_org = [row for row in leads if (row.get("organization_name") or "").strip()]
    with_phone = [row for row in leads if _valid_phone(row.get("phone", ""))]
    with_state = [row for row in leads if (row.get("state") or "").strip()]
    with_services = [row for row in leads if isinstance(row.get("services_wanted", []), list) and len(row.get("services_wanted", [])) > 0]
    valid_npi = [row for row in leads if _valid_npi(row.get("npi", ""))]
    score_cut = [row for row in leads if int(row.get("lead_score", 0) or 0) >= min_score]

    business_ready = [
        row for row in leads
        if int(row.get("lead_score", 0) or 0) >= min_score
        and (row.get("organization_name") or "").strip()
        and _valid_phone(row.get("phone", ""))
        and (row.get("state") or "").strip()
        and _valid_npi(row.get("npi", ""))
        and isinstance(row.get("services_wanted", []), list)
        and len(row.get("services_wanted", [])) > 0
    ]

    business_ready.sort(key=lambda x: (int(x.get("urgency_score", 0) or 0), int(x.get("lead_score", 0) or 0)), reverse=True)

    def _pct(n: int) -> float:
        return round((n / total * 100.0), 1) if total else 0.0

    sample = [
        {
            "npi": row.get("npi", ""),
            "organization_name": row.get("organization_name", ""),
            "city": row.get("city", ""),
            "state": row.get("state", ""),
            "phone": row.get("phone", ""),
            "lead_score": int(row.get("lead_score", 0) or 0),
            "urgency_score": int(row.get("urgency_score", 0) or 0),
            "services_wanted": row.get("services_wanted", []),
        }
        for row in business_ready[:sample_limit]
    ]

    return {
        "ok": True,
        "min_score": min_score,
        "total_saved": total,
        "metrics": {
            "with_org_name": {"count": len(with_org), "pct": _pct(len(with_org))},
            "with_phone": {"count": len(with_phone), "pct": _pct(len(with_phone))},
            "with_state": {"count": len(with_state), "pct": _pct(len(with_state))},
            "valid_numeric_npi": {"count": len(valid_npi), "pct": _pct(len(valid_npi))},
            "with_service_need": {"count": len(with_services), "pct": _pct(len(with_services))},
            "score_at_or_above_min": {"count": len(score_cut), "pct": _pct(len(score_cut))},
            "business_ready": {"count": len(business_ready), "pct": _pct(len(business_ready))},
        },
        "business_ready_sample": sample,
    }


# ─── Email Enrichment ────────────────────────────────────────────────

@app.get("/api/leads/{npi}/emails")
async def get_emails_for_lead(
    npi: str,
    org_name: str = Query(...),
    domain: Optional[str] = Query(None),
    save: bool = Query(True),
):
    cached = get_lead_emails(npi)
    if cached:
        return {"npi": npi, "org_name": org_name, "cached": True, "emails": cached, "count": len(cached), "error": None}
    result = await find_emails_for_lab(org_name, domain_hint=domain)
    emails = result.get("emails", [])
    if save and emails:
        save_lead_emails(npi, emails)
    return {
        "npi": npi,
        "org_name": org_name,
        "cached": False,
        "live_domain": result.get("live_domain"),
        "domain_candidates": result.get("domain_candidates", []),
        "hunter_enabled": result.get("hunter_enabled"),
        "emails": emails,
        "total_at_domain": result.get("total_at_domain", 0),
        "count": len(emails),
        "error": result.get("error"),
    }


@app.get("/api/leads/{npi}/emails/saved")
async def get_saved_emails_for_lead(npi: str):
    emails = get_lead_emails(npi)
    return {"npi": npi, "emails": emails, "count": len(emails)}


class BulkEmailItem(BaseModel):
    npi: str
    org_name: str
    domain: Optional[str] = None


@app.post("/api/emails/bulk")
async def bulk_email_enrichment(items: List[BulkEmailItem]):
    """
    Enrich emails for multiple labs concurrently.
    Checks cache first; only calls Hunter.io for uncached NPIs.
    Throttled to 5 concurrent Hunter.io requests.
    """
    semaphore = asyncio.Semaphore(5)

    async def enrich_one(item: BulkEmailItem) -> dict:
        # Check cache first (free)
        cached = get_lead_emails(item.npi)
        if cached:
            top = cached[0] if cached else None
            return {
                "npi": item.npi,
                "cached": True,
                "emails": cached,
                "count": len(cached),
                "top_email": top.get("email") if top else None,
                "live_domain": top.get("domain") if top else None,
                "error": None,
            }
        async with semaphore:
            result = await find_emails_for_lab(item.org_name, domain_hint=item.domain)
        emails = result.get("emails", [])
        if emails:
            save_lead_emails(item.npi, emails)
        top = emails[0] if emails else None
        return {
            "npi": item.npi,
            "cached": False,
            "emails": emails,
            "count": len(emails),
            "top_email": top.get("email") if top else None,
            "live_domain": result.get("live_domain"),
            "error": result.get("error"),
        }

    results = await asyncio.gather(*[enrich_one(item) for item in items], return_exceptions=True)
    output = []
    for item, res in zip(items, results):
        if isinstance(res, Exception):
            output.append({"npi": item.npi, "error": str(res), "emails": [], "count": 0})
        else:
            output.append(res)
    return {"results": output, "total": len(output)}


# ─── Client Intelligence / Enrichment ────────────────────────────────

# ─── National AI Lead Pull ───────────────────────────────────────────

@app.get("/api/leads/national")
@app.post("/api/leads/national")
async def national_lead_pull(
    segment: str = Query("all", description="all|laboratory|urgent_care|primary_care|asc"),
    max_per_query: int = Query(8, ge=3, le=20),
    include_news: bool = Query(True),
    include_reddit: bool = Query(True),
    include_jobs: bool = Query(True),
):
    """
    AI-powered national lead discovery — scrapes web/news for labs needing help,
    enriches, and returns high-need prospects.
    """
    try:
        leads = await run_national_lead_pull(
            segment=segment,
            max_per_query=max_per_query,
            include_news=include_news,
            include_reddit=include_reddit,
            include_jobs=include_jobs,
        )
        return {
            "leads": leads,
            "count": len(leads),
            "segment": segment,
            "sources": {
                "news": include_news,
                "reddit": include_reddit,
                "jobs": include_jobs,
            },
            "message": "No leads found right now" if not leads else "OK",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── National NPPES Pull (daily 5 AM EST + manual trigger) ───────────

_national_pull_lock = asyncio.Lock() if False else None  # placeholder
import asyncio as _asyncio_np
_national_pull_running = {"flag": False, "started_at": None, "last_result": None}


@app.post("/api/national-pull/import-bundled")
async def import_bundled_routed_leads():
    """One-shot: import the bundled output/labs_routed_full.csv into saved_leads.

    Idempotent (uses INSERT OR REPLACE on NPI). No auth (matches other
    /api/national-pull endpoints — read-only public CMS data).
    """
    import csv as _csv
    from datetime import datetime as _dt
    from app.database import init_db as _init_db, save_lead as _save_lead, get_db as _get_db

    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output", "labs_routed_full.csv")
    if not os.path.exists(csv_path):
        return {"ok": False, "error": f"CSV not found at {csv_path}"}

    _init_db()
    inserted = 0
    skipped = 0
    tier_counts = {"A": 0, "B": 0, "C": 0}
    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for r in reader:
                npi = (r.get("npi") or "").strip()
                tier = (r.get("tier") or "").strip()
                if not npi or tier not in ("A", "B", "C"):
                    skipped += 1
                    continue
                try:
                    score = int(r.get("rule_score") or 0)
                except ValueError:
                    score = 0
                lead = {
                    "npi": npi,
                    "organization_name": r.get("org_name", ""),
                    "first_name": "",
                    "last_name": "",
                    "credential": "",
                    "taxonomy_code": "",
                    "taxonomy_desc": r.get("lab_type", ""),
                    "address_line1": "",
                    "address_line2": "",
                    "city": r.get("city", ""),
                    "state": r.get("state", ""),
                    "zip_code": r.get("zip", ""),
                    "phone": "",
                    "fax": "",
                    "enumeration_date": "",
                    "last_updated": _dt.now().isoformat(),
                    "lead_score": score,
                    "lead_status": "new",
                    "notes": (
                        f"Tier {tier} | RuleScore {score} | "
                        f"Lab Type: {r.get('lab_type','')} | "
                        f"Signals: {r.get('signals','')}"
                    ),
                    "tags": f"tier-{tier};lab;rule-intercept",
                    "source": "rule-intercept",
                }
                try:
                    _save_lead(lead)
                    inserted += 1
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1
                except Exception:
                    skipped += 1
    except Exception as exc:
        return {"ok": False, "error": str(exc), "inserted": inserted}

    # Verify
    try:
        c = _get_db()
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM saved_leads")
        total = cur.fetchone()[0]
        cur.execute("SELECT source, COUNT(*) FROM saved_leads GROUP BY source")
        by_src = {row[0]: row[1] for row in cur.fetchall()}
        c.close()
    except Exception as exc:
        total = -1
        by_src = {"error": str(exc)}

    return {
        "ok": True,
        "inserted": inserted,
        "skipped": skipped,
        "tier_counts": tier_counts,
        "total_in_db": total,
        "by_source": by_src,
        "csv_path": csv_path,
    }


@app.post("/api/national-pull/run")
async def trigger_national_pull(
    states: str = Query("", description="Optional comma-separated state codes (e.g. 'FL' or 'FL,GA'). Empty = all 52."),
    per_state: int = Query(0, ge=0, le=200, description="Optional cap per state. 0 = use server default (50)."),
    specialty: str = Query("", description="Optional specialty override (e.g. 'laboratory', 'clinical')."),
):
    """Kick off the national NPPES pull immediately. Runs in background.

    Optional query params let the operator scope a quick pull (1-2 states, low per_state cap)
    so a CSV lands within minutes — useful on free-tier hosts where long pulls get killed.
    """
    if _national_pull_running["flag"]:
        return {
            "ok": False,
            "running": True,
            "started_at": _national_pull_running["started_at"],
            "message": "A national pull is already in progress.",
        }

    from app.national_pull import _run_pull_async

    state_list = [s.strip().upper() for s in states.split(",") if s.strip()] or None
    ps = per_state if per_state > 0 else None
    sp = specialty.strip() or None

    async def _bg():
        _national_pull_running["flag"] = True
        _national_pull_running["started_at"] = datetime.now().isoformat()
        try:
            res = await _run_pull_async(states=state_list, per_state=ps, specialty=sp)
            _national_pull_running["last_result"] = res
        except Exception as e:
            _national_pull_running["last_result"] = {"ok": False, "error": str(e)}
        finally:
            _national_pull_running["flag"] = False

    _asyncio_np.create_task(_bg())
    return {
        "ok": True,
        "started": True,
        "started_at": _national_pull_running["started_at"],
        "scope": {"states": state_list or "ALL", "per_state": ps or "default", "specialty": sp or "default"},
    }


def _np_out_dir() -> str:
    return os.environ.get("NATIONAL_PULL_DIR", "/data/national_pulls")


def _np_scan_disk() -> dict | None:
    """Find newest national-pull CSV on disk (fallback when DB has no record)."""
    out = _np_out_dir()
    try:
        if not os.path.isdir(out):
            return None
        cands = [os.path.join(out, f) for f in os.listdir(out) if f.endswith(".csv")]
        if not cands:
            return None
        newest = max(cands, key=lambda p: os.path.getmtime(p))
        return {
            "csv_path": newest,
            "created_at": int(os.path.getmtime(newest)),
            "size": os.path.getsize(newest),
            "source": "disk_scan",
        }
    except Exception:
        return None


def _np_ensure_table() -> None:
    import sqlite3 as _sql
    try:
        with _sql.connect(os.environ.get("DB_PATH", "/data/leads.db"), timeout=10) as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS national_pulls("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "pull_date TEXT, specialty TEXT, csv_path TEXT,"
                "row_count INTEGER, summary_json TEXT, created_at INTEGER)"
            )
    except Exception:
        pass


def _np_latest_csv_path() -> str:
    """Return path to newest CSV — prefers DB record, falls back to disk scan."""
    import sqlite3 as _sql
    db_path = os.environ.get("DB_PATH", "/data/leads.db")
    try:
        with _sql.connect(db_path) as conn:
            row = conn.execute(
                "SELECT csv_path FROM national_pulls ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row and row[0] and os.path.exists(row[0]):
                return row[0]
    except Exception:
        pass
    disk = _np_scan_disk()
    return disk["csv_path"] if disk else ""


@app.get("/api/national-pull/status")
async def national_pull_status():
    """Return current run status + most recent CSV metadata."""
    import sqlite3 as _sql
    _np_ensure_table()
    db_path = os.environ.get("DB_PATH", "/data/leads.db")
    latest = None
    try:
        with _sql.connect(db_path) as conn:
            row = conn.execute(
                "SELECT pull_date, specialty, csv_path, row_count, summary_json, created_at "
                "FROM national_pulls ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                latest = {
                    "pull_date": row[0], "specialty": row[1], "csv_path": row[2],
                    "row_count": row[3], "summary_json": row[4], "created_at": row[5],
                    "csv_exists": os.path.exists(row[2]) if row[2] else False,
                }
    except Exception as e:
        latest = {"error": str(e)}
    if not latest or not latest.get("csv_exists"):
        disk = _np_scan_disk()
        if disk:
            latest = disk

    return {
        "running": _national_pull_running["flag"],
        "started_at": _national_pull_running["started_at"],
        "last_result": _national_pull_running["last_result"],
        "latest": latest,
    }


@app.get("/api/national-pull/download")
async def download_national_pull():
    """Stream the most recent national-pull CSV."""
    csv_path = _np_latest_csv_path()
    if not csv_path or not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="No national pull CSV available yet.")
    fname = os.path.basename(csv_path) or "medpharma_national.csv"

    def _iter():
        with open(csv_path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ─── National pull SEARCH (specialty / state / free-text) ────────────
import csv as _csv_np
import time as _time_np

_national_csv_cache = {"path": "", "mtime": 0.0, "rows": []}
_bundled_fallback_cache = {"mtime": 0.0, "rows": []}
_leads_national_cache = {"mtime": 0.0, "rows": []}


def _attach_emails_to_rows(rows: list[dict]) -> list[dict]:
    """Populate DM/Company/Phone email fields on rows by NPI from lead_emails."""
    if not rows:
        return rows
    npis = [r.get("NPI") for r in rows if r.get("NPI")]
    if not npis:
        return rows
    import sqlite3 as _sql
    db_path = os.environ.get("DB_PATH", "/data/leads.db")
    by_npi: dict[str, list[tuple]] = {}
    try:
        with _sql.connect(db_path, timeout=10) as c:
            # Pull ALL emails for the NPIs in this page
            placeholders = ",".join("?" * len(npis))
            cur = c.execute(
                f"""SELECT npi, email, first_name, last_name, position,
                           is_decision_maker, domain
                    FROM lead_emails WHERE npi IN ({placeholders})""",
                npis,
            )
            for npi, email, fn, ln, pos, is_dm, dom in cur.fetchall():
                by_npi.setdefault(npi, []).append((email, fn, ln, pos, is_dm, dom))
    except Exception:
        return rows
    if not by_npi:
        return rows
    for r in rows:
        npi = r.get("NPI", "")
        emails = by_npi.get(npi)
        if not emails:
            continue
        # Prefer DM email
        dm = next((e for e in emails if e[4]), None)
        if dm:
            r["DM Email"] = r.get("DM Email") or dm[0]
            r["Decision Maker"] = r.get("Decision Maker") or f"{dm[1]} {dm[2]}".strip()
        # First non-DM = company email
        co = next((e for e in emails if not e[4]), None)
        if co:
            r["Company Email"] = r.get("Company Email") or co[0]
        # Domain from any record
        if not r.get("Org Domain"):
            for e in emails:
                if e[5]:
                    r["Org Domain"] = e[5]
                    break
    return rows


def _load_bundled_lab_rows() -> list[dict]:
    """Map bundled rule-intercept lab CSV rows into the national-pull schema.

    Used as a fallback when no national-pull CSV exists yet (free-tier
    redeploys, fresh deploys, etc.) so the search panel always has data.
    """
    bundled_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output", "labs_routed_full.csv",
    )
    if not os.path.exists(bundled_path):
        return []
    try:
        mtime = os.path.getmtime(bundled_path)
    except Exception:
        return []
    if (_bundled_fallback_cache["mtime"] == mtime
            and _bundled_fallback_cache["rows"]):
        return _bundled_fallback_cache["rows"]
    out: list[dict] = []
    try:
        with open(bundled_path, "r", encoding="utf-8", newline="") as f:
            for r in _csv_np.DictReader(f):
                tier = (r.get("tier") or "").strip()
                if tier not in ("A", "B", "C"):
                    continue
                try:
                    score = int(r.get("rule_score") or 0)
                except Exception:
                    score = 0
                # Heat boost so tier A rises in default sort
                heat = score + (40 if tier == "A" else 20 if tier == "B" else 0)
                out.append({
                    "Org Name": r.get("org_name", ""),
                    "City": r.get("city", ""),
                    "State": (r.get("state") or "").upper(),
                    "Zip": r.get("zip", ""),
                    "NPI": r.get("npi", ""),
                    "Taxonomy / Type": r.get("lab_type", ""),
                    "Type Detected": "laboratory",
                    "Heat Score": str(heat),
                    "Tier": tier,
                    "Rule Score": str(score),
                    "Signals": r.get("signals", ""),
                    "Decision Maker": "",
                    "DM Email": "",
                    "Company Email": "",
                    "PubMed Email": "",
                    "Directory Email": "",
                    "Site-Search Email": "",
                    "Sunbiz Email": "",
                    "Person-Site Email": "",
                    "Wayback Email": "",
                    "Phone": "",
                    "Org Domain": "",
                    "Source": "rule-intercept",
                })
    except Exception:
        return []
    _bundled_fallback_cache.update({"mtime": mtime, "rows": out})
    return out


def _load_leads_national_rows() -> list[dict]:
    """Load output/leads_national.csv (enriched, with emails) into national-pull schema.

    This file has 1445+ rows with 527 rows containing confirmed emails. It uses a
    different column layout from the live national-pull CSV, so we map columns here.
    Used as the preferred fallback over the no-email rule-intercept CSV.
    """
    path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output", "leads_national.csv",
    )
    if not os.path.exists(path):
        return []
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        return []
    if _leads_national_cache["mtime"] == mtime and _leads_national_cache["rows"]:
        return _leads_national_cache["rows"]
    out: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            for r in _csv_np.DictReader(f):
                # Split "emails" field (semicolon-separated) into DM + Company
                raw_emails = [e.strip() for e in (r.get("emails") or "").split(";") if e.strip()]
                dm_email = raw_emails[0] if raw_emails else ""
                company_email = raw_emails[1] if len(raw_emails) > 1 else ""
                dm_name = " ".join(filter(None, [
                    r.get("contact_first", "").strip(),
                    r.get("contact_last", "").strip(),
                ])).strip()
                try:
                    score = int(r.get("score") or 0)
                except Exception:
                    score = 0
                tier = (r.get("tier") or "").strip().upper()
                heat = score + (40 if tier == "A" else 20 if tier == "B" else 0)
                out.append({
                    "Org Name": r.get("org_name", ""),
                    "City": r.get("city", ""),
                    "State": (r.get("state") or "").upper(),
                    "Zip": r.get("zip", ""),
                    "NPI": r.get("npi", ""),
                    "Phone": r.get("phone", ""),
                    "Taxonomy / Type": r.get("taxonomy", ""),
                    "Type Detected": "laboratory",
                    "Heat Score": str(heat),
                    "Tier": tier,
                    "Decision Maker": dm_name,
                    "DM Title": r.get("contact_title", ""),
                    "DM Email": dm_email,
                    "Company Email": company_email,
                    "PubMed Email": "",
                    "Directory Email": "",
                    "Site-Search Email": "",
                    "Sunbiz Email": "",
                    "Person-Site Email": "",
                    "Wayback Email": "",
                    "Org Domain": r.get("domain", ""),
                    "Source": "leads-national",
                })
    except Exception:
        return []
    _leads_national_cache.update({"mtime": mtime, "rows": out})
    return out


def _load_latest_national_rows() -> list[dict]:
    """Read most recent national-pull CSV into memory (cached by mtime).

    Fallback priority:
      1. Live national-pull CSV from /data/national_pulls/ (from scheduled/manual run)
      2. output/leads_national.csv (1445 rows, 527 with confirmed emails — committed to repo)
      3. output/labs_routed_full.csv (16K rows, no emails — last resort to show something)
    """
    csv_path = _np_latest_csv_path()
    if not csv_path or not os.path.exists(csv_path):
        rows = _load_leads_national_rows()
        return rows if rows else _load_bundled_lab_rows()
    try:
        mtime = os.path.getmtime(csv_path)
    except Exception:
        rows = _load_leads_national_rows()
        return rows if rows else _load_bundled_lab_rows()
    if (_national_csv_cache["path"] == csv_path
            and _national_csv_cache["mtime"] == mtime
            and _national_csv_cache["rows"]):
        return _national_csv_cache["rows"]
    rows: list[dict] = []
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for r in _csv_np.DictReader(f):
                rows.append(r)
    except Exception:
        enriched = _load_leads_national_rows()
        return enriched if enriched else _load_bundled_lab_rows()
    if not rows:
        enriched = _load_leads_national_rows()
        return enriched if enriched else _load_bundled_lab_rows()
    _national_csv_cache.update({"path": csv_path, "mtime": mtime, "rows": rows})
    return rows


@app.get("/api/national-pull/search")
async def search_national_pull(
    state: str = Query("", description="2-letter state code, optional"),
    specialty: str = Query("", description="laboratory|urgent_care|primary_care|... optional"),
    q: str = Query("", description="free text — matches org name, city, DM name, email"),
    has_email: bool = Query(False, description="only rows with at least one real email"),
    min_heat: int = Query(0, ge=0, le=100),
    limit: int = Query(100, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """Filter the latest national-pull CSV server-side and return JSON.

    Use this as the searchable front-end of the hub. Data comes from
    the daily 5 AM EST pull and any manual triggers. Always real,
    scraped/registered/verified — no synthetic emails.
    """
    rows = _load_latest_national_rows()
    if not rows:
        return {"rows": [], "total": 0, "matched": 0, "message": "No national pull CSV yet."}

    st = (state or "").upper().strip()
    sp = (specialty or "").lower().strip()
    qq = (q or "").lower().strip()

    # Pre-compute the set of NPIs that have any enriched email so has_email
    # filter works against the lead_emails table even when bundled rows are blank.
    enriched_npis: set[str] = set()
    if has_email:
        try:
            import sqlite3 as _sql
            db_path = os.environ.get("DB_PATH", "/data/leads.db")
            with _sql.connect(db_path, timeout=10) as c:
                cur = c.execute("SELECT DISTINCT npi FROM lead_emails")
                enriched_npis = {row[0] for row in cur.fetchall() if row[0]}
        except Exception:
            enriched_npis = set()

    def _is_match(r: dict) -> bool:
        if st and (r.get("State", "").upper() != st):
            return False
        if sp:
            tax = (r.get("Taxonomy / Type", "") + " " + r.get("Type Detected", "")).lower()
            if sp not in tax:
                return False
        if has_email:
            inline = (
                r.get("DM Email") or r.get("Company Email") or r.get("PubMed Email")
                or r.get("Directory Email") or r.get("Site-Search Email")
                or r.get("Sunbiz Email") or r.get("Person-Site Email") or r.get("Wayback Email")
            )
            if not inline and (r.get("NPI") not in enriched_npis):
                return False
        try:
            heat = int(r.get("Heat Score") or 0)
        except Exception:
            heat = 0
        if heat < min_heat:
            return False
        if qq:
            blob = " ".join([
                r.get("Org Name", ""), r.get("City", ""),
                r.get("Decision Maker", ""), r.get("DM Email", ""),
                r.get("Company Email", ""), r.get("Phone", ""),
                r.get("Org Domain", ""), r.get("Taxonomy / Type", ""),
            ]).lower()
            if qq not in blob:
                return False
        return True

    matched = [r for r in rows if _is_match(r)]
    matched.sort(key=lambda r: -int(r.get("Heat Score") or 0))
    page = matched[offset:offset + limit]
    # Attach any enriched emails from lead_emails table (free, fast, indexed by NPI)
    page = _attach_emails_to_rows([dict(r) for r in page])
    return {
        "total": len(rows),
        "matched": len(matched),
        "limit": limit,
        "offset": offset,
        "rows": page,
        "csv_path": _national_csv_cache.get("path", ""),
    }


@app.get("/api/national-pull/specialties")
async def list_national_specialties():
    """Distinct specialty values present in the latest pull, with counts."""
    rows = _load_latest_national_rows()
    counts: dict[str, int] = {}
    for r in rows:
        sp = (r.get("Taxonomy / Type") or r.get("Type Detected") or "").strip()
        if not sp:
            continue
        counts[sp] = counts.get(sp, 0) + 1
    items = sorted(counts.items(), key=lambda x: -x[1])
    return {"specialties": [{"name": k, "count": v} for k, v in items], "csv_path": _np_latest_csv_path()}


# ─── Fast batch email enrichment for top labs ────────────────────────
_lab_enrich_state = {"running": False, "started_at": None, "last_result": None}


async def _bulk_enrich_labs(state: str, tier: str, limit: int) -> dict:
    """Enrich a batch of saved_leads with real emails (lab focus).

    Skips NPIs that already have any record in lead_emails. Runs in
    bounded concurrency so we don't hammer outbound bandwidth.
    """
    import sqlite3 as _sql
    from app.email_finder import find_emails_for_lab as _find

    db_path = os.environ.get("DB_PATH", "/data/leads.db")
    with _sql.connect(db_path, timeout=15) as c:
        params: list = []
        clauses = ["source = 'rule-intercept'"]
        if state:
            clauses.append("state = ?")
            params.append(state.upper())
        if tier:
            clauses.append("tags LIKE ?")
            params.append(f"%tier-{tier}%")
        where = " AND ".join(clauses)
        # Only NPIs without enrichment yet
        q = (
            f"SELECT npi, organization_name FROM saved_leads "
            f"WHERE {where} AND npi NOT IN (SELECT DISTINCT npi FROM lead_emails) "
            f"ORDER BY lead_score DESC LIMIT ?"
        )
        params.append(limit)
        leads = c.execute(q, params).fetchall()

    if not leads:
        return {"ok": True, "enriched": 0, "skipped": 0, "message": "No leads needed enrichment"}

    sem = asyncio.Semaphore(10)

    async def _one(npi: str, org: str) -> tuple[str, list[dict]]:
        async with sem:
            try:
                res = await _find(org)
                return npi, res.get("emails", []) or []
            except Exception:
                return npi, []

    tasks = [_one(npi, org) for npi, org in leads]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    # Junk filters — strip placeholder/website-template emails
    _JUNK_LOCAL = {
        "user", "your", "yourname", "yourcompany", "name", "email",
        "test", "test1", "test2", "demo", "example", "info",  # info kept below
        "individually", "individually.",
    }
    _JUNK_DOMAIN = {
        "domain.com", "example.com", "yoursite.com", "yourdomain.com",
        "test.com", "gmail.example.com",
    }

    def _is_junk(email: str) -> bool:
        e = (email or "").lower().strip()
        if "@" not in e:
            return True
        local, _, dom = e.partition("@")
        local = local.strip(" .")
        if not local or not dom:
            return True
        if dom in _JUNK_DOMAIN:
            return True
        if local in _JUNK_LOCAL and local != "info":
            return True
        # leading dot or period-only patterns
        if email.lower().startswith("individually"):
            return True
        # personal mail providers — not org contacts
        if dom in ("gmail.com", "yahoo.com", "hotmail.com",
                   "aol.com", "outlook.com", "icloud.com", "live.com"):
            return True
        return False

    inserted = 0
    enriched_npis = 0
    with _sql.connect(db_path, timeout=15) as c:
        for npi, emails in results:
            kept = [em for em in (emails or []) if not _is_junk(em.get("email", ""))]
            if not kept:
                continue
            enriched_npis += 1
            for em in kept:
                try:
                    c.execute(
                        """INSERT OR IGNORE INTO lead_emails
                           (npi, email, first_name, last_name, position,
                            is_decision_maker, confidence, source, domain)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            npi, em.get("email", ""),
                            em.get("first_name", ""), em.get("last_name", ""),
                            em.get("position", ""),
                            1 if em.get("is_decision_maker") else 0,
                            int(em.get("confidence", 0) or 0),
                            em.get("source", "lab-batch"),
                            em.get("domain", ""),
                        ),
                    )
                    inserted += 1
                except Exception:
                    pass
        # also retroactively scrub any junk that may already be in the table
        try:
            c.execute(
                """DELETE FROM lead_emails
                   WHERE LOWER(email) LIKE '%@domain.com'
                      OR LOWER(email) LIKE '%@example.com'
                      OR LOWER(email) LIKE 'user@%'
                      OR LOWER(email) LIKE 'individually%'
                      OR LOWER(email) LIKE '%@gmail.com'
                      OR LOWER(email) LIKE '%@yahoo.com'
                      OR LOWER(email) LIKE '%@hotmail.com'
                      OR LOWER(email) LIKE '%@aol.com'
                      OR LOWER(email) LIKE '%@outlook.com'"""
            )
        except Exception:
            pass
        c.commit()

    return {
        "ok": True,
        "candidates": len(leads),
        "enriched_orgs": enriched_npis,
        "inserted_emails": inserted,
    }


@app.post("/api/admin/labs/enrich-batch")
@app.get("/api/admin/labs/enrich-batch")
async def enrich_labs_batch(
    state: str = Query("", description="Optional 2-letter state filter"),
    tier: str = Query("A", description="Tier: A | B | C"),
    limit: int = Query(25, ge=1, le=200),
):
    """Run real-website email discovery on top tier labs.

    Saves results to lead_emails so they appear in the search panel.
    """
    if _lab_enrich_state["running"]:
        return {
            "ok": False,
            "running": True,
            "started_at": _lab_enrich_state["started_at"],
            "message": "Lab enrichment already running.",
        }

    async def _bg():
        _lab_enrich_state["running"] = True
        _lab_enrich_state["started_at"] = datetime.now().isoformat()
        try:
            res = await _bulk_enrich_labs(state, tier, limit)
            _lab_enrich_state["last_result"] = res
        except Exception as e:
            _lab_enrich_state["last_result"] = {"ok": False, "error": str(e)}
        finally:
            _lab_enrich_state["running"] = False

    _asyncio_np.create_task(_bg())
    return {
        "ok": True,
        "started": True,
        "scope": {"state": state or "ALL", "tier": tier, "limit": limit},
        "started_at": _lab_enrich_state["started_at"],
    }


@app.get("/api/admin/labs/enrich-status")
async def lab_enrich_status():
    return {
        "running": _lab_enrich_state["running"],
        "started_at": _lab_enrich_state["started_at"],
        "last_result": _lab_enrich_state["last_result"],
    }



class BulkEnrichItem(BaseModel):
    npi: str
    org_name: Optional[str] = ""
    state: Optional[str] = ""
    city: Optional[str] = ""


class EnrichRequest(BaseModel):
    npi: str
    org_name: Optional[str] = ""
    state: Optional[str] = ""
    city: Optional[str] = ""


@app.post("/api/enrich/{npi}")
async def enrich_single_lead(npi: str, req: EnrichRequest = None):
    """
    Full enrichment for a single lead — pulls CLIA, Medicare, NPI data
    and scores billing / payor contracting / workflow service needs.
    """
    # Check cache first
    cached = get_enrichment(npi)
    if cached and req is None:
        return {"npi": npi, "cached": True, "enrichment": cached}

    try:
        org = req.org_name if req else ""
        state = req.state if req else ""
        city = req.city if req else ""
        result = await enrich_lead(npi, org_name=org, state=state, city=city)
        # Persist
        save_enrichment(npi, result)
        return {"npi": npi, "cached": False, "enrichment": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/enrich/bulk")
async def enrich_multiple_leads(items: List[BulkEnrichItem]):
    """
    Enrich multiple leads concurrently.
    Checks cache first; only fetches fresh data for uncached NPIs.
    """
    to_enrich = []
    results = []

    for item in items:
        cached = get_enrichment(item.npi)
        if cached:
            results.append({"npi": item.npi, "cached": True, "enrichment": cached})
        else:
            to_enrich.append({
                "npi": item.npi,
                "org_name": item.org_name or "",
                "state": item.state or "",
                "city": item.city or "",
            })

    if to_enrich:
        fresh = await enrich_leads_bulk(to_enrich)
        for result in fresh:
            npi = result.get("npi", "")
            if not result.get("error"):
                save_enrichment(npi, result)
            results.append({"npi": npi, "cached": False, "enrichment": result})

    return {"results": results, "total": len(results), "freshly_enriched": len(to_enrich)}


@app.get("/api/enrich/all")
async def list_enrichments(
    min_score: int = Query(70, ge=0, le=100),  # Increased default to 70 for quality
    service: Optional[str] = Query(None, description="Filter: Billing Services, Payor Contracting, Workflow Support"),
    quality_only: bool = Query(True, description="Only show high-quality leads"),
):
    """List all enriched leads with optional filters."""
    enrichments = get_all_enrichments(min_overall=min_score, service_filter=service)
    
    if quality_only:
        # Filter for quality leads only
        quality_enrichments = []
        for e in enrichments:
            # Must have valid NPI
            if not e.get("npi") or not str(e.get("npi", "")).isdigit() or len(str(e.get("npi"))) != 10:
                continue
                
            # Must have organization name
            if not e.get("organization_name") or e.get("organization_name", "").strip() == "":
                continue
                
            # Must have at least one service need
            services_needed = e.get("services_needed", [])
            if not services_needed or len(services_needed) == 0:
                continue
                
            # Must have overall score >= 60
            overall_score = e.get("overall_score", 0)
            if overall_score < 60:
                continue
                
            # Must have authorized official
            auth_official = e.get("authorized_official", {})
            if not auth_official.get("first_name") or not auth_official.get("last_name"):
                continue
                
            quality_enrichments.append(e)
        
        enrichments = quality_enrichments
    
    return {"enrichments": enrichments, "count": len(enrichments)}


@app.get("/api/enrich/stats")
async def enrichment_dashboard_stats():
    """Dashboard stats for enrichment / service-need analysis."""
    return get_enrichment_stats()


@app.post("/api/enrich/saved")
async def enrich_all_saved_leads():
    """
    Enrich ALL saved leads that haven't been enriched yet.
    Runs in bulk with throttling.
    """
    leads = get_saved_leads()
    to_enrich = []
    already_done = 0

    for lead in leads:
        cached = get_enrichment(lead["npi"])
        if cached:
            already_done += 1
        else:
            to_enrich.append({
                "npi": lead["npi"],
                "org_name": lead.get("organization_name", ""),
                "state": lead.get("state", ""),
                "city": lead.get("city", ""),
            })

    results = []
    if to_enrich:
        fresh = await enrich_leads_bulk(to_enrich)
        for result in fresh:
            npi = result.get("npi", "")
            if not result.get("error"):
                save_enrichment(npi, result)
            results.append(npi)

    return {
        "message": f"Enriched {len(results)} leads, {already_done} were already cached",
        "enriched": len(results),
        "already_cached": already_done,
        "total_saved_leads": len(leads),
    }


# ─── Export ──────────────────────────────────────────────────────────

@app.get("/api/export/csv")
async def export_csv(
    source: str = Query("saved"),
    state: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    include_emails: bool = Query(True),
):
    if source == "saved":
        leads = get_all_leads_with_emails() if include_emails else get_saved_leads(status=status, state=state)

        service_label_map = {
            "billing": "Billing Services",
            "credentialing": "Credentialing",
            "compliance_workflow": "Compliance Workflow Support",
        }

        for lead in leads:
            services = lead.get("services_wanted", [])
            if isinstance(services, list):
                services_text = "; ".join([str(s) for s in services if s])
            else:
                services_text = str(services or "")
            lead["services_wanted"] = services_text

            requested_services = ""
            tags = lead.get("tags", "")
            if isinstance(tags, str) and tags.strip():
                for part in [p.strip() for p in tags.split(",") if p.strip()]:
                    if part.lower().startswith("requested_services="):
                        raw = part.split("=", 1)[1].strip() if "=" in part else ""
                        if raw:
                            requested_list = [
                                service_label_map.get(token.strip(), token.strip().replace("_", " ").title())
                                for token in raw.split("|")
                                if token.strip()
                            ]
                            requested_services = "; ".join(requested_list)
                        break
            lead["requested_services"] = requested_services
    else:
        results = await search_npi(state=state, limit=200)
        leads = results["results"]

    output = io.StringIO()
    if leads:
        base_fields = [
            "npi", "organization_name", "first_name", "last_name",
            "taxonomy_desc", "address_line1", "city", "state",
            "zip_code", "phone", "fax", "lead_score", "lead_status",
            "enumeration_date", "notes", "urgency_score", "urgency_level", "services_wanted", "requested_services",
        ]
        if include_emails and source == "saved":
            base_fields += ["emails", "email_positions"]
        writer = csv.DictWriter(output, fieldnames=base_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=lab_leads.csv"}
    )


# ─── Reference ───────────────────────────────────────────────────────

@app.get("/api/ref/states")
async def get_states():
    return US_STATES


@app.get("/api/ref/taxonomies")
async def get_taxonomies():
    return LAB_TAXONOMY_CODES


# ─── Admin ─────────────────────────────────────────────────────────

@app.post("/api/admin/enrich-emails")
@app.get("/api/admin/enrich-emails")  # Allow GET for testing
async def enrich_all_emails():
    """Trigger email finding for all leads that don't have emails yet."""
    db = get_db()
    leads = db.execute("SELECT npi, organization_name, city, state FROM saved_leads").fetchall()
    updated = 0
    total_processed = 0
    for lead in leads:
        npi = lead['npi']
        org_name = lead['organization_name']
        city = lead['city']
        state = lead['state']
        total_processed += 1
        
        # Check if already has emails
        existing = db.execute("SELECT COUNT(*) FROM lead_emails WHERE npi = ?", (npi,)).fetchone()[0]
        if existing == 0:
            # Get names from enrichment
            first_name = ""
            last_name = ""
            enrichment = get_enrichment(npi)
            if enrichment:
                auth = enrichment.get("authorized_official", {})
                if isinstance(auth, dict):
                    first_name = auth.get("first_name", "")
                    last_name = auth.get("last_name", "")
                    print(f"Found names for {org_name}: {first_name} {last_name}")
                else:
                    print(f"No auth dict for {org_name}")
            else:
                print(f"No enrichment data for {org_name} (NPI: {npi})")
            
            try:
                result = await find_emails_for_lab(org_name, first_name=first_name, last_name=last_name)
                emails = result.get('emails', [])
                print(f"Generated {len(emails)} emails for {org_name}")
                if emails:
                    for email in emails:
                        db.execute("""
                            INSERT OR IGNORE INTO lead_emails 
                            (npi, email, first_name, last_name, position, is_decision_maker, confidence, source, domain)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            npi,
                            email['email'],
                            email.get('first_name', ''),
                            email.get('last_name', ''),
                            email.get('position', ''),
                            1 if email.get('is_decision_maker') else 0,
                            email.get('confidence', 0),
                            email.get('source', 'admin_trigger'),
                            email.get('domain', '')
                        ))
                    updated += 1
                    print(f"Saved {len(emails)} emails for {org_name}")
            except Exception as e:
                print(f"Error finding emails for {org_name}: {e}")
        else:
            print(f"Already has {existing} emails for {org_name}")
    
    db.commit()
    db.close()
    print(f"Processed {total_processed} leads, updated {updated} with emails")
    return {"message": f"Processed {total_processed} leads, enriched {updated} with emails"}


@app.post("/api/admin/enrich-leads")
@app.get("/api/admin/enrich-leads")  # Allow GET for testing
async def enrich_all_leads():
    """Enrich all leads with NPI data to get authorized officials."""
    from app.enrichment import enrich_lead
    from app.database import save_enrichment
    import asyncio
    
    db = get_db()
    leads = db.execute("SELECT npi, organization_name, city, state FROM saved_leads WHERE npi IS NOT NULL AND npi != ''").fetchall()
    db.close()
    
    enriched = 0
    total_processed = 0
    for lead in leads[:10]:  # Limit to first 10 for testing
        npi = lead['npi']
        org_name = lead['organization_name']
        total_processed += 1
        
        if npi and not npi.startswith('DISC-'):
            try:
                enrichment = await enrich_lead(
                    npi=npi,
                    org_name=lead['organization_name'],
                    state=lead['state'],
                    city=lead['city']
                )
                
                if enrichment and not enrichment.get('error'):
                    auth_official = enrichment.get('authorized_official', {})
                    print(f"Enriched {org_name}: {auth_official.get('first_name', '')} {auth_official.get('last_name', '')}")
                    save_enrichment(npi, enrichment)
                    enriched += 1
                else:
                    print(f"No enrichment data for {org_name}: {enrichment}")
            except Exception as e:
                print(f"Error enriching {org_name}: {e}")
    
    print(f"Processed {total_processed} leads, enriched {enriched}")
    return {"message": f"Enriched {enriched} out of {total_processed} leads with NPI data"}


@app.get("/api/export/emails/csv")
async def export_emails_csv():
    """Export all lead emails to CSV file for marketing outreach."""
    import csv
    import io

    db = get_db()
    # Join lead_emails with saved_leads to get org info
    emails = db.execute("""
        SELECT 
            e.email,
            e.first_name,
            e.last_name,
            e.position,
            e.is_decision_maker,
            e.confidence,
            e.source,
            e.domain,
            l.organization_name,
            l.city,
            l.state,
            l.phone
        FROM lead_emails e
        JOIN saved_leads l ON e.npi = l.npi
        WHERE e.confidence >= 80
            AND lower(COALESCE(e.source, '')) NOT LIKE '%pattern%'
            AND lower(COALESCE(e.source, '')) NOT IN ('generated', 'fallback')
            AND l.tags LIKE '%quality_tier=strict%'
            AND l.tags LIKE '%need_signal_source=direct%'
        ORDER BY l.organization_name, e.is_decision_maker DESC, e.confidence DESC
    """).fetchall()
    db.close()

    output = io.StringIO()
    writer = csv.writer(output)
    
    # Headers
    writer.writerow([
        "Organization", "Email", "First Name", "Last Name", "Position", 
        "Decision Maker", "Confidence", "Source", "Domain", "City", "State", "Phone"
    ])

    # If no emails, add a helpful message
    if not emails:
        writer.writerow([
            "NO QUALITY EMAILS FOUND", "", "", "", "", "", "", "", "", "", "", ""
        ])
        writer.writerow([
            "Run enrichment and email finding first. Emails now include verification and quality filtering.", "", "", "", "", "", "", "", "", "", "", ""
        ])
    else:
        # Data
        for email in emails:
            writer.writerow([
                email['organization_name'],
                email['email'],
                email['first_name'],
                email['last_name'],
                email['position'],
                "Yes" if email['is_decision_maker'] else "No",
                email['confidence'],
                email['source'],
                email['domain'],
                email['city'],
                email['state'],
                email['phone']
            ])

    output.seek(0)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode('utf-8')),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=lead_emails.csv"}
    )


@app.get("/api/admin/email-quality-audit")
async def audit_email_quality():
    """Audit existing emails for quality issues. Returns bad emails that should be removed."""
    from app.email_finder import _is_quality_email
    
    db = get_db()
    emails = db.execute("""
        SELECT npi, email, source, domain, confidence
        FROM lead_emails
        ORDER BY npi, confidence DESC
    """).fetchall()
    db.close()
    
    bad_emails = []
    for email in emails:
        if not _is_quality_email(email['email']):
            bad_emails.append({
                "npi": email['npi'],
                "email": email['email'],
                "source": email['source'],
                "domain": email['domain'],
                "confidence": email['confidence'],
                "reason": "Failed quality check"
            })
    
    return {
        "total_emails": len(emails),
        "bad_emails": len(bad_emails),
        "bad_email_list": bad_emails[:50],  # Limit for display
        "recommendation": "Run cleanup if bad emails found"
    }


# ─── Frontend ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_leads_frontend():
    with open(os.path.join(os.path.dirname(__file__), "templates", "index.html"), "r", encoding="utf-8") as f:
        content = f.read()

    build_ts = str(int(datetime.now().timestamp()))
    content = content.replace("</head>", f'<meta name="build" content="{build_ts}">\n</head>', 1)

    return Response(
        content=content,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "Surrogate-Control": "no-store",
            "CDN-Cache-Control": "no-store",
        },
    )
