"""Lab Lead Generation app — runs on LAB_PORT (default 8000)."""

import csv
import io
import json
import os
import asyncio
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from app.config import US_STATES, LAB_TAXONOMY_CODES, OPENAI_API_KEY
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

from app.npi_client import (
    search_npi, search_npi_by_taxonomy, get_npi_detail, bulk_search_labs,
)
from app.email_finder import find_emails_for_lab
from app.enrichment import enrich_lead, enrich_leads_bulk
from app.lead_scraper import run_national_lead_pull

app = FastAPI(
    title="MedPharma Lab Leads",
    description="Lab Lead Generator — search NPI Registry for clinical lab prospects",
    version="2.0.1",
)

_scheduler_started = False
_leads_scheduler = None
NATIONWIDE_SEGMENTS = ["laboratory", "urgent_care", "primary_care", "asc"]
NPI_FALLBACK_STATES = ["TX", "CA", "FL", "NY", "PA", "OH", "GA", "NC", "MI", "IL"]
NPI_TAXONOMY_HINT = {
    "laboratory": "laboratory",
    "urgent_care": "urgent care",
    "primary_care": "family medicine",
    "asc": "ambulatory surgery",
}
STRICT_MIN_SIGNAL_SCORE = 55
STRICT_MIN_SERVICE_SCORE = 45
STRICT_MIN_DOMAIN_SCORE = 40
STRICT_POOL_TAG = "strict_quality_pool"
REVIEW_MIN_SIGNAL_SCORE = 45
REVIEW_MIN_SERVICE_SCORE = 35
REVIEW_MIN_DOMAIN_SCORE = 30
REVIEW_POOL_TAG = "review_quality_pool"


def _quality_tier(row: dict, enrichment: dict) -> str | None:
    score = int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0)
    if score < REVIEW_MIN_SIGNAL_SCORE:
        return None

    org_name = (row.get("org_name") or "").strip()
    state = (row.get("state") or "").strip()
    if not org_name or not state:
        return None

    npi = str((enrichment or {}).get("npi") or row.get("npi") or "").strip()
    has_valid_npi = npi.isdigit() and len(npi) == 10

    auth = enrichment.get("authorized_official", {}) if isinstance(enrichment.get("authorized_official", {}), dict) else {}
    has_named_official = bool((auth.get("first_name") or "").strip() or (auth.get("last_name") or "").strip())
    if not (has_valid_npi or has_named_official):
        return None

    phone = (row.get("phone") or "").strip()
    has_phone = bool(phone and phone not in {"—", "N/A", "na"})
    if not has_phone:
        return None

    service_needs = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
    services_needed = service_needs.get("services_needed", []) if isinstance(service_needs.get("services_needed", []), list) else []
    if not services_needed:
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

    if score >= STRICT_MIN_SIGNAL_SCORE:
        return "strict"
    return "review"


def _clear_quality_pools() -> int:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM saved_leads WHERE lead_status = 'new' AND (tags LIKE ? OR tags LIKE ?)",
        (f"%{STRICT_POOL_TAG}%", f"%{REVIEW_POOL_TAG}%"),
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return int(deleted or 0)


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
    filtered_out = 0
    for row in discovered:
        enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment", {}), dict) else {}

        if not enrichment or enrichment.get("error"):
            filtered_out += 1
            continue

        tier = _quality_tier(row, enrichment)
        if tier is None:
            filtered_out += 1
            continue

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
            "notes": f"[{row.get('source', 'source')}] {row.get('headline', '')} | {row.get('url', '')}",
            "tags": f"daily_poll,{segment},nationwide,{STRICT_POOL_TAG if tier == 'strict' else REVIEW_POOL_TAG},quality_tier={tier}",
        }

        save_lead(lead_payload)
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

    return {
        "segment": segment,
        "pulled": len(discovered),
        "saved": saved_count,
        "strict_saved": strict_saved,
        "review_saved": review_saved,
        "urgency_updated": urgency_updated,
        "filtered_out": filtered_out,
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


async def run_daily_lead_poll(segment: str = "all") -> dict:
    """Pull fresh external signals, save leads, enrich, and update urgency fields."""
    if segment == "all":
        segments = list(NATIONWIDE_SEGMENTS)
        deleted_old = _clear_quality_pools()
        per_segment = []
        totals = {"pulled": 0, "saved": 0, "strict_saved": 0, "review_saved": 0, "urgency_updated": 0, "filtered_out": 0}

        for seg in segments:
            result = await _pull_and_save_segment(seg, max_per_query=12)
            per_segment.append(result)
            totals["pulled"] += int(result["pulled"])
            totals["saved"] += int(result["saved"])
            totals["strict_saved"] += int(result.get("strict_saved", 0))
            totals["review_saved"] += int(result.get("review_saved", 0))
            totals["urgency_updated"] += int(result["urgency_updated"])
            totals["filtered_out"] += int(result["filtered_out"])

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
            "filtered_out": totals["filtered_out"],
            "deleted_previous_pool": deleted_old,
            "polled_at": datetime.now().isoformat(),
        }

    single = await _pull_and_save_segment(segment, max_per_query=10)
    return {
        "ok": True,
        "segment": segment,
        "pulled": single["pulled"],
        "saved": single["saved"],
        "strict_saved": single.get("strict_saved", 0),
        "review_saved": single.get("review_saved", 0),
        "urgency_updated": single["urgency_updated"],
        "filtered_out": single["filtered_out"],
        "polled_at": datetime.now().isoformat(),
    }


async def _scheduled_daily_poll_job():
    try:
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
        trigger=CronTrigger(hour=9, minute=0, timezone=tz),
        id="daily_lead_poll_9am_et",
        replace_existing=True,
    )
    _leads_scheduler.start()
    _scheduler_started = True


@app.on_event("startup")
async def startup():
    init_db()
    _start_daily_poll_scheduler()


# ─── Search ──────────────────────────────────────────────────────────

@app.get("/api/search/labs")
async def search_labs(
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    taxonomy: Optional[str] = Query(None),
    zip_code: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    try:
        results = await search_npi(
            state=state, city=city, organization_name=name,
            taxonomy_description=taxonomy or "laboratory",
            postal_code=zip_code, limit=limit, skip=skip,
        )
        log_search("npi_search", json.dumps({"state": state, "city": city, "name": name}), results["result_count"])
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search/bulk")
async def bulk_search(
    states: str = Query(...),
    limit_per_state: int = Query(50, ge=1, le=200),
):
    state_list = [s.strip().upper() for s in states.split(",") if s.strip()]
    if len(state_list) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 states")
    try:
        results = await bulk_search_labs(state_list, limit_per_state)
        log_search("bulk_search", json.dumps({"states": state_list}), results["result_count"])
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class AILeadFindRequest(BaseModel):
    needs: str
    state: Optional[str] = None
    city: Optional[str] = None
    limit: int = 50
    strict: bool = True
    min_ai_match_score: int = 55
    require_multi_service: bool = True


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


@app.post("/api/leads/ai-find")
async def ai_find_leads(req: AILeadFindRequest):
    """
    AI-guided lead finder for service-specific prospecting.
    Uses intent parsing + enrichment scoring to return leads likely needing:
    revenue cycle, credentialing, and compliance workflow support.
    """
    limit = max(1, min(int(req.limit or 50), 100))
    intent = _parse_ai_intent(req.needs)
    requested_services = intent.get("requested_services", ["billing", "credentialing", "compliance_workflow"])

    try:
        search = await search_npi(
            state=(req.state or None),
            city=(req.city or None),
            organization_name=None,
            taxonomy_description="laboratory",
            postal_code=None,
            limit=limit,
            skip=0,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI lead search failed: {e}")

    results = search.get("results", [])
    if not results:
        return {
            "leads": [],
            "count": 0,
            "requested_services": requested_services,
            "intent_source": intent.get("notes", "fallback"),
            "message": "No labs found for the selected geography.",
        }

    enrich_items = [
        {
            "npi": row.get("npi", ""),
            "org_name": row.get("organization_name", ""),
            "state": row.get("state", ""),
            "city": row.get("city", ""),
        }
        for row in results
        if row.get("npi")
    ]

    enriched = await enrich_leads_bulk(enrich_items)
    enrich_map = {e.get("npi"): e for e in enriched if isinstance(e, dict) and e.get("npi")}

    ranked = []
    for row in results:
        npi = row.get("npi")
        enrichment = enrich_map.get(npi, {})
        match_score, matched_services, match_reasons, overall_service_score, priority = _service_matches(enrichment, requested_services)
        if not matched_services:
            continue

        phone = (row.get("phone") or "").strip()
        has_phone = bool(phone and phone not in {"—", "N/A", "na"})
        has_reason_evidence = len(match_reasons) >= 2
        multi_service_match = len(matched_services) >= 2

        is_qualified = (
            match_score >= max(0, min(100, int(req.min_ai_match_score or 55)))
            and has_reason_evidence
            and has_phone
            and (multi_service_match if req.require_multi_service else True)
        )

        if req.strict and not is_qualified:
            continue

        merged = dict(row)
        merged["ai_match_score"] = match_score
        merged["ai_matched_services"] = matched_services
        merged["ai_match_reasons"] = match_reasons
        merged["ai_priority"] = priority
        merged["ai_overall_service_score"] = overall_service_score
        merged["ai_has_phone"] = has_phone
        merged["ai_qualified"] = is_qualified
        merged["ai_pipeline_reason"] = (
            "Qualified: strong pain signals and outreach-ready contact"
            if is_qualified else
            "Not qualified: weak score or insufficient evidence/contactability"
        )
        merged["enrichment"] = enrichment
        ranked.append(merged)

    ranked.sort(
        key=lambda x: (
            1 if x.get("ai_qualified") else 0,
            x.get("ai_match_score", 0),
            x.get("ai_overall_service_score", 0),
            x.get("lead_score", 0),
        ),
        reverse=True,
    )

    total = ranked[:limit]
    qualified_count = sum(1 for row in total if row.get("ai_qualified"))

    return {
        "leads": total,
        "count": len(total),
        "qualified_count": qualified_count,
        "requested_services": requested_services,
        "intent_source": intent.get("notes", "fallback"),
        "strict_mode": bool(req.strict),
        "message": "No qualified buyer-intent leads found; broaden geography or disable strict mode." if not ranked else "OK",
    }


@app.get("/api/search/taxonomy")
async def search_by_taxonomy(
    code: str = Query(...),
    state: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    try:
        return await search_npi_by_taxonomy(code, state, limit, skip)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/npi/{npi}")
async def npi_detail(npi: str):
    try:
        result = await get_npi_detail(npi)
        if not result:
            raise HTTPException(status_code=404, detail="NPI not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
):
    leads = get_saved_leads(status=status, state=state, min_score=min_score)
    if urgency_level:
        leads = [row for row in leads if (row.get("urgency_level", "").lower() == urgency_level.lower())]
    if urgent_only:
        leads = [row for row in leads if int(row.get("urgency_score", 0) or 0) >= 62]
    return {"leads": leads, "count": len(leads)}


@app.post("/api/leads/poll-daily")
async def poll_leads_now(segment: str = Query("all", description="all|laboratory|urgent_care|primary_care|asc")):
    """Manual trigger for daily polling and urgency updates (same logic as 9 AM scheduler)."""
    try:
        return await run_daily_lead_poll(segment=segment)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Daily poll failed: {e}")


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

class EnrichRequest(BaseModel):
    npi: str
    org_name: Optional[str] = ""
    state: Optional[str] = ""
    city: Optional[str] = ""


class BulkEnrichItem(BaseModel):
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
    min_score: int = Query(0, ge=0, le=100),
    service: Optional[str] = Query(None, description="Filter: Billing Services, Payor Contracting, Workflow Support"),
):
    """List all enriched leads with optional filters."""
    enrichments = get_all_enrichments(min_overall=min_score, service_filter=service)
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


# ─── Frontend ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_leads_frontend():
    with open(os.path.join(os.path.dirname(__file__), "templates", "index.html"), "r") as f:
        return f.read()
