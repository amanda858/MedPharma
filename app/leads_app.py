"""Lab Lead Generation app — runs on LAB_PORT (default 8000)."""

import csv
import io
import json
import os
import asyncio
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel

from app.config import US_STATES, LAB_TAXONOMY_CODES, OPENAI_API_KEY
from app.database import (
    init_db, save_lead, get_saved_leads, update_lead,
    delete_lead, get_lead_stats, log_search,
    save_lead_emails, get_lead_emails, get_all_leads_with_emails,
    save_enrichment, get_enrichment, get_all_enrichments, get_enrichment_stats,
)
from app.npi_client import (
    search_npi, search_npi_by_taxonomy, get_npi_detail, bulk_search_labs,
)
from app.email_finder import find_emails_for_lab
from app.enrichment import enrich_lead, enrich_leads_bulk
from app.lead_scraper import run_national_lead_pull

app = FastAPI(
    title="MedPharma Lab Leads",
    description="Lab Lead Generator — search NPI Registry for clinical lab prospects",
    version="2.0.0",
)


@app.on_event("startup")
async def startup():
    init_db()


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
):
    leads = get_saved_leads(status=status, state=state, min_score=min_score)
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
async def national_lead_pull():
    """
    AI-powered national lead discovery — scrapes web/news for labs needing help,
    enriches, and returns high-need prospects.
    """
    try:
        leads = await run_national_lead_pull()
        return {
            "leads": leads,
            "count": len(leads),
            "source": "news+web",
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
    else:
        results = await search_npi(state=state, limit=200)
        leads = results["results"]

    output = io.StringIO()
    if leads:
        base_fields = [
            "npi", "organization_name", "first_name", "last_name",
            "taxonomy_desc", "address_line1", "city", "state",
            "zip_code", "phone", "fax", "lead_score", "lead_status",
            "enumeration_date", "notes",
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
