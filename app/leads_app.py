"""Lab Lead Generation app — runs on LAB_PORT (default 8000)."""

import csv
import io
import json
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel

from app.config import US_STATES, LAB_TAXONOMY_CODES
from app.database import (
    init_db, save_lead, get_saved_leads, update_lead,
    delete_lead, get_lead_stats, log_search,
    save_lead_emails, get_lead_emails, get_all_leads_with_emails,
)
from app.npi_client import (
    search_npi, search_npi_by_taxonomy, get_npi_detail, bulk_search_labs,
)
from app.email_finder import find_emails_for_lab

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
    with open("app/templates/index.html", "r") as f:
        return f.read()
