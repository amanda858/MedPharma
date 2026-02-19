"""FastAPI application — Lab Lead Generation API."""

import csv
import io
import json
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.config import US_STATES, LAB_TAXONOMY_CODES
from app.database import (
    init_db, save_lead, get_saved_leads, update_lead,
    delete_lead, get_lead_stats, log_search,
)
from app.npi_client import (
    search_npi, search_npi_by_taxonomy, get_npi_detail, bulk_search_labs,
)
from app.client_db import init_client_hub_db
from app.client_routes import router as client_hub_router

app = FastAPI(
    title="MedPharma Hub",
    description="MedPharma Revenue Cycle Management — Client Portal, Credentialing, Enrollment, EDI, and Claims",
    version="2.0.0",
)


@app.on_event("startup")
async def startup():
    init_db()
    init_client_hub_db()


app.include_router(client_hub_router)


# ─── Search Endpoints ────────────────────────────────────────────────

@app.get("/api/search/labs")
async def search_labs(
    state: Optional[str] = Query(None, description="2-letter state code"),
    city: Optional[str] = Query(None, description="City name"),
    name: Optional[str] = Query(None, description="Organization name"),
    taxonomy: Optional[str] = Query(None, description="Taxonomy description keyword"),
    zip_code: Optional[str] = Query(None, description="ZIP code"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Search for laboratories using NPI Registry."""
    try:
        results = await search_npi(
            state=state,
            city=city,
            organization_name=name,
            taxonomy_description=taxonomy or "laboratory",
            postal_code=zip_code,
            limit=limit,
            skip=skip,
        )

        # Log the search
        params = json.dumps({"state": state, "city": city, "name": name, "taxonomy": taxonomy})
        log_search("npi_search", params, results["result_count"])

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search/bulk")
async def bulk_search(
    states: str = Query(..., description="Comma-separated state codes (e.g., TX,CA,FL)"),
    limit_per_state: int = Query(50, ge=1, le=200),
):
    """Search for labs across multiple states at once."""
    state_list = [s.strip().upper() for s in states.split(",") if s.strip()]
    if len(state_list) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 states per bulk search")

    try:
        results = await bulk_search_labs(state_list, limit_per_state)
        params = json.dumps({"states": state_list})
        log_search("bulk_search", params, results["result_count"])
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search/taxonomy")
async def search_by_taxonomy(
    code: str = Query(..., description="Taxonomy code"),
    state: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Search by specific taxonomy code."""
    try:
        results = await search_npi_by_taxonomy(code, state, limit, skip)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/npi/{npi}")
async def npi_detail(npi: str):
    """Get detailed info for a specific NPI."""
    try:
        result = await get_npi_detail(npi)
        if not result:
            raise HTTPException(status_code=404, detail="NPI not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Lead Management Endpoints ───────────────────────────────────────

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
    """Save a lead to the database."""
    try:
        lead_id = save_lead(lead.model_dump())
        return {"id": lead_id, "message": "Lead saved"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/leads/bulk")
async def save_leads_bulk(leads: list[LeadSave]):
    """Save multiple leads at once."""
    saved = []
    errors = []
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
    """List saved leads with optional filters."""
    leads = get_saved_leads(status=status, state=state, min_score=min_score)
    return {"leads": leads, "count": len(leads)}


@app.put("/api/leads/{lead_id}")
async def update_lead_endpoint(lead_id: int, updates: LeadUpdate):
    """Update a saved lead's status, score, notes, or tags."""
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    update_lead(lead_id, update_data)
    return {"message": "Lead updated"}


@app.delete("/api/leads/{lead_id}")
async def delete_lead_endpoint(lead_id: int):
    """Delete a saved lead."""
    delete_lead(lead_id)
    return {"message": "Lead deleted"}


@app.get("/api/leads/stats")
async def leads_stats():
    """Get dashboard statistics for saved leads."""
    return get_lead_stats()


# ─── Export ───────────────────────────────────────────────────────────

@app.get("/api/export/csv")
async def export_csv(
    source: str = Query("saved", description="'saved' for saved leads, 'search' for search results"),
    state: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
):
    """Export leads as CSV file."""
    if source == "saved":
        leads = get_saved_leads(status=status, state=state)
    else:
        # Export from a search
        results = await search_npi(state=state, limit=200)
        leads = results["results"]

    output = io.StringIO()
    if leads:
        writer = csv.DictWriter(output, fieldnames=[
            "npi", "organization_name", "first_name", "last_name",
            "taxonomy_desc", "address_line1", "city", "state",
            "zip_code", "phone", "fax", "lead_score", "enumeration_date"
        ], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=lab_leads.csv"}
    )


# ─── Reference Data ──────────────────────────────────────────────────

@app.get("/api/ref/states")
async def get_states():
    """Get list of US states."""
    return US_STATES


@app.get("/api/ref/taxonomies")
async def get_taxonomies():
    """Get lab-related taxonomy codes."""
    return LAB_TAXONOMY_CODES


# ─── Frontend ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main dashboard."""
    with open("app/templates/index.html", "r") as f:
        return f.read()


@app.get("/hub", response_class=HTMLResponse)
async def serve_client_hub():
    """Serve the MedPharma Client Hub."""
    with open("app/templates/client_hub.html", "r") as f:
        return f.read()
