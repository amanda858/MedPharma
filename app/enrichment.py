"""
Client Intelligence Enrichment — pulls public data to identify
prospects needing billing services, payor contracting & workflow support.

Data sources (all free, no API key):
  1. CMS CLIA Database    — lab certification, test complexity, compliance
  2. CMS NPPES NPI        — provider details, taxonomy, multi-site detection
  3. CMS Medicare Data    — enrollment status, participation, claims indicators
  4. CMS Open Data        — provider utilization & payment data
  5. SAM.gov              — govt contracting status / exclusions

Service-need signals:
  - BILLING:     high test volume, multiple taxonomies, recent enumeration
  - PAYOR:       limited payor participation, new labs, multi-state presence
  - WORKFLOW:    multiple locations, high staff count, complex test menus
"""

import re
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional


# ─── CMS / Public API endpoints (all free) ──────────────────────────

CLIA_API = "https://data.cms.gov/data-api/v1/dataset/c98df97a-a391-4e57-90e9-17fba0a584de/data"
NPI_API = "https://npiregistry.cms.hhs.gov/api/"
MEDICARE_PROVIDER_API = "https://data.cms.gov/data-api/v1/dataset/4d18e898-bbb7-4e5e-8e7b-8d7924c1ef30/data"  # Provider enrollment
PROVIDER_UTILIZATION_API = "https://data.cms.gov/data-api/v1/dataset/8889d81e-2ee1-5e53-97a6-2b4c7f26a9e3/data"  # Utilization

HTTP_TIMEOUT = 25.0


# ─── CLIA Enrichment ────────────────────────────────────────────────

async def fetch_clia_data(org_name: str, state: str = None, city: str = None) -> dict:
    """
    Pull CLIA lab certification data from CMS.
    Returns cert type, test complexity, compliance status, cert dates.
    """
    params = {"keyword": org_name, "size": 5}
    if state:
        params["filter[prvdr_state_cd]"] = state.upper()
    if city:
        params["filter[prvdr_city_nm]"] = city.upper()

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(CLIA_API, params=params)
            if resp.status_code != 200:
                return {"source": "clia", "found": False, "error": f"HTTP {resp.status_code}"}
            data = resp.json()

        if not data:
            return {"source": "clia", "found": False, "labs": []}

        labs = []
        for rec in data[:5]:
            lab = {
                "clia_number": rec.get("prvdr_num", ""),
                "facility_name": rec.get("fac_name", rec.get("prvdr_name", "")),
                "cert_type": rec.get("gnrl_cntl_type_cd", ""),
                "cert_type_desc": _clia_cert_type(rec.get("gnrl_cntl_type_cd", "")),
                "test_complexity": _clia_complexity(rec),
                "state": rec.get("prvdr_state_cd", ""),
                "city": rec.get("prvdr_city_nm", ""),
                "zip": rec.get("prvdr_zip_cd", ""),
                "cert_effective_date": rec.get("gnrl_cntl_efctv_dt", ""),
                "cert_expiration_date": rec.get("pgm_trmntn_cd", ""),
                "compliance_status": _clia_compliance(rec),
                "last_survey_date": rec.get("lst_srvy_dt", ""),
            }
            labs.append(lab)

        return {
            "source": "clia",
            "found": True,
            "count": len(labs),
            "labs": labs,
        }
    except Exception as e:
        return {"source": "clia", "found": False, "error": str(e)}


def _clia_cert_type(code: str) -> str:
    types = {
        "A": "Compliance — Certificate of Accreditation",
        "C": "Certificate of Compliance",
        "R": "Certificate of Registration",
        "W": "Certificate of Waiver",
        "P": "PPM Certificate (Provider-Performed Microscopy)",
    }
    return types.get(code, f"Unknown ({code})")


def _clia_complexity(rec: dict) -> str:
    """Determine test complexity level from CLIA record."""
    # Check various fields that indicate test levels
    waiver = rec.get("gnrl_cntl_type_cd", "") == "W"
    if waiver:
        return "Waived Only"
    ppm = rec.get("gnrl_cntl_type_cd", "") == "P"
    if ppm:
        return "Moderate (PPM)"
    return "Moderate/High Complexity"


def _clia_compliance(rec: dict) -> str:
    """Assess compliance status."""
    term_code = rec.get("pgm_trmntn_cd", "")
    if term_code and term_code.strip():
        return "Terminated/Inactive"
    return "Active"


# ─── NPI Cross-Reference (multi-location, taxonomy depth) ───────────

async def fetch_npi_deep(npi: str) -> dict:
    """
    Deep NPI lookup — gets all taxonomies, all addresses, other names.
    Identifies multi-location labs, multiple service lines, etc.
    """
    params = {"version": "2.1", "number": npi}

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(NPI_API, params=params)
            if resp.status_code != 200:
                return {"source": "npi_deep", "found": False, "error": f"HTTP {resp.status_code}"}
            data = resp.json()

        count = data.get("result_count", 0)
        if count == 0:
            return {"source": "npi_deep", "found": False}

        record = data["results"][0]
        basic = record.get("basic", {})
        addresses = record.get("addresses", [])
        taxonomies = record.get("taxonomies", [])
        identifiers = record.get("identifiers", [])
        other_names = record.get("other_names", [])
        endpoints = record.get("endpoints", [])

        # Count locations
        practice_locations = [a for a in addresses if a.get("address_purpose") == "LOCATION"]
        mailing_locations = [a for a in addresses if a.get("address_purpose") == "MAILING"]

        # Count unique states from addresses
        states_present = list(set(
            a.get("state", "") for a in addresses if a.get("state")
        ))

        # Taxonomy analysis
        tax_list = []
        for t in taxonomies:
            tax_list.append({
                "code": t.get("code", ""),
                "desc": t.get("desc", ""),
                "primary": t.get("primary", False),
                "state": t.get("state", ""),
                "license": t.get("license", ""),
            })

        # Identifiers (Medicare, Medicaid numbers)
        id_list = []
        for ident in identifiers:
            id_list.append({
                "code": ident.get("code", ""),
                "desc": ident.get("desc", ""),
                "identifier": ident.get("identifier", ""),
                "state": ident.get("state", ""),
                "issuer": ident.get("issuer", ""),
            })

        # Endpoints (direct addresses, URLs)
        ep_list = []
        for ep in endpoints:
            ep_list.append({
                "type": ep.get("endpointType", ""),
                "description": ep.get("endpointTypeDescription", ""),
                "endpoint": ep.get("endpoint", ""),
                "affiliation": ep.get("affiliation", ""),
            })

        return {
            "source": "npi_deep",
            "found": True,
            "npi": npi,
            "organization_name": basic.get("organization_name", ""),
            "authorized_official": {
                "first_name": basic.get("authorized_official_first_name", ""),
                "last_name": basic.get("authorized_official_last_name", ""),
                "title": basic.get("authorized_official_title_or_position", ""),
                "phone": basic.get("authorized_official_telephone_number", ""),
                "credential": basic.get("authorized_official_credential", ""),
            },
            "enumeration_date": basic.get("enumeration_date", ""),
            "last_updated": basic.get("last_updated", ""),
            "status": basic.get("status", ""),
            "deactivation_date": basic.get("deactivation_date", ""),
            "reactivation_date": basic.get("reactivation_date", ""),
            "location_count": len(practice_locations),
            "states_present": states_present,
            "multi_state": len(states_present) > 1,
            "multi_location": len(practice_locations) > 1,
            "taxonomy_count": len(tax_list),
            "taxonomies": tax_list,
            "identifiers": id_list,
            "has_medicare_id": any(i.get("desc", "").lower().find("medicare") >= 0 for i in id_list),
            "has_medicaid_id": any(i.get("desc", "").lower().find("medicaid") >= 0 for i in id_list),
            "other_names": [n.get("organization_name", "") for n in other_names],
            "endpoints": ep_list,
            "addresses": [
                {
                    "purpose": a.get("address_purpose", ""),
                    "line1": a.get("address_1", ""),
                    "line2": a.get("address_2", ""),
                    "city": a.get("city", ""),
                    "state": a.get("state", ""),
                    "zip": a.get("postal_code", ""),
                    "phone": a.get("telephone_number", ""),
                    "fax": a.get("fax_number", ""),
                }
                for a in addresses
            ],
        }
    except Exception as e:
        return {"source": "npi_deep", "found": False, "error": str(e)}


# ─── Medicare Provider Enrollment Check ─────────────────────────────

async def fetch_medicare_enrollment(npi: str) -> dict:
    """
    Check if provider is enrolled in Medicare and participation status.
    Uses CMS Provider Enrollment data.
    """
    params = {"filter[NPI]": npi, "size": 5}

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(MEDICARE_PROVIDER_API, params=params)
            if resp.status_code != 200:
                return {"source": "medicare", "found": False, "error": f"HTTP {resp.status_code}"}
            data = resp.json()

        if not data:
            return {
                "source": "medicare",
                "found": False,
                "enrolled": False,
                "note": "Not found in Medicare enrollment data — may need enrollment assistance",
            }

        records = []
        for rec in data[:5]:
            records.append({
                "enrollment_id": rec.get("ENRLMT_ID", ""),
                "provider_type": rec.get("PRVDR_TYPE_DESC", rec.get("PRVDR_TYPE_CD", "")),
                "state": rec.get("STATE_CD", ""),
                "enrollment_date": rec.get("ENRLMT_EFCTV_DT", ""),
                "reassignment_count": rec.get("REASGNMT_CNT", 0),
                "accepting_new": rec.get("ACCPTG_NEW_PATIENTS", ""),
                "participation": rec.get("MDCR_PARTCPTN_IND", ""),
            })

        return {
            "source": "medicare",
            "found": True,
            "enrolled": True,
            "enrollment_count": len(records),
            "records": records,
        }
    except Exception as e:
        return {"source": "medicare", "found": False, "error": str(e)}


# ─── Service-Needs Scoring Engine ───────────────────────────────────

def score_service_needs(npi_data: dict, clia_data: dict, medicare_data: dict) -> dict:
    """
    Analyze enrichment data to score likelihood of needing:
      - Billing services
      - Payor contracting
      - Workflow support / optimization

    Returns scores 0-100 with specific reasons for each.
    """
    billing_score = 0
    billing_reasons = []
    payor_score = 0
    payor_reasons = []
    workflow_score = 0
    workflow_reasons = []

    # ── BILLING SIGNALS ──────────────────────────────────────────────

    # High-complexity testing = complex billing
    if clia_data.get("found"):
        for lab in clia_data.get("labs", []):
            complexity = lab.get("test_complexity", "")
            if "High" in complexity or "Moderate" in complexity:
                billing_score += 25
                billing_reasons.append(f"Lab performs {complexity.lower()} testing — complex CPT coding required")
                break
            elif "Waived" in complexity:
                billing_score += 10
                billing_reasons.append("Waived testing — simpler billing but still needs proper claim submission")

    # Multiple taxonomies = multiple billing specialties
    if npi_data.get("found"):
        tax_count = npi_data.get("taxonomy_count", 0)
        if tax_count > 2:
            billing_score += 20
            billing_reasons.append(f"{tax_count} taxonomy codes — needs multi-specialty billing expertise")
        elif tax_count > 1:
            billing_score += 10
            billing_reasons.append(f"{tax_count} taxonomy codes — multiple service lines to bill")

    # Medicare enrollment adds billing complexity
    if medicare_data.get("found") and medicare_data.get("enrolled"):
        billing_score += 15
        billing_reasons.append("Medicare enrolled — requires CMS-1500/837P compliance and PAMA pricing rules")
        reassign = sum(r.get("reassignment_count", 0) for r in medicare_data.get("records", []))
        if reassign and int(reassign) > 0:
            billing_score += 10
            billing_reasons.append(f"Provider reassignments detected — complex billing relationships")
    elif medicare_data.get("found") is False:
        billing_score += 15
        billing_reasons.append("NOT enrolled in Medicare — missing major revenue stream, needs enrollment + billing setup")

    # Recently enumerated = new lab, likely needs billing setup
    if npi_data.get("found"):
        enum_date = npi_data.get("enumeration_date", "")
        if enum_date:
            try:
                ed = datetime.strptime(enum_date, "%Y-%m-%d")
                age_years = (datetime.now() - ed).days / 365
                if age_years < 2:
                    billing_score += 20
                    billing_reasons.append(f"New lab (enrolled {enum_date}) — likely needs billing infrastructure")
                elif age_years < 5:
                    billing_score += 10
                    billing_reasons.append(f"Relatively new lab ({enum_date}) — may still be building billing processes")
            except (ValueError, TypeError):
                pass

    # ── PAYOR CONTRACTING SIGNALS ────────────────────────────────────

    # No Medicare enrollment = definitely needs contracting help
    if not medicare_data.get("enrolled"):
        payor_score += 30
        payor_reasons.append("Not enrolled in Medicare — needs payor contracting immediately")

    # CLIA cert type affects payor contracting
    if clia_data.get("found"):
        for lab in clia_data.get("labs", []):
            cert = lab.get("cert_type", "")
            if cert == "W":
                payor_score += 15
                payor_reasons.append("Waiver certificate — limited test menu means fewer payor contracts needed but often underserved")
            elif cert in ("A", "C"):
                payor_score += 20
                payor_reasons.append(f"{lab['cert_type_desc']} — full testing capabilities, needs comprehensive payor network")

    # Multi-state = needs contracting in each state
    if npi_data.get("found"):
        if npi_data.get("multi_state"):
            states = npi_data.get("states_present", [])
            payor_score += 25
            payor_reasons.append(f"Multi-state operation ({', '.join(states)}) — needs payor contracts in each state")
        elif npi_data.get("multi_location"):
            payor_score += 15
            payor_reasons.append(f"{npi_data['location_count']} locations — may need separate payor credentialing per site")

    # New lab = needs to build payor network from scratch
    if npi_data.get("found"):
        enum_date = npi_data.get("enumeration_date", "")
        if enum_date:
            try:
                ed = datetime.strptime(enum_date, "%Y-%m-%d")
                age_years = (datetime.now() - ed).days / 365
                if age_years < 3:
                    payor_score += 25
                    payor_reasons.append(f"Lab is {age_years:.1f} years old — likely still building payor network")
            except (ValueError, TypeError):
                pass

    # No Medicaid ID = missing state Medicaid contracting
    if npi_data.get("found") and not npi_data.get("has_medicaid_id"):
        payor_score += 15
        payor_reasons.append("No Medicaid identifier found — may need state Medicaid enrollment")

    # ── WORKFLOW SUPPORT SIGNALS ─────────────────────────────────────

    # Multi-location = workflow coordination challenges
    if npi_data.get("found"):
        loc_count = npi_data.get("location_count", 0)
        if loc_count > 3:
            workflow_score += 30
            workflow_reasons.append(f"{loc_count} practice locations — significant workflow coordination needed")
        elif loc_count > 1:
            workflow_score += 20
            workflow_reasons.append(f"{loc_count} locations — needs standardized workflows across sites")

    # Multiple taxonomies = diverse service lines needing workflow management
    if npi_data.get("found"):
        tax_count = npi_data.get("taxonomy_count", 0)
        if tax_count > 3:
            workflow_score += 25
            workflow_reasons.append(f"{tax_count} service lines — complex operational workflow")
        elif tax_count > 1:
            workflow_score += 15
            workflow_reasons.append(f"{tax_count} service lines — needs coordinated workflows")

    # High-complexity testing requires robust workflow
    if clia_data.get("found"):
        for lab in clia_data.get("labs", []):
            complexity = lab.get("test_complexity", "")
            if "High" in complexity:
                workflow_score += 25
                workflow_reasons.append("High-complexity testing requires QC protocols, proficiency testing, and SOPs")
                break
            elif "Moderate" in complexity:
                workflow_score += 15
                workflow_reasons.append("Moderate-complexity testing needs documented procedures and quality oversight")
                break

    # CLIA compliance issues = workflow problems
    if clia_data.get("found"):
        for lab in clia_data.get("labs", []):
            if lab.get("compliance_status") == "Terminated/Inactive":
                workflow_score += 20
                workflow_reasons.append("CLIA certification issue detected — needs compliance workflow remediation")

    # Multi-state = regulatory compliance across states
    if npi_data.get("found") and npi_data.get("multi_state"):
        workflow_score += 15
        workflow_reasons.append("Multi-state operations require state-specific regulatory workflow compliance")

    # Other names / DBAs suggest complex operations
    if npi_data.get("found"):
        other_names = npi_data.get("other_names", [])
        if len(other_names) > 1:
            workflow_score += 10
            workflow_reasons.append(f"{len(other_names)} alternate names/DBAs — suggests complex organizational structure")

    # Cap scores at 100
    billing_score = min(billing_score, 100)
    payor_score = min(payor_score, 100)
    workflow_score = min(workflow_score, 100)

    # Overall score is weighted average
    overall = int(billing_score * 0.35 + payor_score * 0.40 + workflow_score * 0.25)

    # Determine priority services
    services_needed = []
    if payor_score >= 40:
        services_needed.append("Payor Contracting")
    if billing_score >= 40:
        services_needed.append("Billing Services")
    if workflow_score >= 40:
        services_needed.append("Workflow Support")

    return {
        "overall_score": overall,
        "billing": {
            "score": billing_score,
            "level": _score_level(billing_score),
            "reasons": billing_reasons,
        },
        "payor_contracting": {
            "score": payor_score,
            "level": _score_level(payor_score),
            "reasons": payor_reasons,
        },
        "workflow": {
            "score": workflow_score,
            "level": _score_level(workflow_score),
            "reasons": workflow_reasons,
        },
        "services_needed": services_needed,
        "priority": _score_level(overall),
        "recommendation": _generate_recommendation(billing_score, payor_score, workflow_score, services_needed),
    }


def _score_level(score: int) -> str:
    if score >= 70:
        return "high"
    elif score >= 40:
        return "medium"
    return "low"


def _generate_recommendation(billing: int, payor: int, workflow: int, services: list) -> str:
    """Generate a human-readable recommendation."""
    if not services:
        return "Low service need detected. This prospect may already have established operations. Consider a discovery call to identify hidden pain points."

    parts = []
    if payor >= 70:
        parts.append("URGENT: This lab has significant payor contracting gaps. Lead with contracting services — help them get credentialed and in-network with major payors to unlock revenue.")
    elif payor >= 40:
        parts.append("This lab could benefit from expanded payor network. Offer a payor contracting audit to identify coverage gaps.")

    if billing >= 70:
        parts.append("Complex billing environment detected. Position RCM services with emphasis on reducing denials and maximizing reimbursement for their test complexity level.")
    elif billing >= 40:
        parts.append("Billing needs identified. Offer a revenue cycle assessment to demonstrate potential improvement areas.")

    if workflow >= 70:
        parts.append("Operational complexity suggests immediate workflow optimization opportunity. Offer process mapping and SOP development.")
    elif workflow >= 40:
        parts.append("Workflow improvements could help streamline their operations. Suggest a workflow assessment.")

    return " ".join(parts) if parts else "Moderate service potential. Schedule a discovery call to assess specific needs."


# ─── Full Enrichment Pipeline ───────────────────────────────────────

async def enrich_lead(
    npi: str,
    org_name: str = "",
    state: str = "",
    city: str = "",
) -> dict:
    """
    Run full enrichment pipeline for a single lead.
    Pulls data from multiple sources concurrently, then scores service needs.
    """
    async def _empty_clia() -> dict:
        return {"source": "clia", "found": False, "labs": []}

    # Run all data fetches concurrently
    npi_task = fetch_npi_deep(npi)
    clia_task = fetch_clia_data(org_name, state=state, city=city) if org_name else _empty_clia()
    medicare_task = fetch_medicare_enrollment(npi)

    npi_data, clia_data, medicare_data = await asyncio.gather(
        npi_task, clia_task, medicare_task,
        return_exceptions=True,
    )

    # Handle exceptions
    if isinstance(npi_data, Exception):
        npi_data = {"source": "npi_deep", "found": False, "error": str(npi_data)}
    if isinstance(clia_data, Exception):
        clia_data = {"source": "clia", "found": False, "error": str(clia_data)}
    if isinstance(medicare_data, Exception):
        medicare_data = {"source": "medicare", "found": False, "error": str(medicare_data)}

    # Score service needs
    service_needs = score_service_needs(npi_data, clia_data, medicare_data)

    return {
        "npi": npi,
        "organization_name": org_name or npi_data.get("organization_name", ""),
        "enriched_at": datetime.now().isoformat(),
        "data_sources": {
            "npi": npi_data,
            "clia": clia_data,
            "medicare": medicare_data,
        },
        "service_needs": service_needs,
        "authorized_official": npi_data.get("authorized_official", {}),
        "location_count": npi_data.get("location_count", 0),
        "multi_state": npi_data.get("multi_state", False),
        "states_present": npi_data.get("states_present", []),
    }


async def enrich_leads_bulk(leads: list[dict], max_concurrent: int = 5) -> list[dict]:
    """
    Enrich multiple leads concurrently with throttling.
    Each lead dict should have: npi, org_name, state (optional), city (optional)
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _enrich_one(lead: dict) -> dict:
        async with semaphore:
            try:
                return await enrich_lead(
                    npi=lead["npi"],
                    org_name=lead.get("org_name", ""),
                    state=lead.get("state", ""),
                    city=lead.get("city", ""),
                )
            except Exception as e:
                return {
                    "npi": lead["npi"],
                    "error": str(e),
                    "service_needs": {"overall_score": 0, "billing": {}, "payor_contracting": {}, "workflow": {}},
                }

    results = await asyncio.gather(*[_enrich_one(lead) for lead in leads], return_exceptions=True)
    output = []
    for lead, result in zip(leads, results):
        if isinstance(result, Exception):
            output.append({"npi": lead["npi"], "error": str(result)})
        else:
            output.append(result)

    return output
