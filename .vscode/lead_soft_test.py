import asyncio
import json
import os
import sys
from collections import Counter
from typing import List, Dict

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.lead_scraper import run_national_lead_pull
from app.npi_client import search_npi
from app.enrichment import enrich_lead
from app.leads_app import _quality_tier, _extract_need_signal, _infer_service_needs_from_text

SEGMENTS = ["laboratory", "urgent_care", "primary_care", "asc", "hospital", "clinic", "diagnostic"]
NATIONWIDE_STATES = ["TX", "CA", "FL", "NY", "PA", "OH", "GA", "NC", "MI", "IL"]

NPI_TAXONOMY_HINT = {
    "laboratory": "laboratory",
    "urgent_care": "urgent care",
    "primary_care": "family medicine",
    "asc": "ambulatory surgery",
    "hospital": "hospital",
    "clinic": "clinic",
    "diagnostic": "radiology",
}


async def _fallback_npi_segment(segment: str, limit_per_state: int = 8) -> List[Dict]:
    taxonomy = NPI_TAXONOMY_HINT.get(segment, "laboratory")
    tasks = [
        search_npi(state=st, taxonomy_description=taxonomy, limit=limit_per_state)
        for st in NATIONWIDE_STATES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    rows = []
    seen_npi = set()
    for res in results:
        if isinstance(res, Exception):
            continue
        for item in (res or {}).get("results", []):
            npi = str(item.get("npi", "") or "").strip()
            if not npi or npi in seen_npi:
                continue
            seen_npi.add(npi)
            rows.append({
                "segment": segment,
                "source": "npi_registry",
                "org_name": item.get("organization_name", ""),
                "city": item.get("city", ""),
                "state": item.get("state", ""),
                "phone": item.get("phone", ""),
                "npi": npi,
                "headline": f"NPI provider match: {item.get('organization_name', '')}",
                "signal_score": int(item.get("lead_score", 0) or 0),
                "overall_priority_score": int(item.get("lead_score", 0) or 0),
                "taxonomy_desc": item.get("taxonomy_desc", ""),
                "enrichment": {},
            })
    return rows


async def _enrich_subset(rows: List[Dict], cap: int = 20) -> None:
    subset = rows[:cap]
    tasks = [
        enrich_lead(
            npi=str(r.get("npi", "") or ""),
            org_name=r.get("org_name", ""),
            state=r.get("state", ""),
            city=r.get("city", ""),
        )
        for r in subset
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for row, enr in zip(subset, results):
        if isinstance(enr, Exception):
            continue
        if isinstance(enr, dict):
            row["enrichment"] = enr
            sn = enr.get("service_needs", {}) if isinstance(enr.get("service_needs", {}), dict) else {}
            enrich_score = int(sn.get("overall_score", 0) or 0)
            row["overall_priority_score"] = int(round((int(row.get("signal_score", 0) or 0) * 0.6) + (enrich_score * 0.4)))


async def pull_segment(segment: str):
    try:
        rows = await asyncio.wait_for(
            run_national_lead_pull(
                segment=segment,
                max_per_query=2,
                include_news=True,
                include_reddit=True,
                include_jobs=True,
            ),
            timeout=40,
        )
        fallback_rows = await asyncio.wait_for(_fallback_npi_segment(segment), timeout=45)

        merged = []
        seen = set()
        for item in rows + fallback_rows:
            key = str(item.get("npi", "") or "").strip() or (
                f"{item.get('org_name','')}|{item.get('city','')}|{item.get('state','')}|{item.get('source','')}"
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)

        rows = merged
        return {"segment": segment, "ok": True, "rows": rows, "error": None}
    except Exception as exc:
        try:
            fallback_rows = await asyncio.wait_for(_fallback_npi_segment(segment), timeout=45)
            return {"segment": segment, "ok": True, "rows": fallback_rows, "error": None}
        except Exception as fallback_exc:
            return {"segment": segment, "ok": False, "rows": [], "error": f"{exc} | fallback={fallback_exc}"}


async def main():
    results = []
    for segment in SEGMENTS:
        results.append(await pull_segment(segment))

    all_rows = []
    segment_counts = {}
    segment_errors = {}

    for res in results:
        segment_counts[res["segment"]] = len(res["rows"])
        if not res["ok"]:
            segment_errors[res["segment"]] = res["error"]
        all_rows.extend(res["rows"])

    await _enrich_subset(all_rows, cap=25)

    service_counter = Counter()
    with_phone = 0
    with_official = 0
    poc_rows = []
    strict_rows = []
    review_rows = []

    for row in all_rows:
        enrichment = row.get("enrichment", {}) if isinstance(row.get("enrichment", {}), dict) else {}
        sn = enrichment.get("service_needs", {}) if isinstance(enrichment.get("service_needs", {}), dict) else {}
        requested = sn.get("services_needed", []) if isinstance(sn.get("services_needed", []), list) else []

        # Mirror production sparse-enrichment fallback so test output matches real gate behavior.
        if not requested:
            inferred = _infer_service_needs_from_text(row, row.get("segment", "all"))
            if inferred:
                enrichment["service_needs"] = inferred
                sn = inferred
                requested = inferred.get("services_needed", []) if isinstance(inferred.get("services_needed", []), list) else []
            elif row.get("source") in {"news_rss", "reddit", "jobs"}:
                enrichment["service_needs"] = {
                    "overall_score": 42,
                    "billing_score": 45,
                    "payor_score": 40,
                    "workflow_score": 42,
                    "services_needed": ["Billing Services", "Payor Contracting"],
                }
                sn = enrichment["service_needs"]
                requested = sn.get("services_needed", [])

        if requested:
            service_counter.update(requested)

        phone = (row.get("phone") or "").strip()
        if phone and phone not in {"—", "N/A", "na"}:
            with_phone += 1

        auth = enrichment.get("authorized_official", {}) if isinstance(enrichment.get("authorized_official", {}), dict) else {}
        has_official = bool((auth.get("first_name") or "").strip() or (auth.get("last_name") or "").strip())
        if has_official:
            with_official += 1

        score = int(row.get("overall_priority_score", row.get("signal_score", 0)) or 0)
        overall_service = int(sn.get("overall_score", 0) or 0)
        billing = int(sn.get("billing_score", 0) or 0)
        payor = int(sn.get("payor_score", 0) or 0)
        workflow = int(sn.get("workflow_score", 0) or 0)

        tier = _quality_tier(row, enrichment)
        has_need_signal, _, need_signal_source = _extract_need_signal(
            row,
            enrichment,
            segment=row.get("segment", "all"),
        )
        if not has_need_signal:
            tier = None

        # Mirror production review->strict promotion logic.
        if tier == "review":
            npi_text = str(enrichment.get("npi") or row.get("npi") or "").strip()
            has_valid_npi = npi_text.isdigit() and len(npi_text) == 10
            has_phone = bool(phone and phone not in {"—", "N/A", "na"})
            if (
                need_signal_source == "direct"
                and score >= 65
                and len(requested) >= 2
                and overall_service >= 50
                and (has_phone or has_official or has_valid_npi)
            ):
                tier = "strict"

        is_strict = tier == "strict"
        is_review = tier == "review"

        if is_strict:
            strict_rows.append(row)
        elif is_review:
            review_rows.append(row)

        poc_rows.append({
            "org": row.get("org_name", ""),
            "segment": row.get("segment", ""),
            "city": row.get("city", ""),
            "state": row.get("state", ""),
            "phone": phone,
            "service_requests": requested,
            "official_first": auth.get("first_name", ""),
            "official_last": auth.get("last_name", ""),
            "official_title": auth.get("title", ""),
            "score": score,
            "need_signal_source": need_signal_source,
            "strict_quality": is_strict,
            "review_quality": (not is_strict and is_review),
        })

    poc_rows.sort(key=lambda x: x.get("score", 0), reverse=True)

    out = {
        "total_leads": len(all_rows),
        "strict_quality_leads": len(strict_rows),
        "review_quality_leads": len(review_rows),
        "segment_counts": segment_counts,
        "segment_errors": segment_errors,
        "service_request_counts": dict(service_counter),
        "points_of_contact": {
            "with_phone": with_phone,
            "with_authorized_official": with_official,
        },
        "top_poc_samples": [row for row in poc_rows if row.get("strict_quality") or row.get("review_quality")][:12],
    }

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
