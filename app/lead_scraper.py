"""
Lead Scraper — AI-powered web/NLP discovery for national clients needing billing/payor/workflow help.

Sources:
  - News, press releases, state health, CMS, LinkedIn, Google, Twitter
  - Scrapes for signals: new labs, denied claims, compliance issues, payor changes, staff shortages
  - Uses NLP to extract organization names, locations, and pain points
  - Feeds into enrichment pipeline
"""

import os
import re
import asyncio
import httpx
import xml.etree.ElementTree as ET
from typing import List, Dict
from app.enrichment import enrich_lead

SEARCH_QUERIES = [
    "clinical laboratory new opening",
    "lab denied claims",
    "lab compliance issues",
    "lab payor contract terminated",
    "lab staff shortage",
    "lab billing problems",
    "lab workflow optimization",
    "lab expansion",
    "lab acquisition",
    "lab credentialing issues",
]

LAB_KEYWORDS = [
    "lab", "laboratory", "diagnostics", "pathology", "clinical testing", "reference lab",
]

NEGATIVE_CONTEXT = [
    "school", "university", "student", "course", "classroom", "academic program",
]

SERVICE_SIGNALS = {
    "billing": ["denied claims", "revenue cycle", "billing", "claims", "reimbursement", "collections"],
    "payor": ["payor", "payer", "contract", "credentialing", "in-network", "medicare", "medicaid"],
    "workflow": ["staff shortage", "backlog", "turnaround", "workflow", "capacity", "operations"],
    "growth": ["expansion", "acquisition", "opening", "new location", "launch"],
    "compliance": ["compliance", "citation", "audit", "clia", "regulatory", "violation"],
}

GOOGLE_NEWS_API = "https://newsapi.org/v2/everything"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

async def scrape_news_leads(query: str, max_results: int = 10) -> List[Dict]:
    """
    Scrape Google News/NewsAPI for lab-related signals.
    Returns list of dicts: {org_name, city, state, headline, url, signal}
    """
    if NEWS_API_KEY:
        params = {
            "q": query,
            "language": "en",
            "pageSize": max_results,
            "apiKey": NEWS_API_KEY,
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(GOOGLE_NEWS_API, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    leads = []
                    for article in data.get("articles", []):
                        headline = article.get("title", "")
                        url = article.get("url", "")
                        org_name, city, state = extract_lab_info(headline)
                        if org_name:
                            leads.append({
                                "org_name": org_name,
                                "city": city,
                                "state": state,
                                "headline": headline,
                                "url": url,
                                "signal": query,
                            })
                    if leads:
                        return leads
        except Exception:
            pass

    return await scrape_google_news_rss(query, max_results=max_results)


async def scrape_google_news_rss(query: str, max_results: int = 10) -> List[Dict]:
    """Fallback source that requires no API key."""
    params = {
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(GOOGLE_NEWS_RSS, params=params)
            if resp.status_code != 200:
                return []

        root = ET.fromstring(resp.text)
        channel = root.find("channel")
        if channel is None:
            return []

        leads = []
        for item in channel.findall("item")[:max_results]:
            headline = (item.findtext("title") or "").strip()
            url = (item.findtext("link") or "").strip()
            org_name, city, state = extract_lab_info(headline)
            if org_name:
                leads.append({
                    "org_name": org_name,
                    "city": city,
                    "state": state,
                    "headline": headline,
                    "url": url,
                    "signal": query,
                })
        return leads
    except Exception:
        return []


def extract_lab_info(text: str) -> tuple:
    """
    Simple NLP to extract lab name, city, state from headline.
    """
    # Example: "Quest Diagnostics opens new lab in Dallas, TX"
    m = re.search(r"([A-Za-z0-9 .&'-]+) (?:lab|laboratory) (?:in|opens|launches|expands|acquires) ([A-Za-z .'-]+), ([A-Z]{2})", text, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip().upper()
    # Fallback: just org name
    m2 = re.search(r"([A-Za-z0-9 .&'-]+) (Diagnostics|Laboratory|Lab)", text)
    if m2:
        return m2.group(0).strip(), "", ""
    return "", "", ""


def _text_relevance_score(headline: str, query: str) -> dict:
    """Score how likely this item is a true client lead needing services."""
    text = f"{headline} {query}".lower()

    score = 0
    matched = []

    if any(k in text for k in LAB_KEYWORDS):
        score += 25
        matched.append("lab-context")

    for signal, terms in SERVICE_SIGNALS.items():
        hits = [t for t in terms if t in text]
        if hits:
            if signal in ("billing", "payor", "workflow"):
                score += 20
            elif signal in ("compliance", "growth"):
                score += 12
            matched.append(f"{signal}:{', '.join(hits[:2])}")

    if any(neg in text for neg in NEGATIVE_CONTEXT):
        score -= 20
        matched.append("negative-context")

    score = max(0, min(100, score))
    return {"signal_score": score, "matched_signals": matched}


def _looks_like_real_lead(headline: str, score: int) -> bool:
    text = headline.lower()
    has_lab_context = any(k in text for k in LAB_KEYWORDS)
    has_negative = any(k in text for k in NEGATIVE_CONTEXT)
    return has_lab_context and score >= 25 and not (has_negative and score < 45)


async def discover_national_leads(max_per_query: int = 10) -> List[Dict]:
    """
    Run all queries, aggregate unique leads.
    """
    all_leads = []
    seen = set()
    for q in SEARCH_QUERIES:
        leads = await scrape_news_leads(q, max_results=max_per_query)
        for lead in leads:
            ai = _text_relevance_score(lead.get("headline", ""), q)
            lead.update(ai)
            if not _looks_like_real_lead(lead.get("headline", ""), lead.get("signal_score", 0)):
                continue
            key = (lead["org_name"], lead["city"], lead["state"])
            if key not in seen and lead["org_name"]:
                seen.add(key)
                all_leads.append(lead)
    all_leads.sort(key=lambda x: x.get("signal_score", 0), reverse=True)
    return all_leads


async def enrich_discovered_leads(leads: List[Dict]) -> List[Dict]:
    """
    Enrich discovered leads using enrichment pipeline.
    """
    tasks = [
        enrich_lead(
            npi=f"DISC-{abs(hash((lead['org_name'], lead.get('city', ''), lead.get('state', '')))) % 10_000_000_000}",
            org_name=lead["org_name"],
            state=lead.get("state", ""),
            city=lead.get("city", ""),
        )
        for lead in leads
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    output = []
    for lead, res in zip(leads, results):
        if isinstance(res, Exception):
            lead["enrichment_error"] = str(res)
            lead["overall_priority_score"] = lead.get("signal_score", 0)
        else:
            lead["enrichment"] = res
            enrich_score = res.get("service_needs", {}).get("overall_score", 0)
            lead["overall_priority_score"] = int((lead.get("signal_score", 0) * 0.55) + (enrich_score * 0.45))
        output.append(lead)
    return output


async def run_national_lead_pull():
    """
    Full pipeline: discover, enrich, return leads needing help.
    """
    leads = await discover_national_leads()
    enriched = await enrich_discovered_leads(leads)
    # Combined AI + enrichment threshold so results don't collapse to zero
    filtered = [
        l for l in enriched
        if l.get("overall_priority_score", 0) >= 35
        or l.get("signal_score", 0) >= 45
    ]

    filtered.sort(key=lambda x: x.get("overall_priority_score", x.get("signal_score", 0)), reverse=True)
    return filtered[:50]
