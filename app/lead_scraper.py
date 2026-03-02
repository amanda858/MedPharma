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

SEGMENT_QUERIES = {
    "laboratory": [
        "clinical laboratory denied claims",
        "laboratory credentialing issues",
        "lab compliance audit findings",
        "laboratory revenue cycle backlog",
        "site:linkedin.com/jobs laboratory billing specialist",
    ],
    "urgent_care": [
        "urgent care denied claims",
        "urgent care credentialing delays",
        "urgent care compliance citation",
        "urgent care revenue cycle improvement",
        "site:linkedin.com/jobs urgent care billing manager",
    ],
    "primary_care": [
        "primary care billing denials",
        "primary care payer credentialing issues",
        "primary care compliance workflow",
        "family practice reimbursement problems",
        "site:linkedin.com/jobs primary care revenue cycle",
    ],
    "asc": [
        "ambulatory surgery center denied claims",
        "asc credentialing backlog",
        "ambulatory surgery center compliance issues",
        "asc payer contracting challenges",
        "site:linkedin.com/jobs ambulatory surgery center billing",
    ],
}

HEALTHCARE_KEYWORDS = [
    "lab", "laboratory", "diagnostics", "pathology", "clinical testing", "reference lab",
    "urgent care", "primary care", "family practice", "medical group", "clinic",
    "ambulatory surgery", "surgery center", "asc",
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
REDDIT_SEARCH_API = "https://www.reddit.com/search.json"
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
ADZUNA_API = "https://api.adzuna.com/v1/api/jobs/us/search/1"

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
                    "source": "news_rss",
                })
        return leads
    except Exception:
        return []


async def scrape_reddit_leads(query: str, max_results: int = 10) -> List[Dict]:
    """Track social intent from Reddit discussions."""
    params = {
        "q": query,
        "sort": "new",
        "limit": max_results,
        "t": "month",
    }
    headers = {"User-Agent": "MedPharmaLeadScout/1.0"}

    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True, headers=headers) as client:
            resp = await client.get(REDDIT_SEARCH_API, params=params)
            if resp.status_code != 200:
                return []
            data = resp.json()

        leads = []
        children = (((data or {}).get("data") or {}).get("children") or [])
        for item in children[:max_results]:
            payload = (item or {}).get("data") or {}
            title = payload.get("title", "")
            text = payload.get("selftext", "")
            headline = f"{title} {text[:180]}".strip()
            org_name, city, state = extract_healthcare_org_info(headline)
            if not org_name:
                continue
            permalink = payload.get("permalink", "")
            leads.append({
                "org_name": org_name,
                "city": city,
                "state": state,
                "headline": title,
                "url": f"https://reddit.com{permalink}" if permalink else "https://reddit.com",
                "signal": query,
                "source": "reddit",
                "subreddit": payload.get("subreddit", ""),
            })
        return leads
    except Exception:
        return []


async def scrape_job_need_signals(query: str, max_results: int = 10) -> List[Dict]:
    """Track buyer intent from hiring demand (billing/credentialing/compliance roles)."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        return []

    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": max_results,
        "what": query,
        "content-type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(ADZUNA_API, params=params)
            if resp.status_code != 200:
                return []
            data = resp.json()

        leads = []
        for job in (data or {}).get("results", [])[:max_results]:
            company = (job.get("company") or {}).get("display_name", "")
            if not company:
                continue
            loc = (job.get("location") or {}).get("display_name", "")
            state_match = re.search(r",\s*([A-Z]{2})\b", loc or "")
            state = state_match.group(1) if state_match else ""
            headline = job.get("title", "")
            org_name, city, extracted_state = extract_healthcare_org_info(f"{company} {headline} {loc}")
            if not org_name:
                org_name = company
            leads.append({
                "org_name": org_name,
                "city": city,
                "state": extracted_state or state,
                "headline": headline,
                "url": job.get("redirect_url", ""),
                "signal": query,
                "source": "jobs",
                "job_company": company,
            })
        return leads
    except Exception:
        return []


def extract_healthcare_org_info(text: str) -> tuple:
    """
    Simple NLP to extract lab name, city, state from headline.
    """
    # Example: "Quest Diagnostics opens new lab in Dallas, TX"
    m = re.search(
        r"([A-Za-z0-9 .&'-]+?)\s+(?:lab|laboratory|diagnostics|urgent care|primary care|ambulatory surgery center|surgery center|clinic|medical group)\b(?:.*?\bin\s+([A-Za-z .'-]+),\s*([A-Z]{2}))?",
        text,
        re.IGNORECASE,
    )
    if m:
        org = (m.group(1) or "").strip()
        city = (m.group(2) or "").strip() if m.lastindex and m.lastindex >= 2 else ""
        state = (m.group(3) or "").strip().upper() if m.lastindex and m.lastindex >= 3 else ""
        return org, city, state
    m2 = re.search(
        r"([A-Za-z0-9 .&'-]+?)\s+(Diagnostics|Laboratory|Lab|Urgent Care|Primary Care|Clinic|Medical Group|Ambulatory Surgery Center|Surgery Center|ASC)",
        text,
        re.IGNORECASE,
    )
    if m2:
        return m2.group(0).strip(), "", ""
    return "", "", ""


def _text_relevance_score(headline: str, query: str) -> dict:
    """Score how likely this item is a true client lead needing services."""
    text = f"{headline} {query}".lower()

    score = 0
    matched = []

    if any(k in text for k in HEALTHCARE_KEYWORDS):
        score += 25
        matched.append("healthcare-context")

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
    has_healthcare_context = any(k in text for k in HEALTHCARE_KEYWORDS)
    has_negative = any(k in text for k in NEGATIVE_CONTEXT)
    return has_healthcare_context and score >= 25 and not (has_negative and score < 45)


def _build_queries(segment: str) -> List[str]:
    key = (segment or "all").strip().lower()
    if key in SEGMENT_QUERIES:
        return SEGMENT_QUERIES[key]

    combined = []
    for values in SEGMENT_QUERIES.values():
        combined.extend(values)
    return combined


async def discover_national_leads(
    max_per_query: int = 10,
    segment: str = "all",
    include_news: bool = True,
    include_reddit: bool = True,
    include_jobs: bool = True,
) -> List[Dict]:
    """
    Run all queries, aggregate unique leads.
    """
    all_leads = []
    seen = set()
    queries = _build_queries(segment)
    for q in queries:
        source_batches = []
        if include_news:
            source_batches.append(scrape_news_leads(q, max_results=max_per_query))
        if include_reddit:
            source_batches.append(scrape_reddit_leads(q, max_results=max_per_query))
        if include_jobs:
            source_batches.append(scrape_job_need_signals(q, max_results=max_per_query))

        if not source_batches:
            continue

        results = await asyncio.gather(*source_batches, return_exceptions=True)
        flattened = []
        for batch in results:
            if isinstance(batch, Exception):
                continue
            flattened.extend(batch)

        for lead in flattened:
            ai = _text_relevance_score(lead.get("headline", ""), q)
            lead.update(ai)
            if not _looks_like_real_lead(lead.get("headline", ""), lead.get("signal_score", 0)):
                continue
            key = (lead["org_name"], lead["city"], lead["state"], lead.get("source", ""))
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


async def run_national_lead_pull(
    segment: str = "all",
    max_per_query: int = 8,
    include_news: bool = True,
    include_reddit: bool = True,
    include_jobs: bool = True,
):
    """
    Full pipeline: discover, enrich, return leads needing help.
    """
    leads = await discover_national_leads(
        max_per_query=max_per_query,
        segment=segment,
        include_news=include_news,
        include_reddit=include_reddit,
        include_jobs=include_jobs,
    )
    enriched = await enrich_discovered_leads(leads)
    # Combined AI + enrichment threshold so results don't collapse to zero
    filtered = [
        l for l in enriched
        if l.get("overall_priority_score", 0) >= 35
        or l.get("signal_score", 0) >= 45
    ]

    filtered.sort(key=lambda x: x.get("overall_priority_score", x.get("signal_score", 0)), reverse=True)
    return filtered[:50]
