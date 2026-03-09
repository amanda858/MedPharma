import sys
import os
import asyncio
sys.path.append('.')

from app.config import HUNTER_API_KEY
from app.database import init_db, save_lead, save_lead_emails
from app.email_finder import find_emails_for_lab
from app.lead_scraper import run_national_lead_pull
from app.npi_client import bulk_search_labs

FALLBACK_STATES = ["TX", "CA", "FL", "NY", "PA", "OH"]

async def _scheduled_daily_lead_pull():
    try:
        leads = await run_national_lead_pull(segment="all", max_per_query=50, include_news=True, include_reddit=True, include_jobs=True)
        if not leads:
            # Fallback to real NPI registry organizations when web signals are sparse.
            npi_batch = await bulk_search_labs(FALLBACK_STATES, limit_per_state=12)
            fallback = []
            for item in npi_batch.get("results", [])[:60]:
                fallback.append({
                    "npi": item.get("npi", ""),
                    "org_name": item.get("organization_name", ""),
                    "city": item.get("city", ""),
                    "state": item.get("state", ""),
                    "source": "npi_registry",
                    "signal": "NPI organization registry match",
                    "signal_score": int(item.get("lead_score", 0) or 0),
                    "overall_priority_score": int(item.get("lead_score", 0) or 0),
                    "taxonomy_desc": item.get("taxonomy_desc", ""),
                })
            leads = fallback

        print(f"Pulled {len(leads)} leads")
        saved_count = 0
        email_count = 0
        for lead in leads:
            if lead.get('overall_priority_score', 0) >= 55:  # Keep quality bias but avoid empty output
                npi = lead.get('npi', '')
                if npi and not str(npi).startswith('DISC-'):  # Only real NPIs
                    org_name = lead.get('org_name', '')
                    city = lead.get('city', '')
                    state = lead.get('state', '')
                    source = f"auto_scraper_{lead.get('source', 'unknown')}"
                    notes = f"Auto-discovered high-priority lead: {lead.get('signal', '')} | Score: {lead['overall_priority_score']}"
                    lead_payload = {
                        "npi": npi,
                        "organization_name": org_name,
                        "city": city,
                        "state": state,
                        "taxonomy_desc": lead.get('taxonomy_desc', ''),
                        "lead_score": int(lead.get('overall_priority_score', 0) or 0),
                        "lead_status": "new",
                        "notes": notes,
                        "tags": "daily_runner,nationwide,quality_tier=review,need_signal=yes,need_signal_source=direct",
                        "source": source,
                    }
                    try:
                        save_lead(lead_payload)
                    except Exception as save_err:
                        if "no column named source" in str(save_err).lower():
                            lead_payload.pop("source", None)
                            save_lead(lead_payload)
                        else:
                            raise
                    saved_count += 1
                    print(f"Saved lead: {org_name}")

                    # Try email enrichment regardless of Hunter key; finder falls back to scraping.
                    try:
                        email_result = await find_emails_for_lab(org_name)
                        found = email_result.get("emails", []) if isinstance(email_result, dict) else []
                        if found:
                            saved_email_count = save_lead_emails(npi, found)
                            email_count += int(saved_email_count or 0)
                            print(f"Found {saved_email_count} quality emails for {org_name}")
                    except Exception as e:
                        print(f"Email finding failed for {npi}: {e}")
        print(f"Daily lead pull completed! Saved {saved_count} leads, found {email_count} emails")
    except Exception as e:
        print(f"Daily lead pull failed: {e}")

async def main():
    init_db()
    print("Running daily lead pull now...")
    await _scheduled_daily_lead_pull()

if __name__ == "__main__":
    asyncio.run(main())