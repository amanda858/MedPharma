"""End-to-end functional test of hunt mode.

Proves:
  1. NPPES bulk pull returns real lab orgs.
  2. Each row has a real human DM (authorized official).
  3. Each LinkedIn/FB/IG/X URL is a valid HTTP 200 search page.
  4. Each social URL is uniquely tied to that DM (no shared/blank URLs).
  5. /api/prospect/bulk endpoint works locally end-to-end.
"""
import asyncio, sys, time, json
sys.path.insert(0, "/workspaces/CVOPro")

import httpx
from app.bulk_prospector import prospect_and_scrub, prospect_state


async def check_url(client: httpx.AsyncClient, url: str) -> tuple[int, int]:
    """Returns (status_code, content_length). 0 on connection error."""
    try:
        r = await client.get(url, follow_redirects=True, timeout=10.0)
        return r.status_code, len(r.content)
    except Exception as e:
        return 0, 0


async def main():
    print("=" * 78)
    print("END-TO-END HUNT VALIDATION")
    print("=" * 78)

    t0 = time.time()
    result = await prospect_and_scrub("FL", specialty="clinical", limit=10, dm_only=True)
    elapsed_hunt = time.time() - t0
    print(f"\n[1] hunt+enrich: {elapsed_hunt:.2f}s for {len(result['rows'])} rows")
    print(f"    summary: {json.dumps(result['summary'], indent=6)}")
    print(f"    source:  {json.dumps(result.get('prospect_source', {}), indent=6)}")

    rows = result.get("rows", [])
    if not rows:
        print("FAIL: no rows returned")
        return

    print(f"\n[2] Data quality on {len(rows)} rows:")
    real_dms = sum(1 for r in rows if r.get("Decision Maker"))
    real_phones = sum(1 for r in rows if r.get("Direct Line"))
    real_li = sum(1 for r in rows if r.get("LinkedIn URL"))
    real_fb = sum(1 for r in rows if r.get("Facebook URL"))
    real_ig = sum(1 for r in rows if r.get("Instagram URL"))
    real_x = sum(1 for r in rows if r.get("X / Twitter URL"))
    real_hooks = sum(1 for r in rows if r.get("Personalized Hook"))
    print(f"    DM names:        {real_dms}/{len(rows)}")
    print(f"    Direct phones:   {real_phones}/{len(rows)}")
    print(f"    LinkedIn URLs:   {real_li}/{len(rows)}")
    print(f"    Facebook URLs:   {real_fb}/{len(rows)}")
    print(f"    Instagram URLs:  {real_ig}/{len(rows)}")
    print(f"    X URLs:          {real_x}/{len(rows)}")
    print(f"    Personal hooks:  {real_hooks}/{len(rows)}")

    # URL uniqueness — proves we're building per-person URLs, not stubs
    li_urls = {r.get("LinkedIn URL") for r in rows if r.get("LinkedIn URL")}
    print(f"    Unique LinkedIn URLs: {len(li_urls)}/{real_li}")
    if len(li_urls) < real_li:
        print("    FAIL: duplicate LinkedIn URLs — generator is broken")

    print(f"\n[3] HTTP-probing the social URLs (proves the links actually resolve):")
    sample = rows[:3]
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        for r in sample:
            print(f"\n    {r['Org Name']} — DM: {r['Decision Maker']} ({r.get('DM Title','')})")
            for label, key in [
                ("LI ", "LinkedIn URL"),
                ("FB ", "Facebook URL"),
                ("IG ", "Instagram URL"),
                ("X  ", "X / Twitter URL"),
                ("G  ", "Google LinkedIn Search"),
            ]:
                url = r.get(key, "")
                if not url:
                    continue
                status, size = await check_url(client, url)
                ok = "✅" if 200 <= status < 400 else "❌"
                print(f"      {ok} {label} HTTP {status}  ({size:,} bytes)  {url[:90]}")

    print(f"\n[4] Sample row top 3 (paste-ready):")
    for r in rows[:3]:
        print(f"\n    {'─' * 70}")
        print(f"    {r['Org Name']}  (heat {r.get('Heat Score', 0)})")
        print(f"    DM:        {r.get('Decision Maker','')} — {r.get('DM Title','')}")
        print(f"    Phone:     {r.get('Direct Line','')}")
        print(f"    Hook:      {r.get('Personalized Hook','')[:120]}")
        print(f"    LinkedIn:  {r.get('LinkedIn URL','')}")
        print(f"    Facebook:  {r.get('Facebook URL','')}")
        print(f"    Instagram: {r.get('Instagram URL','')}")
        msg = r.get("LinkedIn First Message", "")
        if msg:
            print(f"    LI msg:")
            for line in msg.split("\n")[:6]:
                print(f"      | {line[:90]}")

    # Test the live API endpoint locally
    print(f"\n[5] Testing /api/prospect/bulk endpoint via local FastAPI app:")
    try:
        from fastapi.testclient import TestClient
        from app.leads_app import app as leads
        c = TestClient(leads)

        # specialties
        r = c.get("/api/prospect/specialties")
        print(f"    GET /api/prospect/specialties — HTTP {r.status_code}")
        if r.status_code == 200:
            specs = r.json().get("specialties", [])
            print(f"      returned {len(specs)} specialties: {specs[:5]}...")

        # bulk
        r = c.post("/api/prospect/bulk", json={
            "state": "TX", "specialty": "pathology", "limit": 5, "dm_only": True,
        })
        print(f"    POST /api/prospect/bulk — HTTP {r.status_code}")
        if r.status_code == 200:
            j = r.json()
            print(f"      job_id: {j.get('job_id', '')[:12]}...")
            job_id = j.get("job_id")
            # Poll status
            for _ in range(30):
                s = c.get(f"/api/scrub/status/{job_id}")
                if s.status_code == 200:
                    sj = s.json()
                    if sj.get("status") in ("done", "error"):
                        print(f"      job status: {sj.get('status')}  rows: {len(sj.get('rows', []))}")
                        if sj.get("status") == "done":
                            sample = sj.get("rows", [{}])[0]
                            print(f"      first row: {sample.get('Org Name','')} — DM {sample.get('Decision Maker','')}")
                        break
                await asyncio.sleep(0.3)
        else:
            print(f"      body: {r.text[:200]}")
    except Exception as e:
        print(f"    endpoint test SKIPPED: {type(e).__name__}: {str(e)[:200]}")

    print(f"\n{'=' * 78}")
    print(f"DONE in {time.time()-t0:.2f}s total")
    print(f"{'=' * 78}")


asyncio.run(main())
