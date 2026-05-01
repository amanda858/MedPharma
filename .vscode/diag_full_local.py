"""Run the FULL pipeline locally — bypasses Render deploy lag.

Scrubs a small CSV through the same code paths the live API uses, then
runs the in-house verifier against every candidate, prints what would
ship to the user.
"""
import asyncio, sys, csv, io
sys.path.insert(0, "/workspaces/CVOPro")

# Force-reimport in case a stale module is loaded
if "app.scrubber" in sys.modules:
    del sys.modules["app.scrubber"]

from app.scrubber import scrub_rows, parse_uploaded

async def main():
    rows = (
        b"organization_name,city,state\n"
        b"Genova Diagnostics,Asheville,NC\n"
        b"Boston Heart Diagnostics,Framingham,MA\n"
        b"Atherotech Inc,Birmingham,AL\n"
        b"Spectra Laboratories,Milpitas,CA\n"
    )
    headers, parsed = parse_uploaded(rows, "test.csv")
    print(f"parsed {len(parsed)} rows")
    result = await scrub_rows(headers, parsed, max_rows=10)
    print(f"\nsummary: {result['summary']}\n")
    if result.get("daily_top_10"):
        print(f"🔥 DAILY TOP 10 (hit these first):")
        for i, r in enumerate(result["daily_top_10"][:5], 1):
            print(f"   {i}. {r['Org Name']} — heat {r.get('Heat Score',0)} ({r.get('Heat Reasons','')})")
        print()
    for r in result["rows"]:
        print(f"=== {r['Org Name']} (verified: {r['Verified Domain'] or '—'}) ===")
        print(f"   🔥 HEAT: {r.get('Heat Score',0)}  [{r.get('Heat Reasons','')}]")
        if r.get("Recency Signal"):
            print(f"   📅 {r['Recency Signal']}")
        print(f"   Tier: {r['Tier']}  Lead Score: {r['Lead Score']}  Fit: {r['Fit Score']}")
        if r.get("Personalized Hook"):
            print(f"   🎯 Hook: {r['Personalized Hook']}")
        if r.get("Decision Maker"):
            print(f"   DM: {r['Decision Maker']} — {r['DM Title']}")
            if r.get("DM Email"):
                print(f"     ✉️  {r['DM Email']}")
        if r.get("Direct Line"):
            print(f"   Direct line: {r['Direct Line']}")
        if r.get("LinkedIn URL"):
            print(f"   🔗 LinkedIn:   {r['LinkedIn URL']}")
        if r.get("Facebook URL"):
            print(f"   📘 Facebook:   {r['Facebook URL']}")
        if r.get("Instagram URL"):
            print(f"   📸 Instagram:  {r['Instagram URL']}")
        if r.get("X / Twitter URL"):
            print(f"   ✖️  X/Twitter:  {r['X / Twitter URL']}")
        if r.get("Facebook Company Page"):
            print(f"   🏢 FB page:    {r['Facebook Company Page']}")
        if r.get("LinkedIn Connection Note"):
            print(f"   📝 LI note:    {r['LinkedIn Connection Note']}")
        if r.get("SMS Template"):
            print(f"   📱 SMS:        {r['SMS Template']}")
        if r.get("Reply: Already Have Biller"):
            print(f"   🛡️  Reply (already have biller): {r['Reply: Already Have Biller'][:140]}...")
        for i in range(1, 6):
            e = r.get(f"Email {i}", "")
            if not e:
                continue
            sc = r.get(f"Email {i} Score", "")
            print(f"   Email {i}: {e}  [score {sc}]")
        if r.get("Scrub Status") and r["Scrub Status"] != "ok":
            print(f"   ⚠️  scrub: {r['Scrub Status']} {r.get('Scrub Error','')}")
        print()

asyncio.run(main())
