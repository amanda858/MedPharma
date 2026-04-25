"""Test the bulk prospector end-to-end — pulls real NPPES data."""
import asyncio, sys
sys.path.insert(0, "/workspaces/CVOPro")
from app.bulk_prospector import prospect_state, prospect_and_scrub, SPECIALTY_KEYWORDS


async def main():
    print("=" * 70)
    print("TEST 1: raw NPPES pull — 20 clinical labs in Florida")
    print("=" * 70)
    rows = await prospect_state("FL", specialty="clinical", limit=20)
    print(f"got {len(rows)} orgs")
    for r in rows[:10]:
        print(f"   • {r['organization_name']}  ({r['city']}, {r['state']})"
              f"   NPI {r['npi']}  enum {r['enumeration_date']}  tax: {r['taxonomy'][:40]}")

    print("\n" + "=" * 70)
    print("TEST 2: new enrollments only — last 90 days, pathology in TX")
    print("=" * 70)
    new_rows = await prospect_state("TX", specialty="pathology", limit=20, new_only=True)
    print(f"got {len(new_rows)} brand-new orgs (enumerated <90d ago)")
    for r in new_rows[:10]:
        print(f"   • {r['organization_name']}  enum {r['enumeration_date']}")

    print("\n" + "=" * 70)
    print("TEST 3: DM-only hunt — 10 clinical labs in NC (no email work)")
    print("=" * 70)
    import time
    t0 = time.time()
    result = await prospect_and_scrub("NC", specialty="clinical", limit=10, dm_only=True)
    elapsed = time.time() - t0
    print(f"summary: {result['summary']}")
    print(f"source:  {result.get('prospect_source')}")
    print(f"elapsed: {elapsed:.2f}s")
    print(f"\n🔥 Top leads:")
    for r in result.get("daily_top_10", [])[:10]:
        print(f"\n   {r['Org Name']} — heat {r.get('Heat Score',0)}")
        print(f"      {r.get('Heat Reasons','')}")
        if r.get("Decision Maker"):
            print(f"      DM: {r['Decision Maker']} ({r.get('DM Title','')})")
        if r.get("Direct Line"):
            print(f"      ☎️  {r['Direct Line']}")
        if r.get("LinkedIn URL"):
            print(f"      🔗 LI: {r['LinkedIn URL'][:80]}...")
        if r.get("Facebook URL"):
            print(f"      📘 FB: {r['Facebook URL'][:80]}...")
        if r.get("Instagram URL"):
            print(f"      📸 IG: {r['Instagram URL'][:80]}...")
        if r.get("Personalized Hook"):
            print(f"      🎯 {r['Personalized Hook'][:140]}")

asyncio.run(main())
