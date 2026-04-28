#!/usr/bin/env python3
"""Run a real lead hunt LOCALLY and write a production-grade CSV.
Use this when Render is slow — same code path, output goes to /tmp.
"""
import asyncio, csv, sys, os, time
sys.path.insert(0, "/workspaces/CVOPro")

from app.bulk_prospector import prospect_state, _enrich_dm_only

STATE = os.environ.get("HUNT_STATE", "FL")
SPECIALTY = os.environ.get("HUNT_SPECIALTY", "clinical")
LIMIT = int(os.environ.get("HUNT_LIMIT", "25"))
OUT = os.environ.get("HUNT_OUT", f"/tmp/leads_{STATE}_{SPECIALTY}_{int(time.time())}.csv")


async def main():
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Pulling NPPES prospects: {STATE} / {SPECIALTY} / limit={LIMIT}")
    prospects = await prospect_state(state=STATE, specialty=SPECIALTY, limit=LIMIT)
    print(f"  got {len(prospects)} raw prospects in {time.time()-t0:.1f}s")
    if not prospects:
        print("NO PROSPECTS — abort"); sys.exit(1)

    t1 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Enriching (LinkedIn resolver + NPPES backup people)...")
    result = await _enrich_dm_only(prospects)
    rows = result.get("rows") or []
    summary = result.get("summary") or {}
    print(f"  enriched in {time.time()-t1:.1f}s   summary={summary}")

    if not rows:
        print("NO ROWS — abort"); sys.exit(1)

    # Sort by Heat Score desc so the user sees the best ones first
    rows.sort(key=lambda r: -(r.get("Heat Score") or 0))

    # Write CSV
    headers = list(rows[0].keys())
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"\n[{time.strftime('%H:%M:%S')}] Wrote {len(rows)} rows -> {OUT}")

    # Print top 5 to console
    print("\n=== TOP 5 LEADS ===")
    for i, r in enumerate(rows[:5], 1):
        dm = r.get("Decision Maker") or "(no DM)"
        title = r.get("DM Title") or ""
        org = r.get("Org Name", "")
        addr = r.get("Address", "")
        city = r.get("City", "")
        state = r.get("State", "")
        phone = r.get("Direct Line") or r.get("Phone") or ""
        em = r.get("DM Email") or ""
        em_c = r.get("DM Email Confidence") or 0
        co_em = r.get("Company Email") or ""
        dom = r.get("Org Domain") or ""
        li = r.get("LinkedIn URL") or ""
        li_search = r.get("LinkedIn Search URL") or ""
        bk = r.get("Backup Contact") or ""
        bk_t = r.get("Backup Title") or ""
        bk_p = r.get("Backup Phone") or ""
        bk_li = r.get("Backup LinkedIn") or r.get("Backup LinkedIn Search URL") or ""
        heat = r.get("Heat Score") or 0
        tier = r.get("Tier") or ""
        print(f"\n#{i}  [Heat {heat} / Tier {tier}]")
        print(f"    Org:    {org}")
        print(f"    Addr:   {addr}, {city} {state}")
        print(f"    DM:     {dm}  ({title})  ph={phone}")
        if em: print(f"    Email:  {em}  (conf={em_c}, domain={dom})")
        if co_em: print(f"    CoMail: {co_em}")
        if li:        print(f"    LI:     {li}")
        elif li_search: print(f"    LI find: {li_search}")
        if bk: print(f"    Backup: {bk}  ({bk_t})  ph={bk_p}")
        if bk_li: print(f"    Bk-LI:  {bk_li}")

    # Counts
    with_dm = sum(1 for r in rows if r.get("Decision Maker"))
    with_phone = sum(1 for r in rows if r.get("Direct Line") or r.get("Phone"))
    with_li = sum(1 for r in rows if r.get("LinkedIn URL"))
    with_li_search = sum(1 for r in rows if r.get("LinkedIn Search URL"))
    with_backup = sum(1 for r in rows if r.get("Backup Contact"))
    with_backup_li_search = sum(1 for r in rows if r.get("Backup LinkedIn Search URL"))
    with_email = sum(1 for r in rows if r.get("DM Email"))
    with_co_email = sum(1 for r in rows if r.get("Company Email"))
    with_pubmed = sum(1 for r in rows if r.get("PubMed Email"))
    with_directory = sum(1 for r in rows if r.get("Directory Email"))
    with_any_email = sum(1 for r in rows if r.get("DM Email") or r.get("Company Email") or r.get("PubMed Email") or r.get("Directory Email"))
    with_clia = sum(1 for r in rows if r.get("CLIA Number"))
    with_clia_accred = sum(1 for r in rows if r.get("CLIA Accreditations"))
    with_clia_fax = sum(1 for r in rows if r.get("CLIA Fax"))
    with_domain = sum(1 for r in rows if r.get("Org Domain"))
    with_verified_email = sum(1 for r in rows if (r.get("DM Email Confidence") or 0) >= 70)
    print(f"\n=== SUMMARY ===")
    print(f"  total rows:                {len(rows)}")
    print(f"  rows w/ DM name:           {with_dm}")
    print(f"  rows w/ phone:             {with_phone}")
    print(f"  rows w/ DM Email (real):   {with_email}")
    print(f"  rows w/ PubMed Email:      {with_pubmed}")
    print(f"  rows w/ Directory Email:   {with_directory}")
    print(f"  rows w/ Company Email:     {with_co_email}")
    print(f"  rows w/ any real email:    {with_any_email}  ({100*with_any_email//max(1,len(rows))}%)")
    print(f"  rows w/ verified email (>=70): {with_verified_email}")
    print(f"  rows w/ live domain:       {with_domain}")
    print(f"  rows w/ CLIA match:        {with_clia}")
    print(f"  rows w/ CLIA accreditation:{with_clia_accred}")
    print(f"  rows w/ CLIA fax:          {with_clia_fax}")
    print(f"  rows w/ LinkedIn (verified): {with_li}")
    print(f"  rows w/ LinkedIn Search URL: {with_li_search}")
    print(f"  rows w/ backup person:     {with_backup}")
    print(f"  rows w/ backup LI search:  {with_backup_li_search}")
    print(f"\nFile: {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
