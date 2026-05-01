"""Quick standalone test of LinkedIn finder against real names."""
import asyncio, sys
sys.path.insert(0, "/workspaces/CVOPro")
from app.linkedin_finder import find_linkedin_profile, _ddg_html, _bing_html
import httpx

async def main():
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as c:
        for q in [
            '"Jeffrey Ledford" "Genova Diagnostics" site:linkedin.com/in',
            '"Matthew Urbanek" site:linkedin.com/in',
            'Jeffrey Ledford Genova Diagnostics linkedin',
        ]:
            print(f"\n--- DDG: {q!r}")
            r = await _ddg_html(q, c)
            print(f"   results: {r[:3]}")
            print(f"--- Bing: {q!r}")
            r2 = await _bing_html(q, c)
            print(f"   results: {r2[:3]}")

    print("\n--- full pipeline ---")
    for first, last, org in [
        ("Jeffrey", "Ledford", "Genova Diagnostics"),
        ("Matthew", "Urbanek", "Boston Heart Diagnostics"),
        ("Curtis", "Johnson", "Spectra Laboratories"),
    ]:
        result = await find_linkedin_profile(first, last, org)
        print(f"   {first} {last} @ {org}: {result}")

asyncio.run(main())
