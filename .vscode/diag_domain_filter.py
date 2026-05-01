"""Local proof: with the fix, candidate domains for these orgs prefer the
multi-token base AND we MX-filter so wrong domains get dropped."""
import asyncio, sys
sys.path.insert(0, "/workspaces/CVOPro")
from app.scrubber import _candidate_domains
from app.email_verifier import lookup_mx

async def main():
    orgs = [
        "Genova Diagnostics",
        "Boston Heart Diagnostics",
        "Atherotech Inc",
        "Spectra Laboratories",
        "Bioreference Laboratories",
    ]
    for org in orgs:
        cands = _candidate_domains(org)[:8]
        print(f"\n{org}")
        print(f"  candidates (top-8): {cands}")
        # MX-filter
        mx_ok = []
        for c in cands:
            try:
                mx = await asyncio.wait_for(lookup_mx(c), timeout=4.0)
            except Exception:
                mx = []
            if mx:
                mx_ok.append((c, mx[0]))
        print(f"  MX-passing: {mx_ok[:3]}")

asyncio.run(main())
