import asyncio, sys
sys.path.insert(0, "/workspaces/CVOPro")
from app.npi_client import find_org_official, person_email_patterns

async def main():
    test_orgs = [
        ("Bioreference Laboratories", "NJ"),
        ("Sonic Healthcare", "TX"),
        ("Atherotech", "AL"),
        ("Genova Diagnostics", "NC"),
        ("Spectra Laboratories", "CA"),
        ("Boston Heart Diagnostics", "MA"),
    ]
    for name, state in test_orgs:
        r = await find_org_official(name, state=state)
        if r:
            patterns = person_email_patterns(r["first"], r["last"], "example.com")
            print(f"\n{name} ({state}) → MATCH")
            print(f"  NPI: {r['npi']}")
            print(f"  Org: {r['org_name']}")
            print(f"  Official: {r['first']} {r['last']} — {r['title']}")
            print(f"  Phone: {r['phone']}  Address: {r['address']}, {r['city']} {r['state']}")
            print(f"  Email patterns: {patterns[:5]}")
        else:
            print(f"\n{name} ({state}) → NO MATCH")

asyncio.run(main())
