import asyncio
import json
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.npi_client import search_npi

SAMPLES = [
    ("LEMEDIX LAB LLC", "IL"),
    ("AVR LAB SERVICES", "OH"),
    ("DME BILLING ENTERPRISES LLC", "GA"),
    ("PATHOLOGIC DIAGNOSTIC LLC", "SC"),
    ("DESTINY DIAGNOSTICS LLC", "NC"),
]


async def check_one(name, state):
    try:
        res = await search_npi(state=state, organization_name=name, taxonomy_description="laboratory", limit=5)
        rows = res.get("results", [])
        top = rows[0] if rows else {}
        return {
            "org": name,
            "state": state,
            "npi_matches": len(rows),
            "top_npi": top.get("npi", ""),
            "top_org": top.get("organization_name", ""),
            "top_phone": top.get("phone", ""),
            "top_city": top.get("city", ""),
            "top_last_updated": top.get("last_updated", ""),
        }
    except Exception as exc:
        return {"org": name, "state": state, "error": str(exc)}


async def main():
    results = await asyncio.gather(*[check_one(name, state) for name, state in SAMPLES])
    print(json.dumps(results, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
