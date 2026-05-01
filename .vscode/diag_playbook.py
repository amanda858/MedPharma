import sys
sys.path.insert(0, "/workspaces/CVOPro")
import traceback

try:
    from app.playbook import (
        personalized_hook, objection_handlers, heat_score,
        enrich_templates_with_hook, _recency_signal,
    )
    print("imports ok")
    print(personalized_hook("Jeffrey", "Genova Diagnostics", "Clinical Medical Laboratory", "", "NC", "2024-06-01"))
    print(_recency_signal("2024-06-01"))
    print(_recency_signal("2026-04-01"))
    print(heat_score(lead_score=37, fit_score=58, has_dm=True, has_direct_line=True, has_verified_domain=True, has_social=True, last_updated="2026-04-01", state="NC"))
except Exception:
    traceback.print_exc()

# Now run the actual scrubber and capture exceptions
print("\n--- scrubber test ---")
import asyncio
from app.scrubber import scrub_rows, parse_uploaded
import importlib, app.scrubber as sc, app.playbook as pb
importlib.reload(pb); importlib.reload(sc)

# Monkey-patch to surface playbook errors
orig_pp = sc
async def main():
    rows = b"organization_name,city,state\nGenova Diagnostics,Asheville,NC\n"
    headers, parsed = parse_uploaded(rows, "x.csv")
    res = await sc.scrub_rows(headers, parsed, max_rows=2)
    r = res["rows"][0]
    print("heat:", r.get("Heat Score"), "reasons:", r.get("Heat Reasons"))
    print("hook:", r.get("Personalized Hook"))
    print("recency:", r.get("Recency Signal"))
    print("npi_last_updated:", r.get("NPI Last Updated"))
    print("LI first msg:", r.get("LinkedIn First Message"))

asyncio.run(main())
