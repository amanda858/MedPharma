"""Probe the LIVE deployed endpoints to confirm hunt mode works in production."""
import httpx, json, time

BASE = "https://medpharma-hub.onrender.com"
LEADS_PREFIX = "/admin/leads"


def main():
    with httpx.Client(timeout=60, follow_redirects=True) as c:
        print(f"\n{'─'*70}\nProbing live: {BASE}{LEADS_PREFIX}\n{'─'*70}")

        # 1) specialties
        url = f"{BASE}{LEADS_PREFIX}/api/prospect/specialties"
        r = c.get(url)
        print(f"\n[1] GET {url}")
        print(f"    HTTP {r.status_code}")
        if r.status_code == 200:
            specs = r.json().get("specialties", [])
            print(f"    {len(specs)} specialties: {specs}")
        else:
            print(f"    body: {r.text[:200]}")

        # 2) bulk hunt
        bulk_url = f"{BASE}{LEADS_PREFIX}/api/prospect/bulk"
        body = {"state": "FL", "specialty": "clinical", "limit": 5, "dm_only": True}
        print(f"\n[2] POST {bulk_url}")
        print(f"    body: {json.dumps(body)}")
        r = c.post(bulk_url, json=body)
        print(f"    HTTP {r.status_code}")
        if r.status_code != 200:
            print(f"    body: {r.text[:300]}")
            return
        j = r.json()
        job_id = j.get("job_id")
        print(f"    job_id: {job_id}")

        # 3) poll
        status_url = f"{BASE}{LEADS_PREFIX}/api/scrub/status/{job_id}"
        print(f"\n[3] Polling {status_url}")
        for i in range(60):
            s = c.get(status_url)
            if s.status_code != 200:
                print(f"    iter {i}: HTTP {s.status_code}")
                time.sleep(1)
                continue
            sj = s.json()
            st = sj.get("status")
            print(f"    iter {i}: status={st}  done_rows={sj.get('done_rows', 0)}")
            if st in ("done", "error"):
                rows = sj.get("rows") or sj.get("preview") or sj.get("daily_top_10") or []
                print(f"\n[4] FINAL: {len(rows)} rows  status={st}")
                if sj.get("error"):
                    print(f"    error: {sj.get('error')}")
                print(f"    summary: {json.dumps(sj.get('summary', {}), indent=6)}")
                print(f"    job keys: {sorted(sj.keys())}")
                # If a download URL is given, fetch the CSV/JSON for full rows
                dl = sj.get("download") or {}
                print(f"    download keys: {sorted(dl.keys()) if isinstance(dl, dict) else dl}")
                for r0 in rows[:3]:
                    print(f"\n    {r0.get('Org Name','')}")
                    print(f"      DM:        {r0.get('Decision Maker','')} ({r0.get('DM Title','')})")
                    print(f"      Phone:     {r0.get('Direct Line','')}")
                    print(f"      Heat:      {r0.get('Heat Score','')}")
                    print(f"      LinkedIn:  {r0.get('LinkedIn URL','')[:80]}")
                    print(f"      Facebook:  {r0.get('Facebook URL','')[:80]}")
                    print(f"      Instagram: {r0.get('Instagram URL','')[:80]}")
                    print(f"      Hook:      {r0.get('Personalized Hook','')[:100]}")
                break
            time.sleep(1)
        else:
            print(f"    job did not finish in 60s")


main()

