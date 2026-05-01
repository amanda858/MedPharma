import urllib.request, json, time, http.cookiejar
BASE="https://medpharma-hub.onrender.com"

# wait for all enrichment to finish (the enrich-top-states task is sequential)
print("=== wait for enrich-top-states sequence to finish ===")
done = False
last_result_seen = None
no_change_count = 0
for i in range(300):  # up to ~50 min
    try:
        with urllib.request.urlopen(f"{BASE}/admin/leads/api/admin/labs/enrich-status", timeout=30) as r:
            d = json.loads(r.read().decode())
            running = d.get("running")
            last = d.get("last_result")
            if last != last_result_seen:
                print(f"  {i+1}: running={running} last={last}")
                last_result_seen = last
                no_change_count = 0
            else:
                no_change_count += 1
                if no_change_count % 10 == 0:
                    print(f"  {i+1}: still running={running}")
            # If not running and we've seen result for 6 cycles (no new state triggered), assume sequence is done
            if not running and no_change_count >= 4:
                print("  >>> ALL ENRICHMENT FINISHED")
                done = True
                break
    except Exception as e:
        print(f"  {i+1}: ERR {type(e).__name__}")
    time.sleep(10)

print("\n=== TALLY: leads with email ===")
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request(f"{BASE}/hub/api/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"}, method="POST"), timeout=20)

with op.open(f"{BASE}/admin/leads/api/national-pull/search?has_email=true&limit=500", timeout=60) as r:
    d = json.loads(r.read().decode())
    print(f"\nTOTAL ENRICHED: {d.get('matched')}")
    by_state = {}
    for row in d.get('rows', []):
        s = row.get('State','?')
        by_state[s] = by_state.get(s,0)+1
    print(f"by state: {dict(sorted(by_state.items(), key=lambda kv:-kv[1]))}")
    print(f"\nsample (first 30):")
    for row in d.get('rows', [])[:30]:
        em = row.get('DM Email') or row.get('Company Email')
        dom = row.get('Org Domain')
        print(f"  {row.get('State')} | {row.get('Org Name')[:45]:45} | {em or '-':40} | {dom}")
