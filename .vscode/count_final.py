import csv
from collections import Counter

p = "/workspaces/CVOPro/output/FINAL_apollo_upload.csv"
rows = list(csv.DictReader(open(p)))
print(f"TOTAL ROWS: {len(rows)}")
print(f"TIERS: {dict(Counter(x['Tier'] for x in rows))}")
print(f"PRIORITY: {dict(Counter(x['Priority'] for x in rows))}")
print(f"TOP 15 STATES: {Counter(x['State'] for x in rows).most_common(15)}")
labcorp = sum(1 for x in rows if 'LABORATORY CORPORATION' in x['Company Name'].upper() or 'LABCORP' in x['Company Name'].upper())
print(f"LABCORP-LIKE ROWS: {labcorp}")
ngs_only = sum(1 for x in rows if 'ngs' in x['Signals'].lower() and not any(k in x['Company Name'].lower() for k in ['genomic','molecular','genetic','toxicol','dna','pgx','specialty']))
print(f"ROWS WHERE 'ngs' SIGNAL IS ONLY THING (likely false positive): {ngs_only}")
top50 = "/workspaces/CVOPro/output/FINAL_top_50.csv"
top = list(csv.DictReader(open(top50)))
print(f"\nTOP 50 FILE ROWS: {len(top)}")
