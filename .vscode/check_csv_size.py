import os
p = "/workspaces/CVOPro/output/labs_routed_full.csv"
print("rows:", sum(1 for _ in open(p)))
print("size_mb:", round(os.path.getsize(p)/1024/1024, 2))
