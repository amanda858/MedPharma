#!/bin/bash
echo "=== seed CSV ==="
ls -la /workspaces/MedPharma/data/seed_national_pull.csv 2>&1 || echo "NOT FOUND"
echo
echo "=== row count ==="
wc -l /workspaces/MedPharma/data/seed_national_pull.csv 2>&1 || true
echo
echo "=== headers ==="
head -1 /workspaces/MedPharma/data/seed_national_pull.csv 2>&1 | tr ',' '\n' | nl | head -30 || true
echo
echo "=== sample data row ==="
sed -n '2p' /workspaces/MedPharma/data/seed_national_pull.csv 2>&1 | head -c 500 || true
