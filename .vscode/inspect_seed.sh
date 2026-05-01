#!/bin/bash
set -e
cd /workspaces/CVOPro
echo "=== seed CSV ==="
wc -l data/seed_national_pull.csv
echo "--- headers ---"
head -1 data/seed_national_pull.csv | tr ',' '\n' | head -50
echo "--- sample row 2 (truncated) ---"
sed -n '2p' data/seed_national_pull.csv | head -c 600
echo
echo "=== git log -5 ==="
git log --oneline -5
echo "=== git status ==="
git status --short
