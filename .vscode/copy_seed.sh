#!/bin/bash
set -e
cd /workspaces/CVOPro
mkdir -p app/seed
cp data/seed_national_pull.csv app/seed/seed_national_pull.csv
ls -la app/seed/
wc -l app/seed/seed_national_pull.csv
