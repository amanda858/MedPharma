#!/bin/bash
# Wait for new debug endpoint with db_tables field, then dump it.
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  ts=$(date -u +%T)
  code=$(curl -s -o /tmp/d.json -w "%{http_code}" https://medpharma-hub.onrender.com/admin/leads/api/national-pull/debug)
  echo "[$i $ts] HTTP $code"
  if [ "$code" = "200" ] && grep -q db_tables /tmp/d.json 2>/dev/null; then
    echo "=== NEW DEBUG LIVE ==="
    python3 -m json.tool /tmp/d.json
    exit 0
  fi
  sleep 20
done
echo "TIMEOUT"
exit 1
