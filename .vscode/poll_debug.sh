#!/bin/bash
for i in 1 2 3 4 5 6 7 8 9 10 11 12; do
  ts=$(date -u +%T)
  code=$(curl -s -o /tmp/d.json -w "%{http_code}" https://medpharma-hub.onrender.com/admin/leads/api/national-pull/debug)
  echo "[$i $ts] HTTP $code"
  if [ "$code" = "200" ] && grep -q resolved_latest_csv /tmp/d.json 2>/dev/null; then
    echo "=== LIVE ==="
    python3 -m json.tool /tmp/d.json
    exit 0
  fi
  head -c 200 /tmp/d.json; echo
  sleep 20
done
echo "TIMEOUT"
exit 1
