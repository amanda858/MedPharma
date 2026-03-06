#!/usr/bin/env python3
"""Reliable daily operations runner for lead polling and readiness checks.

This script is designed for daily use:
- Authenticates to hub
- Starts (or joins) a poll run
- Waits for poll completion
- Verifies strict actionable leads with retry stabilization
- Exits non-zero only on a true readiness failure
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import httpx


BASE = os.getenv("LEADS_BASE_URL", "https://medpharma-hub.onrender.com").rstrip("/")
USERNAME = os.getenv("HUB_ADMIN_USERNAME", "admin")
PASSWORD = os.getenv("HUB_ADMIN_PASSWORD", "admin123")

POLL_TIMEOUT_S = int(os.getenv("DAILY_OPS_POLL_TIMEOUT_S", "300") or 300)
POLL_INTERVAL_S = int(os.getenv("DAILY_OPS_POLL_INTERVAL_S", "6") or 6)
STRICT_RETRIES = int(os.getenv("DAILY_OPS_STRICT_RETRIES", "3") or 3)
STRICT_RETRY_SLEEP_S = int(os.getenv("DAILY_OPS_STRICT_RETRY_SLEEP_S", "10") or 10)


def _login(client: httpx.Client) -> int:
    resp = client.post(
        f"{BASE}/hub/api/login",
        json={"username": USERNAME, "password": PASSWORD},
    )
    return int(resp.status_code)


def _fetch_counts(client: httpx.Client) -> dict[str, Any]:
    strict = client.get(
        f"{BASE}/admin/leads/api/leads?quality_only=true&need_signal_only=true&require_email=true"
    )
    broad = client.get(
        f"{BASE}/admin/leads/api/leads?quality_only=false&need_signal_only=false&require_email=false"
    )
    out: dict[str, Any] = {
        "strict_status": int(strict.status_code),
        "broad_status": int(broad.status_code),
    }
    if strict.status_code == 200:
        payload = strict.json()
        leads = payload.get("leads", []) if isinstance(payload.get("leads", []), list) else []
        out["strict_count"] = int(payload.get("count", len(leads)) or 0)
        out["strict_sample"] = [
            {
                "org": row.get("organization_name") or row.get("org_name"),
                "emails": row.get("emails"),
            }
            for row in leads[:3]
        ]
    if broad.status_code == 200:
        payload = broad.json()
        leads = payload.get("leads", []) if isinstance(payload.get("leads", []), list) else []
        out["broad_count"] = int(payload.get("count", len(leads)) or 0)
    return out


def _poll_status(client: httpx.Client) -> dict[str, Any]:
    resp = client.get(f"{BASE}/admin/leads/api/leads/poll-status")
    if resp.status_code != 200:
        return {"ok": False, "status_code": int(resp.status_code), "body": resp.text[:200]}
    try:
        payload = resp.json()
    except Exception:
        return {"ok": False, "status_code": int(resp.status_code), "body": "invalid-json"}
    return payload if isinstance(payload, dict) else {"ok": False, "body": "unexpected-payload"}


def _wait_for_poll_completion(client: httpx.Client, timeout_s: int, interval_s: int) -> tuple[bool, dict[str, Any], int]:
    deadline = time.time() + timeout_s
    checks = 0
    last: dict[str, Any] = {}
    while time.time() < deadline:
        checks += 1
        _login(client)
        status_payload = _poll_status(client)
        last = status_payload
        running = bool((status_payload.get("status") or {}).get("running"))
        if not running:
            return True, last, checks
        time.sleep(interval_s)
    return False, last, checks


def main() -> int:
    out: dict[str, Any] = {
        "base": BASE,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            out["healthz"] = int(client.get(f"{BASE}/healthz").status_code)
            buildz = client.get(f"{BASE}/buildz")
            out["buildz_status"] = int(buildz.status_code)
            if buildz.status_code == 200:
                try:
                    out["buildz"] = buildz.json()
                except Exception:
                    out["buildz"] = {"raw": buildz.text[:180]}

            out["login_status"] = _login(client)
            out["pre_counts"] = _fetch_counts(client)

            poll_start = client.post(f"{BASE}/admin/leads/api/leads/poll-daily?segment=all&fast=true")
            out["poll_start_status"] = int(poll_start.status_code)
            if poll_start.headers.get("content-type", "").startswith("application/json"):
                try:
                    out["poll_start_body"] = poll_start.json()
                except Exception:
                    out["poll_start_body"] = {"raw": poll_start.text[:180]}
            else:
                out["poll_start_body"] = {"raw": poll_start.text[:180]}

            completed, final_status, checks = _wait_for_poll_completion(
                client,
                timeout_s=POLL_TIMEOUT_S,
                interval_s=POLL_INTERVAL_S,
            )
            out["poll_completed"] = bool(completed)
            out["poll_wait_checks"] = int(checks)
            out["poll_status"] = final_status

            _login(client)

            stable_counts: dict[str, Any] = {}
            strict_history: list[int] = []
            for attempt in range(1, STRICT_RETRIES + 1):
                stable_counts = _fetch_counts(client)
                strict_count = int(stable_counts.get("strict_count", 0) or 0)
                strict_history.append(strict_count)
                if strict_count > 0:
                    break
                if attempt < STRICT_RETRIES:
                    time.sleep(STRICT_RETRY_SLEEP_S)
                    _login(client)

            out["post_counts"] = stable_counts
            out["strict_history"] = strict_history

            strict_count = int((out.get("post_counts") or {}).get("strict_count", 0) or 0)
            if not completed:
                out["status"] = "warning"
                out["code"] = "POLL_TIMEOUT"
                out["message"] = "Poll did not complete before timeout; rerun readiness check shortly"
                print(json.dumps(out, ensure_ascii=False))
                return 2

            if strict_count <= 0:
                out["status"] = "error"
                out["code"] = "STRICT_ZERO"
                out["message"] = "Strict actionable leads are zero after completed poll and retries"
                print(json.dumps(out, ensure_ascii=False))
                return 1

            out["status"] = "ok"
            out["code"] = "READY"
            out["message"] = "Daily ops completed and strict actionable leads are available"
            print(json.dumps(out, ensure_ascii=False))
            return 0
    except Exception as exc:
        out["status"] = "error"
        out["code"] = "UNHANDLED_EXCEPTION"
        out["message"] = str(exc)
        print(json.dumps(out, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
