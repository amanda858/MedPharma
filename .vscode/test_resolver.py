#!/usr/bin/env python3
"""Smoke-test the resolver chain locally."""
import sys, os
sys.path.insert(0, "/workspaces/CVOPro")
# Wipe cache for a clean test
import os
for p in ("/tmp/linkedin_resolver_cache.db",):
    if os.path.exists(p):
        os.remove(p)
from app.linkedin_resolver import (
    resolve_linkedin_profile, resolve_facebook_profile, resolve_instagram_profile,
    resolve_company_linkedin, resolve_employee_at_company, reset_run_budget
)

reset_run_budget()
cases = [
    ("Derek", "Pagan", "1LAB DIAGNOSTICS"),
    ("Wilson", "Molina", "24-7 LABORATORIES"),
    ("Richard", "Westenbarger", "90 MINUTE LABORATORY"),
    ("Satya", "Nadella", "Microsoft"),
]
for first, last, org in cases:
    print(f"\n=== {first} {last} @ {org}")
    print("  LinkedIn:    ", resolve_linkedin_profile(first, last, org) or "(none)")
    print("  Facebook:    ", resolve_facebook_profile(first, last, org) or "(none)")
    print("  Instagram:   ", resolve_instagram_profile(first, last, org) or "(none)")
    print("  Co LI Page:  ", resolve_company_linkedin(org) or "(none)")
    emps = resolve_employee_at_company(org, max_results=3)
    print(f"  Co Employees ({len(emps)}):")
    for e in emps:
        print("    ", e)
