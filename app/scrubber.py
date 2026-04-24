"""Generic CSV/XLSX scrubber.

Accepts ANY uploaded sheet of companies/orgs and:
  1. Auto-detects columns for name, phone, city, state, zip, website, email.
  2. For each row, runs rule_intercept to classify + score fit.
  3. Attempts to verify a real website using domain-guessing + content match
     (lab phone or city/state or org token must appear on the candidate site).
  4. Scrapes contact/about/team pages for on-domain emails (filters junk).
  5. Returns a normalized list of rows ready for Excel/CSV download.

Zero external paid APIs. Free, deterministic, rule-based.
"""

from __future__ import annotations

import asyncio
import csv
import io
import re
from typing import Any, Iterable, Optional

import httpx

from rule_intercept import intercept_excel_upload, intercept_request, score_lab_lead


# ─── Column auto-detection ──────────────────────────────────────────────

_COL_HINTS: dict[str, list[str]] = {
    "name":    ["organization", "org", "company", "lab name", "name", "business", "account", "entity"],
    "phone":   ["phone", "telephone", "tel", "mobile", "contact phone", "main"],
    "city":    ["city", "town", "locality"],
    "state":   ["state", "region", "province", "st"],
    "zip":     ["zip", "postal", "postcode"],
    "website": ["website", "url", "domain", "site", "homepage", "web"],
    "email":   ["email", "e-mail", "mail", "contact email"],
    "address": ["address", "street", "addr"],
    "npi":     ["npi"],
    "taxonomy":["taxonomy", "specialty", "type", "category"],
}


def detect_columns(headers: list[str]) -> dict[str, Optional[str]]:
    norm = {h: re.sub(r"[^a-z0-9]+", " ", (h or "").lower()).strip() for h in headers}
    mapped: dict[str, Optional[str]] = {k: None for k in _COL_HINTS}

    for field, hints in _COL_HINTS.items():
        for h, n in norm.items():
            if not n:
                continue
            for hint in hints:
                if hint == n or hint in n.split() or (len(hint) >= 4 and hint in n):
                    mapped[field] = h
                    break
            if mapped[field]:
                break
    return mapped


# ─── File parsing ───────────────────────────────────────────────────────

def parse_uploaded(content: bytes, filename: str) -> tuple[list[str], list[dict]]:
    """Return (headers, rows) from CSV/XLSX/XLS bytes."""
    name = (filename or "").lower()
    if name.endswith((".xlsx", ".xls")):
        try:
            from openpyxl import load_workbook
        except Exception as e:  # pragma: no cover
            raise RuntimeError(f"openpyxl required for Excel uploads: {e}")
        wb = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        try:
            header_row = next(rows_iter)
        except StopIteration:
            return [], []
        headers = [str(c).strip() if c is not None else "" for c in header_row]
        rows: list[dict] = []
        for r in rows_iter:
            if r is None:
                continue
            row = {headers[i]: ("" if v is None else str(v).strip()) for i, v in enumerate(r) if i < len(headers)}
            if any(v for v in row.values()):
                rows.append(row)
        return headers, rows

    # Default: CSV (or unknown — try CSV)
    text = content.decode("utf-8-sig", errors="replace")
    sniff = csv.Sniffer()
    try:
        dialect = sniff.sniff(text[:4096], delimiters=",;\t|")
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = list(reader.fieldnames or [])
    rows = []
    for r in reader:
        clean = {k: (v.strip() if isinstance(v, str) else "") for k, v in r.items() if k}
        if any(clean.values()):
            rows.append(clean)
    return headers, rows


# ─── Email + domain heuristics ─────────────────────────────────────────

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
ASSET_RE = re.compile(r"(@2x|\.png|\.jpg|\.jpeg|\.svg|\.webp|\.gif)$", re.I)
PHONE_DIGITS = re.compile(r"\D+")
# Matches US phone numbers in common formats: (555) 555-5555 / 555-555-5555 / 5555555555
PHONE_RE = re.compile(r"\b(\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})\b")

_PUBLIC_MAIL = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "protonmail.com", "live.com", "msn.com",
}
_BLOCKED_DOMAINS = {
    "sentry.io", "wixpress.com", "wordpress.com", "godaddy.com", "domainmarket.com",
    "wix.com", "squarespace.com", "cloudflare.com", "2x.com", "tektronix.com",
    "questdiagnostics.com", "google.com", "facebook.com", "instagram.com",
    "twitter.com", "linkedin.com", "youtube.com",
}
_GENERIC_LOCAL = {
    "info", "contact", "hello", "hi", "admin", "support", "sales", "office",
    "reception", "webmaster", "mail", "general", "inquiries", "enquiries",
    "marketing", "help", "noreply", "no-reply", "donotreply", "postmaster",
    "notifications", "alerts", "press", "media", "careers", "jobs", "hr",
    "customerservice", "service", "interested",
}
_STOP_WORDS = {
    "llc", "inc", "pllc", "pa", "pc", "llp", "lp", "ltd", "corp", "corporation",
    "company", "co", "the", "and", "of", "for", "lab", "labs", "laboratory",
    "laboratories", "diagnostics", "diagnostic", "medical", "services",
    "service", "center", "centre", "group", "clinical", "pathology", "health",
    "care", "solutions", "solution", "systems", "system", "associates",
    "associate", "partners", "partnership",
}
_PLACEHOLDER_LOCALS = {"user", "example", "yourname", "firstname", "lastname", "test", "demo"}


def _digits(s: str) -> str:
    return PHONE_DIGITS.sub("", s or "")


def _tokens(name: str) -> list[str]:
    toks = re.findall(r"[a-z][a-z0-9]{2,}", (name or "").lower())
    return [t for t in toks if t not in _STOP_WORDS and len(t) >= 3]


def _candidate_domains(name: str, hint: str = "") -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(d: str):
        d = (d or "").lower().strip().lstrip(".")
        d = re.sub(r"^https?://", "", d).rstrip("/")
        d = d.split("/")[0]
        if d and d not in seen and "." in d and 4 <= len(d) <= 60:
            seen.add(d)
            out.append(d)

    if hint:
        add(hint)
        # Also try root domain of hint
        parts = hint.split(".")
        if len(parts) > 2:
            add(".".join(parts[-2:]))

    toks = _tokens(name)
    if toks:
        # Deterministic priority: simplest org-like bases first.
        preferred_tlds = (".com", ".org", ".net", ".co", ".us", ".io", ".health", ".bio")

        ordered_bases: list[str] = []

        def push_base(base: str) -> None:
            b = (base or "").strip().lower()
            if b and b not in ordered_bases and 3 <= len(b) <= 40:
                ordered_bases.append(b)

        push_base(toks[0])
        if len(toks) >= 2:
            push_base(toks[0] + toks[1])
            push_base("-".join(toks[:2]))
        if len(toks) >= 3:
            push_base("".join(toks[:3]))
            push_base("-".join(toks[:3]))

        base_root = "".join(toks[:2]) if len(toks) >= 2 else toks[0]
        for suf in ("lab", "labs", "diagnostics", "health", "medical", "rx", "group"):
            push_base(base_root + suf)

        for b in ordered_bases:
            for tld in preferred_tlds:
                add(b + tld)

    return out[:30]


def _email_quality(email: str, name: str = "", title: str = "", org: str = "") -> int:
    """Score 0-100. <30 = junk, drop."""
    if not email or "@" not in email:
        return 0
    email = email.lower().strip()
    local, _, domain = email.partition("@")
    if ASSET_RE.search(email):
        return 0
    if domain in _BLOCKED_DOMAINS:
        return 0
    if local in _PLACEHOLDER_LOCALS:
        return 0
    if not re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", email):
        return 0

    # Domain must match org (unless public mail or no org provided)
    if org and domain not in _PUBLIC_MAIL:
        droot = re.sub(r"^www\.", "", domain).split(".")[0]
        toks = _tokens(org)
        match = False
        for t in toks:
            if t == droot or (len(t) >= 4 and t in droot) or (len(droot) >= 4 and droot in t):
                match = True
                break
        if not match:
            return 0

    score = 0
    if domain not in _PUBLIC_MAIL:
        score += 30
    else:
        score += 10
    if local not in _GENERIC_LOCAL and ("." in local or "_" in local or re.match(r"^[a-z]{3,}$", local)):
        score += 35
    elif local in _GENERIC_LOCAL:
        score += 10
    tlow = (title or "").lower()
    if any(h in tlow for h in (
        "director", "manager", "officer", "president", "ceo", "coo", "cfo",
        "vp", "owner", "principal", "chief", "billing", "rcm", "compliance",
        "enrollment", "credential", "operations", "quality", "qa",
        "administrator", "controller", "supervisor",
    )):
        score += 25
    if name and len(name.strip()) > 2:
        score += 10
    return min(100, score)


# ─── Async fetching / scraping ──────────────────────────────────────────

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}
_PAGES = ["", "/contact", "/contact-us", "/about", "/about-us", "/team",
          "/leadership", "/staff", "/providers", "/people", "/our-team"]


async def _fetch(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        return None
    return None


async def _verify_and_scrape(
    client: httpx.AsyncClient,
    org: str,
    phone: str = "",
    city: str = "",
    state: str = "",
    website_hint: str = "",
) -> tuple[Optional[str], list[str], str]:
    """Find a verified domain, scrape on-domain emails, and extract a direct line."""
    phone_d = _digits(phone)[-10:]
    city_l = (city or "").lower().strip()
    state_l = (state or "").lower().strip()

    candidates = _candidate_domains(org, website_hint)
    chosen: Optional[str] = None

    # Adaptive threshold:
    #   phone available  → strict  (phone match alone = 50 pts, very reliable)
    #   city available   → medium  (city + 1 token = 23 pts)
    #   name-only (lab)  → lenient (2 name tokens = 16 pts, sufficient for labs)
    if phone_d:
        min_score = 30
    elif city_l:
        min_score = 20
    else:
        min_score = 16

    for dom in candidates:
        for sch in ("https://", "http://"):
            html = await _fetch(client, sch + dom)
            if not html:
                continue
            txt = html.lower()
            score = 0
            if phone_d and phone_d in _digits(html):
                score += 50
            if city_l and len(city_l) > 2 and city_l in txt:
                score += 15
            if state_l and len(state_l) == 2 and state_l in txt:
                score += 6
            for t in _tokens(org):
                if t in txt:
                    score += 8
            if score >= min_score:
                chosen = dom
                break
        if chosen:
            break

    if not chosen:
        return None, [], ""

    # Scrape pages for emails + phone numbers
    emails: set[str] = set()
    raw_phones: list[str] = []

    async def _pull(url: str) -> None:
        html = await _fetch(client, url)
        if html:
            for e in EMAIL_RE.findall(html):
                emails.add(e.lower())
            for p in PHONE_RE.findall(html):
                raw_phones.append(p)

    await asyncio.gather(*[_pull("https://" + chosen + p) for p in _PAGES])

    # Filter emails to on-domain, non-junk, sort personal-first
    good: list[str] = []
    for e in emails:
        local, _, dom = e.partition("@")
        if dom != chosen:
            continue
        if ASSET_RE.search(e):
            continue
        if local in _PLACEHOLDER_LOCALS:
            continue
        good.append(e)
    good.sort(key=lambda e: (e.split("@")[0] in _GENERIC_LOCAL, e))

    # Extract best direct line — prefer non-toll-free, not the same as caller-provided phone
    direct_line = ""
    seen_digits: set[str] = set()
    _TOLL_FREE = {"800", "888", "877", "866", "855", "844", "833"}
    for raw in raw_phones:
        d = _digits(raw)[-10:]
        if len(d) != 10 or d in seen_digits:
            continue
        seen_digits.add(d)
        if d[:3] in _TOLL_FREE:
            continue
        if phone_d and d == phone_d:
            continue  # already known — not a new direct line
        direct_line = f"({d[0:3]}) {d[3:6]}-{d[6:10]}"
        break
    # Fallback: any number (including toll-free) if nothing non-toll-free found
    if not direct_line:
        for d in seen_digits:
            if len(d) == 10 and (not phone_d or d != phone_d):
                direct_line = f"({d[0:3]}) {d[3:6]}-{d[6:10]}"
                break

    return chosen, good[:5], direct_line


# ─── Public scrub API ───────────────────────────────────────────────────

async def scrub_rows(
    headers: list[str],
    rows: list[dict],
    *,
    concurrency: int = 8,
    per_row_timeout: float = 60.0,
    max_rows: int = 1000,
) -> dict[str, Any]:
    """Run the full scrub pass. Returns dict with mapped columns and result rows."""
    cols = detect_columns(headers)
    rows = rows[:max_rows]

    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency * 4, max_keepalive_connections=concurrency * 2)
    client = httpx.AsyncClient(
        headers=_HEADERS, timeout=8.0, follow_redirects=True, limits=limits,
    )

    async def _do(row: dict) -> dict:
        org = (row.get(cols["name"]) or "").strip() if cols["name"] else ""
        phone = (row.get(cols["phone"]) or "").strip() if cols["phone"] else ""
        city = (row.get(cols["city"]) or "").strip() if cols["city"] else ""
        state = (row.get(cols["state"]) or "").strip() if cols["state"] else ""
        zipc = (row.get(cols["zip"]) or "").strip() if cols["zip"] else ""
        web = (row.get(cols["website"]) or "").strip() if cols["website"] else ""
        existing_email = (row.get(cols["email"]) or "").strip() if cols["email"] else ""
        addr = (row.get(cols["address"]) or "").strip() if cols["address"] else ""
        npi = (row.get(cols["npi"]) or "").strip() if cols["npi"] else ""
        tax = (row.get(cols["taxonomy"]) or "").strip() if cols["taxonomy"] else ""

        verified_domain: Optional[str] = None
        scraped_emails: list[str] = []
        direct_line: str = ""
        scrub_status = "ok"
        scrub_error = ""

        if org:
            try:
                async with sem:
                    verified_domain, scraped_emails, direct_line = await asyncio.wait_for(
                        _verify_and_scrape(client, org, phone=phone, city=city, state=state, website_hint=web),
                        timeout=per_row_timeout,
                    )
            except asyncio.TimeoutError:
                scrub_status = "timeout"
                scrub_error = f"row scrape exceeded {per_row_timeout}s"
            except Exception as e:
                scrub_status = "error"
                scrub_error = str(e)[:200]
        else:
            scrub_status = "skipped"
            scrub_error = "no organization name detected"

        # Build candidate emails: scraped + existing
        candidates: list[tuple[int, str]] = []
        for e in scraped_emails:
            s = _email_quality(e, org=org)
            if s > 0:
                candidates.append((s, e))
        if existing_email:
            for e in re.split(r"[;,\s]+", existing_email):
                e = e.strip()
                if not e:
                    continue
                s = _email_quality(e, org=org)
                if s > 0:
                    candidates.append((s, e))
        # Dedup, sort
        seen: set[str] = set()
        ranked: list[tuple[int, str]] = []
        for s, e in sorted(candidates, key=lambda x: -x[0]):
            if e in seen:
                continue
            seen.add(e)
            ranked.append((s, e))

        # ── Lab intelligence scoring (rule_intercept) ──────────────────
        lab_intel = score_lab_lead(org, lab_type=tax, state=state)

        # Fit score = lab quality score + contact-info richness bonuses
        fit = lab_intel["score"]
        if verified_domain:
            fit = min(100, fit + 5)
        if ranked:
            fit = min(100, fit + 8)
        if direct_line:
            fit = min(100, fit + 3)

        return {
            "Lead Score": lab_intel["score"],
            "Lab Tier": lab_intel["tier"],
            "Priority": lab_intel["priority"],
            "Org Name": org,
            "Taxonomy / Type": tax,
            "Lab Type Detected": lab_intel.get("lab_type_detected", ""),
            "NPI": npi,
            "Address": addr,
            "City": city,
            "State": state,
            "ZIP": zipc,
            "Phone": phone,
            "Direct Line": direct_line,
            "Email 1": ranked[0][1] if len(ranked) >= 1 else "",
            "Email 1 Score": ranked[0][0] if len(ranked) >= 1 else "",
            "Email 2": ranked[1][1] if len(ranked) >= 2 else "",
            "Email 2 Score": ranked[1][0] if len(ranked) >= 2 else "",
            "Email 3": ranked[2][1] if len(ranked) >= 3 else "",
            "Email 3 Score": ranked[2][0] if len(ranked) >= 3 else "",
            "Existing Email (input)": existing_email,
            "Original Website": web,
            "Verified Domain": verified_domain or "",
            "Lead Signals": "; ".join(lab_intel.get("signals", [])),
            "Fit Score": fit,
            "Intercept Category": lab_intel["category"],
            "Intercept Confidence": 1.0,
            "Scrub Status": scrub_status,
            "Scrub Error": scrub_error,
        }

    try:
        results = await asyncio.gather(*[_do(r) for r in rows], return_exceptions=True)
    finally:
        await client.aclose()

    out: list[dict] = []
    error_count = 0
    for r in results:
        if isinstance(r, Exception):
            error_count += 1
            out.append({"Scrub Status": "error", "Scrub Error": str(r)[:200]})
        else:
            out.append(r)

    out.sort(key=lambda r: (-int(r.get("Lead Score", 0) or 0), -int(r.get("Fit Score", 0) or 0)))

    summary = {
        "input_rows": len(rows),
        "output_rows": len(out),
        "verified_domains": sum(1 for r in out if r.get("Verified Domain")),
        "rows_with_email": sum(1 for r in out if r.get("Email 1")),
        "errors": error_count,
        "detected_columns": cols,
    }
    return {"summary": summary, "rows": out}


# ─── Output writers ─────────────────────────────────────────────────────

OUTPUT_FIELDS = [
    # ── Prioritization ───────────────────────────────────────────────
    "Lead Score", "Lab Tier", "Priority",
    # ── Organization ─────────────────────────────────────────────────
    "Org Name", "Taxonomy / Type", "Lab Type Detected",
    "NPI", "Address", "City", "State", "ZIP",
    # ── Contact ──────────────────────────────────────────────────────
    "Phone", "Direct Line",
    "Email 1", "Email 1 Score",
    "Email 2", "Email 2 Score",
    "Email 3", "Email 3 Score",
    "Existing Email (input)",
    # ── Web ───────────────────────────────────────────────────────────
    "Original Website", "Verified Domain",
    # ── Intelligence ──────────────────────────────────────────────────
    "Lead Signals",
    # ── Metadata ─────────────────────────────────────────────────────
    "Fit Score", "Intercept Category", "Intercept Confidence",
    "Scrub Status", "Scrub Error",
]


def to_csv_bytes(rows: Iterable[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k, "") for k in OUTPUT_FIELDS})
    return buf.getvalue().encode("utf-8")


def to_xlsx_bytes(rows: Iterable[dict]) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment

    wb = Workbook()
    ws = wb.active
    ws.title = "Scrubbed Leads"
    ws.append(OUTPUT_FIELDS)
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="2563EB")
    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="left", vertical="center")
    for r in rows:
        ws.append([r.get(k, "") for k in OUTPUT_FIELDS])
    # Reasonable column widths
    widths = {
        "Lead Score": 12, "Lab Tier": 10, "Priority": 12,
        "Org Name": 38, "Taxonomy / Type": 28, "Lab Type Detected": 24,
        "NPI": 12, "Address": 28, "City": 16, "State": 8, "ZIP": 10,
        "Phone": 18, "Direct Line": 18,
        "Email 1": 34, "Email 1 Score": 12,
        "Email 2": 34, "Email 2 Score": 12,
        "Email 3": 34, "Email 3 Score": 12,
        "Existing Email (input)": 32,
        "Original Website": 26, "Verified Domain": 26,
        "Lead Signals": 48,
        "Fit Score": 10, "Intercept Category": 14, "Intercept Confidence": 14,
        "Scrub Status": 12, "Scrub Error": 28,
    }
    for i, h in enumerate(OUTPUT_FIELDS, start=1):
        ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = widths.get(h, 18)
    ws.freeze_panes = "A2"
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
