"""
In-house email verifier — no third-party API dependency.

Pipeline per address:
  1. Syntax / disposable / role-account checks
  2. DNS MX lookup via Cloudflare DNS-over-HTTPS (no extra deps)
  3. SMTP RCPT TO probe with catch-all detection
  4. Score 0-100 + verdict: deliverable | risky | catch-all | undeliverable | unknown

Designed to degrade gracefully when SMTP port 25 is blocked (cloud hosts
commonly block outbound 25). When SMTP is unavailable, MX presence + domain
quality still produce a useful confidence score.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Optional

import httpx

# ─── Static lists ───────────────────────────────────────────────────────

_DISPOSABLE = {
    "mailinator.com", "guerrillamail.com", "tempmail.com", "10minutemail.com",
    "yopmail.com", "trashmail.com", "throwaway.email", "fakeinbox.com",
    "sharklasers.com", "getnada.com", "maildrop.cc", "dispostable.com",
}

_ROLE_LOCALS = {
    "info", "contact", "admin", "support", "sales", "hello", "office",
    "billing", "credentialing", "rcm", "lab", "director", "marketing",
    "hr", "press", "media", "abuse", "postmaster", "webmaster", "noreply",
    "no-reply", "donotreply", "service", "team", "help", "general",
}

_SYNTAX_RE = re.compile(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$")

# ─── Caches ─────────────────────────────────────────────────────────────

_MX_CACHE: dict[str, tuple[float, list[str]]] = {}
_CATCHALL_CACHE: dict[str, tuple[float, bool]] = {}
_VERIFY_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 6 * 3600  # 6 hours


def _cache_get(c: dict, key: str):
    e = c.get(key)
    if not e:
        return None
    ts, val = e
    if time.time() - ts > _CACHE_TTL:
        c.pop(key, None)
        return None
    return val


def _cache_put(c: dict, key: str, val):
    c[key] = (time.time(), val)


# ─── DNS over HTTPS (Cloudflare) ────────────────────────────────────────

async def lookup_mx(domain: str, client: Optional[httpx.AsyncClient] = None) -> list[str]:
    """Return MX hosts sorted by priority. Empty list = no MX."""
    domain = (domain or "").strip().lower().rstrip(".")
    if not domain:
        return []
    cached = _cache_get(_MX_CACHE, domain)
    if cached is not None:
        return cached

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=8.0)
    try:
        for resolver in (
            "https://cloudflare-dns.com/dns-query",
            "https://dns.google/resolve",
        ):
            try:
                r = await client.get(
                    resolver,
                    params={"name": domain, "type": "MX"},
                    headers={"accept": "application/dns-json"},
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                answers = data.get("Answer") or []
                mx: list[tuple[int, str]] = []
                for a in answers:
                    if a.get("type") != 15:  # MX
                        continue
                    parts = (a.get("data") or "").split()
                    if len(parts) == 2:
                        try:
                            pri = int(parts[0])
                            host = parts[1].rstrip(".")
                            mx.append((pri, host))
                        except ValueError:
                            continue
                if mx:
                    mx.sort()
                    hosts = [h for _, h in mx]
                    _cache_put(_MX_CACHE, domain, hosts)
                    return hosts
            except Exception:
                continue

        # Fallback — A record means there might be a server, but no MX = unreliable
        _cache_put(_MX_CACHE, domain, [])
        return []
    finally:
        if own_client:
            await client.aclose()


# ─── SMTP RCPT probe ────────────────────────────────────────────────────

async def _smtp_probe(
    mx_host: str,
    addresses: list[str],
    helo_domain: str = "medpharma-hub.onrender.com",
    mail_from: str = "verify@medpharma-hub.onrender.com",
    timeout: float = 8.0,
) -> dict[str, str]:
    """Open SMTP connection to MX and probe each address with RCPT TO.

    Returns {address: "ok" | "rejected" | "tempfail" | "error"}.
    Best effort — many providers block port 25 outbound.
    """
    results: dict[str, str] = {a: "error" for a in addresses}
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(mx_host, 25), timeout=timeout
        )
    except Exception:
        return results

    async def _send(line: str) -> str:
        writer.write((line + "\r\n").encode("ascii", errors="ignore"))
        await writer.drain()
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=timeout)
            return data.decode("ascii", errors="ignore").strip()
        except Exception:
            return ""

    try:
        # Greeting
        try:
            await asyncio.wait_for(reader.readline(), timeout=timeout)
        except Exception:
            return results
        ehlo = await _send(f"EHLO {helo_domain}")
        if not ehlo.startswith("2"):
            await _send(f"HELO {helo_domain}")
        # MAIL FROM
        mf = await _send(f"MAIL FROM:<{mail_from}>")
        if not mf.startswith("2"):
            return results
        for addr in addresses:
            resp = await _send(f"RCPT TO:<{addr}>")
            code = resp[:3]
            if code.startswith("2"):
                results[addr] = "ok"
            elif code.startswith("4"):
                results[addr] = "tempfail"
            elif code.startswith("5"):
                results[addr] = "rejected"
            else:
                results[addr] = "error"
        try:
            await _send("QUIT")
        except Exception:
            pass
    finally:
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=2.0)
        except Exception:
            pass
    return results


async def detect_catchall(domain: str, mx_hosts: list[str]) -> bool:
    """Probe a random nonsense address. If accepted, domain is catch-all."""
    cached = _cache_get(_CATCHALL_CACHE, domain)
    if cached is not None:
        return cached
    if not mx_hosts:
        _cache_put(_CATCHALL_CACHE, domain, False)
        return False
    probe = f"zz-noexist-{int(time.time()) % 100000}@{domain}"
    res = await _smtp_probe(mx_hosts[0], [probe])
    is_catchall = res.get(probe) == "ok"
    _cache_put(_CATCHALL_CACHE, domain, is_catchall)
    return is_catchall


# ─── Public API ─────────────────────────────────────────────────────────

async def verify_email(
    email: str,
    client: Optional[httpx.AsyncClient] = None,
    do_smtp: bool = True,
) -> dict:
    """Verify a single email. Returns:
      {
        email, valid_syntax, is_role, is_disposable,
        mx_found, mx_hosts, smtp_result, catchall, score, verdict, reason
      }
    """
    out = {
        "email": email,
        "valid_syntax": False,
        "is_role": False,
        "is_disposable": False,
        "mx_found": False,
        "mx_hosts": [],
        "smtp_result": None,
        "catchall": None,
        "score": 0,
        "verdict": "undeliverable",
        "reason": "",
    }
    if not email or "@" not in email:
        out["reason"] = "no @"
        return out
    email = email.lower().strip()
    out["email"] = email
    if not _SYNTAX_RE.match(email):
        out["reason"] = "bad syntax"
        return out
    out["valid_syntax"] = True

    local, _, domain = email.partition("@")
    if domain in _DISPOSABLE:
        out["is_disposable"] = True
        out["reason"] = "disposable domain"
        return out
    if local in _ROLE_LOCALS:
        out["is_role"] = True

    cached = _cache_get(_VERIFY_CACHE, email)
    if cached is not None:
        return cached

    mx = await lookup_mx(domain, client=client)
    out["mx_hosts"] = mx
    out["mx_found"] = bool(mx)
    if not mx:
        out["reason"] = "no MX"
        out["verdict"] = "undeliverable"
        out["score"] = 0
        _cache_put(_VERIFY_CACHE, email, out)
        return out

    if not do_smtp:
        # No SMTP probe — score on MX + role/syntax signals only
        score = 45 if mx else 0
        if out["is_role"]:
            score -= 5
        out["score"] = max(0, min(100, score))
        out["verdict"] = "risky" if score >= 30 else "unknown"
        out["reason"] = "MX-only check (smtp disabled)"
        _cache_put(_VERIFY_CACHE, email, out)
        return out

    # SMTP probe
    catchall = await detect_catchall(domain, mx)
    out["catchall"] = catchall
    smtp_res = await _smtp_probe(mx[0], [email])
    rcpt = smtp_res.get(email, "error")
    out["smtp_result"] = rcpt

    if rcpt == "rejected":
        out["verdict"] = "undeliverable"
        out["score"] = 5
        out["reason"] = "SMTP 5xx — mailbox does not exist"
    elif rcpt == "ok" and not catchall:
        out["verdict"] = "deliverable"
        out["score"] = 90 if not out["is_role"] else 75
        out["reason"] = "SMTP RCPT accepted"
    elif rcpt == "ok" and catchall:
        out["verdict"] = "catch-all"
        out["score"] = 55 if not out["is_role"] else 45
        out["reason"] = "domain is catch-all — cannot confirm mailbox"
    elif rcpt == "tempfail":
        out["verdict"] = "risky"
        out["score"] = 40
        out["reason"] = "SMTP 4xx temporary failure"
    else:
        # SMTP probe failed (port blocked, greylisting, etc.) — fall back to MX-only
        score = 45
        if out["is_role"]:
            score -= 5
        out["score"] = score
        out["verdict"] = "risky"
        out["reason"] = "SMTP probe inconclusive (port 25 may be blocked)"

    _cache_put(_VERIFY_CACHE, email, out)
    return out


async def verify_batch(
    emails: list[str],
    do_smtp: bool = True,
    concurrency: int = 6,
) -> list[dict]:
    """Verify many emails in parallel. Reuses one HTTP client for DNS lookups."""
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=8.0) as client:
        async def _one(e: str) -> dict:
            async with sem:
                try:
                    return await verify_email(e, client=client, do_smtp=do_smtp)
                except Exception as exc:
                    return {
                        "email": e, "valid_syntax": False, "score": 0,
                        "verdict": "unknown", "reason": f"verify error: {exc}",
                    }
        return await asyncio.gather(*[_one(e) for e in emails])
