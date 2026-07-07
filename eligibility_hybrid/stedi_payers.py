"""Stedi Payers API — the free payer-ID crosswalk.

Turns a hand-typed payor name ("Humana Medicare", "Railroad Medicare",
"WellCare") into the real payer id Stedi routes on, so a 270 actually reaches
the payer instead of bouncing. Uses the SAME Stedi API key as eligibility, on a
separate host:

    GET  https://payers.us.stedi.com/2024-04-01/payers/search
         ?query=<name|id|alias>&eligibilityCheck=SUPPORTED     (fuzzy match)
    GET  https://payers.us.stedi.com/2024-04-01/payers
         ?pageSize=<n>&pageToken=<t>                           (full list)

Auth is the `Authorization: <api-key>` header (identical to the eligibility
API). Fuzzy, case-insensitive matching: `cig`, `62308`, and `SX071` all resolve
to Cigna.

HONESTY CONTRACT (identical to the eligibility connector):
  • With no API key it is NOT configured and returns nothing — it never guesses
    a payer id.
  • Network / HTTP errors are raised, not swallowed into a fabricated match, so
    the caller can fall back to a safe default instead of a wrong payer.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

DEFAULT_BASE_URL = "https://payers.us.stedi.com/2024-04-01"


class StediPayers:
    """Thin client over Stedi's Payers API (list + fuzzy search)."""

    def __init__(self, api_key: str = "", base_url: str = "", timeout: int = 30):
        self.api_key = (api_key or "").strip()
        self.base_url = (base_url or DEFAULT_BASE_URL).strip().rstrip("/")
        self.timeout = timeout

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.base_url)

    # ── HTTP GET (JSON) ───────────────────────────────────────────────────────
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = self.base_url + path
        if params:
            clean = {k: v for k, v in params.items() if v not in (None, "")}
            query = urllib.parse.urlencode(clean)
            if query:
                url += "?" + query
        request = urllib.request.Request(
            url, headers={"Authorization": self.api_key,
                          "Accept": "application/json"}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as r:
                return json.loads(r.read().decode("utf-8", errors="replace") or "{}")
        except urllib.error.HTTPError as e:  # pragma: no cover - needs live key
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="replace")[:400]
            except Exception:
                pass
            raise RuntimeError(f"Stedi Payers HTTP {e.code}: {detail}")
        except Exception as e:  # pragma: no cover - network
            raise RuntimeError(f"Stedi Payers request failed: {e}")

    # ── public API ────────────────────────────────────────────────────────────
    def search(self, query: str, eligibility_only: bool = True,
               page_size: int = 20) -> list[dict]:
        """Fuzzy-search the Payer Network. Returns normalized summaries, most
        relevant first. Empty list when unconfigured or no query."""
        if not self.configured or not (query or "").strip():
            return []
        params: dict = {"query": query.strip(), "pageSize": page_size}
        if eligibility_only:
            params["eligibilityCheck"] = "SUPPORTED"
        data = self._get("/payers/search", params)
        out: list[dict] = []
        for item in (data.get("items") or []):
            payer = (item or {}).get("payer") or {}
            if payer:
                out.append(_payer_summary(payer))
        return out

    def resolve_payer_id(self, query: str) -> Optional[str]:
        """Best single payer id (primaryPayerId) for a typed name/id/alias, or
        None if nothing matched. This is the value to drop into an eligibility
        check's `tradingPartnerServiceId`."""
        for hit in self.search(query, eligibility_only=True, page_size=5):
            pid = hit.get("primary_payer_id") or hit.get("stedi_id")
            if pid:
                return pid
        return None

    def list_payers(self, page_size: int = 100,
                    page_token: str = "") -> tuple[list[dict], str]:
        """One page of the full payer list. Returns (summaries, next_page_token);
        an empty token means the last page."""
        if not self.configured:
            return [], ""
        data = self._get("/payers", {"pageSize": page_size, "pageToken": page_token})
        rows = [_payer_summary(p) for p in (data.get("items") or []) if p]
        return rows, str(data.get("nextPageToken") or "")


def _payer_summary(p: dict) -> dict:
    """Flatten a Stedi PayerRecord to the fields we route/display on."""
    support = p.get("transactionSupport") or {}
    elig = support.get("eligibilityCheck", "")
    return {
        "stedi_id": p.get("stediId", ""),
        "primary_payer_id": p.get("primaryPayerId", ""),
        "display_name": p.get("displayName", ""),
        "aliases": p.get("aliases") or [],
        "programs": p.get("programs") or [],
        "coverage_types": p.get("coverageTypes") or [],
        "eligibility_support": elig,
        "eligibility_supported": elig in ("SUPPORTED", "ENROLLMENT_REQUIRED"),
        "enrollment_required": elig == "ENROLLMENT_REQUIRED",
    }


def build_stedi_payers(api_key: str = "", base_url: str = "") -> StediPayers:
    """Factory reading STEDI_API_KEY (and optional STEDI_PAYERS_URL) from env."""
    return StediPayers(
        api_key=api_key or os.getenv("STEDI_API_KEY", ""),
        base_url=base_url or os.getenv("STEDI_PAYERS_URL", ""),
    )


def resolve_payer_id(query: str, api_key: str = "") -> Optional[str]:
    """Convenience: resolve a typed payer name/id to a real payer id, or None."""
    return build_stedi_payers(api_key=api_key).resolve_payer_id(query)
