"""Application configuration."""

import os

# NPI Registry API (free, no key required)
NPI_API_BASE = "https://npiregistry.cms.hhs.gov/api/"
NPI_API_VERSION = "2.1"

# CLIA Data source (CMS public data)
CLIA_DATA_URL = "https://data.cms.gov/provider-compliance/certification-and-compliance/clia-laboratory"

# ── Business timezone ──
# The team operates on US/Eastern time and the daily report scheduler fires in
# that zone. "Today" rollups (dashboards, end-of-day reports, production
# snapshots) must therefore be anchored to the business timezone, NOT the
# server's UTC clock — otherwise a report that runs at 9 PM ET lands on the next
# UTC calendar day, querying an empty future day so the figures appear stuck and
# the day's work does not reflect.
BUSINESS_TIMEZONE = os.getenv("BUSINESS_TIMEZONE", "America/New_York")


def business_now():
    """Current datetime in the business timezone (US/Eastern by default).

    Resolves the zone via the stdlib ``zoneinfo`` first, then ``pytz``, and
    finally a fixed UTC-5 offset, so it works whether or not the optional tz
    packages are installed.
    """
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(BUSINESS_TIMEZONE))
    except Exception:
        try:
            import pytz
            return datetime.now(pytz.timezone(BUSINESS_TIMEZONE))
        except Exception:
            return datetime.now(timezone(timedelta(hours=-5)))


def business_today():
    """Current date (``date`` object) in the business timezone."""
    return business_now().date()


def business_today_iso():
    """Current date as a ``YYYY-MM-DD`` string in the business timezone."""
    return business_now().strftime("%Y-%m-%d")


# App settings
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", os.getenv("APP_PORT", "8000")))

# Separate ports for each service
LAB_PORT = int(os.getenv("LAB_PORT", "8000"))    # Lab Lead Generator
HUB_PORT = int(os.getenv("HUB_PORT", "5240"))    # Client Hub

# Public base URL of the Client Hub, used to build ABSOLUTE links in emails
# (report "Open Hub" buttons, chat-mention deep links, etc.). Relative links
# like "/hub?chat=5" do not work once an email leaves the app, so every email
# CTA must resolve through this value. May be supplied with or without a
# trailing "/hub" path — link builders normalize either form.
HUB_BASE_URL = os.getenv("HUB_BASE_URL", "https://medpharma-hub.onrender.com/hub")

# Prefer mounted persistent disk when available (Render: /data).
_DEFAULT_DB_PATH = "/data/leads.db" if os.path.isdir("/data") else "data/leads.db"
DATABASE_PATH = os.getenv("DB_PATH", _DEFAULT_DB_PATH)
DB_PATH = DATABASE_PATH  # alias used by other modules

# Hunter.io API key — free tier: 25 searches/month (sign up at hunter.io)
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")

# SerpAPI key — free tier: 100 searches/month (sign up at serpapi.com)
# Used to resolve real LinkedIn profile URLs via Google Search JSON API.
SERP_API_KEY = os.getenv("SERP_API_KEY", "")

# OpenAI API key (optional — AI narratives fall back to rule-based without it)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ── Notification settings ──
# Email (SMTP) — for sending activity alerts
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")          # e.g. alerts@medprosc.com
SMTP_PASS = os.getenv("SMTP_PASS", "")          # Gmail: use App Passwords
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "eric@medprosc.com")

# SMS (Twilio) — for text message alerts
TWILIO_SID = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM = os.getenv("TWILIO_FROM", "")       # Twilio phone number
NOTIFY_PHONE = os.getenv("NOTIFY_PHONE", "+18036263500")

# Search defaults
DEFAULT_RESULTS_LIMIT = 50
MAX_RESULTS_LIMIT = 200

# US States for filtering
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam"
}

# Lab taxonomy codes (NPI classification)
LAB_TAXONOMY_CODES = {
    "291U00000X": "Clinical Medical Laboratory",
    "292200000X": "Dental Laboratory",
    "293D00000X": "Physiological Laboratory",
    "291900000X": "Military Clinical Medical Laboratory",
    "246QB0000X": "Specialist/Technologist, Pathology - Blood Banking",
    "246QC1000X": "Specialist/Technologist, Pathology - Chemistry",
    "246QC2700X": "Specialist/Technologist, Pathology - Cytotechnology",
    "246QH0401X": "Specialist/Technologist, Pathology - Hemapheresis Practitioner",
    "246QH0600X": "Specialist/Technologist, Pathology - Histology",
    "246QI0000X": "Specialist/Technologist, Pathology - Immunology",
    "246QL0900X": "Specialist/Technologist, Pathology - Laboratory Management",
    "246QL0901X": "Specialist/Technologist, Pathology - Laboratory Management, Diplomate",
    "246QM0706X": "Specialist/Technologist, Pathology - Medical Technologist",
    "246QM0900X": "Specialist/Technologist, Pathology - Microbiology",
}
