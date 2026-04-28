"""Application configuration."""

import os


def _resolve_writable_data_dir() -> str:
    """Pick a writable data directory.

    Priority:
      1) Honor explicit DATA_DIR env var if it's writable.
      2) `/data` if it exists and is writable (Render paid disk).
      3) `/tmp/medpharma_data` (Render free-tier ephemeral fallback).
      4) Local `./data` for dev.
    """
    candidates = []
    explicit = os.getenv("DATA_DIR", "").strip()
    if explicit:
        candidates.append(explicit)
    candidates += ["/data", "/tmp/medpharma_data", "data"]
    for d in candidates:
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".write_test")
            with open(probe, "w") as f:
                f.write("ok")
            os.remove(probe)
            return d
        except Exception:
            continue
    # Last resort: cwd
    return "."


DATA_DIR = _resolve_writable_data_dir()
# Surface the resolved dir back into the env so subprocesses + other modules see it.
os.environ["DATA_DIR"] = DATA_DIR

# NPI Registry API (free, no key required)
NPI_API_BASE = "https://npiregistry.cms.hhs.gov/api/"
NPI_API_VERSION = "2.1"

# CLIA Data source (CMS public data)
CLIA_DATA_URL = "https://data.cms.gov/provider-compliance/certification-and-compliance/clia-laboratory"

# App settings
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", os.getenv("APP_PORT", "8000")))

# Separate ports for each service
LAB_PORT = int(os.getenv("LAB_PORT", "8000"))    # Lab Lead Generator
HUB_PORT = int(os.getenv("HUB_PORT", "5240"))    # Client Hub

# DB lives inside the resolved data dir. Honor explicit DB_PATH if the user/operator
# set one and its parent dir is writable; otherwise auto-route to DATA_DIR/leads.db.
def _resolve_db_path() -> str:
    explicit = os.getenv("DB_PATH", "").strip()
    if explicit:
        parent = os.path.dirname(explicit) or "."
        try:
            os.makedirs(parent, exist_ok=True)
            probe = os.path.join(parent, ".db_write_test")
            with open(probe, "w") as f:
                f.write("ok")
            os.remove(probe)
            return explicit
        except Exception:
            pass
    return os.path.join(DATA_DIR, "leads.db")


DATABASE_PATH = _resolve_db_path()
os.environ["DB_PATH"] = DATABASE_PATH

# Hunter.io API key (set via environment variable)
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")

# OpenAI API key (set via environment variable for AI report generation)
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
