"""Application configuration."""

import os

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

DATABASE_PATH = os.getenv("DB_PATH", "data/leads.db")

# Hunter.io API key
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "ccc11a9f717c2adde35fe72018a272d86b5a5d9b")

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
