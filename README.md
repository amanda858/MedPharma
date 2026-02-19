# CVOPro — Lab Lead Generator

[Download MedPharma Hub](https://github.com/amanda858/MedPharma/archive/refs/heads/main.zip)

Find clinical laboratories across the United States for **medical billing**, **compliance**, and **payor contracting** services. Stop exhausting yourself with Google Ads and LinkedIn — pull real, verified leads directly from federal databases.

## What It Does

CVOPro searches **public federal databases** to find labs that are potential clients:

| Source | Data | Why It Matters |
|--------|------|----------------|
| **NPI Registry** (NPPES) | Every registered healthcare provider in the US | Real addresses, phone numbers, taxonomy codes — no guessing |
| **CLIA Database** | All CLIA-certified laboratories | Lab type, certification level, test volumes |

### Features

- **Single-State Search** — Find labs in any state by city, name, ZIP, or specialty
- **Multi-State Search** — Search up to 10 states at once to build territory lists
- **Lead Scoring** — Auto-scores labs 0-100 based on how likely they need billing/compliance help
- **Save & Manage Leads** — Save leads, track status (New > Contacted > Qualified > Proposal > Closed)
- **CSV Export** — Export any search or your saved leads to CSV for CRM import
- **Detail View** — Full NPI details, address, phone, fax, taxonomy, enumeration date
- **Dashboard** — Track your pipeline with stats and breakdowns

## Quick Start

```bash
pip install -r requirements.txt
python run.py
```

Open **<http://localhost:8000>** in your browser.

## How Lead Scoring Works

Each lab gets a score from 0-100 based on:

| Factor | Points | Why |
|--------|--------|-----|
| Organization (not solo provider) | +15 | Orgs have more billing volume |
| Clinical Medical Lab taxonomy | +20 | Primary target for billing services |
| Other lab taxonomy | +10 | Still relevant |
| Has phone number | +5 | Reachable lead |
| Recently updated NPI (2024+) | +10 | Actively operating |
| Updated in 2023 | +5 | Likely still active |

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/search/labs` | Search NPI Registry |
| GET | `/api/search/bulk` | Multi-state search |
| GET | `/api/npi/{npi}` | Get NPI details |
| POST | `/api/leads` | Save a lead |
| POST | `/api/leads/bulk` | Save multiple leads |
| GET | `/api/leads` | List saved leads |
| PUT | `/api/leads/{id}` | Update lead status/notes |
| DELETE | `/api/leads/{id}` | Delete a lead |
| GET | `/api/leads/stats` | Dashboard stats |
| GET | `/api/export/csv` | Export to CSV |

## Data Sources

- **NPI Registry**: <https://npiregistry.cms.hhs.gov> — Free, no API key required, updated weekly
- **CLIA**: <https://data.cms.gov> — CMS public data on certified laboratories

All data is publicly available from the Centers for Medicare & Medicaid Services (CMS).
