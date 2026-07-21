# MedPharma — Client Hub

[Download MedPharma Hub](https://github.com/amanda858/MedPharma/archive/refs/heads/main.zip)

Client operations hub for claims, credentialing, enrollment, EDI setup, provider management, reporting, documents, and team workflows.

## Quick Start

```bash
pip install -r requirements.txt
python run.py
```

Open **<http://localhost:5240/hub>** in your browser.

## Premium Roadmap and Backlog

Execution-ready product and engineering backlog:

- `docs/premium-implementation-backlog.md`

## Enable Live Hub Notifications (Render)

To send real email/SMS (Eric owner reports from Jessica + RCM activity), set these on the **medpharma-hub** Render service:

- `NOTIFY_EMAIL=eric@medprosc.com`
- `NOTIFY_PHONE=+18036263500`
- `NOTIFY_ON_USERS=jessica`
- `SENDGRID_FROM=notifications@medprosc.com`

Choose **one email provider**:

- SendGrid: `SENDGRID_API_KEY`
- OR SMTP: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`

For SMS (Twilio):

- `TWILIO_SID`
- `TWILIO_TOKEN`
- `TWILIO_FROM`

Then redeploy the hub service and verify with:

- `GET /hub/api/notifications/status`
- `POST /hub/api/notifications/test`

Expected healthy status:

- `email_configured: true`
- `twilio_configured: true`

## Use medpharmahub.com (No Render URL)

The app is configured for custom domains on the `medpharma-hub` service:

- `medpharmahub.com`
- `www.medpharmahub.com`

To make it live:

1. Buy/own `medpharmahub.com` at any registrar (Cloudflare/Namecheap/GoDaddy).
2. In DNS, point:
   - `@` (apex) -> Render target (ALIAS/ANAME or A record per Render dashboard)
   - `www` -> CNAME to your Render hostname (for example `medpharma-hub.onrender.com`)

3. In Render service settings, confirm both domains are attached and SSL is issued.
4. Set your primary domain to `https://medpharmahub.com` and enable redirect from `www`.

Note: domain purchase is not free by default, but once owned, connecting it on Render is supported on the app side.

## Primary Surface

Use the hub at `/hub`. Legacy `/leads` and `/admin/leads` routes have been retired.

## Eligibility Engine

Every eligibility completion produces and persists the same machine-readable
five-stage state: `OPS`, `TRACK`, `COMMUNICATE`, `APPROVE`, `EXECUTE`,
`ELIGIBILITY_ENGINE`, and `ERRORS`. Member identifiers are masked and hashed in
the lifecycle state; raw payer evidence remains restricted to the eligibility
audit table.

Live active/inactive verification requires provider identity, one configured
source, and an authorized admin attestation that an appropriate signed BAA or
applicable data-use/trading-partner agreement is in effect before patient data
is transmitted:

- Stedi: `STEDI_API_KEY`, `STEDI_PROVIDER_NPI`, and `STEDI_PROVIDER_NAME`
- pVerify: `PVERIFY_CLIENT_ID`, `PVERIFY_CLIENT_SECRET`, provider NPI, and provider name
- CMS HETS (traditional Medicare only): HETS endpoint, submitter, credentials, provider NPI, and provider name

Credentials may be stored through the encrypted admin settings UI or supplied
as environment variables. Shared identity can use `ELIGIBILITY_PROVIDER_NPI`
and `ELIGIBILITY_PROVIDER_NAME`. Standalone engine use can set
`ELIGIBILITY_BAA_ATTESTED=1` only after the agreement is actually in effect.
The admin UI stores the attestation with its audit trail. Batch review also
requires `ELIG_SANDBOX=0`; otherwise it remains deterministic sandbox review.
Never commit credentials to this repository.

Machine-readable operations:

- `GET /hub/api/eligibility/{id}/engine-state`
- `POST /hub/api/eligibility/{id}/verify`
- `POST /hub/api/eligibility-check`
- `POST /hub/api/universal-eligibility-eval`
- `GET /hub/api/admin/eligibility/engine-tracker`
- `GET|POST /hub/api/admin/eligibility/rules`
- `DELETE /hub/api/admin/eligibility/rules/{id}` (soft deactivation)

Payer rules are deterministic, sourced, versioned, and scoped by facility,
payer, plan/product, CPT, criteria, and effective dates. A rule change places
affected records on hold until re-verification; it never fabricates payer data.

`eligibility_hybrid.universal_eligibility_engine(patient, insurance, provider,
visit)` applies the universal active-plan, network, plan-type, specialty, CPT,
prior-authorization, referral, and ICD-10 rules. `eligible` means at least one
ordered service is covered by the supplied payer facts. `billing_ready` is true
only when every ordered service is covered and required referral/prior-auth
evidence is present. The function reports missing payer facts instead of
assuming they are false or unrestricted.

Run the focused engine validation suite:

```bash
# VS Code: Run Task -> FINALIZE_PROVEN_COMPLIANT_TRUTH
```

The task runs all focused provider, policy, lifecycle, persistence, and route
contracts, compiles the Python owners, parses both hub scripts, and reports
success only when every local check passes. A live payer round trip still
requires configured credentials, agreement attestation, and approved test data.
