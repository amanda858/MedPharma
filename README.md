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
