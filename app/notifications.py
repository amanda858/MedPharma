"""
Notification system — Email & SMS alerts for team activity.

Sends notifications to configured admin contacts when team members
(jessica, rcm, or any non-admin user) make updates.

Configuration via environment variables:
  SMTP_HOST      — SMTP server (default: smtp.gmail.com)
  SMTP_PORT      — SMTP port (default: 587)
  SMTP_USER      — Email account to send from
  SMTP_PASS      — App password (Gmail: use App Passwords)
  NOTIFY_EMAIL   — Destination email for notifications
  TWILIO_SID     — Twilio Account SID
  TWILIO_TOKEN   — Twilio Auth Token
  TWILIO_FROM    — Twilio phone number (E.164 format, e.g. +18001234567)
  NOTIFY_PHONE   — Destination phone for SMS (E.164 format)
"""

import os
import logging
import threading
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

log = logging.getLogger("notifications")

# ── Configuration ──
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "eric@medprosc.com")

TWILIO_SID = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM = os.getenv("TWILIO_FROM", "")
NOTIFY_PHONE = os.getenv("NOTIFY_PHONE", "+18036263500")

# Users whose activity triggers notifications (non-admin team members)
NOTIFY_ON_USERS = {"jessica", "rcm"}


def _should_notify(username: str) -> bool:
    """Return True if this user's activity should trigger notifications."""
    return username.lower() in NOTIFY_ON_USERS


def _send_email(subject: str, body: str):
    """Send email notification via SMTP (runs in background thread)."""
    if not SMTP_USER or not SMTP_PASS or not NOTIFY_EMAIL:
        log.debug("Email notification skipped — SMTP_USER/SMTP_PASS/NOTIFY_EMAIL not configured")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = SMTP_USER
        msg["To"] = NOTIFY_EMAIL
        msg["Subject"] = subject

        # Plain text version
        msg.attach(MIMEText(body, "plain"))

        # HTML version
        html_body = f"""
        <html>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; color: #1e293b;">
            <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden;">
                <div style="background: linear-gradient(135deg, #4f46e5, #6366f1); padding: 20px 24px;">
                    <h2 style="color: white; margin: 0; font-size: 18px;">🔔 MedPharma Hub — Activity Alert</h2>
                </div>
                <div style="padding: 24px;">
                    <p style="white-space: pre-line; line-height: 1.7; font-size: 14px; color: #334155;">{body}</p>
                    <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                    <p style="font-size: 11px; color: #94a3b8;">This is an automated notification from MedPharma Hub.</p>
                </div>
            </div>
        </body>
        </html>"""
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, NOTIFY_EMAIL, msg.as_string())
        log.info(f"Email notification sent to {NOTIFY_EMAIL}: {subject}")
    except Exception as e:
        log.error(f"Failed to send email notification: {e}")


def _send_sms(message: str):
    """Send SMS notification via Twilio (runs in background thread)."""
    if not TWILIO_SID or not TWILIO_TOKEN or not TWILIO_FROM or not NOTIFY_PHONE:
        log.debug("SMS notification skipped — Twilio credentials not configured")
        return
    try:
        import httpx
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        data = {
            "To": NOTIFY_PHONE,
            "From": TWILIO_FROM,
            "Body": message,
        }
        resp = httpx.post(url, data=data, auth=(TWILIO_SID, TWILIO_TOKEN), timeout=15)
        if resp.status_code in (200, 201):
            log.info(f"SMS notification sent to {NOTIFY_PHONE}")
        else:
            log.error(f"Twilio SMS failed ({resp.status_code}): {resp.text}")
    except Exception as e:
        log.error(f"Failed to send SMS notification: {e}")


def notify_activity(username: str, action: str, section: str, detail: str = ""):
    """
    Fire email + SMS notification if the user is a monitored team member.

    Args:
        username: The user who performed the action
        action:   What they did (e.g. "created", "updated", "deleted", "imported")
        section:  Which section (e.g. "Claims", "Credentialing", "Enrollment")
        detail:   Optional extra detail (e.g. "Patient: John Doe, Payor: Aetna")
    """
    if not _should_notify(username):
        return

    timestamp = datetime.now().strftime("%b %d, %Y at %I:%M %p")

    # Email (more detailed)
    subject = f"MedPharma Hub: {username} {action} {section}"
    body = (
        f"Team member '{username}' {action} a record in {section}.\n\n"
        f"Time: {timestamp}\n"
        f"Action: {action.upper()}\n"
        f"Section: {section}\n"
    )
    if detail:
        body += f"Details: {detail}\n"

    # SMS (shorter)
    sms = f"MedPharma: {username} {action} {section}"
    if detail:
        # Truncate detail for SMS (limit ~120 chars to stay in 1 SMS)
        short_detail = detail[:80] + "…" if len(detail) > 80 else detail
        sms += f" — {short_detail}"

    # Fire both in background threads so the API response isn't delayed
    threading.Thread(target=_send_email, args=(subject, body), daemon=True).start()
    threading.Thread(target=_send_sms, args=(sms,), daemon=True).start()


def notify_bulk_activity(username: str, action: str, section: str, count: int, detail: str = ""):
    """Notify for bulk operations (imports, bulk status changes)."""
    if not _should_notify(username):
        return

    timestamp = datetime.now().strftime("%b %d, %Y at %I:%M %p")

    subject = f"MedPharma Hub: {username} {action} {count} {section} records"
    body = (
        f"Team member '{username}' {action} {count} records in {section}.\n\n"
        f"Time: {timestamp}\n"
        f"Action: {action.upper()}\n"
        f"Section: {section}\n"
        f"Records affected: {count}\n"
    )
    if detail:
        body += f"Details: {detail}\n"

    sms = f"MedPharma: {username} {action} {count} {section} records"

    threading.Thread(target=_send_email, args=(subject, body), daemon=True).start()
    threading.Thread(target=_send_sms, args=(sms,), daemon=True).start()
