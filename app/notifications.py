"""
Notification system — Daily summary Email & SMS alerts for team activity.

Buffers all activity during a user's session and sends ONE consolidated
summary notification when the user logs out (ends their work for the day).

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
from collections import defaultdict
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

# ── In-memory activity buffer (keyed by username) ──
# Each entry: list of {"action", "section", "detail", "timestamp"}
_activity_buffer: dict[str, list[dict]] = defaultdict(list)
_buffer_lock = threading.Lock()


def _should_notify(username: str) -> bool:
    """Return True if this user's activity should trigger notifications."""
    return username.lower() in NOTIFY_ON_USERS


# ── Public API — called from route handlers ──

def notify_activity(username: str, action: str, section: str, detail: str = ""):
    """Buffer a single activity event (does NOT send immediately)."""
    if not _should_notify(username):
        return
    with _buffer_lock:
        _activity_buffer[username.lower()].append({
            "action": action,
            "section": section,
            "detail": detail,
            "timestamp": datetime.now().strftime("%I:%M %p"),
        })


def notify_bulk_activity(username: str, action: str, section: str, count: int, detail: str = ""):
    """Buffer a bulk activity event (does NOT send immediately)."""
    if not _should_notify(username):
        return
    with _buffer_lock:
        _activity_buffer[username.lower()].append({
            "action": f"{action} {count} records in",
            "section": section,
            "detail": detail,
            "timestamp": datetime.now().strftime("%I:%M %p"),
        })


def flush_and_notify(username: str):
    """
    Called at logout — sends ONE consolidated summary of everything
    the user did during their session, then clears the buffer.
    """
    key = username.lower()
    with _buffer_lock:
        activities = list(_activity_buffer.pop(key, []))

    if not activities or not _should_notify(username):
        return

    # Build the summary
    now = datetime.now()
    date_str = now.strftime("%B %d, %Y")
    time_str = now.strftime("%I:%M %p")

    # Group by section for a clean summary
    by_section: dict[str, list[dict]] = defaultdict(list)
    for a in activities:
        by_section[a["section"]].append(a)

    # ── Plain text body ──
    lines = [
        f"Daily Work Summary for '{username}'",
        f"Date: {date_str}  |  Logged out: {time_str}",
        f"Total actions: {len(activities)}",
        "",
    ]
    for section, items in by_section.items():
        lines.append(f"── {section} ({len(items)} actions) ──")
        for item in items:
            line = f"  • {item['timestamp']} — {item['action']} {item['section']}"
            if item.get("detail"):
                line += f" ({item['detail']})"
            lines.append(line)
        lines.append("")

    body = "\n".join(lines)

    # ── HTML body for email ──
    section_html = ""
    for section, items in by_section.items():
        rows = ""
        for item in items:
            detail_txt = f"<br><span style='color:#64748b;font-size:12px'>{item['detail']}</span>" if item.get("detail") else ""
            rows += f"""<tr>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;color:#64748b">{item['timestamp']}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">{item['action']}{detail_txt}</td>
            </tr>"""
        section_html += f"""
        <div style="margin-bottom:16px">
            <div style="font-weight:700;font-size:14px;color:#1e293b;padding:8px 0;border-bottom:2px solid #6366f1">{section} — {len(items)} action{'s' if len(items)!=1 else ''}</div>
            <table style="width:100%;border-collapse:collapse">{rows}</table>
        </div>"""

    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; color: #1e293b;">
        <div style="max-width: 640px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden;">
            <div style="background: linear-gradient(135deg, #4f46e5, #6366f1); padding: 20px 24px;">
                <h2 style="color: white; margin: 0; font-size: 18px;">📋 MedPharma Hub — Daily Work Summary</h2>
                <p style="color: rgba(255,255,255,0.8); margin: 6px 0 0; font-size: 13px;">{username} • {date_str}</p>
            </div>
            <div style="padding: 24px;">
                <div style="display:flex;gap:24px;margin-bottom:20px;">
                    <div style="background:#eef2ff;border-radius:10px;padding:14px 20px;flex:1;text-align:center">
                        <div style="font-size:28px;font-weight:800;color:#4f46e5">{len(activities)}</div>
                        <div style="font-size:11px;font-weight:600;color:#6366f1;text-transform:uppercase">Total Actions</div>
                    </div>
                    <div style="background:#f0fdf4;border-radius:10px;padding:14px 20px;flex:1;text-align:center">
                        <div style="font-size:28px;font-weight:800;color:#16a34a">{len(by_section)}</div>
                        <div style="font-size:11px;font-weight:600;color:#16a34a;text-transform:uppercase">Sections</div>
                    </div>
                    <div style="background:#fef3c7;border-radius:10px;padding:14px 20px;flex:1;text-align:center">
                        <div style="font-size:16px;font-weight:700;color:#d97706">{time_str}</div>
                        <div style="font-size:11px;font-weight:600;color:#d97706;text-transform:uppercase">Logged Out</div>
                    </div>
                </div>
                {section_html}
                <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                <p style="font-size: 11px; color: #94a3b8;">Automated daily summary from MedPharma Hub.</p>
            </div>
        </div>
    </body>
    </html>"""

    subject = f"MedPharma Hub: {username}'s daily summary — {len(activities)} actions on {date_str}"

    # SMS — concise one-liner
    section_counts = ", ".join(f"{s}: {len(items)}" for s, items in by_section.items())
    sms = f"MedPharma: {username} completed {len(activities)} actions today — {section_counts}"
    if len(sms) > 155:
        sms = sms[:152] + "…"

    # Fire both in background threads
    threading.Thread(target=_send_email, args=(subject, body, html_body), daemon=True).start()
    threading.Thread(target=_send_sms, args=(sms,), daemon=True).start()
    log.info(f"Daily summary queued for {username}: {len(activities)} actions across {len(by_section)} sections")


# ── Send helpers ──

def _send_email(subject: str, body: str, html_body: str = ""):
    """Send email notification via SMTP."""
    if not SMTP_USER or not SMTP_PASS or not NOTIFY_EMAIL:
        log.debug("Email notification skipped — SMTP_USER/SMTP_PASS/NOTIFY_EMAIL not configured")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = SMTP_USER
        msg["To"] = NOTIFY_EMAIL
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        if html_body:
            msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, NOTIFY_EMAIL, msg.as_string())
        log.info(f"Email sent to {NOTIFY_EMAIL}: {subject}")
    except Exception as e:
        log.error(f"Failed to send email: {e}")


def _send_sms(message: str):
    """Send SMS notification via Twilio."""
    if not TWILIO_SID or not TWILIO_TOKEN or not TWILIO_FROM or not NOTIFY_PHONE:
        log.debug("SMS notification skipped — Twilio not configured")
        return
    try:
        import httpx
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        data = {"To": NOTIFY_PHONE, "From": TWILIO_FROM, "Body": message}
        resp = httpx.post(url, data=data, auth=(TWILIO_SID, TWILIO_TOKEN), timeout=15)
        if resp.status_code in (200, 201):
            log.info(f"SMS sent to {NOTIFY_PHONE}")
        else:
            log.error(f"Twilio SMS failed ({resp.status_code}): {resp.text}")
    except Exception as e:
        log.error(f"Failed to send SMS: {e}")
