"""
Notification system — Individual Progress Report.

Buffers all activity during a user's session and sends ONE consolidated
"Individual Progress" report when the user logs out, including:
  • Activity breakdown by section
  • Industry-standard RCM benchmarks comparison
  • AI-powered productivity analysis (via OpenAI, with rule-based fallback)

Configuration via environment variables:
  SENDGRID_API_KEY — SendGrid API key for sending email
  NOTIFY_EMAIL     — Comma-separated destination emails for notifications
  SENDGRID_FROM    — Sender email address (must be verified in SendGrid)
  TWILIO_SID     — Twilio Account SID
  TWILIO_TOKEN   — Twilio Auth Token
  TWILIO_FROM    — Twilio phone number (E.164 format, e.g. +18001234567)
  NOTIFY_PHONE   — Destination phone for SMS (E.164 format)
  OPENAI_API_KEY — For AI productivity narrative (optional; falls back to rule-based)
"""

import os
import logging
import threading
import json
import smtplib
import urllib.request
import urllib.parse
import urllib.error
import base64
from collections import defaultdict
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger("notifications")


def _normalize_phone(value: str) -> str:
    """Normalize phone input to E.164 when possible (US-focused fallback)."""
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("+"):
        return raw
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return raw

# ── Configuration ──
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
SENDGRID_FROM = os.getenv("SENDGRID_FROM", "notifications@medprosc.com")
NOTIFY_EMAILS = [e.strip() for e in os.getenv("NOTIFY_EMAIL", "eric@medprosc.com").split(",") if e.strip()]

TWILIO_SID = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM = os.getenv("TWILIO_FROM", "")
NOTIFY_PHONE = _normalize_phone(os.getenv("NOTIFY_PHONE", "+18036263500"))

# Free carrier email-to-SMS gateways — no Twilio required.
# Set NOTIFY_PHONE_CARRIER to your carrier name (e.g. "att", "verizon", "tmobile").
# The 10-digit phone from NOTIFY_PHONE is sent as an email to <number>@<gateway>.
_CARRIER_GATEWAYS: dict[str, str] = {
    "att": "txt.att.net",
    "at&t": "txt.att.net",
    "verizon": "vtext.com",
    "vzw": "vtext.com",
    "tmobile": "tmomail.net",
    "t-mobile": "tmomail.net",
    "sprint": "messaging.sprintpcs.com",
    "metro": "mymetropcs.com",
    "metropcs": "mymetropcs.com",
    "boost": "sms.myboostmobile.com",
    "cricket": "sms.cricketwireless.net",
    "uscellular": "email.uscc.net",
    "us cellular": "email.uscc.net",
    "straighttalk": "vtext.com",
    "tracfone": "txt.att.net",
    "googlefi": "msg.fi.google.com",
    "fi": "msg.fi.google.com",
}
NOTIFY_PHONE_CARRIER = os.getenv("NOTIFY_PHONE_CARRIER", "").strip().lower()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
IN_APP_ONLY_MODE = os.getenv("NOTIFY_IN_APP_ONLY", "0").strip().lower() in {"1", "true", "yes", "on"}


def _carrier_sms_email(phone: str, carrier: str) -> str | None:
    """Return email-to-SMS gateway address for a phone+carrier combo, or None."""
    gateway = _CARRIER_GATEWAYS.get(carrier.strip().lower(), "")
    if not gateway:
        return None
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"{digits}@{gateway}"


def _live_config() -> dict:
    """Read notification credentials FRESH from env vars every call.
    This avoids stale module-level caches if env vars were set after import."""
    sg_key = os.getenv("SENDGRID_API_KEY", "") or SENDGRID_API_KEY
    sg_from = os.getenv("SENDGRID_FROM", "notifications@medprosc.com") or SENDGRID_FROM
    emails = [e.strip() for e in os.getenv("NOTIFY_EMAIL", "eric@medprosc.com").split(",") if e.strip()] or NOTIFY_EMAILS
    t_sid = os.getenv("TWILIO_SID", "") or TWILIO_SID
    t_tok = os.getenv("TWILIO_TOKEN", "") or TWILIO_TOKEN
    t_from = _normalize_phone(os.getenv("TWILIO_FROM", "") or TWILIO_FROM)
    phone = _normalize_phone(os.getenv("NOTIFY_PHONE", "+18036263500")) or NOTIFY_PHONE
    carrier = os.getenv("NOTIFY_PHONE_CARRIER", "").strip().lower() or NOTIFY_PHONE_CARRIER
    smtp_h = os.getenv("SMTP_HOST", "smtp.gmail.com") or SMTP_HOST
    smtp_p = int(os.getenv("SMTP_PORT", "587") or SMTP_PORT)
    smtp_u = os.getenv("SMTP_USER", "") or SMTP_USER
    smtp_pw = os.getenv("SMTP_PASS", "") or SMTP_PASS
    in_app = os.getenv("NOTIFY_IN_APP_ONLY", "0").strip().lower() in {"1", "true", "yes", "on"}
    return {
        "SENDGRID_API_KEY": sg_key, "SENDGRID_FROM": sg_from, "NOTIFY_EMAILS": emails,
        "TWILIO_SID": t_sid, "TWILIO_TOKEN": t_tok, "TWILIO_FROM": t_from,
        "NOTIFY_PHONE": phone, "NOTIFY_PHONE_CARRIER": carrier,
        "SMTP_HOST": smtp_h, "SMTP_PORT": smtp_p,
        "SMTP_USER": smtp_u, "SMTP_PASS": smtp_pw, "IN_APP_ONLY_MODE": in_app,
    }

# Users whose activity triggers notifications (team members only).
# Owner (Eric) should receive reports, not be tracked as a worker by default.
# Supports comma-separated env var, e.g. NOTIFY_ON_USERS="jessica,rcm"
# Use NOTIFY_ON_USERS="*" to enable for all users.
# Default is "*" so every staff/client action funnels to the admin inbox; set
# NOTIFY_ON_USERS in the environment to restrict it to a specific allowlist.
_notify_on_users_env = os.getenv("NOTIFY_ON_USERS", "*").strip()
NOTIFY_ON_USERS = {
    u.strip().lower() for u in _notify_on_users_env.split(",") if u.strip()
} if _notify_on_users_env and _notify_on_users_env != "*" else {"*"}

# SMTP (primary send path — no SendGrid account required)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")


# ── Industry-standard RCM benchmarks (actions per 8-hour day) ──
# Sources: MGMA, HBMA, AAPC industry reports for medical billing / credentialing
INDUSTRY_BENCHMARKS = {
    "Claims":          {"daily_target": 180, "unit": "claims touched", "per_hour": 25,
                        "note": "Industry avg: 25-35 claims processed/hr (AR follow-up, posting, submissions)"},
    "Credentialing":   {"daily_target": 12,  "unit": "credentialing actions", "per_hour": 1.5,
                        "note": "Industry avg: 3-5 new apps + 8-15 follow-ups/day"},
    "EDI":             {"daily_target": 40,  "unit": "EDI transactions", "per_hour": 5,
                        "note": "Industry avg: 40-60 clearinghouse transactions/day"},
    "Production":      {"daily_target": 10,  "unit": "production log entries", "per_hour": 1.25,
                        "note": "Standard: logging tasks, time tracking, QA notes"},
    "SLA Tracking":    {"daily_target": 15,  "unit": "SLA updates", "per_hour": 2,
                        "note": "Industry avg: 15-25 SLA/TAT status updates/day"},
    "Notes":           {"daily_target": 20,  "unit": "notes", "per_hour": 2.5,
                        "note": "Standard documentation pace for RCM workflows"},
}
# Catch-all for sections not explicitly listed
_DEFAULT_BENCHMARK = {"daily_target": 15, "unit": "actions", "per_hour": 2,
                      "note": "General administrative RCM benchmark"}

# ── In-memory activity buffer (keyed by username) ──
# Each entry: list of {"action", "section", "detail", "timestamp", "raw_ts"}
_activity_buffer: dict[str, list[dict]] = defaultdict(list)
_session_start: dict[str, datetime] = {}          # first activity time per user
_buffer_lock = threading.Lock()

# Auto-flush tuning (no manual logout required)
AUTO_FLUSH_ACTION_THRESHOLD = int(os.getenv("NOTIFY_AUTO_FLUSH_THRESHOLD", "20"))
AUTO_FLUSH_MAX_AGE_MINUTES = int(os.getenv("NOTIFY_AUTO_FLUSH_MAX_AGE_MINUTES", "60"))


def _should_notify(username: str) -> bool:
    """Return True if this user's activity should trigger notifications."""
    u = (username or "").lower()
    if "*" in NOTIFY_ON_USERS:
        return True
    alias_map = {
        "jess": {"jess", "jessica"},
        "jessica": {"jess", "jessica"},
        "eric": {"eric", "admin"},
        "admin": {"eric", "admin"},
        "rcm": {"rcm"},
    }
    aliases = alias_map.get(u, {u})
    return any(a in NOTIFY_ON_USERS for a in aliases)


def _get_benchmark(section: str) -> dict:
    """Return the industry benchmark dict for a section (fuzzy match)."""
    s = section.strip().lower()
    for key, bench in INDUSTRY_BENCHMARKS.items():
        if key.lower() in s or s in key.lower():
            return {**bench, "section_key": key}
    return {**_DEFAULT_BENCHMARK, "section_key": section}


# ───────────────────────────────────────────────────────────────────────
#  AI Productivity Analysis
# ───────────────────────────────────────────────────────────────────────

def _generate_ai_summary(username: str, date_str: str, session_hrs: float,
                         by_section: dict, benchmarks_data: list, overall_pct: float) -> str:
    """
    Call OpenAI to produce a 3-5 sentence narrative evaluating the employee's
    productivity against RCM industry standards.  Falls back to rule-based if
    API key is missing or the call fails.
    """
    section_summary = "\n".join(
        f"  - {b['section']}: {b['actual']} actions done, benchmark {b['target']}/day "
        f"({b['pct']}% of target). {b['note']}"
        for b in benchmarks_data
    )

    prompt = f"""You are a medical billing team lead reviewing an employee's daily production.

Employee: {username}
Date: {date_str}
Active session: {session_hrs:.1f} hours
Overall productivity: {overall_pct:.0f}% of industry standard

Section-by-section breakdown:
{section_summary}

Industry context: Standard RCM workday is 7.5-8 hrs. Medical billing specialists
should process 25-35 claims/hr, credentialing staff handle 3-5 new apps + 15 follow-ups/day.

Write a concise 3-5 sentence "Individual Progress Assessment" that:
1. States whether this employee met, exceeded, or fell short of daily expectations
2. Highlights their strongest area and any area needing improvement
3. Assesses whether the employee worked a productive and sufficient day
4. Gives one specific, actionable recommendation

Keep the tone professional but direct — this is an internal team lead report.
Do NOT use bullet points; write in paragraph form. Do not include any greeting.
"""

    if not OPENAI_API_KEY:
        return _rule_based_summary(username, session_hrs, benchmarks_data, overall_pct)

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You are a healthcare RCM operations team lead."},
                      {"role": "user", "content": prompt}],
            max_tokens=350,
            temperature=0.7,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"OpenAI productivity analysis failed: {e}")
        return _rule_based_summary(username, session_hrs, benchmarks_data, overall_pct)


def _rule_based_summary(username: str, session_hrs: float,
                        benchmarks_data: list, overall_pct: float) -> str:
    """Fallback narrative when OpenAI is unavailable."""
    # Find best and worst sections
    if not benchmarks_data:
        return f"{username} had no measurable activity to evaluate."

    best = max(benchmarks_data, key=lambda b: b["pct"])
    worst = min(benchmarks_data, key=lambda b: b["pct"])

    if overall_pct >= 100:
        rating = "exceeded daily production expectations"
    elif overall_pct >= 75:
        rating = "met most daily production expectations"
    elif overall_pct >= 50:
        rating = "fell below expected daily production volume"
    else:
        rating = "significantly underperformed against industry benchmarks"

    summary = (
        f"{username} {rating} with an overall productivity score of {overall_pct:.0f}% "
        f"across a {session_hrs:.1f}-hour session. "
    )
    if best["pct"] > 0:
        summary += (
            f"Their strongest area was {best['section']} at {best['pct']:.0f}% of target "
            f"({best['actual']} of {best['target']} expected). "
        )
    if worst["section"] != best["section"] and worst["pct"] < 80:
        summary += (
            f"{worst['section']} needs attention — only {worst['pct']:.0f}% of the daily benchmark was reached. "
        )

    if session_hrs < 6:
        summary += (
            f"The session duration of {session_hrs:.1f} hours is below the standard 7.5-8 hour workday, "
            f"which likely contributed to lower output. Consider reviewing time management."
        )
    elif overall_pct < 75:
        summary += "Recommend reviewing workflow efficiency and reducing non-productive time."
    else:
        summary += "Keep up the consistent work."

    return summary


# ── Public API — called from route handlers ──

def notify_activity(username: str, action: str, section: str, detail: str = "", sms_copy: bool = False):
    """Buffer a single activity event.
    If sms_copy=True, also send an immediate concise SMS copy."""
    if not _should_notify(username):
        return
    should_flush = False
    sms_text = ""
    with _buffer_lock:
        now = datetime.now()
        key = username.lower()
        if key not in _session_start:
            _session_start[key] = now
        _activity_buffer[key].append({
            "action": action,
            "section": section,
            "detail": detail,
            "timestamp": now.strftime("%I:%M %p"),
            "raw_ts": now,
        })
        should_flush = len(_activity_buffer[key]) >= AUTO_FLUSH_ACTION_THRESHOLD
        if sms_copy:
            sms_text = f"{username}: {action} {section}"
            if detail:
                sms_text += f" | {detail}"
            if len(sms_text) > 155:
                sms_text = sms_text[:152] + "…"

    if should_flush:
        threading.Thread(target=flush_and_notify, args=(username,), daemon=True).start()
    if sms_text:
        threading.Thread(target=_send_sms, args=(sms_text,), daemon=True).start()


def notify_bulk_activity(username: str, action: str, section: str, count: int, detail: str = ""):
    """Buffer a bulk activity event (does NOT send immediately)."""
    if not _should_notify(username):
        return
    should_flush = False
    with _buffer_lock:
        now = datetime.now()
        key = username.lower()
        if key not in _session_start:
            _session_start[key] = now
        _activity_buffer[key].append({
            "action": f"{action} {count} records in",
            "section": section,
            "detail": detail,
            "timestamp": now.strftime("%I:%M %p"),
            "raw_ts": now,
        })
        should_flush = len(_activity_buffer[key]) >= AUTO_FLUSH_ACTION_THRESHOLD

    if should_flush:
        threading.Thread(target=flush_and_notify, args=(username,), daemon=True).start()


def flush_all_pending_notifications():
    """
    Flush buffered notifications for users with enough activity or stale sessions.
    This prevents dependence on manual logout.
    """
    with _buffer_lock:
        snapshot = {
            user: list(items)
            for user, items in _activity_buffer.items()
            if items
        }

    now = datetime.now()
    for user, items in snapshot.items():
        if not items:
            continue
        last_ts = items[-1].get("raw_ts")
        age_min = ((now - last_ts).total_seconds() / 60.0) if isinstance(last_ts, datetime) else 0
        if len(items) >= AUTO_FLUSH_ACTION_THRESHOLD or age_min >= AUTO_FLUSH_MAX_AGE_MINUTES:
            try:
                flush_and_notify(user)
            except Exception as e:
                log.error(f"Auto flush failed for {user}: {e}")


def flush_and_notify(username: str):
    """
    Called at logout — builds a full **Individual Progress** report with:
      • Activity breakdown by section
      • Industry benchmark comparison
      • AI-powered productivity narrative
    Sends one email + SMS, then clears the buffer.
    """
    key = username.lower()
    with _buffer_lock:
        activities = list(_activity_buffer.pop(key, []))
        session_start = _session_start.pop(key, None)

    if not activities or not _should_notify(username):
        return

    # ── Timing ──
    now = datetime.now()
    date_str = now.strftime("%B %d, %Y")
    time_str = now.strftime("%I:%M %p")
    if session_start:
        session_hrs = (now - session_start).total_seconds() / 3600
        start_str = session_start.strftime("%I:%M %p")
    else:
        session_hrs = 0
        start_str = "N/A"

    # ── Group by section ──
    by_section: dict[str, list[dict]] = defaultdict(list)
    for a in activities:
        by_section[a["section"]].append(a)

    # ── Benchmark comparison ──
    benchmarks_data = []
    total_pct_sum, total_sections = 0, 0
    for section, items in by_section.items():
        bench = _get_benchmark(section)
        actual = len(items)
        # Pro-rate target if session < 8 hrs
        effective_hrs = min(session_hrs, 8) if session_hrs > 0 else 8
        prorated_target = max(1, round(bench["daily_target"] * (effective_hrs / 8)))
        pct = round((actual / prorated_target) * 100, 1) if prorated_target else 0
        benchmarks_data.append({
            "section": section,
            "actual": actual,
            "target": prorated_target,
            "full_day_target": bench["daily_target"],
            "pct": pct,
            "per_hour": bench["per_hour"],
            "unit": bench["unit"],
            "note": bench["note"],
        })
        total_pct_sum += pct
        total_sections += 1

    overall_pct = round(total_pct_sum / total_sections, 1) if total_sections else 0

    # ── Productivity rating badge ──
    if overall_pct >= 110:
        rating_label, rating_color, rating_bg = "EXCEEDS STANDARDS", "#15803d", "#dcfce7"
    elif overall_pct >= 85:
        rating_label, rating_color, rating_bg = "MEETS STANDARDS", "#2563eb", "#dbeafe"
    elif overall_pct >= 60:
        rating_label, rating_color, rating_bg = "BELOW EXPECTATIONS", "#d97706", "#fef3c7"
    else:
        rating_label, rating_color, rating_bg = "NEEDS IMPROVEMENT", "#dc2626", "#fee2e2"

    # ── AI Summary ──
    ai_summary = _generate_ai_summary(username, date_str, session_hrs,
                                       by_section, benchmarks_data, overall_pct)

    # ── Plain text body ──
    lines = [
        "═══════════════════════════════════════════",
        "       INDIVIDUAL PROGRESS REPORT",
        "═══════════════════════════════════════════",
        "",
        f"  Employee:  {username}",
        f"  Date:      {date_str}",
        f"  Session:   {start_str} — {time_str} ({session_hrs:.1f} hrs)",
        f"  Rating:    {rating_label} ({overall_pct:.0f}%)",
        "",
        "───────────────────────────────────────────",
        "  AI PRODUCTIVITY ASSESSMENT",
        "───────────────────────────────────────────",
        "",
        f"  {ai_summary}",
        "",
        "───────────────────────────────────────────",
        "  PRODUCTION vs INDUSTRY BENCHMARKS",
        "───────────────────────────────────────────",
        "",
    ]
    for b in benchmarks_data:
        bar_filled = min(20, round(b["pct"] / 5))
        bar = "█" * bar_filled + "░" * (20 - bar_filled)
        lines.append(f"  {b['section']}")
        lines.append(f"    Done: {b['actual']}  |  Target: {b['target']}  |  {b['pct']:.0f}%")
        lines.append(f"    [{bar}]")
        lines.append(f"    {b['note']}")
        lines.append("")

    lines += [
        "───────────────────────────────────────────",
        "  ACTIVITY DETAIL",
        "───────────────────────────────────────────",
        "",
    ]
    for section, items in by_section.items():
        lines.append(f"  ── {section} ({len(items)} actions) ──")
        for item in items:
            line = f"    • {item['timestamp']} — {item['action']} {item['section']}"
            if item.get("detail"):
                line += f" ({item['detail']})"
            lines.append(line)
        lines.append("")

    body = "\n".join(lines)

    # ── HTML body for email ──
    # Benchmark rows
    bench_rows_html = ""
    for b in benchmarks_data:
        pct_clamped = min(b["pct"], 100)
        if b["pct"] >= 100:
            bar_color = "#22c55e"
        elif b["pct"] >= 70:
            bar_color = "#3b82f6"
        elif b["pct"] >= 40:
            bar_color = "#f59e0b"
        else:
            bar_color = "#ef4444"
        bench_rows_html += f"""
        <tr>
            <td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;font-weight:600;font-size:13px;white-space:nowrap">{b['section']}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-size:14px;font-weight:700">{b['actual']}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-size:13px;color:#64748b">{b['target']}</td>
            <td style="padding:10px 8px;border-bottom:1px solid #f1f5f9;width:140px">
                <div style="background:#f1f5f9;border-radius:6px;height:12px;overflow:hidden">
                    <div style="background:{bar_color};height:100%;width:{pct_clamped}%;border-radius:6px"></div>
                </div>
            </td>
            <td style="padding:10px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-weight:700;font-size:13px;color:{bar_color}">{b['pct']:.0f}%</td>
        </tr>"""

    # Activity detail rows
    section_html = ""
    for section, items in by_section.items():
        rows = ""
        for item in items:
            detail_txt = f"<br><span style='color:#64748b;font-size:11px'>{item['detail']}</span>" if item.get("detail") else ""
            rows += f"""<tr>
                <td style="padding:6px 12px;border-bottom:1px solid #f8fafc;font-size:12px;color:#94a3b8;white-space:nowrap">{item['timestamp']}</td>
                <td style="padding:6px 12px;border-bottom:1px solid #f8fafc;font-size:12px">{item['action']}{detail_txt}</td>
            </tr>"""
        section_html += f"""
        <div style="margin-bottom:12px">
            <div style="font-weight:600;font-size:13px;color:#475569;padding:6px 0;border-bottom:1px solid #e2e8f0">{section} — {len(items)} action{'s' if len(items)!=1 else ''}</div>
            <table style="width:100%;border-collapse:collapse">{rows}</table>
        </div>"""

    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; color: #1e293b; background: #f8fafc;">
        <div style="max-width: 680px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; background: white;">

            <!-- HEADER -->
            <div style="background: linear-gradient(135deg, #0f172a, #1e293b); padding: 24px 28px;">
                <h1 style="color: white; margin: 0; font-size: 22px; font-weight: 800; letter-spacing: 0.5px;">📊 INDIVIDUAL PROGRESS REPORT</h1>
                <div style="margin-top: 12px; display: flex; gap: 20px;">
                    <div>
                        <div style="font-size: 11px; text-transform: uppercase; color: #94a3b8; font-weight: 600;">Employee</div>
                        <div style="font-size: 16px; color: #f1f5f9; font-weight: 700;">{username.upper()}</div>
                    </div>
                    <div>
                        <div style="font-size: 11px; text-transform: uppercase; color: #94a3b8; font-weight: 600;">Date</div>
                        <div style="font-size: 16px; color: #f1f5f9; font-weight: 700;">{date_str}</div>
                    </div>
                    <div>
                        <div style="font-size: 11px; text-transform: uppercase; color: #94a3b8; font-weight: 600;">Session</div>
                        <div style="font-size: 16px; color: #f1f5f9; font-weight: 700;">{start_str} — {time_str}</div>
                    </div>
                </div>
            </div>

            <div style="padding: 24px 28px;">

                <!-- KPI CARDS ROW -->
                <div style="display:flex;gap:16px;margin-bottom:24px;">
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;flex:1;text-align:center">
                        <div style="font-size:30px;font-weight:800;color:#1e293b">{len(activities)}</div>
                        <div style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.5px">Total Actions</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;flex:1;text-align:center">
                        <div style="font-size:30px;font-weight:800;color:#1e293b">{len(by_section)}</div>
                        <div style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.5px">Sections Worked</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:16px;flex:1;text-align:center">
                        <div style="font-size:30px;font-weight:800;color:#1e293b">{session_hrs:.1f}</div>
                        <div style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.5px">Hours Active</div>
                    </div>
                    <div style="background:{rating_bg};border:1px solid {rating_color}22;border-radius:10px;padding:16px;flex:1.2;text-align:center">
                        <div style="font-size:30px;font-weight:800;color:{rating_color}">{overall_pct:.0f}%</div>
                        <div style="font-size:10px;font-weight:700;color:{rating_color};text-transform:uppercase;letter-spacing:0.5px">{rating_label}</div>
                    </div>
                </div>

                <!-- AI PRODUCTIVITY ASSESSMENT -->
                <div style="background:linear-gradient(135deg,#ede9fe,#e0e7ff);border-left:4px solid #6366f1;border-radius:8px;padding:18px 20px;margin-bottom:24px;">
                    <div style="font-size:12px;font-weight:800;text-transform:uppercase;color:#4338ca;letter-spacing:1px;margin-bottom:8px;">🤖 AI Productivity Assessment</div>
                    <div style="font-size:13px;line-height:1.7;color:#1e293b;">{ai_summary}</div>
                </div>

                <!-- BENCHMARK COMPARISON TABLE -->
                <div style="margin-bottom:24px;">
                    <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:8px;">
                        Production vs Industry Benchmarks
                    </div>
                    <table style="width:100%;border-collapse:collapse;">
                        <thead>
                            <tr style="background:#f8fafc;">
                                <th style="padding:8px 12px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Section</th>
                                <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Actual</th>
                                <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Target</th>
                                <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Progress</th>
                                <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Score</th>
                            </tr>
                        </thead>
                        <tbody>{bench_rows_html}</tbody>
                    </table>
                </div>

                <!-- ACTIVITY DETAIL (collapsed look) -->
                <div style="margin-bottom:16px;">
                    <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:8px;">
                        Activity Detail
                    </div>
                    {section_html}
                </div>

                <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                <p style="font-size: 11px; color: #94a3b8; text-align: center; margin: 0;">
                    Individual Progress Report — MedPharma Hub — {date_str}
                </p>
            </div>
        </div>
    </body>
    </html>"""

    subject = f"Individual Progress: {username} — {rating_label} ({overall_pct:.0f}%) — {date_str}"

    # SMS — concise summary with rating
    section_counts = ", ".join(f"{s}: {len(items)}" for s, items in by_section.items())
    sms = (f"Progress Report: {username} | {date_str} | "
           f"{rating_label} ({overall_pct:.0f}%) | "
           f"{len(activities)} actions in {session_hrs:.1f}hrs | {section_counts}")
    if len(sms) > 155:
        sms = (f"Progress: {username} | {rating_label} ({overall_pct:.0f}%) | "
               f"{len(activities)} actions in {session_hrs:.1f}hrs")
        if len(sms) > 155:
            sms = sms[:152] + "…"

    # Fire both in background threads
    threading.Thread(target=_send_email, args=(subject, body, html_body), daemon=True).start()
    threading.Thread(target=_send_sms, args=(sms,), daemon=True).start()
    log.info(f"Individual progress report queued for {username}: {rating_label} "
             f"({overall_pct:.0f}%) — {len(activities)} actions across {len(by_section)} sections")


# ── Send helpers ──

def _send_email(subject: str, body: str, html_body: str = ""):
    """Send email notification via SendGrid v3 API.
    Uses _live_config() to read credentials fresh (avoids stale cache)."""
    cfg = _live_config()
    if cfg["IN_APP_ONLY_MODE"]:
        log.info(f"In-app notification mode active — email send simulated: {subject}")
        return

    emails = cfg["NOTIFY_EMAILS"]
    sg_key = cfg["SENDGRID_API_KEY"]
    sg_from = cfg["SENDGRID_FROM"]
    smtp_h = cfg["SMTP_HOST"]
    smtp_p = cfg["SMTP_PORT"]
    smtp_u = cfg["SMTP_USER"]
    smtp_pw = cfg["SMTP_PASS"]

    if not emails:
        log.debug("Email notification skipped — NOTIFY_EMAILS not configured")
        return

    # Primary: SendGrid
    if sg_key:
        try:
            import httpx
            content = []
            if body:
                content.append({"type": "text/plain", "value": body})
            if html_body:
                content.append({"type": "text/html", "value": html_body})
            if not content:
                content.append({"type": "text/plain", "value": "(no content)"})

            recipients = [{"email": addr} for addr in emails]
            payload = {
                "personalizations": [{"to": recipients}],
                "from": {"email": sg_from, "name": "MedPharma Hub"},
                "subject": subject,
                "content": content,
            }
            resp = httpx.post(
                "https://api.sendgrid.com/v3/mail/send",
                json=payload,
                headers={
                    "Authorization": f"Bearer {sg_key}",
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
            if resp.status_code in (200, 202):
                log.info(f"Email sent via SendGrid to {', '.join(emails)}: {subject}")
                return
            log.error(f"SendGrid failed ({resp.status_code}): {resp.text}")
        except Exception as e:
            log.error(f"Failed to send email via SendGrid: {e}")

    # Fallback: SMTP
    if not smtp_h or not smtp_u or not smtp_pw:
        log.error("Email notification skipped — no working provider configured (SendGrid/SMTP)")
        return

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_u or sg_from
        msg["To"] = ", ".join(emails)

        plain = body or "(no content)"
        msg.attach(MIMEText(plain, "plain"))
        if html_body:
            msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_h, smtp_p, timeout=20) as server:
            server.starttls()
            server.login(smtp_u, smtp_pw)
            server.sendmail(msg["From"], emails, msg.as_string())
        log.info(f"Email sent via SMTP to {', '.join(emails)}: {subject}")
    except Exception as e:
        log.error(f"Failed to send email via SMTP: {e}")


def _send_sms(message: str):
    """Send SMS notification.
    Priority:
      1. Twilio (if TWILIO_SID/TOKEN/FROM configured)
      2. Carrier email-to-SMS via SMTP (if NOTIFY_PHONE_CARRIER + SMTP configured — FREE, no sign-up)
      3. Skip silently with a log warning
    """
    cfg = _live_config()
    if cfg["IN_APP_ONLY_MODE"]:
        log.info("In-app notification mode active — SMS send simulated")
        return

    t_sid = cfg["TWILIO_SID"]
    t_tok = cfg["TWILIO_TOKEN"]
    t_from = cfg["TWILIO_FROM"]
    phone = cfg["NOTIFY_PHONE"]
    carrier = cfg["NOTIFY_PHONE_CARRIER"]

    # Path 1: Twilio
    if t_sid and t_tok and t_from and phone:
        try:
            import httpx
            url = f"https://api.twilio.com/2010-04-01/Accounts/{t_sid}/Messages.json"
            data = {"To": phone, "From": t_from, "Body": message}
            resp = httpx.post(url, data=data, auth=(t_sid, t_tok), timeout=15)
            if resp.status_code in (200, 201):
                log.info(f"SMS sent via Twilio to {phone}")
                return
            log.error(f"Twilio SMS failed ({resp.status_code}): {resp.text}")
        except Exception as e:
            log.error(f"Failed to send SMS via Twilio: {e}")
        return

    # Path 2: Carrier email-to-SMS (free — just needs SMTP + NOTIFY_PHONE_CARRIER)
    sms_email = _carrier_sms_email(phone, carrier) if carrier else None
    smtp_h = cfg["SMTP_HOST"]
    smtp_u = cfg["SMTP_USER"]
    smtp_pw = cfg["SMTP_PASS"]
    smtp_p = cfg["SMTP_PORT"]

    if sms_email and smtp_h and smtp_u and smtp_pw:
        try:
            import email.mime.text as _mt
            msg = _mt.MIMEText(message[:160])
            msg["Subject"] = ""
            msg["From"] = smtp_u
            msg["To"] = sms_email
            with smtplib.SMTP(smtp_h, smtp_p, timeout=20) as server:
                server.ehlo()
                server.starttls()
                server.login(smtp_u, smtp_pw)
                server.sendmail(smtp_u, [sms_email], msg.as_string())
            log.info(f"SMS sent via carrier email-to-SMS ({carrier}) to {sms_email}")
            return
        except Exception as e:
            log.error(f"Failed to send SMS via carrier email-to-SMS: {e}")
        return

    if not carrier:
        log.debug("SMS skipped — set NOTIFY_PHONE_CARRIER (e.g. att, verizon, tmobile) to enable free SMS via SMTP")
    else:
        log.debug(f"SMS skipped — carrier '{carrier}' configured but SMTP credentials missing (SMTP_USER/SMTP_PASS)")


def _send_email_force(subject: str, body: str, html_body: str = ""):
    """Send email — always attempts delivery (ignores IN_APP_ONLY_MODE).
    Uses stdlib urllib.request (no third-party dependency).
    Tries SendGrid first, then SMTP fallback."""
    cfg = _live_config()
    emails = cfg["NOTIFY_EMAILS"]
    sg_key = cfg["SENDGRID_API_KEY"]
    sg_from = cfg["SENDGRID_FROM"]
    smtp_h = cfg["SMTP_HOST"]
    smtp_p = cfg["SMTP_PORT"]
    smtp_u = cfg["SMTP_USER"]
    smtp_pw = cfg["SMTP_PASS"]

    if not emails:
        raise ValueError("No email recipients configured (NOTIFY_EMAIL env var)")

    log.info(f"[TEST] Email attempt — SendGrid={'YES' if sg_key else 'NO'}, SMTP={'YES' if smtp_u else 'NO'}, to={emails}")

    sg_error = None

    # Primary: SendGrid (using stdlib urllib)
    if sg_key:
        try:
            content = []
            if body:
                content.append({"type": "text/plain", "value": body})
            if html_body:
                content.append({"type": "text/html", "value": html_body})
            if not content:
                content.append({"type": "text/plain", "value": "(no content)"})

            recipients = [{"email": addr} for addr in emails]
            payload = json.dumps({
                "personalizations": [{"to": recipients}],
                "from": {"email": sg_from, "name": "MedPharma Hub"},
                "subject": subject,
                "content": content,
            }).encode("utf-8")

            req = urllib.request.Request(
                "https://api.sendgrid.com/v3/mail/send",
                data=payload,
                headers={
                    "Authorization": f"Bearer {sg_key}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=20)
            status = resp.getcode()
            if status in (200, 202):
                log.info(f"[TEST] Email sent via SendGrid to {', '.join(emails)}: {subject}")
                return
            resp_body = resp.read().decode("utf-8", errors="replace")[:300]
            sg_error = f"SendGrid returned {status}: {resp_body}"
            log.error(f"[TEST] {sg_error}")
        except urllib.error.HTTPError as he:
            resp_body = he.read().decode("utf-8", errors="replace")[:300]
            sg_error = f"SendGrid HTTP {he.code}: {resp_body}"
            log.error(f"[TEST] {sg_error}")
        except Exception as e:
            sg_error = f"SendGrid error: {e}"
            log.error(f"[TEST] {sg_error}")

    # Fallback: SMTP
    if smtp_h and smtp_u and smtp_pw:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = smtp_u or sg_from
            msg["To"] = ", ".join(emails)
            msg.attach(MIMEText(body or "(no content)", "plain"))
            if html_body:
                msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(smtp_h, smtp_p, timeout=25) as server:
                server.starttls()
                server.login(smtp_u, smtp_pw)
                server.sendmail(msg["From"], emails, msg.as_string())
            log.info(f"[TEST] Email sent via SMTP to {', '.join(emails)}: {subject}")
            return
        except Exception as e:
            smtp_error = f"SMTP error: {e}"
            log.error(f"[TEST] {smtp_error}")
            if sg_error:
                raise RuntimeError(f"{sg_error} | {smtp_error}")
            raise RuntimeError(smtp_error)

    if sg_error:
        raise RuntimeError(sg_error)
    raise ValueError("No email provider configured — set SENDGRID_API_KEY or SMTP_USER+SMTP_PASS in Render environment")


def _send_sms_force(message: str):
    """Send SMS — always attempts delivery (ignores IN_APP_ONLY_MODE).
    Uses stdlib urllib.request (no third-party dependency).
    Reads credentials LIVE from env vars to avoid stale cached values."""
    cfg = _live_config()
    t_sid = cfg["TWILIO_SID"]
    t_tok = cfg["TWILIO_TOKEN"]
    t_from = cfg["TWILIO_FROM"]
    phone = cfg["NOTIFY_PHONE"]

    if not t_sid or not t_tok or not t_from:
        missing = [k for k, v in {"TWILIO_SID": t_sid, "TWILIO_TOKEN": t_tok, "TWILIO_FROM": t_from}.items() if not v]
        raise ValueError(f"Twilio not configured — missing: {', '.join(missing)}")
    if not phone:
        raise ValueError("No SMS recipient configured — set NOTIFY_PHONE env var")

    log.info(f"[TEST] SMS attempt — from={t_from}, to={phone}, sid={t_sid[:8]}...")

    url = f"https://api.twilio.com/2010-04-01/Accounts/{t_sid}/Messages.json"
    post_data = urllib.parse.urlencode({
        "To": phone,
        "From": t_from,
        "Body": message,
    }).encode("utf-8")
    auth_str = base64.b64encode(f"{t_sid}:{t_tok}".encode()).decode()

    req = urllib.request.Request(
        url,
        data=post_data,
        headers={
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=20)
        status = resp.getcode()
        resp_body = resp.read().decode("utf-8", errors="replace")
        if status in (200, 201):
            log.info(f"[TEST] SMS sent to {phone} (status {status})")
            # Try to extract Message SID for confirmation
            try:
                msg_data = json.loads(resp_body)
                log.info(f"[TEST] Twilio Message SID: {msg_data.get('sid', 'N/A')}")
            except Exception:
                pass
            return
        raise RuntimeError(f"Twilio SMS failed ({status}): {resp_body[:300]}")
    except urllib.error.HTTPError as he:
        resp_body = he.read().decode("utf-8", errors="replace")[:500]
        # Parse Twilio error for clearer message
        try:
            err_data = json.loads(resp_body)
            twilio_msg = err_data.get("message", resp_body[:300])
            twilio_code = err_data.get("code", "")
            raise RuntimeError(f"Twilio error {he.code} (code {twilio_code}): {twilio_msg}")
        except (json.JSONDecodeError, RuntimeError) as inner:
            if isinstance(inner, RuntimeError):
                raise
            raise RuntimeError(f"Twilio HTTP {he.code}: {resp_body[:300]}")


def send_direct_sms(message: str, to_phone: str = "") -> dict:
    """Send an immediate SMS via Twilio.
    If to_phone is omitted, uses NOTIFY_PHONE from env."""
    cfg = _live_config()
    t_sid = cfg["TWILIO_SID"]
    t_tok = cfg["TWILIO_TOKEN"]
    t_from = cfg["TWILIO_FROM"]
    phone = _normalize_phone(to_phone) if to_phone else cfg["NOTIFY_PHONE"]

    if not t_sid or not t_tok or not t_from:
        missing = [k for k, v in {"TWILIO_SID": t_sid, "TWILIO_TOKEN": t_tok, "TWILIO_FROM": t_from}.items() if not v]
        raise ValueError(f"Twilio not configured — missing: {', '.join(missing)}")
    if not phone:
        raise ValueError("No SMS recipient configured — set NOTIFY_PHONE or pass to_phone")

    text = (message or "").strip()
    if not text:
        raise ValueError("SMS message cannot be empty")

    url = f"https://api.twilio.com/2010-04-01/Accounts/{t_sid}/Messages.json"
    post_data = urllib.parse.urlencode({
        "To": phone,
        "From": t_from,
        "Body": text,
    }).encode("utf-8")
    auth_str = base64.b64encode(f"{t_sid}:{t_tok}".encode()).decode()

    req = urllib.request.Request(
        url,
        data=post_data,
        headers={
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    try:
        resp = urllib.request.urlopen(req, timeout=20)
        status = resp.getcode()
        body = resp.read().decode("utf-8", errors="replace")
        data = json.loads(body) if body else {}
        if status not in (200, 201):
            raise RuntimeError(f"Twilio SMS failed ({status}): {body[:300]}")
        return {
            "ok": True,
            "to": phone,
            "from": t_from,
            "sid": data.get("sid", ""),
            "status": data.get("status", "queued"),
        }
    except urllib.error.HTTPError as he:
        resp_body = he.read().decode("utf-8", errors="replace")[:500]
        try:
            err_data = json.loads(resp_body)
            twilio_msg = err_data.get("message", resp_body[:300])
            twilio_code = err_data.get("code", "")
            raise RuntimeError(f"Twilio error {he.code} (code {twilio_code}): {twilio_msg}")
        except (json.JSONDecodeError, RuntimeError) as inner:
            if isinstance(inner, RuntimeError):
                raise
            raise RuntimeError(f"Twilio HTTP {he.code}: {resp_body[:300]}")


def send_test_notification(triggered_by: str = "system") -> dict:
    """Send an immediate test notification to configured recipients/channels.
    Test notifications ALWAYS attempt real delivery (bypass IN_APP_ONLY_MODE).
    Runs SYNCHRONOUSLY — no threading — to ensure errors are captured clearly."""
    cfg = _live_config()
    now = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    subject = f"MedPharma Hub Test Notification — {now}"
    emails = cfg["NOTIFY_EMAILS"]
    phone = cfg["NOTIFY_PHONE"]

    body = (
        f"This is a test notification from MedPharma Hub.\n"
        f"Triggered by: {triggered_by}\n"
        f"Time: {now}\n"
        f"Recipients: {', '.join(emails) if emails else 'none'}\n"
        f"Mode: {'IN_APP_ONLY (bypassed for test)' if cfg['IN_APP_ONLY_MODE'] else 'External delivery'}\n"
        f"SMS target: {phone or 'not configured'}"
    )
    html_body = f"""
    <html><body style="font-family:Arial,sans-serif;padding:20px">
      <div style="max-width:500px;margin:0 auto;border:2px solid #22c55e;border-radius:12px;overflow:hidden">
        <div style="background:linear-gradient(135deg,#0f172a,#1e293b);padding:20px;color:white">
          <h2 style="margin:0">✅ MedPharma Hub — Notifications Active</h2>
        </div>
        <div style="padding:20px">
          <p><b>Triggered by:</b> {triggered_by}</p>
          <p><b>Time:</b> {now}</p>
          <p><b>Email to:</b> {', '.join(emails)}</p>
          <p><b>SMS to:</b> {phone or 'not configured'}</p>
          <p style="color:#22c55e;font-weight:bold;font-size:16px">If you received this, your notification pipeline is working!</p>
        </div>
      </div>
    </body></html>
    """
    sms = f"MedPharma Hub test OK | by {triggered_by} | {now}"
    if len(sms) > 155:
        sms = sms[:152] + "…"

    results = {"email_sent": False, "sms_sent": False, "email_error": None, "sms_error": None}

    # ── EMAIL (synchronous) ──
    try:
        _send_email_force(subject, body, html_body)
        results["email_sent"] = True
    except Exception as e:
        results["email_error"] = str(e)
        log.error(f"[TEST] Email failed: {e}")

    # ── SMS (synchronous) ──
    try:
        _send_sms_force(sms)
        results["sms_sent"] = True
    except Exception as e:
        results["sms_error"] = str(e)
        log.error(f"[TEST] SMS failed: {e}")

    status = get_notification_status()
    status["ok"] = True
    status.update(results)
    log.info(f"[TEST] Results: email_sent={results['email_sent']}, sms_sent={results['sms_sent']}, "
             f"email_error={results['email_error']}, sms_error={results['sms_error']}")
    return status


def get_notification_status() -> dict:
    """Return current notification channel configuration status.
    Reads env vars LIVE to avoid stale module-level cache."""
    cfg = _live_config()
    sendgrid_configured = bool(cfg["SENDGRID_API_KEY"])
    smtp_configured = bool(cfg["SMTP_HOST"] and cfg["SMTP_USER"] and cfg["SMTP_PASS"])
    twilio_configured = bool(cfg["TWILIO_SID"] and cfg["TWILIO_TOKEN"] and cfg["TWILIO_FROM"] and cfg["NOTIFY_PHONE"])

    missing_twilio = []
    if not cfg["TWILIO_SID"]:
        missing_twilio.append("TWILIO_SID")
    if not cfg["TWILIO_TOKEN"]:
        missing_twilio.append("TWILIO_TOKEN")
    if not cfg["TWILIO_FROM"]:
        missing_twilio.append("TWILIO_FROM")
    if not cfg["NOTIFY_PHONE"]:
        missing_twilio.append("NOTIFY_PHONE")

    missing_email = []
    if not sendgrid_configured and not smtp_configured:
        missing_email.append("SENDGRID_API_KEY or SMTP_USER/SMTP_PASS")

    return {
        "email_recipients": cfg["NOTIFY_EMAILS"],
        "sms_target": cfg["NOTIFY_PHONE"],
        "sendgrid_configured": sendgrid_configured,
        "smtp_configured": smtp_configured,
        "twilio_configured": twilio_configured,
        "email_configured": bool(sendgrid_configured or smtp_configured),
        "missing_twilio_fields": missing_twilio,
        "missing_email_fields": missing_email,
        "notify_on_users": sorted(list(NOTIFY_ON_USERS)),
        "in_app_only_mode": cfg["IN_APP_ONLY_MODE"],
        "delivery_mode": "in_app_only" if cfg["IN_APP_ONLY_MODE"] else "external",
    }


def get_notification_debug() -> dict:
    """Return detailed diagnostic info for debugging notification delivery.
    Credential values are masked (first 4 chars shown)."""
    cfg = _live_config()

    def mask(val):
        if not val:
            return "NOT SET"
        s = str(val)
        if len(s) <= 4:
            return "****"
        return s[:4] + "*" * min(len(s) - 4, 12)

    return {
        "env_vars": {
            "SENDGRID_API_KEY": mask(cfg["SENDGRID_API_KEY"]),
            "SENDGRID_FROM": cfg["SENDGRID_FROM"],
            "NOTIFY_EMAIL": ", ".join(cfg["NOTIFY_EMAILS"]),
            "TWILIO_SID": mask(cfg["TWILIO_SID"]),
            "TWILIO_TOKEN": mask(cfg["TWILIO_TOKEN"]),
            "TWILIO_FROM": cfg["TWILIO_FROM"] or "NOT SET",
            "NOTIFY_PHONE": cfg["NOTIFY_PHONE"] or "NOT SET",
            "SMTP_HOST": cfg["SMTP_HOST"] or "NOT SET",
            "SMTP_PORT": str(cfg["SMTP_PORT"]),
            "SMTP_USER": mask(cfg["SMTP_USER"]),
            "SMTP_PASS": mask(cfg["SMTP_PASS"]),
            "NOTIFY_IN_APP_ONLY": os.getenv("NOTIFY_IN_APP_ONLY", "1"),
            "NOTIFY_ON_USERS": os.getenv("NOTIFY_ON_USERS", "jessica,rcm"),
        },
        "effective": {
            "in_app_only_mode": cfg["IN_APP_ONLY_MODE"],
            "sendgrid_ready": bool(cfg["SENDGRID_API_KEY"]),
            "smtp_ready": bool(cfg["SMTP_HOST"] and cfg["SMTP_USER"] and cfg["SMTP_PASS"]),
            "twilio_ready": bool(cfg["TWILIO_SID"] and cfg["TWILIO_TOKEN"] and cfg["TWILIO_FROM"]),
            "email_provider": "SendGrid" if cfg["SENDGRID_API_KEY"] else ("SMTP" if (cfg["SMTP_USER"] and cfg["SMTP_PASS"]) else "NONE"),
            "email_recipients": cfg["NOTIFY_EMAILS"],
            "sms_recipient": cfg["NOTIFY_PHONE"],
            "sms_from": cfg["TWILIO_FROM"] or "NOT SET",
        },
        "module_cache_vs_live": {
            "sendgrid_cached": bool(SENDGRID_API_KEY),
            "sendgrid_live": bool(cfg["SENDGRID_API_KEY"]),
            "twilio_cached": bool(TWILIO_SID),
            "twilio_live": bool(cfg["TWILIO_SID"]),
            "smtp_cached": bool(SMTP_USER),
            "smtp_live": bool(cfg["SMTP_USER"]),
            "in_app_cached": IN_APP_ONLY_MODE,
            "in_app_live": cfg["IN_APP_ONLY_MODE"],
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
#  DAILY OVERALL ACCOUNT SUMMARY — Scheduled at 6 PM EST
# ═══════════════════════════════════════════════════════════════════════════

def _fmt_money(val):
    """Format a number as $X,XXX.XX"""
    return f"${val:,.2f}"


def _generate_account_ai_summary(d: dict, date_str: str) -> str:
    """
    Use OpenAI to generate a concise executive summary of the overall account
    health.  Falls back to rule-based if OpenAI unavailable.
    """
    prompt = f"""You are a healthcare RCM operations manager writing a brief end-of-day executive summary.

Date: {date_str}

Key metrics:
- Total AR: ${d['total_ar']:,.2f} across {d['total_claims']} claims ({d['active_claims']} active)
- Today: {d['submitted_today']} submitted, {d['paid_today']} paid, {d['denied_today']} denied
- MTD: {d['submitted_mtd']} submitted, {d['paid_mtd']} paid, {d['denied_mtd']} denied
- Payments today: ${d['payments_today']:,.2f} | MTD: ${d['payments_mtd']:,.2f} | YTD: ${d['payments_ytd']:,.2f}
- Clean claim rate: {d['clean_claim_rate']}% | Denial rate: {d['denial_rate']}% | Net collection: {d['net_collection_rate']}%
- Avg days to pay: {d['avg_days_to_pay']} | SLA breaches: {d['sla_breaches']}
- AR Aging: Current ${d['ar_aging']['current']:,.2f} | 31-60 ${d['ar_aging']['31_60']:,.2f} | 61-90 ${d['ar_aging']['61_90']:,.2f} | 90+ ${d['ar_aging']['90_plus']:,.2f}
- Credentialing: {d['cred_total']} total ({d['cred_approved']} approved, {d['cred_pending']} pending, {d['cred_not_started']} not started)
- EDI: {d['edi_total']} total ({d['edi_live']} live)
- User Production today: {d.get('production_submissions_today', 0)} total ({d.get('production_logs_today', 0)} log entries, {d.get('production_files_today', 0)} file uploads)
- Serving {d['total_clients']} clients | {d['today_actions']} system actions today

Industry benchmarks for context:
- Clean claim rate should be ≥95%, denial rate ≤5%, net collection ≥95%
- Avg days to pay: <30 days is excellent, 30-45 acceptable, >45 needs attention
- AR >90 days should be <15% of total AR

Write a 4-6 sentence executive summary that:
1. Highlights the overall financial health of the practice
2. Calls out any red flags (high denial rate, aging AR, SLA breaches)
3. Notes today's productivity (claims flow, payments received)
4. Provides one key recommendation for tomorrow
Keep it professional and data-driven. No greeting, no bullet points — paragraph form only.
"""
    if not OPENAI_API_KEY:
        return _rule_based_account_summary(d)

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "You are a healthcare RCM executive."},
                      {"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.7,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"OpenAI account summary failed: {e}")
        return _rule_based_account_summary(d)


def _rule_based_account_summary(d: dict) -> str:
    """Fallback account summary without AI."""
    flags = []
    if d["denial_rate"] > 5:
        flags.append(f"denial rate of {d['denial_rate']}% exceeds the 5% industry target")
    if d["clean_claim_rate"] < 95:
        flags.append(f"clean claim rate of {d['clean_claim_rate']}% is below the 95% benchmark")
    if d["avg_days_to_pay"] > 45:
        flags.append(f"average days to pay of {d['avg_days_to_pay']} exceeds the 45-day threshold")
    if d["sla_breaches"] > 0:
        flags.append(f"{d['sla_breaches']} SLA breaches require attention")

    total_aging = sum(d["ar_aging"].values())
    if total_aging > 0 and d["ar_aging"]["90_plus"] / total_aging > 0.15:
        flags.append(f"AR >90 days represents {d['ar_aging']['90_plus']/total_aging*100:.0f}% of outstanding balances")

    summary = (
        f"The practice manages ${d['total_ar']:,.2f} in total accounts receivable across "
        f"{d['active_claims']} active claims with a net collection rate of {d['net_collection_rate']}%. "
        f"Today saw {d['submitted_today']} claims submitted, {d['paid_today']} paid, and "
        f"${d['payments_today']:,.2f} in payments posted. "
    )

    if flags:
        summary += "Areas needing attention: " + "; ".join(flags) + ". "
    else:
        summary += "All key performance indicators are within healthy ranges. "

    if d["cred_pending"] > 0:
        summary += f"Credentialing has {d['cred_pending']} applications pending completion. "

    summary += "Continue monitoring AR aging and prioritize follow-up on accounts approaching SLA deadlines."
    return summary


def send_daily_account_summary():
    """
    Compile and send the Overall Account Summary email + SMS.
    Called by the scheduler at 6 PM EST daily.
    Includes per-user production breakdown.
    """
    try:
        from app.client_db import get_daily_account_summary, get_user_production_snapshot
        d = get_daily_account_summary()
        prod_snapshot = get_user_production_snapshot()  # today's production
    except Exception as e:
        log.error(f"Failed to fetch daily account summary data: {e}")
        return

    now = datetime.now()
    date_str = now.strftime("%m/%d/%Y")
    date_str_long = now.strftime("%B %d, %Y")
    day_of_week = now.strftime("%A")

    # AI summary
    ai_summary = _generate_account_ai_summary(d, date_str_long)

    # ── Per-user production rows for email ──
    user_prod_rows_html = ""
    prod_users = prod_snapshot.get("user_stats", [])
    file_uploads = prod_snapshot.get("file_uploads", {})
    if prod_users:
        for u in prod_users:
            files = file_uploads.get(u["username"], 0)
            cats = u["categories"].replace(",", ", ") if u["categories"] else "—"
            user_prod_rows_html += f"""
            <tr>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;font-weight:600">{u['username'].title()}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-size:13px">{u['entry_count']}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-size:13px;font-weight:700;color:#2563eb">{u['total_hours']}h</td>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-size:13px">{u['total_qty']}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:center;font-size:13px">{files}</td>
                <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:11px;color:#64748b">{cats}</td>
            </tr>"""
    else:
        user_prod_rows_html = """<tr><td colspan="6" style="padding:16px;text-align:center;color:#94a3b8;font-size:13px">
            ⚠️ No production data logged today. Jessica & RCM should submit daily work entries.
        </td></tr>"""

    # ── Production detail entries for email ──
    prod_detail_html = ""
    for entry in prod_snapshot.get("entries", [])[:20]:
        prod_detail_html += f"""
        <tr>
            <td style="padding:4px 8px;font-size:11px;border-bottom:1px solid #f8fafc">{entry['username'].title()}</td>
            <td style="padding:4px 8px;font-size:11px;border-bottom:1px solid #f8fafc">{entry['category']}</td>
            <td style="padding:4px 8px;font-size:11px;border-bottom:1px solid #f8fafc">{entry['task_description'][:50]}</td>
            <td style="padding:4px 8px;font-size:11px;border-bottom:1px solid #f8fafc;text-align:center">{entry['quantity']}</td>
            <td style="padding:4px 8px;font-size:11px;border-bottom:1px solid #f8fafc;text-align:center">{entry['time_spent']}h</td>
        </tr>"""

    # ── Status distribution rows ──
    status_rows_html = ""
    for status, count in sorted(d.get("status_distribution", {}).items(), key=lambda x: -x[1]):
        if status == "Paid":
            color = "#22c55e"
        elif status in ("Denied", "Appeals"):
            color = "#ef4444"
        elif status in ("Submitted", "In Progress"):
            color = "#3b82f6"
        else:
            color = "#64748b"
        status_rows_html += f"""
        <tr>
            <td style="padding:6px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">{status}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #f1f5f9;text-align:right;font-weight:700;font-size:13px;color:{color}">{count:,}</td>
        </tr>"""

    # ── Payor rows ──
    payor_rows_html = ""
    for p in d.get("top_payors", [])[:8]:
        payor_rows_html += f"""
        <tr>
            <td style="padding:6px 12px;border-bottom:1px solid #f1f5f9;font-size:12px">{p['payor']}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #f1f5f9;text-align:right;font-size:12px">{p['count']:,}</td>
            <td style="padding:6px 12px;border-bottom:1px solid #f1f5f9;text-align:right;font-size:12px">{_fmt_money(p['charges'])}</td>
        </tr>"""

    # ── Credentialing status rows ──
    cred_rows_html = ""
    for status, count in sorted(d.get("cred_stats", {}).items(), key=lambda x: -x[1]):
        cred_rows_html += f'<span style="display:inline-block;background:#f1f5f9;border-radius:6px;padding:4px 10px;margin:2px;font-size:12px"><b>{count}</b> {status}</span>'

    # ── EDI status rows ──
    edi_rows_html = ""
    for status, count in sorted(d.get("edi_stats", {}).items(), key=lambda x: -x[1]):
        edi_rows_html += f'<span style="display:inline-block;background:#f1f5f9;border-radius:6px;padding:4px 10px;margin:2px;font-size:12px"><b>{count}</b> {status}</span>'

    # ── AR Aging bar ──
    aging = d.get("ar_aging", {})
    total_aging = sum(aging.values()) or 1

    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; color: #1e293b; background: #f8fafc;">
        <div style="max-width: 700px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; background: white;">

            <!-- HEADER -->
            <div style="background: linear-gradient(135deg, #1e3a5f, #2563eb); padding: 28px 32px;">
                <h1 style="color: white; margin: 0; font-size: 24px; font-weight: 800; letter-spacing: 0.5px;">📋 MEDPHARMA DAILY REPORT</h1>
                <p style="color: rgba(255,255,255,0.85); margin: 8px 0 0; font-size: 15px; font-weight: 500;">{day_of_week}, {date_str} — Daily Account Summary</p>
                <p style="color: rgba(255,255,255,0.65); margin: 4px 0 0; font-size: 12px;">MedPharma Revenue Cycle Management — {d['total_clients']} Active Client{'s' if d['total_clients']!=1 else ''}</p>
            </div>

            <div style="padding: 28px 32px;">

                <!-- AI EXECUTIVE SUMMARY -->
                <div style="background:linear-gradient(135deg,#ede9fe,#e0e7ff);border-left:4px solid #6366f1;border-radius:8px;padding:18px 20px;margin-bottom:28px;">
                    <div style="font-size:12px;font-weight:800;text-transform:uppercase;color:#4338ca;letter-spacing:1px;margin-bottom:8px;">🤖 AI Executive Summary</div>
                    <div style="font-size:13px;line-height:1.7;color:#1e293b;">{ai_summary}</div>
                </div>

                <!-- FINANCIAL KPIs -->
                <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:16px;">
                    💰 Financial Overview
                </div>
                <div style="display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap;">
                    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:14px 16px;flex:1;min-width:120px;text-align:center">
                        <div style="font-size:22px;font-weight:800;color:#15803d">{_fmt_money(d['total_ar'])}</div>
                        <div style="font-size:10px;font-weight:700;color:#16a34a;text-transform:uppercase">Total AR</div>
                    </div>
                    <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:14px 16px;flex:1;min-width:120px;text-align:center">
                        <div style="font-size:22px;font-weight:800;color:#2563eb">{_fmt_money(d['payments_mtd'])}</div>
                        <div style="font-size:10px;font-weight:700;color:#3b82f6;text-transform:uppercase">Payments MTD</div>
                    </div>
                    <div style="background:#fefce8;border:1px solid #fde68a;border-radius:10px;padding:14px 16px;flex:1;min-width:120px;text-align:center">
                        <div style="font-size:22px;font-weight:800;color:#ca8a04">{d['net_collection_rate']}%</div>
                        <div style="font-size:10px;font-weight:700;color:#d97706;text-transform:uppercase">Net Collection</div>
                    </div>
                    <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:14px 16px;flex:1;min-width:120px;text-align:center">
                        <div style="font-size:22px;font-weight:800;color:#dc2626">{d['denial_rate']}%</div>
                        <div style="font-size:10px;font-weight:700;color:#ef4444;text-transform:uppercase">Denial Rate</div>
                    </div>
                </div>

                <!-- CLAIMS KPIs -->
                <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:16px;">
                    📄 Claims Overview
                </div>
                <div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:24px;font-weight:800;color:#1e293b">{d['total_claims']:,}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Total Claims</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:24px;font-weight:800;color:#2563eb">{d['active_claims']:,}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Active</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:24px;font-weight:800;color:#22c55e">{d['claims_paid']:,}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Paid</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:24px;font-weight:800;color:#ef4444">{d['claims_denied']:,}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Denied</div>
                    </div>
                </div>

                <!-- TODAY'S ACTIVITY -->
                <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;margin-bottom:12px;">
                    <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:8px;">Today's Activity</div>
                    <div style="display:flex;gap:20px;flex-wrap:wrap;">
                        <div><span style="font-weight:800;color:#2563eb;font-size:18px">{d['submitted_today']}</span> <span style="font-size:12px;color:#64748b">Submitted</span></div>
                        <div><span style="font-weight:800;color:#22c55e;font-size:18px">{d['paid_today']}</span> <span style="font-size:12px;color:#64748b">Paid</span></div>
                        <div><span style="font-weight:800;color:#ef4444;font-size:18px">{d['denied_today']}</span> <span style="font-size:12px;color:#64748b">Denied</span></div>
                        <div><span style="font-weight:800;color:#16a34a;font-size:18px">{_fmt_money(d['payments_today'])}</span> <span style="font-size:12px;color:#64748b">Payments</span></div>
                        <div><span style="font-weight:800;color:#7c3aed;font-size:18px">{d.get('production_submissions_today', 0)}</span> <span style="font-size:12px;color:#64748b">User Production</span></div>
                    </div>
                </div>

                <!-- PERFORMANCE METRICS -->
                <div style="display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap;">
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:18px;font-weight:800;color:#1e293b">{d['clean_claim_rate']}%</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Clean Claim</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:18px;font-weight:800;color:#1e293b">{d['avg_days_to_pay']}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Avg Days to Pay</div>
                    </div>
                    <div style="background:{'#fee2e2' if d['sla_breaches']>0 else '#f8fafc'};border:1px solid {'#fecaca' if d['sla_breaches']>0 else '#e2e8f0'};border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:18px;font-weight:800;color:{'#dc2626' if d['sla_breaches']>0 else '#1e293b'}">{d['sla_breaches']}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">SLA Breaches</div>
                    </div>
                    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;flex:1;min-width:100px;text-align:center">
                        <div style="font-size:18px;font-weight:800;color:#1e293b">{d['today_actions']}</div>
                        <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase">Actions Today</div>
                    </div>
                </div>

                <!-- AR AGING -->
                <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:12px;">
                    ⏳ AR Aging Distribution
                </div>
                <div style="display:flex;gap:8px;margin-bottom:8px;">
                    <div style="flex:{aging.get('current',0)/total_aging};background:#22c55e;height:14px;border-radius:4px 0 0 4px" title="Current"></div>
                    <div style="flex:{aging.get('31_60',0)/total_aging};background:#f59e0b;height:14px" title="31-60"></div>
                    <div style="flex:{aging.get('61_90',0)/total_aging};background:#f97316;height:14px" title="61-90"></div>
                    <div style="flex:{aging.get('90_plus',0)/total_aging};background:#ef4444;height:14px;border-radius:0 4px 4px 0" title="90+"></div>
                </div>
                <div style="display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap;font-size:12px;">
                    <div><span style="display:inline-block;width:10px;height:10px;background:#22c55e;border-radius:2px;margin-right:4px"></span>Current: {_fmt_money(aging.get('current',0))}</div>
                    <div><span style="display:inline-block;width:10px;height:10px;background:#f59e0b;border-radius:2px;margin-right:4px"></span>31-60: {_fmt_money(aging.get('31_60',0))}</div>
                    <div><span style="display:inline-block;width:10px;height:10px;background:#f97316;border-radius:2px;margin-right:4px"></span>61-90: {_fmt_money(aging.get('61_90',0))}</div>
                    <div><span style="display:inline-block;width:10px;height:10px;background:#ef4444;border-radius:2px;margin-right:4px"></span>90+: {_fmt_money(aging.get('90_plus',0))}</div>
                </div>

                <!-- STATUS DISTRIBUTION + TOP PAYORS side-by-side -->
                <div style="display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap;">
                    <div style="flex:1;min-width:200px">
                        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:8px;">Claim Status Distribution</div>
                        <table style="width:100%;border-collapse:collapse">{status_rows_html}</table>
                    </div>
                    <div style="flex:1;min-width:200px">
                        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:8px;">Top Payors</div>
                        <table style="width:100%;border-collapse:collapse">
                            <thead><tr>
                                <th style="padding:4px 12px;text-align:left;font-size:10px;color:#94a3b8;text-transform:uppercase">Payor</th>
                                <th style="padding:4px 12px;text-align:right;font-size:10px;color:#94a3b8;text-transform:uppercase">Claims</th>
                                <th style="padding:4px 12px;text-align:right;font-size:10px;color:#94a3b8;text-transform:uppercase">Charges</th>
                            </tr></thead>
                            <tbody>{payor_rows_html}</tbody>
                        </table>
                    </div>
                </div>

                <!-- CREDENTIALING / EDI -->
                <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:16px;">
                    🏥 Credentialing & EDI
                </div>
                <div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">
                    <div style="flex:1;min-width:140px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;">
                        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:6px;">Credentialing ({d['cred_total']})</div>
                        <div>{cred_rows_html or '<span style="font-size:12px;color:#94a3b8">No records</span>'}</div>
                    </div>
                    <div style="flex:1;min-width:140px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;">
                        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:6px;">EDI/ERA/EFT ({d['edi_total']})</div>
                        <div>{edi_rows_html or '<span style="font-size:12px;color:#94a3b8">No records</span>'}</div>
                    </div>
                </div>

                <!-- MTD COMPARISON -->
                <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;margin-bottom:24px;">
                    <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:8px;">Month-to-Date Summary</div>
                    <div style="display:flex;gap:20px;flex-wrap:wrap;font-size:13px;">
                        <div>📤 <b>{d['submitted_mtd']}</b> Submitted</div>
                        <div>✅ <b>{d['paid_mtd']}</b> Paid</div>
                        <div>❌ <b>{d['denied_mtd']}</b> Denied</div>
                        <div>💵 <b>{_fmt_money(d['payments_mtd'])}</b> Collected</div>
                        <div>📅 <b>{_fmt_money(d['payments_ytd'])}</b> YTD</div>
                    </div>
                </div>

                <!-- USER PRODUCTION SNAPSHOT -->
                <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #7c3aed;margin-bottom:16px;">
                    👥 User Production — {date_str}
                </div>
                <table style="width:100%;border-collapse:collapse;margin-bottom:12px;">
                    <thead>
                        <tr style="background:#f8fafc;">
                            <th style="padding:8px 12px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Employee</th>
                            <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Entries</th>
                            <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Hours</th>
                            <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Qty</th>
                            <th style="padding:8px 12px;text-align:center;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Files</th>
                            <th style="padding:8px 12px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase">Categories</th>
                        </tr>
                    </thead>
                    <tbody>{user_prod_rows_html}</tbody>
                </table>

                {'<div style="margin-bottom:24px;"><div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:6px;">Work Detail</div><table style="width:100%;border-collapse:collapse;"><thead><tr style="background:#f8fafc;"><th style="padding:4px 8px;font-size:10px;color:#94a3b8;text-transform:uppercase;text-align:left">User</th><th style="padding:4px 8px;font-size:10px;color:#94a3b8;text-transform:uppercase;text-align:left">Category</th><th style="padding:4px 8px;font-size:10px;color:#94a3b8;text-transform:uppercase;text-align:left">Description</th><th style="padding:4px 8px;font-size:10px;color:#94a3b8;text-transform:uppercase;text-align:center">Qty</th><th style="padding:4px 8px;font-size:10px;color:#94a3b8;text-transform:uppercase;text-align:center">Time</th></tr></thead><tbody>' + prod_detail_html + '</tbody></table></div>' if prod_detail_html else ''}

                <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                <p style="font-size: 11px; color: #94a3b8; text-align: center; margin: 0;">
                    MedPharma Daily Report — {date_str}
                </p>
            </div>
        </div>
    </body>
    </html>"""

    # ── Plain text version ──
    # Build per-user production lines
    prod_text_lines = []
    if prod_users:
        for u in prod_users:
            files = file_uploads.get(u["username"], 0)
            prod_text_lines.append(
                f"  {u['username'].title():12s}  {u['entry_count']} entries  |  {u['total_hours']}h  |  {u['total_qty']} qty  |  {files} files  |  {u['categories']}"
            )
    else:
        prod_text_lines.append("  ⚠️ No production data logged today")

    body_lines = [
        "═══════════════════════════════════════════",
        "     MEDPHARMA DAILY REPORT",
        f"     {day_of_week}, {date_str}",
        "═══════════════════════════════════════════",
        "",
        "AI EXECUTIVE SUMMARY:",
        ai_summary,
        "",
        "───── FINANCIAL ─────",
        f"  Total AR:          {_fmt_money(d['total_ar'])}",
        f"  Payments Today:    {_fmt_money(d['payments_today'])}",
        f"  Payments MTD:      {_fmt_money(d['payments_mtd'])}",
        f"  Payments YTD:      {_fmt_money(d['payments_ytd'])}",
        f"  Net Collection:    {d['net_collection_rate']}%",
        f"  Denial Rate:       {d['denial_rate']}%",
        f"  Clean Claim Rate:  {d['clean_claim_rate']}%",
        f"  Avg Days to Pay:   {d['avg_days_to_pay']}",
        "",
        "───── CLAIMS ─────",
        f"  Total: {d['total_claims']:,}  |  Active: {d['active_claims']:,}  |  Paid: {d['claims_paid']:,}  |  Denied: {d['claims_denied']:,}",
        f"  Today: {d['submitted_today']} submitted, {d['paid_today']} paid, {d['denied_today']} denied",
        f"  MTD:   {d['submitted_mtd']} submitted, {d['paid_mtd']} paid, {d['denied_mtd']} denied",
        "",
        "───── AR AGING ─────",
        f"  Current:  {_fmt_money(aging.get('current',0))}",
        f"  31-60:    {_fmt_money(aging.get('31_60',0))}",
        f"  61-90:    {_fmt_money(aging.get('61_90',0))}",
        f"  90+:      {_fmt_money(aging.get('90_plus',0))}",
        "",
        "───── CREDENTIALING/EDI ─────",
        f"  Credentialing: {d['cred_total']} ({d['cred_approved']} approved, {d['cred_pending']} pending)",
        f"  EDI:           {d['edi_total']} ({d['edi_live']} live)",
        "",
        f"───── USER PRODUCTION — {date_str} ─────",
    ] + prod_text_lines + [
        "",
        f"  SLA Breaches: {d['sla_breaches']}  |  System Actions Today: {d['today_actions']}",
    ]
    body = "\n".join(body_lines)

    subject = f"MedPharma Daily Report — {date_str} — AR {_fmt_money(d['total_ar'])}"

    threading.Thread(target=_send_email, args=(subject, body, html_body), daemon=True).start()
    log.info(f"MedPharma Daily Report email queued: {date_str}, AR {_fmt_money(d['total_ar'])}, "
             f"{d['total_claims']} claims, {len(prod_users)} users logged production")


# ═══════════════════════════════════════════════════════════════════════════
#  SCHEDULER — 5:30 PM & 6 PM EST daily
# ═══════════════════════════════════════════════════════════════════════════

# User emails for individual reminders
USER_EMAILS = {
    "jessica": "jessica@medprosc.com",
    "rcm": "rcm@medprosc.com",
}

_scheduler_started = False


def send_production_reminders():
    """
    Send reminder emails to jessica@medprosc.com and rcm@medprosc.com
    at 5:30 PM EST if they have NOT uploaded any production data today.
    """
    from datetime import date
    today = date.today().isoformat()

    for username, email in USER_EMAILS.items():
        try:
            from app.client_db import has_production_data_today
            if has_production_data_today(username, today):
                log.info(f"Production reminder skipped for {username} — data already uploaded for {today}")
                continue

            subject = f"⏰ Reminder: Upload Your Daily Production — {datetime.now().strftime('%B %d, %Y')}"
            html_body = f"""
            <html>
            <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; color: #1e293b; background: #f8fafc;">
                <div style="max-width: 560px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; background: white;">
                    <div style="background: linear-gradient(135deg, #f59e0b, #d97706); padding: 24px 28px;">
                        <h1 style="color: white; margin: 0; font-size: 20px; font-weight: 800;">⏰ Daily Production Reminder</h1>
                        <p style="color: rgba(255,255,255,0.9); margin: 6px 0 0; font-size: 14px;">
                            {datetime.now().strftime('%A, %B %d, %Y')} — 5:30 PM EST
                        </p>
                    </div>
                    <div style="padding: 24px 28px;">
                        <p style="font-size: 15px; line-height: 1.7; margin: 0 0 16px;">
                            Hi <strong>{username.title()}</strong>,
                        </p>
                        <div style="background: #fef3c7; border-left: 4px solid #f59e0b; border-radius: 8px; padding: 16px 20px; margin-bottom: 20px;">
                            <p style="font-size: 14px; line-height: 1.6; margin: 0; color: #92400e;">
                                📋 You have <strong>not uploaded</strong> any production data for today yet.
                                Please log your daily work entries or upload your production report before end of day.
                            </p>
                        </div>
                        <p style="font-size: 13px; color: #64748b; line-height: 1.6; margin: 0 0 20px;">
                            Log in to <a href="https://medpharmasc.com" style="color: #2563eb; text-decoration: none; font-weight: 600;">MedPharma Hub</a>
                            and go to <strong>User Production</strong> to submit your work for today. You can either
                            log individual tasks or upload an Excel/PDF report.
                        </p>
                        <div style="text-align: center; margin: 24px 0;">
                            <a href="https://medpharmasc.com" style="display:inline-block;background:#2563eb;color:white;padding:12px 32px;
                                border-radius:8px;text-decoration:none;font-weight:700;font-size:14px;">
                                Log In & Upload Production
                            </a>
                        </div>
                        <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                        <p style="font-size: 11px; color: #94a3b8; text-align: center; margin: 0;">
                            This is an automated reminder from MedPharma Hub. If you've already submitted your data, please disregard.
                        </p>
                    </div>
                </div>
            </body>
            </html>"""
            body = (f"Hi {username.title()}, you have not uploaded production data for today ({today}). "
                    f"Please log in to MedPharma Hub and submit your daily work before end of day.")

            _send_email_to(email, subject, body, html_body)
            log.info(f"Production reminder sent to {username} ({email})")
        except Exception as e:
            log.error(f"Failed to send production reminder to {username}: {e}")


def _send_email_to(to_email: str, subject: str, body: str, html_body: str = "") -> tuple[bool, str]:
    """Send email to a specific recipient via SendGrid v3 API.
    Uses _live_config() to read credentials fresh.

    Returns (sent, via) where ``via`` is 'sendgrid', 'smtp', or a reason
    string when nothing went out. The previous signature returned None;
    callers that ignore the tuple still work fine.
    """
    if not to_email:
        return False, "missing recipient"
    cfg = _live_config()

    # Primary: SendGrid
    sg_key = cfg["SENDGRID_API_KEY"]
    sg_from = cfg["SENDGRID_FROM"]
    if sg_key:
        try:
            import httpx
            content = []
            if body:
                content.append({"type": "text/plain", "value": body})
            if html_body:
                content.append({"type": "text/html", "value": html_body})
            if not content:
                content.append({"type": "text/plain", "value": "(no content)"})

            payload = {
                "personalizations": [{"to": [{"email": to_email}]}],
                "from": {"email": sg_from, "name": "MedPharma Hub"},
                "subject": subject,
                "content": content,
            }
            resp = httpx.post(
                "https://api.sendgrid.com/v3/mail/send",
                json=payload,
                headers={
                    "Authorization": f"Bearer {sg_key}",
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
            if resp.status_code in (200, 202):
                log.info(f"Email sent to {to_email}: {subject}")
                return True, "sendgrid"
            log.error(f"SendGrid failed ({resp.status_code}): {resp.text}")
        except Exception as e:
            log.error(f"Failed to send email to {to_email} via SendGrid: {e}")

    # Fallback: SMTP
    smtp_h = cfg["SMTP_HOST"]
    smtp_p = cfg["SMTP_PORT"]
    smtp_u = cfg["SMTP_USER"]
    smtp_pw = cfg["SMTP_PASS"]
    if not smtp_h or not smtp_u or not smtp_pw:
        log.error("Email skipped — no working provider configured (SendGrid/SMTP)")
        return False, "no provider configured (SendGrid/SMTP env vars missing)"

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = smtp_u or sg_from
        msg["To"] = to_email
        plain = body or "(no content)"
        msg.attach(MIMEText(plain, "plain"))
        if html_body:
            msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_h, smtp_p, timeout=20) as server:
            server.starttls()
            server.login(smtp_u, smtp_pw)
            server.sendmail(msg["From"], [to_email], msg.as_string())
        log.info(f"Email sent via SMTP to {to_email}: {subject}")
        return True, "smtp"
    except Exception as e:
        log.error(f"Failed to send email to {to_email} via SMTP: {e}")
        return False, f"smtp error: {e}"


def send_team_progress_reports():
    """
    Send consolidated progress reports for tracked users (e.g., Jessica/RCM)
    to configured owner recipients at 7:00 PM Eastern.
    """
    users = [u for u in NOTIFY_ON_USERS if u and u != "*"]
    if not users:
        log.info("Team progress dispatch skipped — no explicit tracked users configured")
        return

    sent = 0
    for username in sorted(users):
        try:
            flush_and_notify(username)
            sent += 1
        except Exception as e:
            log.error(f"Team progress dispatch failed for {username}: {e}")
    log.info(f"Team progress dispatch completed for {sent} user(s): {', '.join(sorted(users))}")


# ─── End-of-Day Team Report (Lexi & Eric) ────────────────────────────────

# Default recipients for the EOD per-user / per-client breakdown email.
# Override at runtime with the EOD_REPORT_EMAIL env var (comma-separated).
EOD_REPORT_DEFAULT_RECIPIENTS = ["lexi@medprosc.com", "eric@medprosc.com"]


def _eod_recipients() -> list[str]:
    raw = os.getenv("EOD_REPORT_EMAIL", "").strip()
    if raw:
        return [e.strip() for e in raw.split(",") if e.strip()]
    return list(EOD_REPORT_DEFAULT_RECIPIENTS)


def _esc_html(s) -> str:
    """Minimal HTML escape — we're building markup with f-strings."""
    return (
        str(s if s is not None else "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# Friendly per-tab icons (matches the sidebar in client_hub.html).
_EOD_TAB_ICONS = {
    "Claims":        "💼",
    "Credentialing": "🎓",
    "Enrollment":    "📝",
    "EDI":           "🔌",
    "Production":    "⏱️",
    "Documents":     "📁",
    "Notes":         "🗒️",
    "Chat":          "💬",
    "Audit":         "🔍",
    "Pageviews":     "👁️",
}


def _render_eod_report_html(report: dict) -> tuple[str, str]:
    """Render the EOD report dict into (text_body, html_body).

    The report dict comes from app.client_db.get_eod_team_report().
    """
    date_iso = report.get("report_date", "")
    try:
        from datetime import datetime as _dt
        d = _dt.strptime(date_iso, "%Y-%m-%d")
        date_long = d.strftime("%A, %B %d, %Y")
    except Exception:
        date_long = date_iso

    headlines = report.get("headlines", {}) or {}
    tabs = report.get("tab_keys", []) or []
    users = report.get("users", []) or []
    team_totals = report.get("team_totals", {}) or {}

    # ── plain-text fallback ──
    text_lines = [
        f"MedPharma End-of-Day Team Report — {date_long}",
        "",
        f"Active team members today: {headlines.get('active_users', 0)}",
        f"Claims created: {headlines.get('claims_new', 0)} · updated: {headlines.get('claims_touched', 0)}",
        f"Credentialing new: {headlines.get('cred_new', 0)} · Enrollment new: {headlines.get('enroll_new', 0)} · EDI new: {headlines.get('edi_new', 0)}",
        f"Production entries: {headlines.get('production_rows', 0)} ({headlines.get('production_hours', 0)} hrs)",
        f"Notes: {headlines.get('notes_new', 0)} · Files uploaded: {headlines.get('files_uploaded', 0)} · Chat messages: {headlines.get('chat_messages', 0)} · Audit events: {headlines.get('audit_events', 0)}",
        "",
        "Per-user breakdown:",
    ]
    for u in users:
        text_lines.append(
            f"\n* {u.get('contact_name') or u.get('username')} "
            f"<{u.get('email','')}> — {u.get('active_hours',0)}h active / "
            f"{u.get('idle_hours',0)}h idle / {u.get('total_actions',0)} actions"
        )
        for cname, cb in (u.get("clients") or {}).items():
            chunks = [f"{k}={v}" for k, v in cb["totals"].items() if v]
            if not chunks:
                continue
            text_lines.append(f"    - {cname}: {', '.join(chunks)}")
            for it in cb["items"][:5]:
                text_lines.append(f"        · [{it['tab']}] {it['action']} — {it['title']}")
    text_body = "\n".join(text_lines)

    # ── HTML ──
    headline_card = lambda label, value, color="#1d4ed8": f"""
        <div style="flex:1;min-width:140px;background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px 16px;margin:6px">
            <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;font-weight:600">{label}</div>
            <div style="font-size:22px;color:{color};font-weight:800;margin-top:4px">{value}</div>
        </div>
    """

    headlines_html = "".join([
        headline_card("Active team", headlines.get("active_users", 0)),
        headline_card("Claims new",     headlines.get("claims_new", 0)),
        headline_card("Claims touched", headlines.get("claims_touched", 0), "#2563eb"),
        headline_card("Credentialing",  headlines.get("cred_new", 0), "#7c3aed"),
        headline_card("Enrollment",     headlines.get("enroll_new", 0), "#7c3aed"),
        headline_card("EDI",            headlines.get("edi_new", 0), "#0891b2"),
        headline_card("Production hrs", f"{headlines.get('production_hours', 0)}", "#16a34a"),
        headline_card("Notes",          headlines.get("notes_new", 0), "#0f766e"),
        headline_card("Files",          headlines.get("files_uploaded", 0), "#0f766e"),
        headline_card("Chat msgs",      headlines.get("chat_messages", 0), "#db2777"),
    ])

    # Per-user blocks (with per-client per-tab breakdown).
    user_blocks_html = ""
    if not users:
        user_blocks_html = (
            "<div style=\"padding:18px;color:#94a3b8;font-size:14px;text-align:center\">"
            "No team activity recorded today. Either the team didn't log in, "
            "or the tracker missed events. Check the Audit Log to confirm."
            "</div>"
        )
    else:
        for u in users:
            uname = _esc_html(u.get("contact_name") or u.get("username", ""))
            email = _esc_html(u.get("email", ""))
            role  = _esc_html((u.get("role") or "").title())
            hrs_a = u.get("active_hours", 0)
            hrs_i = u.get("idle_hours", 0)
            acts  = u.get("total_actions", 0)
            highlights = u.get("highlights") or []

            # Per-user tab strip
            tab_strip = ""
            for t in tabs:
                v = u["totals"].get(t, 0)
                if v == 0:
                    continue
                tab_strip += (
                    f'<span style="display:inline-block;background:#eef2ff;'
                    f'color:#1d4ed8;border-radius:6px;padding:4px 10px;'
                    f'margin:2px 4px 2px 0;font-size:12px;font-weight:600">'
                    f'{_EOD_TAB_ICONS.get(t,"")} {t} <b>{v}</b></span>'
                )
            if not tab_strip:
                tab_strip = (
                    '<span style="color:#94a3b8;font-size:12px;font-style:italic">'
                    'Logged in but no tracked tab activity</span>'
                )

            # Per-client rows
            client_rows = ""
            for cname, cb in (u.get("clients") or {}).items():
                tab_cells = ""
                any_nonzero = False
                for t in tabs:
                    v = cb["totals"].get(t, 0)
                    if v:
                        any_nonzero = True
                    cell_style = "padding:4px 8px;border:1px solid #f1f5f9;text-align:center;font-size:12px"
                    if v:
                        cell_style += ";background:#eef2ff;color:#1d4ed8;font-weight:700"
                    else:
                        cell_style += ";color:#cbd5e1"
                    tab_cells += f'<td style="{cell_style}">{v or "·"}</td>'
                if not any_nonzero:
                    continue
                # Sample items (max 6) so the reader sees what was actually done.
                items_html = ""
                for it in cb["items"][:6]:
                    items_html += (
                        f'<div style="font-size:12px;color:#475569;'
                        f'padding:2px 0;border-bottom:1px dashed #f1f5f9">'
                        f'<span style="display:inline-block;min-width:96px;color:#1d4ed8;font-weight:600">'
                        f'{_EOD_TAB_ICONS.get(it["tab"],"")} {_esc_html(it["tab"])}</span> '
                        f'{_esc_html(it["action"])} — {_esc_html(it["title"])}</div>'
                    )
                client_rows += f"""
                <tr><td colspan="{len(tabs)+1}" style="padding:10px 8px 4px;font-weight:700;color:#0f172a;font-size:13px;border-top:1px solid #e2e8f0">{_esc_html(cname)}</td></tr>
                <tr>
                    <td style="padding:4px 8px;font-size:12px;color:#64748b;border:1px solid #f1f5f9">Tabs touched</td>
                    {tab_cells}
                </tr>
                <tr>
                    <td colspan="{len(tabs)+1}" style="padding:6px 8px 12px;border:1px solid #f8fafc;background:#fafafa">{items_html or '<span style="color:#94a3b8;font-size:12px">No itemised changes captured.</span>'}</td>
                </tr>
                """

            if not client_rows:
                client_rows = (
                    f'<tr><td colspan="{len(tabs)+1}" '
                    f'style="padding:10px;color:#94a3b8;font-size:12px;font-style:italic">'
                    f'No per-client work captured — see "Tabs Touched" badges above for raw page activity.</td></tr>'
                )

            tab_header_cells = "".join(
                f'<th style="padding:6px 8px;background:#f8fafc;border:1px solid #f1f5f9;font-size:11px;text-transform:uppercase;letter-spacing:.4px;color:#475569">{_EOD_TAB_ICONS.get(t,"")} {t}</th>'
                for t in tabs
            )

            highlights_html = ""
            if highlights:
                hi_list = "".join(f"<li style=\"font-size:12px;color:#475569;padding:2px 0\">{_esc_html(h)}</li>" for h in highlights)
                highlights_html = (
                    f'<div style="margin-top:10px"><div style="font-size:12px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.4px">Audit highlights</div>'
                    f'<ul style="margin:6px 0 0;padding-left:20px">{hi_list}</ul></div>'
                )

            user_blocks_html += f"""
            <div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:18px 20px;margin:14px 0;box-shadow:0 1px 3px rgba(15,23,42,.04)">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
                    <div>
                        <div style="font-size:17px;font-weight:800;color:#0f172a">{uname} <span style="font-size:11px;color:#94a3b8;font-weight:600;text-transform:uppercase;margin-left:6px">{role}</span></div>
                        <div style="font-size:12px;color:#64748b;margin-top:2px">{email}</div>
                    </div>
                    <div style="text-align:right">
                        <div style="font-size:12px;color:#64748b">Time on hub today</div>
                        <div style="font-size:18px;font-weight:800;color:#16a34a">{hrs_a}h <span style="color:#94a3b8;font-weight:600;font-size:12px">active</span></div>
                        <div style="font-size:12px;color:#64748b">{hrs_i}h idle · {acts} actions</div>
                    </div>
                </div>
                <div style="margin-top:12px">{tab_strip}</div>
                {highlights_html}
                <table style="width:100%;border-collapse:collapse;margin-top:14px;font-family:system-ui,Segoe UI,Arial,sans-serif">
                    <thead><tr><th style="padding:6px 8px;background:#f8fafc;border:1px solid #f1f5f9;font-size:11px;text-transform:uppercase;letter-spacing:.4px;color:#475569;text-align:left">Client</th>{tab_header_cells}</tr></thead>
                    <tbody>{client_rows}</tbody>
                </table>
            </div>
            """

    # Team-wide tab totals strip
    team_strip = ""
    for t in tabs:
        v = team_totals.get(t, 0)
        if v == 0:
            continue
        team_strip += (
            f'<span style="display:inline-block;background:rgba(255,255,255,.18);'
            f'color:#fff;border-radius:6px;padding:6px 12px;margin:3px 4px;'
            f'font-size:13px;font-weight:600">{_EOD_TAB_ICONS.get(t,"")} {t} <b>{v}</b></span>'
        )
    if not team_strip:
        team_strip = '<span style="color:rgba(255,255,255,.65);font-style:italic">No tab activity recorded today.</span>'

    html_body = f"""<html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;padding:18px;color:#0f172a;background:#f1f5f9">
      <div style="max-width:760px;margin:0 auto">

        <div style="background:linear-gradient(135deg,#0f172a,#1d4ed8);padding:24px 28px;border-radius:14px 14px 0 0">
            <h1 style="color:#fff;margin:0;font-size:22px;font-weight:800;letter-spacing:.3px">📋 End-of-Day Team Report</h1>
            <p style="color:rgba(255,255,255,.85);margin:6px 0 0;font-size:14px">{_esc_html(date_long)} — per user, per client, per tab</p>
            <div style="margin-top:12px">{team_strip}</div>
        </div>

        <div style="background:#fff;padding:14px 16px;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0">
            <div style="display:flex;flex-wrap:wrap">{headlines_html}</div>
        </div>

        <div style="background:#f8fafc;padding:16px;border:1px solid #e2e8f0;border-top:0;border-radius:0 0 14px 14px">
            <h2 style="margin:0 0 10px;font-size:15px;color:#0f172a">Per-User Breakdown</h2>
            {user_blocks_html}
        </div>

        <p style="text-align:center;color:#94a3b8;font-size:11px;margin-top:18px">
            MedPharma Hub · Auto-sent end-of-day · Configure with EOD_REPORT_EMAIL env var
        </p>
      </div>
    </body></html>"""
    return text_body, html_body


def send_eod_team_report(report_date: str = None, force: bool = False) -> dict:
    """Compose and email the end-of-day team report.

    Default recipients: lexi@medprosc.com + eric@medprosc.com
    (override with EOD_REPORT_EMAIL, comma-separated).

    Each recipient is sent individually via _send_email_to so SendGrid
    delivery is per-recipient (we can see who bounced).

    Returns a delivery report dict {date, recipients, sent, failed,
    headlines, user_count}.
    """
    from datetime import datetime as _dt
    try:
        from app.client_db import get_eod_team_report
    except Exception as e:
        log.error(f"EOD report aggregator import failed: {e}")
        return {"ok": False, "error": str(e)}

    if not report_date:
        report_date = _dt.now().strftime("%Y-%m-%d")

    try:
        report = get_eod_team_report(report_date)
    except Exception as e:
        log.error(f"EOD report aggregation failed: {e}")
        return {"ok": False, "error": str(e), "date": report_date}

    headlines = report.get("headlines", {}) or {}
    if not force and not report.get("users") and sum(headlines.values()) == 0:
        log.info(f"EOD report skipped — zero activity for {report_date}")
        return {"ok": True, "skipped": "no activity", "date": report_date}

    text_body, html_body = _render_eod_report_html(report)
    try:
        from datetime import datetime as _dt2
        d_long = _dt2.strptime(report_date, "%Y-%m-%d").strftime("%a %b %d, %Y")
    except Exception:
        d_long = report_date
    subject = (
        f"📋 MedPharma EOD — {d_long} — "
        f"{headlines.get('active_users', 0)} active, "
        f"{headlines.get('claims_new', 0)} new claims, "
        f"{headlines.get('production_hours', 0)}h logged"
    )

    recipients = _eod_recipients()
    sent, failed = [], []
    if not recipients:
        log.error("EOD report has no recipients configured")
        return {"ok": False, "error": "no recipients", "date": report_date}

    for to_email in recipients:
        try:
            ok_sent, via = _send_email_to(to_email, subject, text_body, html_body)
        except Exception as e:
            log.error(f"EOD report send to {to_email} crashed: {e}")
            failed.append({"email": to_email, "error": str(e), "via": "exception"})
            continue
        if ok_sent:
            sent.append({"email": to_email, "via": via})
        else:
            failed.append({"email": to_email, "via": via})

    log.info(
        f"EOD report dispatched for {report_date}: sent={len(sent)} failed={len(failed)} "
        f"users={len(report.get('users', []))}"
    )
    return {
        "ok": True,
        "date": report_date,
        "recipients": recipients,
        "sent": sent,
        "failed": failed,
        "user_count": len(report.get("users", [])),
        "headlines": headlines,
    }


def _build_demo_eod_report() -> dict:
    """Fabricate a realistic EOD report so the email preview shows what
    the final product looks like even when the live DB is quiet. Same
    shape as get_eod_team_report() — fed directly to the renderer.
    """
    from datetime import datetime as _dt
    today = _dt.now().strftime("%Y-%m-%d")
    tab_keys = ["Claims", "Credentialing", "Enrollment", "EDI", "Production",
                "Documents", "Notes", "Chat", "Audit", "Pageviews"]

    def _tabs(**vals):
        out = {k: 0 for k in tab_keys}
        out.update(vals)
        return out

    users = [
        {
            "username": "jessica@medprosc.com",
            "contact_name": "Jessica",
            "email": "jessica@medprosc.com",
            "role": "staff",
            "active_hours": 6.4,
            "idle_hours": 1.1,
            "actions": 142,
            "first_seen": f"{today}T08:53:12",
            "last_seen": f"{today}T17:18:44",
            "totals": _tabs(Claims=18, Notes=7, Production=3, Pageviews=92, Audit=4),
            "total_actions": 124,
            "highlights": [
                "claim_status_update claims_master — moved CLM-44218 from A/R Follow-Up → Paid",
                "note_added notes_log — Aetna confirmed reprocess; eta 14 days",
            ],
            "clients": {
                "Apex Pain Management": {
                    "totals": _tabs(Claims=12, Notes=4, Pageviews=58, Audit=2),
                    "items": [
                        {"tab": "Claims",     "action": "created", "title": "CLM-44218 (Billed/Submitted)"},
                        {"tab": "Claims",     "action": "updated", "title": "CLM-44109 (Paid)"},
                        {"tab": "Claims",     "action": "updated", "title": "CLM-44091 (Appeals)"},
                        {"tab": "Notes",      "action": "noted",   "title": "Claim CLM-44091 — sent reconsideration packet to Cigna"},
                        {"tab": "Notes",      "action": "noted",   "title": "Claim CLM-44218 — Aetna confirmed receipt"},
                    ],
                },
                "Coastal Orthopedics": {
                    "totals": _tabs(Claims=6, Notes=3, Production=3, Pageviews=34, Audit=2),
                    "items": [
                        {"tab": "Claims",     "action": "updated", "title": "CLM-22107 (Denied → Appeals)"},
                        {"tab": "Production", "action": "logged",  "title": "A/R Follow-Up: Worked Cigna denials batch (12 · 2.5h)"},
                        {"tab": "Notes",      "action": "noted",   "title": "Claim CLM-22107 — Cigna requires CPT modifier docs"},
                    ],
                },
            },
        },
        {
            "username": "susan@medprosc.com",
            "contact_name": "Susan",
            "email": "susan@medprosc.com",
            "role": "staff",
            "active_hours": 5.8,
            "idle_hours": 0.7,
            "actions": 96,
            "first_seen": f"{today}T09:02:50",
            "last_seen": f"{today}T16:44:18",
            "totals": _tabs(Credentialing=9, Enrollment=4, Documents=2, Notes=5, Pageviews=61, Audit=3),
            "total_actions": 84,
            "highlights": [
                "credentialing_submit credentialing — Dr Chen / BCBS-SC initial app submitted",
                "file_upload client_files — uploaded CAQH attestation PDF",
            ],
            "clients": {
                "Apex Pain Management": {
                    "totals": _tabs(Credentialing=6, Notes=3, Documents=1, Pageviews=38, Audit=2),
                    "items": [
                        {"tab": "Credentialing", "action": "created", "title": "Dr Chen · BCBS-SC (Submitted)"},
                        {"tab": "Credentialing", "action": "updated", "title": "Dr Patel · Aetna (Approved)"},
                        {"tab": "Documents",     "action": "uploaded","title": "Patel-CAQH-attestation.pdf · Credentialing"},
                        {"tab": "Notes",         "action": "noted",   "title": "Credentialing Dr Chen — recruiter packet forwarded"},
                    ],
                },
                "Magnolia Imaging": {
                    "totals": _tabs(Credentialing=3, Enrollment=4, Documents=1, Notes=2, Pageviews=23, Audit=1),
                    "items": [
                        {"tab": "Enrollment",   "action": "created", "title": "Dr Lee · Humana (Submitted)"},
                        {"tab": "Enrollment",   "action": "updated", "title": "Dr Lee · Cigna (Approved)"},
                        {"tab": "Credentialing","action": "updated", "title": "Dr Lee · BCBS-SC (Follow-up scheduled)"},
                    ],
                },
            },
        },
        {
            "username": "rcm@medprosc.com",
            "contact_name": "RCM",
            "email": "rcm@medprosc.com",
            "role": "admin",
            "active_hours": 4.2,
            "idle_hours": 0.6,
            "actions": 71,
            "first_seen": f"{today}T10:11:09",
            "last_seen": f"{today}T15:33:51",
            "totals": _tabs(Production=5, EDI=3, Notes=2, Chat=4, Pageviews=40, Audit=2),
            "total_actions": 56,
            "highlights": [
                "production_log team_production — 4.5h posted (ERA reconciliation)",
                "edi_status edi_setup — Apex GoLive confirmed for 06/12",
            ],
            "clients": {
                "Apex Pain Management": {
                    "totals": _tabs(Production=3, EDI=2, Notes=1, Chat=2, Pageviews=24, Audit=1),
                    "items": [
                        {"tab": "Production", "action": "logged", "title": "ERA Reconciliation: Tied out Aetna 06/03 ERA (28 · 2.0h)"},
                        {"tab": "EDI",        "action": "updated","title": "Apex · UHC (ERA Active, EFT Active)"},
                        {"tab": "Chat",       "action": "messaged","title": "room 'Apex — RCM Daily Standup'"},
                    ],
                },
                "Coastal Orthopedics": {
                    "totals": _tabs(Production=2, EDI=1, Notes=1, Chat=2, Pageviews=16, Audit=1),
                    "items": [
                        {"tab": "Production", "action": "logged",  "title": "Posting: BCBS-SC EOB batch (14 · 1.0h)"},
                        {"tab": "EDI",        "action": "updated", "title": "Coastal · Aetna (ERA Pending → Active)"},
                        {"tab": "Chat",       "action": "messaged","title": "room 'Coastal — Posting Queue'"},
                    ],
                },
            },
        },
        {
            "username": "eric@medprosc.com",
            "contact_name": "Eric",
            "email": "eric@medprosc.com",
            "role": "admin",
            "active_hours": 2.3,
            "idle_hours": 0.4,
            "actions": 38,
            "first_seen": f"{today}T11:24:01",
            "last_seen": f"{today}T15:08:22",
            "totals": _tabs(Chat=11, Pageviews=27, Audit=2),
            "total_actions": 40,
            "highlights": [
                "client_review clients — opened Apex profile; reviewed open AR queue",
                "chat_room_create chat_rooms — created 'Apex — RCM Daily Standup'",
            ],
            "clients": {
                "Apex Pain Management": {
                    "totals": _tabs(Chat=7, Pageviews=18, Audit=1),
                    "items": [
                        {"tab": "Chat", "action": "messaged", "title": "room 'Apex — RCM Daily Standup'"},
                        {"tab": "Chat", "action": "messaged", "title": "room 'Apex — Credentialing Sync'"},
                    ],
                },
                "Coastal Orthopedics": {
                    "totals": _tabs(Chat=4, Pageviews=9, Audit=1),
                    "items": [
                        {"tab": "Chat", "action": "messaged", "title": "room 'Coastal — Posting Queue'"},
                    ],
                },
            },
        },
        {
            "username": "admin@medprosc.com",
            "contact_name": "Lexi",
            "email": "lexi@medprosc.com",
            "role": "admin",
            "active_hours": 1.7,
            "idle_hours": 0.2,
            "actions": 24,
            "first_seen": f"{today}T13:02:11",
            "last_seen": f"{today}T15:42:00",
            "totals": _tabs(Audit=6, Pageviews=18),
            "total_actions": 24,
            "highlights": [
                "client_create clients — Magnolia Imaging onboarded",
                "user_invite clients — invited admin@magnoliaimg.com (staff)",
                "module_toggle clients — enabled Documents + Chat for Coastal",
            ],
            "clients": {
                "Magnolia Imaging": {
                    "totals": _tabs(Audit=4, Pageviews=11),
                    "items": [
                        {"tab": "Audit", "action": "created", "title": "client_create — Magnolia Imaging onboarded"},
                        {"tab": "Audit", "action": "updated", "title": "user_invite — admin@magnoliaimg.com"},
                    ],
                },
                "Coastal Orthopedics": {
                    "totals": _tabs(Audit=2, Pageviews=7),
                    "items": [
                        {"tab": "Audit", "action": "updated", "title": "module_toggle — enabled Documents + Chat"},
                    ],
                },
            },
        },
    ]

    team_totals = {k: 0 for k in tab_keys}
    for u in users:
        for k, v in u["totals"].items():
            team_totals[k] = team_totals.get(k, 0) + v

    headlines = {
        "claims_new": 18,
        "claims_touched": 23,
        "cred_new": 9,
        "enroll_new": 4,
        "edi_new": 3,
        "production_rows": 5,
        "production_hours": 8.5,
        "notes_new": 14,
        "files_uploaded": 3,
        "chat_messages": 15,
        "audit_events": 17,
        "active_users": len(users),
    }

    return {
        "report_date": today,
        "generated_at": _dt.now().isoformat(timespec="seconds"),
        "tab_keys": tab_keys,
        "users": users,
        "team_totals": team_totals,
        "headlines": headlines,
        "client_count": 3,
    }


def send_eod_team_report_demo() -> dict:
    """Render & email a fully-populated DEMO end-of-day report.

    Same recipients as the real one (lexi@medprosc.com + eric@medprosc.com
    by default, overridable via EOD_REPORT_EMAIL). Use this when the live
    DB is quiet but you want to verify the email actually looks right.
    """
    report = _build_demo_eod_report()
    text_body, html_body = _render_eod_report_html(report)
    headlines = report["headlines"]
    try:
        from datetime import datetime as _dt
        d_long = _dt.strptime(report["report_date"], "%Y-%m-%d").strftime("%a %b %d, %Y")
    except Exception:
        d_long = report["report_date"]
    subject = (
        f"📋 MedPharma EOD [DEMO] — {d_long} — "
        f"{headlines['active_users']} active, "
        f"{headlines['claims_new']} new claims, "
        f"{headlines['production_hours']}h logged"
    )

    recipients = _eod_recipients()
    sent, failed = [], []
    for to_email in recipients:
        try:
            ok_sent, via = _send_email_to(to_email, subject, text_body, html_body)
        except Exception as e:
            log.error(f"EOD demo send to {to_email} crashed: {e}")
            failed.append({"email": to_email, "via": f"exception: {e}"})
            continue
        if ok_sent:
            sent.append({"email": to_email, "via": via})
        else:
            failed.append({"email": to_email, "via": via})

    log.info(f"EOD DEMO dispatched: sent={len(sent)} failed={len(failed)}")
    return {
        "ok": True,
        "demo": True,
        "date": report["report_date"],
        "recipients": recipients,
        "sent": sent,
        "failed": failed,
        "user_count": len(report["users"]),
        "headlines": headlines,
    }


def start_daily_scheduler():
    """
        Start APScheduler to fire:
            - send_production_reminders at 5:30 PM EST (for jessica & rcm)
            - send_daily_account_summary at 6:00 PM EST
    Safe to call multiple times — only starts once.
    """
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz

        est = pytz.timezone("US/Eastern")
        scheduler = BackgroundScheduler(daemon=True)

        # 5:30 PM EST — Production reminders
        scheduler.add_job(
            send_production_reminders,
            CronTrigger(hour=17, minute=30, timezone=est),
            id="daily_production_reminders",
            name="5:30 PM EST Production Reminders",
            replace_existing=True,
        )

        # 6:00 PM EST — Account summary report
        scheduler.add_job(
            send_daily_account_summary,
            CronTrigger(hour=18, minute=0, timezone=est),
            id="daily_account_summary",
            name="6 PM EST Overall Account Summary",
            replace_existing=True,
        )

        # 6:30 PM EST — End-of-Day per-user / per-client / per-tab team report
        scheduler.add_job(
            send_eod_team_report,
            CronTrigger(hour=18, minute=30, timezone=est),
            id="daily_eod_team_report",
            name="6:30 PM EST EOD Team Report (lexi+eric)",
            replace_existing=True,
        )

        scheduler.start()
        log.info("Daily scheduler started — 5:00 national pull, 5:30 reminders, 6:00 summary, 6:30 EOD team report")
    except ImportError:
        # Fallback: use a simple threading timer that checks every 60 seconds
        log.warning("apscheduler not installed — falling back to threading-based scheduler")
        _start_thread_scheduler()
    except Exception as e:
        log.error(f"Failed to start scheduler: {e}")
        _start_thread_scheduler()


def _start_thread_scheduler():
    """Fallback scheduler using threading — checks every 60s for 5:30 and 6:00 PM EST."""
    import time as _time

    def _check_loop():
        last_reminder_date = None
        last_sent_date = None
        last_eod_date = None
        while True:
            try:
                # Get current time in US/Eastern
                try:
                    import pytz
                    est = pytz.timezone("US/Eastern")
                    now_est = datetime.now(est)
                except ImportError:
                    # No pytz — approximate EST as UTC-5
                    from datetime import timedelta, timezone
                    est_tz = timezone(timedelta(hours=-5))
                    now_est = datetime.now(est_tz)

                today = now_est.date()

                # 5:30 PM — Production reminders
                if now_est.hour == 17 and 30 <= now_est.minute < 35 and last_reminder_date != today:
                    last_reminder_date = today
                    log.info("Thread scheduler firing production reminders")
                    send_production_reminders()

                # 6:00 PM — Daily account summary
                if now_est.hour == 18 and now_est.minute < 5 and last_sent_date != today:
                    last_sent_date = today
                    log.info("Thread scheduler firing daily account summary")
                    send_daily_account_summary()

                # 6:30 PM — End-of-Day per-user / per-client / per-tab report
                if now_est.hour == 18 and 30 <= now_est.minute < 35 and last_eod_date != today:
                    last_eod_date = today
                    log.info("Thread scheduler firing EOD team report")
                    send_eod_team_report()

            except Exception as e:
                log.error(f"Thread scheduler error: {e}")
            _time.sleep(60)

    t = threading.Thread(target=_check_loop, daemon=True)
    t.start()
    log.info("Fallback thread scheduler started — 5:30 reminders + 6:00 summary + 6:30 EOD team report")
