"""
Notification system — Team Lead Production Report.

Buffers all activity during a user's session and sends ONE consolidated
"Team Lead Production" report when the user logs out, including:
  • Activity breakdown by section
  • Industry-standard RCM benchmarks comparison
  • AI-powered productivity analysis (via OpenAI, with rule-based fallback)

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
  OPENAI_API_KEY — For AI productivity narrative (optional; falls back to rule-based)
"""

import os
import logging
import threading
import smtplib
import json
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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Users whose activity triggers notifications (non-admin team members)
NOTIFY_ON_USERS = {"jessica", "rcm"}

# ── Industry-standard RCM benchmarks (actions per 8-hour day) ──
# Sources: MGMA, HBMA, AAPC industry reports for medical billing / credentialing
INDUSTRY_BENCHMARKS = {
    "Claims":          {"daily_target": 180, "unit": "claims touched", "per_hour": 25,
                        "note": "Industry avg: 25-35 claims processed/hr (AR follow-up, posting, submissions)"},
    "Credentialing":   {"daily_target": 12,  "unit": "credentialing actions", "per_hour": 1.5,
                        "note": "Industry avg: 3-5 new apps + 8-15 follow-ups/day"},
    "Enrollment":      {"daily_target": 8,   "unit": "enrollment actions", "per_hour": 1,
                        "note": "Industry avg: 5-8 payer enrollment submissions/day"},
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


def _should_notify(username: str) -> bool:
    """Return True if this user's activity should trigger notifications."""
    return username.lower() in NOTIFY_ON_USERS


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
should process 25-35 claims/hr, credentialing staff handle 3-5 new apps + 15 follow-ups/day,
enrollment specialists submit 5-8 payer enrollments/day.

Write a concise 3-5 sentence "Team Lead Production Assessment" that:
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

def notify_activity(username: str, action: str, section: str, detail: str = ""):
    """Buffer a single activity event (does NOT send immediately)."""
    if not _should_notify(username):
        return
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


def notify_bulk_activity(username: str, action: str, section: str, count: int, detail: str = ""):
    """Buffer a bulk activity event (does NOT send immediately)."""
    if not _should_notify(username):
        return
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


def flush_and_notify(username: str):
    """
    Called at logout — builds a full **Team Lead Production** report with:
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
        "       TEAM LEAD PRODUCTION REPORT",
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
                <h1 style="color: white; margin: 0; font-size: 22px; font-weight: 800; letter-spacing: 0.5px;">📊 TEAM LEAD PRODUCTION</h1>
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
                    Team Lead Production Report — MedPharma Hub — {date_str}
                </p>
            </div>
        </div>
    </body>
    </html>"""

    subject = f"Team Lead Production: {username} — {rating_label} ({overall_pct:.0f}%) — {date_str}"

    # SMS — concise summary with rating
    section_counts = ", ".join(f"{s}: {len(items)}" for s, items in by_section.items())
    sms = (f"Team Lead Production: {username} | {date_str} | "
           f"{rating_label} ({overall_pct:.0f}%) | "
           f"{len(activities)} actions in {session_hrs:.1f}hrs | {section_counts}")
    if len(sms) > 155:
        sms = (f"Team Lead: {username} | {rating_label} ({overall_pct:.0f}%) | "
               f"{len(activities)} actions in {session_hrs:.1f}hrs")
        if len(sms) > 155:
            sms = sms[:152] + "…"

    # Fire both in background threads
    threading.Thread(target=_send_email, args=(subject, body, html_body), daemon=True).start()
    threading.Thread(target=_send_sms, args=(sms,), daemon=True).start()
    log.info(f"Team Lead Production report queued for {username}: {rating_label} "
             f"({overall_pct:.0f}%) — {len(activities)} actions across {len(by_section)} sections")


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
