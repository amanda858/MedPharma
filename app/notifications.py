"""
Notification system — Team Lead Production Report.

Buffers all activity during a user's session and sends ONE consolidated
"Team Lead Production" report when the user logs out, including:
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
from collections import defaultdict
from datetime import datetime

log = logging.getLogger("notifications")

# ── Configuration ──
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
SENDGRID_FROM = os.getenv("SENDGRID_FROM", "notifications@medprosc.com")
NOTIFY_EMAILS = [e.strip() for e in os.getenv("NOTIFY_EMAIL", "eric@medprosc.com,amanda@medprosc.com").split(",") if e.strip()]

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
    """Send email notification via SendGrid v3 API."""
    if not SENDGRID_API_KEY or not NOTIFY_EMAILS:
        log.debug("Email notification skipped — SENDGRID_API_KEY/NOTIFY_EMAILS not configured")
        return
    try:
        import httpx
        content = []
        if body:
            content.append({"type": "text/plain", "value": body})
        if html_body:
            content.append({"type": "text/html", "value": html_body})
        if not content:
            content.append({"type": "text/plain", "value": "(no content)"})

        recipients = [{"email": addr} for addr in NOTIFY_EMAILS]
        payload = {
            "personalizations": [{"to": recipients}],
            "from": {"email": SENDGRID_FROM, "name": "MedPharma Hub"},
            "subject": subject,
            "content": content,
        }
        resp = httpx.post(
            "https://api.sendgrid.com/v3/mail/send",
            json=payload,
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        if resp.status_code in (200, 202):
            log.info(f"Email sent via SendGrid to {', '.join(NOTIFY_EMAILS)}: {subject}")
        else:
            log.error(f"SendGrid failed ({resp.status_code}): {resp.text}")
    except Exception as e:
        log.error(f"Failed to send email via SendGrid: {e}")


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
- Enrollment: {d['enroll_total']} total ({d['enroll_approved']} approved, {d['enroll_pending']} pending)
- EDI: {d['edi_total']} total ({d['edi_live']} live)
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
    """
    try:
        from app.client_db import get_daily_account_summary
        d = get_daily_account_summary()
    except Exception as e:
        log.error(f"Failed to fetch daily account summary data: {e}")
        return

    now = datetime.now()
    date_str = now.strftime("%B %d, %Y")
    day_of_week = now.strftime("%A")

    # AI summary
    ai_summary = _generate_account_ai_summary(d, date_str)

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

    # ── Enrollment status rows ──
    enroll_rows_html = ""
    for status, count in sorted(d.get("enroll_stats", {}).items(), key=lambda x: -x[1]):
        enroll_rows_html += f'<span style="display:inline-block;background:#f1f5f9;border-radius:6px;padding:4px 10px;margin:2px;font-size:12px"><b>{count}</b> {status}</span>'

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
                <h1 style="color: white; margin: 0; font-size: 24px; font-weight: 800; letter-spacing: 0.5px;">📋 OVERALL ACCOUNT SUMMARY</h1>
                <p style="color: rgba(255,255,255,0.85); margin: 8px 0 0; font-size: 15px; font-weight: 500;">{day_of_week}, {date_str} — 6:00 PM EST Daily Report</p>
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

                <!-- CREDENTIALING / ENROLLMENT / EDI -->
                <div style="font-size:14px;font-weight:800;color:#1e293b;text-transform:uppercase;letter-spacing:0.5px;padding-bottom:8px;border-bottom:2px solid #1e293b;margin-bottom:16px;">
                    🏥 Credentialing, Enrollment & EDI
                </div>
                <div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;">
                    <div style="flex:1;min-width:140px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;">
                        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:6px;">Credentialing ({d['cred_total']})</div>
                        <div>{cred_rows_html or '<span style="font-size:12px;color:#94a3b8">No records</span>'}</div>
                    </div>
                    <div style="flex:1;min-width:140px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px;">
                        <div style="font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;margin-bottom:6px;">Enrollment ({d['enroll_total']})</div>
                        <div>{enroll_rows_html or '<span style="font-size:12px;color:#94a3b8">No records</span>'}</div>
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

                <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                <p style="font-size: 11px; color: #94a3b8; text-align: center; margin: 0;">
                    Overall Account Summary — MedPharma Hub — {date_str} 6:00 PM EST
                </p>
            </div>
        </div>
    </body>
    </html>"""

    # ── Plain text version ──
    body_lines = [
        "═══════════════════════════════════════════",
        "     OVERALL ACCOUNT SUMMARY",
        f"     {day_of_week}, {date_str} — 6:00 PM EST",
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
        "───── CREDENTIALING/ENROLLMENT/EDI ─────",
        f"  Credentialing: {d['cred_total']} ({d['cred_approved']} approved, {d['cred_pending']} pending)",
        f"  Enrollment:    {d['enroll_total']} ({d['enroll_approved']} approved, {d['enroll_pending']} pending)",
        f"  EDI:           {d['edi_total']} ({d['edi_live']} live)",
        "",
        f"  SLA Breaches: {d['sla_breaches']}  |  System Actions Today: {d['today_actions']}",
    ]
    body = "\n".join(body_lines)

    subject = f"Overall Account Summary — {date_str} — AR {_fmt_money(d['total_ar'])}"

    # SMS — compact daily summary
    sms = (f"MedPharma Daily: AR {_fmt_money(d['total_ar'])} | "
           f"Today: {d['submitted_today']}sub/{d['paid_today']}paid/{d['denied_today']}den | "
           f"MTD collected {_fmt_money(d['payments_mtd'])} | "
           f"Collection {d['net_collection_rate']}%")
    if len(sms) > 155:
        sms = (f"MedPharma: AR {_fmt_money(d['total_ar'])} | "
               f"{d['submitted_today']}sub/{d['paid_today']}paid | "
               f"Collection {d['net_collection_rate']}%")
        if len(sms) > 155:
            sms = sms[:152] + "…"

    threading.Thread(target=_send_email, args=(subject, body, html_body), daemon=True).start()
    threading.Thread(target=_send_sms, args=(sms,), daemon=True).start()
    log.info(f"Overall Account Summary sent: AR {_fmt_money(d['total_ar'])}, "
             f"{d['total_claims']} claims, {d['active_claims']} active")


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


def _send_email_to(to_email: str, subject: str, body: str, html_body: str = ""):
    """Send email to a specific recipient via SendGrid v3 API."""
    if not SENDGRID_API_KEY:
        log.debug("Email skipped — SENDGRID_API_KEY not configured")
        return
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
            "from": {"email": SENDGRID_FROM, "name": "MedPharma Hub"},
            "subject": subject,
            "content": content,
        }
        resp = httpx.post(
            "https://api.sendgrid.com/v3/mail/send",
            json=payload,
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        if resp.status_code in (200, 202):
            log.info(f"Email sent to {to_email}: {subject}")
        else:
            log.error(f"SendGrid failed ({resp.status_code}): {resp.text}")
    except Exception as e:
        log.error(f"Failed to send email to {to_email}: {e}")

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
        scheduler.start()
        log.info("Daily scheduler started — 5:30 PM reminders + 6:00 PM summary")
    except ImportError:
        # Fallback: use a simple threading timer that checks every 60 seconds
        log.warning("apscheduler not installed — falling back to threading-based scheduler")
        _start_thread_scheduler()
    except Exception as e:
        log.error(f"Failed to start scheduler: {e}")
        _start_thread_scheduler()


def _start_thread_scheduler():
    """Fallback scheduler using threading — checks every 60s for 5:30 PM and 6 PM EST."""
    import time as _time

    def _check_loop():
        last_reminder_date = None
        last_sent_date = None
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
            except Exception as e:
                log.error(f"Thread scheduler error: {e}")
            _time.sleep(60)

    t = threading.Thread(target=_check_loop, daemon=True)
    t.start()
    log.info("Fallback thread scheduler started — 5:30 PM reminders + 6:00 PM summary")
