#!/usr/bin/env python3
"""Test if Bing/DuckDuckGo return extractable LinkedIn profile URLs."""
import re, urllib.request, urllib.parse

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

cases = [
    ("Derek", "Pagan", "1LAB DIAGNOSTICS"),
    ("Wilson", "Molina", ""),
    ("Wilson", "Molina", "Laboratories Florida"),
    ("Eric", "Schmidt", ""),
    ("Bill", "Gates", ""),
    ("Satya", "Nadella", "Microsoft"),
]

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        return urllib.request.urlopen(req, timeout=20).read().decode("utf-8","replace")
    except Exception as e:
        return f"ERR: {e}"

for first, last, org in cases:
    q = f"{first} {last} {org} site:linkedin.com/in".strip()
    print("\n===", q)

    # DuckDuckGo Lite (minimal, no JS, no anti-bot)
    url = f"https://lite.duckduckgo.com/lite/?q={urllib.parse.quote(q)}"
    html = fetch(url)
    if html.startswith("ERR"):
        print("  DDG-Lite:", html[:120])
    else:
        urls = re.findall(r'https?://[a-z]{2,3}\.linkedin\.com/in/[A-Za-z0-9_%-]+', html)
        wrapped = re.findall(r'uddg=([^"&]+)', html)
        for w in wrapped[:8]:
            decoded = urllib.parse.unquote(w)
            if "linkedin.com/in/" in decoded:
                urls.append(decoded)
        print(f"  DDG-Lite matches: {len(urls)}  htmlsize={len(html)}")
        for u in urls[:3]:
            print("    ", u)

    # Brave (no anti-bot, decent index)
    url = f"https://search.brave.com/search?q={urllib.parse.quote(q)}"
    html = fetch(url)
    if html.startswith("ERR"):
        print("  Brave:", html[:120])
    else:
        urls = re.findall(r'https?://[a-z]{2,3}\.linkedin\.com/in/[A-Za-z0-9_%-]+', html)
        print(f"  Brave matches: {len(urls)}  htmlsize={len(html)}")
        for u in urls[:3]:
            print("    ", u)

    # Mojeek
    url = f"https://www.mojeek.com/search?q={urllib.parse.quote(q)}"
    html = fetch(url)
    if html.startswith("ERR"):
        print("  Mojeek:", html[:120])
    else:
        urls = re.findall(r'https?://[a-z]{2,3}\.linkedin\.com/in/[A-Za-z0-9_%-]+', html)
        print(f"  Mojeek matches: {len(urls)}  htmlsize={len(html)}")
        for u in urls[:3]:
            print("    ", u)

