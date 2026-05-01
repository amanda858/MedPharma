"""Inspect raw HTML response from search engines."""
import asyncio, sys
sys.path.insert(0, "/workspaces/CVOPro")
import httpx

H = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

async def main():
    q = '"Jeffrey Ledford" "Genova Diagnostics" linkedin'
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as c:
        for url, params in [
            ("https://html.duckduckgo.com/html/", {"q": q}),
            ("https://duckduckgo.com/html/", {"q": q}),
            ("https://www.bing.com/search", {"q": q}),
            ("https://www.google.com/search", {"q": q}),
        ]:
            try:
                r = await c.get(url, params=params, headers=H)
                txt = r.text
                print(f"\n=== {url} → HTTP {r.status_code}, {len(txt)} bytes")
                # Check for linkedin mentions
                import re
                lk = re.findall(r"linkedin\.com/in/[a-zA-Z0-9\-_%]+", txt)
                print(f"   linkedin matches: {lk[:5]}")
                # Snippet
                if "captcha" in txt.lower() or "are you a robot" in txt.lower():
                    print("   ⚠️ CAPTCHA detected")
                if "blocked" in txt.lower()[:2000]:
                    print("   ⚠️ blocked language detected")
                # First 200 chars stripped
                import re
                stripped = re.sub(r"<[^>]+>", " ", txt[:5000])
                stripped = re.sub(r"\s+", " ", stripped)[:400]
                print(f"   sample: {stripped}")
            except Exception as e:
                print(f"   ERROR: {e}")

asyncio.run(main())
