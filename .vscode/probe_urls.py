import httpx
urls = [
    ('FB top',     'https://www.facebook.com/search/top/?q=DEREK%20PAGAN'),
    ('FB public',  'https://www.facebook.com/public/Derek-Pagan'),
    ('IG google',  'https://www.google.com/search?q=%22DEREK+PAGAN%22+site%3Ainstagram.com'),
    ('IG explore', 'https://www.instagram.com/explore/tags/derekpagan/'),
    ('FB google',  'https://www.google.com/search?q=%22DEREK+PAGAN%22+site%3Afacebook.com'),
]
with httpx.Client(headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}, follow_redirects=True, timeout=10) as c:
    for label,u in urls:
        try:
            r = c.get(u)
            print(f'{label:12s} HTTP {r.status_code}  {len(r.content):>8,} bytes  -> {r.url}')
        except Exception as e:
            print(f'{label:12s} ERROR {e}')
