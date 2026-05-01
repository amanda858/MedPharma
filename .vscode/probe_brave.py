import urllib.request, urllib.parse, re
UA='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
for q in ['Derek Pagan 1LAB DIAGNOSTICS site:linkedin.com/in', 'Wilson Molina 24-7 LABORATORIES site:linkedin.com/in']:
    url = 'https://search.brave.com/search?q=' + urllib.parse.quote(q)
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': UA}), timeout=10)
        html = r.read().decode('utf-8','replace')
        m = re.findall(r'https?://[a-z]{2,3}\.linkedin\.com/in/[A-Za-z0-9_%-]+', html)
        print(q[:40], '->', r.status, 'matches=', len(m), 'size=', len(html))
    except Exception as e:
        print(q[:40], 'ERR', e)
