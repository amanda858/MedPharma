import asyncio, sys, os
sys.path.insert(0, "/workspaces/CVOPro")
from app.email_verifier import verify_email

async def main():
    tests = [
        "ceo@google.com",
        "support@labcorp.com",
        "billing@questdiagnostics.com",
        "info@mayocliniclabs.com",
        "noexist-zzzzz12345@google.com",
        "test@nonexistent-domain-xyz-123.com",
    ]
    for t in tests:
        r = await verify_email(t, do_smtp=True)
        print(f"{t:50s} verdict={r['verdict']:14s} score={r['score']:3d} mx={r['mx_found']} smtp={r.get('smtp_result')} catchall={r.get('catchall')} reason={r['reason']}")

asyncio.run(main())
