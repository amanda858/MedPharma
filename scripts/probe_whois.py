#!/usr/bin/env python3
import sys
try:
    import whois
    print("WHOIS_OK", whois.__version__ if hasattr(whois, "__version__") else "installed")
except ImportError as e:
    print("WHOIS_MISSING", e)
import subprocess
r = subprocess.run(["which", "whois"], capture_output=True, text=True)
print("whois_bin:", r.stdout.strip() or "missing")
