import json
import os
import re
import socket
import subprocess
import tempfile
from typing import Any
from urllib.parse import urlparse


PRIMARY_BASE = os.getenv("LEADS_BASE_URL", "https://medpharmahub.com").rstrip("/")
FALLBACK_BASE = "https://medpharma-hub.onrender.com"


def _host_resolves(url: str) -> bool:
    host = (urlparse(url).hostname or "").strip()
    if not host:
        return False
    try:
        socket.getaddrinfo(host, 443)
        return True
    except Exception:
        return False


BASE = PRIMARY_BASE if _host_resolves(PRIMARY_BASE) else FALLBACK_BASE


def _preflight() -> None:
    custom = "https://medpharmahub.com"
    render = "https://medpharma-hub.onrender.com"
    print("=== PRECHECK ===", flush=True)
    print(f"custom_dns={_host_resolves(custom)}", flush=True)
    print(f"render_dns={_host_resolves(render)}", flush=True)
    print(f"using_base={BASE}", flush=True)
    print(flush=True)


def run_curl(name: str, method: str, url: str, cookie_jar: str, data: str | None = None) -> dict[str, Any]:
    cmd = [
        "curl",
        "-sS",
        "-i",
        "-L",
        "--max-time",
        "25",
        "-X",
        method,
        "-c",
        cookie_jar,
        "-b",
        cookie_jar,
    ]
    if data is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", data]
    cmd += [url]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    output = (proc.stdout or "") + (proc.stderr or "")

    status_matches = re.findall(r"^HTTP/\S+\s+(\d{3})", output, flags=re.M)
    status_code = int(status_matches[-1]) if status_matches else None

    if "\r\n\r\n" in output:
        body = output.split("\r\n\r\n")[-1]
    elif "\n\n" in output:
        body = output.split("\n\n")[-1]
    else:
        body = output

    body = body.strip()

    return {
        "name": name,
        "status_code": status_code,
        "body": body,
    }
def print_result(result: dict[str, Any]) -> None:
    print(f"=== {result['name']} ===", flush=True)
    code_text = result["status_code"] if result["status_code"] is not None else "N/A"
    print(f"HTTP {code_text}", flush=True)
    print(result["body"], flush=True)
    print(flush=True)


def main() -> None:
    _preflight()
    fd, cookie_jar = tempfile.mkstemp(prefix="hub_cookie_")
    os.close(fd)
    try:
        step1 = run_curl(
            "1) POST /hub/api/login",
            "POST",
            f"{BASE}/hub/api/login",
            cookie_jar,
            '{"username":"admin","password":"admin123"}',
        )
        step2 = run_curl(
            "2) POST /hub/api/notifications/test",
            "POST",
            f"{BASE}/hub/api/notifications/test",
            cookie_jar,
        )

        print_result(step1)
        print_result(step2)

        print("=== EXACT STEP 2 RESULT ===", flush=True)
        code_text = step2["status_code"] if step2["status_code"] is not None else "N/A"
        print(f"HTTP {code_text}", flush=True)
        try:
            parsed = json.loads(step2["body"])
            print(json.dumps(parsed, ensure_ascii=False), flush=True)
        except json.JSONDecodeError:
            print(step2["body"], flush=True)
    finally:
        try:
            os.remove(cookie_jar)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()