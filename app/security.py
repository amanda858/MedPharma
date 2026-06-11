"""HIPAA-grade encryption helpers for chat message bodies (and anywhere else
we need to store/protect ePHI at rest).

Storage layout:
    Encrypted bodies are stored as a string with the prefix "enc1:" followed
    by a urlsafe-base64 Fernet token. Rows written before this module shipped
    have no prefix and are returned as-is by `decrypt_message`, so existing
    plaintext history keeps working while every NEW write goes encrypted.

Key resolution (first match wins):
    1. CHAT_ENCRYPTION_KEY env var — a Fernet key (urlsafe-base64, 32 bytes).
    2. A persistent key file at $CHAT_KEY_PATH (default: <data dir>/chat.key).
       If missing it is generated on first boot and chmod'd to 0600.
    3. HUB_SECRET env var, HKDF-stretched to a Fernet key (last-resort
       fallback so encryption never silently degrades to plaintext).

This module intentionally degrades to a best-effort passthrough only when the
`cryptography` package is missing AND no key material is available — that
combination would only happen in a misconfigured dev shell and is loud-logged.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import threading
from pathlib import Path

log = logging.getLogger(__name__)

CHAT_BODY_PREFIX = "enc1:"
_PHI_SAFE_PLACEHOLDER = "[encrypted chat message]"

_lock = threading.Lock()
_cipher = None        # cached Fernet instance (or None if unavailable)
_init_done = False
_init_warning_emitted = False


def _resolve_data_dir() -> Path:
    """Return the directory to drop the auto-generated chat key into.

    Honours the same env vars `app.config` uses so the key lives next to the
    sqlite database on Render's persistent disk."""
    candidates = []
    for env_key in ("CHAT_KEY_DIR", "DATA_DIR"):
        v = (os.getenv(env_key) or "").strip()
        if v:
            candidates.append(Path(v))
    db_path = (os.getenv("DB_PATH") or "").strip()
    if db_path:
        candidates.append(Path(db_path).parent)
    candidates.append(Path("/data"))
    candidates.append(Path(__file__).resolve().parent.parent / "data")
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            # smoke-test writability
            probe = p / ".chat_key_probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return p
        except OSError:
            continue
    # Last resort: cwd
    return Path.cwd()


def _derive_from_secret(secret: str) -> bytes:
    """HKDF-style derive a 32-byte Fernet key from an arbitrary secret."""
    h = hashlib.sha256(b"medpharma-chat-v1|" + secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(h)


def _load_or_create_key() -> bytes | None:
    # Explicit env key first
    env_key = (os.getenv("CHAT_ENCRYPTION_KEY") or "").strip()
    if env_key:
        try:
            # Accept either an already-formatted Fernet key or a passphrase
            raw = env_key.encode("utf-8")
            if len(base64.urlsafe_b64decode(raw + b"==")) == 32:
                return raw
        except Exception:
            pass
        return _derive_from_secret(env_key)

    # Persistent on-disk key (auto-created)
    key_path_env = (os.getenv("CHAT_KEY_PATH") or "").strip()
    key_path = Path(key_path_env) if key_path_env else (_resolve_data_dir() / "chat.key")
    try:
        if key_path.exists():
            data = key_path.read_bytes().strip()
            if data:
                return data
        # Generate fresh
        try:
            from cryptography.fernet import Fernet  # type: ignore
            new_key = Fernet.generate_key()
        except Exception:
            new_key = base64.urlsafe_b64encode(os.urandom(32))
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_bytes(new_key)
        try:
            os.chmod(key_path, 0o600)
        except OSError:
            pass
        log.info("chat encryption: created new key at %s", key_path)
        return new_key
    except OSError as e:
        log.warning("chat encryption: cannot persist key at %s (%s)", key_path, e)

    # Fallback: derive from HUB_SECRET so the key is at least stable across boots
    secret = (os.getenv("HUB_SECRET") or os.getenv("SESSION_SECRET") or "").strip()
    if secret:
        return _derive_from_secret(secret)
    return None


def _get_cipher():
    """Lazy-initialise the Fernet cipher. Returns None only if encryption is
    fundamentally unavailable (no cryptography lib AND no key)."""
    global _cipher, _init_done, _init_warning_emitted
    if _init_done:
        return _cipher
    with _lock:
        if _init_done:
            return _cipher
        try:
            from cryptography.fernet import Fernet  # type: ignore
        except Exception as e:
            if not _init_warning_emitted:
                log.error(
                    "cryptography package missing — chat messages will be "
                    "stored in PLAINTEXT until it's installed: %s", e,
                )
                _init_warning_emitted = True
            _cipher = None
            _init_done = True
            return _cipher
        key = _load_or_create_key()
        if not key:
            if not _init_warning_emitted:
                log.error(
                    "no chat encryption key available — set CHAT_ENCRYPTION_KEY "
                    "or HUB_SECRET, or ensure %s is writable",
                    _resolve_data_dir(),
                )
                _init_warning_emitted = True
            _cipher = None
        else:
            try:
                _cipher = Fernet(key)
                log.info("chat encryption ready (Fernet)")
            except Exception as e:
                log.error("chat Fernet init failed: %s", e)
                _cipher = None
        _init_done = True
        return _cipher


def encryption_status() -> dict:
    """Diagnostic helper for /readyz and admin dashboards."""
    cipher = _get_cipher()
    return {
        "encryption": "fernet" if cipher else "plaintext",
        "ready": bool(cipher),
        "key_source": (
            "env:CHAT_ENCRYPTION_KEY" if (os.getenv("CHAT_ENCRYPTION_KEY") or "").strip()
            else ("file" if (Path(os.getenv("CHAT_KEY_PATH") or (_resolve_data_dir() / "chat.key"))).exists()
                  else "derived")
        ),
    }


def encrypt_message(plaintext: str) -> str:
    """Encrypt a chat message body. Returns "enc1:<token>"; on failure (e.g.
    cipher unavailable) returns the plaintext unchanged so we never lose the
    message — the audit log will already have flagged the misconfig."""
    if plaintext is None:
        return ""
    if not isinstance(plaintext, str):
        plaintext = str(plaintext)
    cipher = _get_cipher()
    if not cipher:
        return plaintext
    try:
        token = cipher.encrypt(plaintext.encode("utf-8")).decode("ascii")
        return f"{CHAT_BODY_PREFIX}{token}"
    except Exception as e:
        log.error("chat encrypt failed: %s", e)
        return plaintext


def decrypt_message(stored: str | None) -> str:
    """Reverse of `encrypt_message`. Returns plaintext. Passes through any
    value that is missing the prefix (legacy plaintext rows)."""
    if stored is None:
        return ""
    if not isinstance(stored, str):
        stored = str(stored)
    if not stored.startswith(CHAT_BODY_PREFIX):
        return stored
    cipher = _get_cipher()
    if not cipher:
        # We literally cannot read this row — return a placeholder so the UI
        # doesn't crash and ops can see something's off.
        return _PHI_SAFE_PLACEHOLDER
    try:
        token = stored[len(CHAT_BODY_PREFIX):].encode("ascii")
        return cipher.decrypt(token).decode("utf-8")
    except Exception as e:
        log.error("chat decrypt failed (token len=%d): %s", len(stored), e)
        return _PHI_SAFE_PLACEHOLDER


def phi_safe_preview(plaintext: str, max_len: int = 0) -> str:
    """Return a length-only marker safe to drop in audit logs / activity
    feeds / email notification bodies. `max_len` is accepted for future use
    but currently ignored — we never leak PHI to those sinks."""
    if not plaintext:
        return "[empty chat message]"
    return f"[chat message • {len(plaintext)} chars]"
