"""Shared build/runtime metadata for hub and leads services."""

import os


# Single source of truth so /buildz endpoints cannot drift across modules.
BUILD_MARKER = os.getenv("BUILD_MARKER", "build-2026-03-05-incident-fix-07")
