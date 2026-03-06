"""Shared build/runtime metadata for hub and leads services."""

import os


# Single source of truth so /buildz endpoints cannot drift across modules.
BUILD_MARKER = (
	os.getenv("BUILD_MARKER")
	or os.getenv("RENDER_GIT_COMMIT")
	or os.getenv("RENDER_GIT_BRANCH")
	or "local-dev"
)
