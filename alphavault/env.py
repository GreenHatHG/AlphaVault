"""
Env loader for local runs.

Rules:
- Prefer environment variables (Docker / prod already has them).
- If there is a local ".env", load it as a convenience.
- Never override existing environment variables.
"""

from __future__ import annotations


def load_dotenv_if_present() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    try:
        load_dotenv(override=False)
    except Exception:
        return
