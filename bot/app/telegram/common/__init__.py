"""Aggregate exports for common telegram interface components.

Original project scattered callback data & helpers; after cleanup we unify them
under `callbacks.py` and reuse safe UI helpers from utils.ui_fail_safe.
"""

from bot.app.telegram.common.ui_fail_safe import *  # noqa: F401,F403
from .callbacks import *  # noqa: F401,F403

__all__: list[str] = []  # populated dynamically by star imports
