"""Application package.

This module provides a small, explicit package initializer for the
`bot.app` package.  Compatibility shims that dynamically injected
aliases into `sys.modules` have been removed to keep module layout
canonical after the refactor.
"""

from .core import db
from .domain import models

__all__ = ["db", "models"]
