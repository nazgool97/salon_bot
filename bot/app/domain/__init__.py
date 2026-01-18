"""Domain package marker for mypy to prevent duplicate module name inference.
Exports models for convenience.
"""

from . import models  # noqa: F401

__all__ = ["models"]
