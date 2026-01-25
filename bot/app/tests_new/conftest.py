"""Test configuration to ensure project package import resolution.

Adds the repository root to sys.path so `import bot` works in CI where the
checkout directory may not be on PYTHONPATH by default.
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
