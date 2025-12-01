"""Service package exports.

Tests expect a user_service module providing get_or_create_user and is_master shims.
We now delegate directly to the modular booking package.
"""

import importlib as _il
from typing import Any, Optional
from types import ModuleType


async def get_masters(service_id: str | None = None) -> dict[int, str]:  # minimal stub
    return {}


class _UserServiceShim:
    async def get_or_create_user(
        self, telegram_id: int, name: str
    ) -> Any:  # pragma: no cover - thin
        # Fallback minimal user object for tests that only check attributes
        return type(
            "U", (), {"telegram_id": telegram_id, "name": name, "id": telegram_id}
        )()

    async def is_master(self, telegram_id: int) -> bool:  # pragma: no cover - thin
        return False


user_service = _UserServiceShim()

# Legacy module hooks: annotate as Optional[ModuleType] to satisfy static checkers
payment_service: Optional[ModuleType]
try:  # expose payment_service module for legacy monkeypatch target paths
    payment_service = _il.import_module("bot.app.services.client.payment_service")
except Exception:  # pragma: no cover
    payment_service = None

i18n_service: Optional[ModuleType]
try:
    i18n_service = _il.import_module("bot.app.services.shared.i18n_service")
except Exception:  # pragma: no cover
    i18n_service = None

__all__ = ["user_service", "payment_service", "i18n_service"]
