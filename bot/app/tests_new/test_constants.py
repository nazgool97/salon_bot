import importlib
import logging

from bot.app.core import constants


def _reload_constants(monkeypatch, **env) -> object:
    """Reload constants with a temporary env state."""
    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    return importlib.reload(constants)


def test_env_helpers_parse_lists_and_bools(monkeypatch):
    module = _reload_constants(
        monkeypatch,
        ADMIN_IDS="1, 2;not-a-number",
        PAGINATION_PAGE_SIZE="11",
        RUN_BOOTSTRAP="yes",
        DEFAULT_CURRENCY="uah",
    )
    assert module.ADMIN_IDS_LIST == [1, 2]
    assert module.DEFAULT_PAGE_SIZE == 11
    assert module.RUN_BOOTSTRAP_ENABLED is True
    assert module.DEFAULT_CURRENCY == "UAH"


def test_env_helpers_fallbacks(monkeypatch):
    module = _reload_constants(monkeypatch, PAGINATION_PAGE_SIZE="oops", ADMIN_IDS=";")
    assert module.DEFAULT_PAGE_SIZE == 5
    assert module.ADMIN_IDS_LIST == []

    module = _reload_constants(monkeypatch, REMINDERS_CHECK_SECONDS="oops")
    assert module.REMINDERS_CHECK_SECONDS == 60
    assert module.REMINDERS_CHECK_SECONDS_INVALID is True

    module = _reload_constants(monkeypatch, REMINDERS_CHECK_SECONDS="15")
    assert module.REMINDERS_CHECK_SECONDS == 15
    assert module.REMINDERS_CHECK_SECONDS_INVALID is False


def test_currency_normalization(monkeypatch):
    module = _reload_constants(monkeypatch)
    assert module._normalize_currency("eur") == "EUR"
    assert module._normalize_currency(" usd ") == "USD"
    assert module._normalize_currency("too-long") is None
    assert module._normalize_currency("") is None


def test_get_logger_returns_debug_level():
    from bot.app.core.logger import get_logger

    logger = get_logger("test-logger")
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.DEBUG
