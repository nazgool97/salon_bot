from bot.app.domain import models


def test_normalize_booking_status_variants():
    assert models.normalize_booking_status("CONFIRMED") is models.BookingStatus.CONFIRMED
    assert models.normalize_booking_status("no-show") is models.BookingStatus.NO_SHOW
    assert models.normalize_booking_status(models.BookingStatus.PAID) is models.BookingStatus.PAID
    assert models.normalize_booking_status("unknown") is None


def test_status_collections():
    assert models.BookingStatus.CANCELLED in models.TERMINAL_STATUSES
    assert models.BookingStatus.CONFIRMED not in models.TERMINAL_STATUSES
    assert models.BookingStatus.RESERVED in models.ACTIVE_STATUSES
    assert models.BookingStatus.CONFIRMED in models.REVENUE_STATUSES
