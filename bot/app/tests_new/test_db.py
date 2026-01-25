from types import SimpleNamespace

from bot.app.core import db


def test_get_engine_uses_env_and_sets_factory(monkeypatch):
    db._reset_engine_for_tests()

    stub_engine = SimpleNamespace(sync_engine="sync")

    def fake_make_engine(url: str):
        assert url == "fake-url"
        return stub_engine

    def fake_async_sessionmaker(engine, expire_on_commit=False):
        assert engine is stub_engine
        return "factory"

    monkeypatch.setenv("DATABASE_URL", "fake-url")
    monkeypatch.setattr(db, "_make_engine", fake_make_engine)
    monkeypatch.setattr(db, "async_sessionmaker", fake_async_sessionmaker)

    engine = db.get_engine()
    assert engine is stub_engine
    assert db.get_session_factory() == "factory"

    db._reset_engine_for_tests()


def test_reset_engine_clears_state():
    db._engine = "e"
    db._session_factory = "sf"
    db._SCHEMA_READY = True
    db._SCHEMA_CHECKING = True

    db._reset_engine_for_tests()

    assert db._engine is None
    assert db._session_factory is None
    assert db._SCHEMA_READY is False
    assert db._SCHEMA_CHECKING is False
