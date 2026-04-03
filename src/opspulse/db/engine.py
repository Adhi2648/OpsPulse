from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from opspulse.core.settings import Settings, get_settings


@lru_cache(maxsize=8)
def _engine_from_url(database_url: str) -> Engine:
    return create_engine(
        database_url,
        future=True,
        pool_pre_ping=True,
    )


def get_engine(settings: Settings | None = None) -> Engine:
    """Create a SQLAlchemy engine for the configured PostgreSQL warehouse."""
    resolved_settings = settings or get_settings()
    return _engine_from_url(resolved_settings.database_url)


def get_session_factory(settings: Settings | None = None) -> sessionmaker[Session]:
    """Return a sessionmaker bound to the configured warehouse engine."""
    return sessionmaker(bind=get_engine(settings), autoflush=False, autocommit=False, future=True)


@contextmanager
def session_scope(settings: Settings | None = None) -> Iterator[Session]:
    """Provide a transaction-scoped SQLAlchemy session."""
    session = get_session_factory(settings)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db_session(settings: Settings | None = None) -> Iterator[Session]:
    """Yield a request-scoped session for FastAPI dependencies."""
    session = get_session_factory(settings)()
    try:
        yield session
    finally:
        session.close()


def check_database_health(settings: Settings | None = None) -> bool:
    """Return True when the configured database is reachable."""
    engine = get_engine(settings)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return True


def clear_engine_cache() -> None:
    """Clear cached SQLAlchemy engines, mainly for tests."""
    _engine_from_url.cache_clear()
