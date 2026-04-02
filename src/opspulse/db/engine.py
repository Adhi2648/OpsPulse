from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from opspulse.core.settings import Settings, get_settings


def get_engine(settings: Settings | None = None) -> Engine:
    """Create a SQLAlchemy engine for the configured PostgreSQL warehouse."""
    resolved_settings = settings or get_settings()
    return create_engine(
        resolved_settings.database_url,
        future=True,
        pool_pre_ping=True,
    )


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
