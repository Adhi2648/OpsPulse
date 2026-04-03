from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy.orm import Session

from opspulse.db.engine import get_db_session


def get_db() -> Iterator[Session]:
    """FastAPI dependency that yields a SQLAlchemy session."""
    yield from get_db_session()
