"""SQLAlchemy engine factory with connection pooling and retry logic."""

import os
import time
from types import TracebackType
from typing import Self

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

from src.utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0  # seconds; doubles each retry


def _build_url() -> str:
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


class DatabaseConnection:
    """Managed SQLAlchemy engine with pooling, retry, and context manager support.

    Example:
        >>> with DatabaseConnection() as db:
        ...     engine = db.get_engine()
        ...     db.test_connection()
    """

    def __init__(self) -> None:
        self._engine: Engine | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> Self:
        self._engine = self._create_engine_with_retry()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._engine is not None:
            self._engine.dispose()
            logger.debug("Engine disposed")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_engine(self) -> Engine:
        """Return the underlying SQLAlchemy :class:`Engine`.

        Raises:
            RuntimeError: If called outside a ``with`` block.
        """
        if self._engine is None:
            raise RuntimeError(
                "Engine is not initialised. Use DatabaseConnection as a context manager."
            )
        return self._engine

    def test_connection(self) -> bool:
        """Execute a trivial query to verify the connection is alive.

        Returns:
            ``True`` if the connection succeeds, ``False`` otherwise.
        """
        try:
            with self.get_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection verified successfully")
            return True
        except OperationalError as exc:
            logger.error(f"Database connection test failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_engine_with_retry(self) -> Engine:
        """Create the engine, retrying up to *_MAX_RETRIES* times.

        Raises:
            OperationalError: After all retries are exhausted.
        """
        url = _build_url()
        last_exc: Exception | None = None

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                engine = create_engine(
                    url,
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True,
                    pool_timeout=30,
                )
                # Force an actual connection to validate credentials
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

                logger.info(
                    f"Connected to PostgreSQL (attempt {attempt}/{_MAX_RETRIES})"
                )
                return engine

            except OperationalError as exc:
                last_exc = exc
                wait = _BASE_BACKOFF ** attempt
                logger.warning(
                    f"Connection attempt {attempt}/{_MAX_RETRIES} failed — "
                    f"retrying in {wait:.0f}s: {exc}"
                )
                if attempt < _MAX_RETRIES:
                    time.sleep(wait)

        raise OperationalError(
            statement=None,
            params=None,
            orig=last_exc,  # type: ignore[arg-type]
        ) from last_exc


def get_engine() -> Engine:
    """Module-level shortcut that builds an engine without retry or pooling context.

    Suitable for one-shot scripts. For long-running processes prefer
    :class:`DatabaseConnection` as a context manager.
    """
    url = _build_url()
    return create_engine(url, pool_pre_ping=True)
