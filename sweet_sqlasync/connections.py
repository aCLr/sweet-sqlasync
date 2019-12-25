from contextlib import asynccontextmanager
from contextvars import ContextVar
from logging import getLogger
from typing import AsyncIterator, Optional

from aiopg.sa import (Engine, SAConnection,  # type: ignore[import]
                      create_engine)
from aiopg.sa.engine import get_dialect
from sqlalchemy.engine.url import make_url

logger = getLogger(__name__)


class EngineInitializationError(Exception):
    pass


__engine: Optional[Engine] = None


def get_engine() -> Engine:
    if __engine is None:
        raise EngineInitializationError("not initialized yet")
    return __engine


async def init_db(
    connection_url: str,
    echo: bool = False,
    minsize: int = 1,
    maxsize: int = 10,
    recycle: int = 60,
    json_serializer=None
) -> Engine:
    global __engine
    if __engine is not None:
        raise EngineInitializationError("connection already initialized")
    if json_serializer is not None:
        dialect = get_dialect(json_serializer=json_serializer)
    else:
        dialect = get_dialect()
    __engine = await create_engine(
        **make_url(connection_url).translate_connect_args(username="user"),
        dialect=dialect,
        echo=echo,
        minsize=minsize,
        maxsize=maxsize,
        pool_recycle=recycle,
    )
    logger.debug("connection opened")
    return __engine


_context_conn: ContextVar[SAConnection] = ContextVar("async_connection")


@asynccontextmanager
async def connection_context() -> AsyncIterator[SAConnection]:
    """
    Acquires connection from pool, releases it on exit from context.

    You can use it with `AsyncQuery.auto_connection()` method call:
    >>> async with connection_context():
    >>>     await Model.query.auto_connection().count()
    >>>     await AnotherModel.query.auto_connection().count()

    Each of queries will use the same connection here.
    Also, you can start transaction with this:
    >>> async with connection_context() as connection:
    >>>     await Model.query.auto_connection().count()
    >>>     async with connection.begin():
    >>>         obj = await AnotherModel.query.auto_connection().first()
    >>>         obj.attr = 1
    >>>         await obj.save(connection)
    """
    conn = _context_conn.get(None)
    if conn is None:
        async with get_engine().acquire() as conn:
            _context_conn.set(conn)
            yield conn
        _context_conn.set(None)
    else:
        yield conn


async def close_db() -> None:
    global __engine
    if __engine is not None:
        __engine.close()
        await __engine.wait_closed()
        logger.debug("database pool closed")
        __engine = None


__all__ = [
    "close_db",
    "connection_context",
    "init_db",
    "EngineInitializationError",
    "get_engine",
]
