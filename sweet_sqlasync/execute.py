from typing import Any

from aiopg.sa import SAConnection  # type: ignore[import]
from aiopg.sa.result import ResultProxy
from sqlalchemy.sql import ClauseElement


async def _fetch(conn: SAConnection, query: ClauseElement, meth: str) -> Any:
    res: ResultProxy = await conn.execute(query)
    async with res.cursor:
        return await getattr(res, meth)()


async def first(conn: SAConnection, query: ClauseElement) -> Any:
    return await _fetch(conn, query, "first")


async def fetchall(conn: SAConnection, query: ClauseElement) -> Any:
    return await _fetch(conn, query, "fetchall")


async def fetchone(conn: SAConnection, query: ClauseElement) -> Any:
    return await _fetch(conn, query, "fetchone")


async def scalar(conn: SAConnection, query: ClauseElement) -> Any:
    return await _fetch(conn, query, "scalar")
