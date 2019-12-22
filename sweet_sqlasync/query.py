from __future__ import annotations

from contextlib import asynccontextmanager
from functools import wraps
from typing import (
    Any, Awaitable, Callable, Collection, Coroutine, Dict,
    List, Optional, Type, TypeVar, AsyncContextManager
)

from aiopg.sa import SAConnection  # type: ignore[import]
from aiopg.sa.result import ResultProxy
from mypy_extensions import Arg
from sqlalchemy import alias, and_, func, orm, select
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from sweet_sqlasync.connections import connection_context

from .execute import fetchall, first, scalar
from .utils import MT, _get_table, _instantiate, _iter_pkey_col_and_val

R = TypeVar("R")


def _check_conn(query) -> Callable[[], AsyncContextManager[None]]:
    if query._async_conn is None and not query._auto_connection:
        raise ValueError("connection for query not specified")
    elif query._auto_connection:

        @asynccontextmanager
        async def context():
            async with connection_context() as conn:
                query._async_conn = conn
                yield
            query._async_conn = None

    else:

        @asynccontextmanager
        async def context():  # dummy context
            yield
    return context

def _check_conn_variadic(
    meth: Callable[[AsyncQuery, Any], Awaitable[R]]
) -> Callable[[AsyncQuery, Any], Awaitable[R]]:
    @wraps(meth)
    async def wrapper(self: AsyncQuery, *args: Any) -> R:

        context = _check_conn(self)

        async with context():
            return await meth(self, *args)

    return wrapper


def _check_conn_no_args(
    meth: Callable[[Arg(AsyncQuery, "self")], Coroutine[Any, Any, R]]
) -> Callable[[Arg(AsyncQuery, "self")], Coroutine[Any, Any, R]]:
    @wraps(meth)
    async def wrapper(self: AsyncQuery) -> R:
        context = _check_conn(self)
        async with context():
            return await meth(self)

    return wrapper


class AsyncQuery(orm.Query):
    _entities: List[Any]
    _entity_zero: Type[Any]

    def __init__(self, entities: MT) -> None:
        self._async_conn: SAConnection = None
        self._auto_connection = False
        super().__init__(entities)

    def with_async_conn(self, conn: SAConnection) -> AsyncQuery:
        self._async_conn = conn
        return self

    def auto_connection(self) -> AsyncQuery:
        """
        Allows not to specify connection manually.
        If uses in `app.models.bases.connection_context`, connection from that context will be used
        (see its documentation for details)
        """
        self._auto_connection = True
        return self

    @_check_conn_no_args
    async def all(self) -> List[MT]:
        raw_result = await fetchall(self._async_conn, self.statement)
        cls_ = self._entity_zero().class_
        return [_instantiate(cls_, row) for row in raw_result]

    @_check_conn_no_args
    async def exists(self) -> bool:
        res = await self.scalar()
        return bool(res)

    @_check_conn_no_args
    async def scalar(self) -> Any:  # type: ignore[override]
        return await scalar(self._async_conn, self.statement)

    @_check_conn_no_args
    async def count(self) -> int:  # type: ignore[override]
        query = select([func.count("*")]).select_from(alias(self.statement))
        return await scalar(self._async_conn, query)

    @_check_conn_no_args  # type: ignore[arg-type]
    async def first(self) -> Optional[MT]:  # type: ignore[override]
        raw_result = await first(self._async_conn, self.statement)
        if raw_result is not None:
            return _instantiate(self._entity_zero().class_, raw_result)
        return None

    @_check_conn_variadic
    async def update(self, values: Dict[str, Any]) -> int:  # type: ignore[override]
        table = _get_table(self)
        query = table.update().where(self.statement._whereclause).values(values)
        res = await self._async_conn.execute(query)
        res.close()
        return res.rowcount

    @_check_conn_variadic
    async def get(self, obj_id: Any) -> MT:  # type: ignore[override]
        obj_class = self._entity_zero().class_
        pkeys = [col for col, _ in _iter_pkey_col_and_val(obj_class)]
        one_key = len(pkeys) == 1
        one_parameter = not isinstance(obj_id, Collection) or len(obj_id) == 1
        if one_key and not one_parameter:
            raise ValueError("expected exactly one value")
        elif one_key and one_parameter:
            filter_ = pkeys[0] == obj_id
        elif len(obj_id) != len(pkeys):
            raise ValueError(f"expected exactly {len(pkeys)} values")
        elif len(obj_id) == len(pkeys):

            filter_ = and_(*[col == obj_id[x] for x, col in enumerate(pkeys)])
        else:
            raise Exception("unexpected conditions")
        obj = await self.filter(filter_).limit(2).all()  # type: ignore[no-untyped-call]
        if not obj:
            raise NoResultFound(f"object {obj_class} with id={obj_id} not found")
        elif len(obj) >= 2:
            raise MultipleResultsFound("Multiple rows were found for get()")
        else:
            return obj[0]

    async def yield_per(self, count):
        context = _check_conn(self)
        async with context():
            res: ResultProxy = await self._async_conn.execute(self.statement)
            async with res.cursor:
                fetched = 0
                while fetched < res.rowcount:
                    rows = await res.fetchmany(count)
                    cls_ = self._entity_zero().class_
                    fetched += len(rows)
                    for row in rows:
                        yield _instantiate(cls_, row)

    @_check_conn_no_args
    async def one(self) -> MT:  # type: ignore[override]
        result = await self.limit(2).all()  # type: ignore[no-untyped-call]
        if not result:
            raise NoResultFound("No row was found for one()")
        if len(result) == 2:
            raise MultipleResultsFound("Multiple rows were found for one()")

        return result

    @_check_conn_no_args
    async def delete(self) -> int:  # type: ignore[override]
        table = _get_table(self)
        query = table.delete().where(self.statement._whereclause)
        res = await self._async_conn.execute(query)
        res.close()
        return res.rowcount
