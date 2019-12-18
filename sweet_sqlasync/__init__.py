import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from logging import getLogger
from typing import (
    AsyncIterator, Any, Dict, TypeVar, Type, Tuple, Callable, Awaitable, Optional, Iterable,
    Protocol,
    List,
    cast,
    Union
)

from aiopg.sa import Engine, SAConnection, create_engine
from psycopg2.extensions import adapt as sqlescape
from sqlalchemy import alias, func, orm, select, and_
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connectable
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapper
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.base import DEFAULT_STATE_ATTR, _generative, instance_dict, instance_state
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import Grouping, BinaryExpression

logger = getLogger(__name__)


class MapperProtocol(Protocol):
    __mapper__: Mapper


class ResultProxyProtocol(Protocol):
    rowcount: int

    def close(self) -> None:
        pass


T = TypeVar('T', bound=MapperProtocol)


def _instantiate(class_: Type[T], row: Dict[str, Any]) -> T:
    pk_cols = class_.__mapper__.primary_key
    identity_class = class_.__mapper__._identity_class  # type: ignore
    identitykey = (
        identity_class,
        tuple([row[column] for column in pk_cols]),
        None,
    )
    instance: T = class_.__mapper__.class_manager.new_instance()
    state = instance_state(instance)
    state.key = identitykey
    state.identity_token = None
    return instance


def _get_pk_filter_clause(instance: T) -> Grouping[bool]:
    clause = []
    for key, col in instance.__mapper__.primary_key.items():
        clause.append(col == getattr(instance, key))
    return and_(*clause).self_group()


def _to_dict(instance: T) -> Dict[str, Any]:
    values = instance_dict(instance)
    values.pop(DEFAULT_STATE_ATTR, None)
    return values


def _key(instance: T) -> Tuple[Any, ...]:
    return instance_state(instance).key


def _set_key(instance: T, *values: Any) -> None:
    for x, key in enumerate(instance.__mapper__.primary_key.keys()):
        setattr(instance, key, values[x])


class _Facade:
    def __init__(self, awaitable: Callable[..., Awaitable[T]]) -> None:
        self.awaitable = awaitable

    def __await__(self):
        return self.awaitable.__await__()

    @property
    def _loop(self):
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def sync(self) -> T:
        return self._loop.run_until_complete(self.awaitable)

    @classmethod
    def decorate(cls, func: Callable[..., Awaitable[T]]):
        @wraps(func)
        def decorator(*args, **kwargs):
            return cls(func(*args, **kwargs))

        return decorator


class _Meta(DeclarativeMeta):
    pass


class BaseModelClass:
    @hybrid_property
    def query(self):
        return AsyncQuery(self)

    def _prepare_saving(
            self, only_fields: Optional[Iterable[str]] = None, force_insert: bool = False
    ) -> Dict[str, Any]:
        """
        Returns primary key field and values, which need to be saved
        """
        values = _to_dict(self)
        if only_fields:
            only_fields = [
                k.key if isinstance(k, InstrumentedAttribute) else k
                for k in only_fields
            ]
            values = {k: v for k, v in values.items() if k in only_fields}

        pk_cols = tuple(self.__mapper__.primary_key.keys())
        if force_insert is False:
            for col in pk_cols:
                values.pop(col, None)
        return values

    @_Facade.decorate
    async def delete(self, connection: SAConnection) -> None:
        cursor = None
        try:
            cursor = await connection.execute(
                self.__table__.delete().where(_get_pk_filter_clause(self))
            )
        finally:
            if cursor is not None:
                cursor.close()

    @_Facade.decorate
    async def refresh(self, connection: SAConnection) -> None:
        res = dict(
            await first(
                connection, self.__table__.select().where(_get_pk_filter_clause(self))
            )
        )
        for key, value in res.items():
            setattr(self, key, value)

    @_Facade.decorate
    async def save(
            self, connection: SAConnection, only_fields: Optional[Iterable[str]] = None,
            force_insert: bool = False
    ) -> bool:
        values = self._prepare_saving(
            only_fields=only_fields,
            force_insert=force_insert,
        )

        async def _execute(query, values):
            cursor = await connection.execute(
                query.values(**values).returning(*self.__mapper__.primary_key.columns)
            )
            try:
                if cursor.returns_rows:
                    _id = await cursor.first()
                    if _id:
                        self._set_key(*_id)
                        return True
                else:
                    return cursor.rowcount > 0
            finally:
                cursor.close()

        if not self._pk or (force_insert is True):
            # INSERT flow
            return await _execute(self.__table__.insert(), values)

        else:
            cursor = await connection.execute(
                self.__table__.update().where(_get_pk_filter_clause(self)).values(**values)
            )
            try:
                if cursor.rowcount:
                    return True
            finally:
                cursor.close()
        return False

    @_Facade.decorate
    async def upsert(self, connection, constraint_column=None):
        if constraint_column not in [column.name for column in self.__table__.c]:
            raise Exception(f"Invalid constraint_column {constraint_column}")

        values = self._prepare_saving()

        on_update_fields = {}
        for column in list(self.__table__.c):
            if column.onupdate and not values.get(column.name):
                on_update_fields[column.name] = column.onupdate.arg

        q = postgresql.insert(self.__table__).values(**values)

        values.update(on_update_fields)
        q = q.on_conflict_do_update(index_elements=[constraint_column], set_=values)

        cursor = await connection.execute(q.returning(*self.__mapper__.primary_key.columns))
        try:
            if cursor.returns_rows:
                _id = await cursor.first()
                if _id:
                    self._set_key(*_id)
                    return True
            else:
                return cursor.rowcount > 0
        finally:
            cursor.close()


R = TypeVar('R')


def _check_conn(meth: Callable[['AsyncQuery'], Awaitable[R]]) -> Callable[['AsyncQuery'], Awaitable[R]]:
    @wraps(meth)
    async def wrapper(self: 'AsyncQuery', *args: Any, **kwargs: Any) -> R:

        if (
                self._async_conn is None
                and self._sync_conn is None
                and not self._auto_connection
        ):
            raise ValueError("connection for query not specified")
        elif self._auto_connection:

            @asynccontextmanager
            async def context():
                async with connection_context() as conn:
                    self._async_conn = conn
                    yield
                self._async_conn = None

        else:

            @asynccontextmanager
            async def context():  # dummy context
                yield

        async with context():
            return await meth(self, *args, **kwargs)

    return wrapper


class AsyncQuery(orm.Query):
    def __init__(self, entities: Any, session: Optional[Any] = None) -> None:
        self._sync_conn = self._async_conn = None
        self._auto_connection = False
        self._entities = []
        super().__init__(entities, session)

    @_generative()
    def with_async_conn(self, conn: SAConnection) -> None:
        self._async_conn = conn
        self._sync_conn = None

    @_generative()
    def with_sync_conn(self, conn: Connectable) -> None:
        self._sync_conn = conn
        self._async_conn = None

    @_generative()
    def auto_connection(self) -> None:
        """
        Allows not to specify connection manually.
        If uses in `app.models.bases.connection_context`, connection from that context will be used
        (see its documentation for details)
        """
        self._auto_connection = True

    @_check_conn
    async def all(self) -> List[Any]:
        if self._async_conn is not None:
            raw_result = await fetchall(self._async_conn, self.statement)
        elif self._sync_conn is not None:
            raw_result = self._sync_conn.execute(self.statement)
        cls_ = self._entity_zero().class_._instantiate
        return [cls_(row) for row in raw_result]

    @_check_conn
    async def exists(self) -> bool:
        return bool(await self.scalar())

    @_check_conn
    async def scalar(self) -> Any:
        if self._async_conn is not None:
            return await scalar(self._async_conn, self.statement)
        elif self._sync_conn is not None:
            return self._sync_conn.scalar(self.statement)

    @_check_conn
    async def count(self) -> int:
        query = select([func.count("*")]).select_from(alias(self.statement))
        if self._async_conn is not None:
            return await scalar(self._async_conn, query)
        elif self._sync_conn is not None:
            return self._sync_conn.scalar(query)

    @_check_conn
    async def first(self) -> Any:
        if self._async_conn is not None:
            raw_result = await first(self._async_conn, self.statement)
        elif self._sync_conn is not None:
            raw_result = list(self._sync_conn.execute(self.statement.limit(1)))[0]
        if raw_result is not None:
            return self._entity_zero().class_._instantiate(raw_result)

    @_check_conn
    async def update(self, values: Dict[str, Any]) -> int:
        if len(self._entities) != 1 and len(self._entities[0].entities) != 1:
            raise ValueError("only one model supported")
        table = self._entities[0].entities[0].__table__
        query = table.update().where(self.statement._whereclause).values(values)
        if self._async_conn:
            res = await self._async_conn.execute(query)
            res.close()
        elif self._sync_conn:
            res = self._sync_conn.execute(query)
        return res.rowcount

    @_check_conn
    async def get(self, obj_id: Any) -> Any:
        obj_class = self._entity_zero().class_._instantiate
        obj = await self.filter(obj_class.id == obj_id).first()  # type: ignore[no-untyped-call]
        if obj is None:
            raise NoResultFound(f"object {obj_class} with id={obj_id} not found")
        return obj

    @_check_conn
    async def one(self) -> Any:
        result = await self.limit(2).all()  # type: ignore[no-untyped-call]
        if not result:
            raise NoResultFound("No row was found for one()")
        if len(result) == 2:
            raise MultipleResultsFound("Multiple rows were found for one()")

        return result

    @_check_conn
    async def delete(self) -> int:  # type: ignore[override]
        if len(self._entities) != 1 and len(self._entities[0].entities) != 1:
            raise ValueError("only one model supported")
        table = self._entities[0].entities[0].__table__
        query = table.delete().where(self.statement._whereclause)
        if self._async_conn:
            res: ResultProxyProtocol = await self._async_conn.execute(query)
            res.close()
        elif self._sync_conn:
            res = self._sync_conn.execute(query)
        else:
            raise ValueError('impossible behavior')
        return res.rowcount


def compile_query(query: ClauseElement) -> str:
    dialect = postgresql.dialect()
    compiled_query = query.compile(dialect=dialect)

    params = {}
    for k, v in compiled_query.params.items():
        if isinstance(v, str):
            params[k] = sqlescape(v.encode("utf-8"))
        else:
            params[k] = sqlescape(v)

    return cast(str, compiled_query.string % params)


async def _fetch(conn: SAConnection, query: ClauseElement, meth: str) -> Any:
    if isinstance(query, AsyncQuery):
        query = query.statement
    res = await conn.execute(query)
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


__db_pool: Engine = None


class PoolAlreadyInitialized(Exception):
    pass


async def init_db(connection_url: str, echo: bool = False, minsize: int = 1, maxsize: int = 10,
                  recycle: int = 60) -> SAConnection:
    global __db_pool
    if __db_pool is not None:
        raise PoolAlreadyInitialized("database already initialized")

    __db_pool = await create_engine(
        **make_url(connection_url).translate_connect_args(username="user"),
        echo=echo,
        minsize=minsize,
        maxsize=maxsize,
        pool_recycle=recycle,
    )
    logger.info("database pool opened")
    return __db_pool


context_conn: ContextVar[SAConnection] = ContextVar("async_connection")


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
    conn = context_conn.get(None)
    if conn is None:
        async with __db_pool.acquire() as conn:
            context_conn.set(conn)
            yield conn
        context_conn.set(None)
    else:
        yield conn


async def close_db() -> None:
    global __db_pool
    if __db_pool is not None:
        __db_pool.close()
        await __db_pool.wait_closed()
        logger.info("database pool closed")
        __db_pool = None
