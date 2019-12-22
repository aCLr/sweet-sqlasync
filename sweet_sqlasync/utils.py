from __future__ import annotations

import asyncio
from functools import wraps
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Collection, Dict,
                    Generator, Optional, Protocol, Set, Tuple, Type, TypeVar,
                    Union)

from sqlalchemy import Column, Table, and_
from sqlalchemy.orm.base import DEFAULT_STATE_ATTR
from sqlalchemy.orm.instrumentation import instance_dict
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.sql.elements import BooleanClauseList

if TYPE_CHECKING:
    from sweet_sqlasync.query import AsyncQuery


class ModelProtocol(Protocol):
    __mapper__: Mapper
    __table__: Table

    def __init__(self, **kwargs: Any) -> None:
        pass


class ResultProxyProtocol(Protocol):
    rowcount: int

    def close(self) -> None:
        pass


MT = TypeVar("MT", bound=ModelProtocol)


def _instantiate(class_: Type[MT], row: Dict[str, Any]) -> MT:
    return class_(**row)


def _iter_pkey_col_and_val(instance_or_class: Union[MT, Type[MT]]) -> Generator[Tuple[Column, Any], None, None]:
    for column in instance_or_class.__mapper__.primary_key:
        yield column, getattr(instance_or_class, column.key)


def _get_pk_filter_clause(instance: MT) -> BooleanClauseList:
    return and_(
        *[column == value for column, value in _iter_pkey_col_and_val(instance)]
    )


def _to_dict(
    instance: MT, only_fields: Optional[Collection[Union[str, Column]]] = None
) -> Dict[str, Any]:
    converted: Set[str] = set()
    if only_fields:
        converted = {f.key if isinstance(f, Column) else f for f in only_fields}
    return {
        k: v
        for k, v in instance_dict(instance).items()
        if k != DEFAULT_STATE_ATTR and (not only_fields or k in converted)
    }


def _get_key(instance: MT) -> Tuple[Any, ...]:
    return tuple(value for _, value in _iter_pkey_col_and_val(instance))


def _set_key(instance: MT, key: Tuple[Any, ...]) -> None:
    pkey_cols = list(_iter_pkey_col_and_val(instance))
    if len(key) != len(pkey_cols):
        raise TypeError(
            f"incorrect number of primary key values, expected {len(pkey_cols)} got {len(key)}"
        )
    for x, (column, _) in enumerate(pkey_cols):
        setattr(instance, column.key, key[x])


def _get_table(query: AsyncQuery) -> Table:
    if len(query._entities) != 1 or len(query._entities[0].entities) != 1:
        raise ValueError("exactly one model supported")
    return query._entities[0].entities[0].__table__  # type: ignore[attr-defined]


R = TypeVar("R")


class _SyncFacade:
    def __init__(self, awaitable: Callable[..., Awaitable[R]]) -> None:
        self.awaitable = awaitable

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[R]:
        return self.awaitable(*args, **kwargs)

    @property
    def _loop(self):
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def sync(self, *args: Any, **kwargs: Any) -> R:
        return self._loop.run_until_complete(self.awaitable(*args, **kwargs))

    @classmethod
    def decorate(cls, func: Callable[..., Awaitable[R]]):
        @wraps(func)
        def decorator(*args, **kwargs):
            return cls(func(*args, **kwargs))

        return decorator
