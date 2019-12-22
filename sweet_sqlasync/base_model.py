import asyncio
from functools import wraps
from typing import (
    Any, Awaitable, Callable, Collection, Dict, Optional,
    TypeVar, Union, Type
)

from aiopg.sa import SAConnection  # type: ignore[import]
from sqlalchemy import Column, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapper
from sqlalchemy.sql.dml import ValuesBase

from sweet_sqlasync.execute import first
from sweet_sqlasync.query import AsyncQuery
from sweet_sqlasync.utils import (MT, _get_key, _get_pk_filter_clause,
                                  _iter_pkey_col_and_val, _set_key, _to_dict)


class class_property:
    def __init__(self, prop):
        self._prop = prop

    def __get__(self, instance: Any, owner: Type[Any]):
        return self._prop(owner)


def _prepare_saving(
    instance: MT,
    only_fields: Optional[Collection[Union['Column[Any]', str]]] = None,
    force_insert: bool = False,
) -> Dict[str, Any]:
    """
    Returns primary key field and values, which need to be saved
    """
    values = _to_dict(instance, only_fields=only_fields)

    if not force_insert is False:
        for column, _ in _iter_pkey_col_and_val(instance):
            values.pop(column.key, None)
    return values


class BaseModel:
    __mapper__: Mapper
    __table__: Table

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # type: ignore[call-arg]

    @class_property
    def query(self) -> AsyncQuery:
        return AsyncQuery(self)

    async def delete(self, connection: SAConnection) -> None:
        cursor = None
        try:
            cursor = await connection.execute(
                self.__table__.delete().where(_get_pk_filter_clause(self))
            )
        finally:
            if cursor is not None:
                cursor.close()

    async def refresh(self, connection: SAConnection) -> None:
        res = dict(
            await first(
                connection, self.__table__.select().where(_get_pk_filter_clause(self))
            )
        )
        for key, value in res.items():
            setattr(self, key, value)

    async def save(
        self,
        connection: SAConnection,
        only_fields: Optional[Collection[Union['Column[Any]', str]]] = None,
        force_insert: bool = False,
    ) -> bool:
        values = _prepare_saving(
            self, only_fields=only_fields, force_insert=force_insert,
        )

        async def _execute(query: ValuesBase, values: Dict[str, Any]) -> bool:
            cursor = await connection.execute(
                query.values(**values).returning(*[c for c, _ in _iter_pkey_col_and_val(self)])
            )
            try:
                if cursor.returns_rows:
                    _id = await cursor.first()
                    _set_key(self, _id)
                    return True
                else:
                    return bool(cursor.rowcount > 0)
            finally:
                cursor.close()
        query: ValuesBase
        if not all(_get_key(self)) or (force_insert is True):
            query = self.__table__.insert()
        else:
            query = self.__table__.update().where(_get_pk_filter_clause(self))
        return await _execute(query, values)

    async def upsert(self, connection: SAConnection, constraint_column=None) -> bool:
        if constraint_column not in [column.name for column in self.__table__.c]:
            raise Exception(f"Invalid constraint_column {constraint_column}")

        values = _prepare_saving(self)

        on_update_fields = {}
        for column in list(self.__table__.c):
            if column.onupdate and not values.get(column.name):
                on_update_fields[column.name] = column.onupdate.arg

        q = postgresql.insert(self.__table__).values(**values)

        values.update(on_update_fields)
        q = q.on_conflict_do_update(index_elements=[constraint_column], set_=values)

        cursor = await connection.execute(
            q.returning(*self.__mapper__.primary_key.columns)
        )
        try:
            if cursor.returns_rows:
                _id = await cursor.first()
                _set_key(self, _id)
                return True
            else:
                return bool(cursor.rowcount > 0)
        finally:
            cursor.close()


BaseModelMixin = BaseModel
