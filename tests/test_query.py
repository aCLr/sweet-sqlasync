from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.orm.exc import NoResultFound

from sweet_sqlasync import AsyncQuery, execute
from sweet_sqlasync.utils import _to_dict
from utils import Model


@pytest.fixture()
def query(request):
    return AsyncQuery(getattr(request, "param", [Model]))


def test___init__(query):
    assert query._async_conn is None
    assert not query._auto_connection


def test_with_async_conn(query):
    query.with_async_conn("x")
    assert query._async_conn == "x"


def test_auto_connection(query):
    query.auto_connection()
    assert query._auto_connection


@pytest.fixture()
def mock_fetch(request):
    with patch.object(execute, "_fetch", AsyncMock(return_value=request.param)):
        yield request.param


VALUES_TO_FETCH_1 = {"id": 1, "sub_id": 2, "key": "3", "value": ""}
VALUES_TO_FETCH_2 = {"id": 23, "sub_id": 543, "key": "foo", "value": None}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_fetch",
    [
        [VALUES_TO_FETCH_1, VALUES_TO_FETCH_2],
        [VALUES_TO_FETCH_2, VALUES_TO_FETCH_1],
        [],
        [VALUES_TO_FETCH_1],
        [VALUES_TO_FETCH_2],
    ],
    indirect=True,
)
async def test_all(mock_fetch, query):
    result = await query.auto_connection().all()
    assert list(map(_to_dict, result)) == mock_fetch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_fetch", [[VALUES_TO_FETCH_1, VALUES_TO_FETCH_2], []], indirect=True
)
async def test_exists(mock_fetch, query):
    result = await query.auto_connection().exists()
    assert result is bool(mock_fetch)


@pytest.mark.asyncio
@pytest.mark.parametrize("mock_fetch", [[1], [2], [], None], indirect=True)
async def test_scalar(mock_fetch, query):
    result = await query.auto_connection().scalar()
    assert result == mock_fetch


@pytest.mark.asyncio
@pytest.mark.parametrize("mock_fetch", [[1], [2], [],], indirect=True)
async def test_count(mock_fetch, query):
    result = await query.auto_connection().count()
    assert result == mock_fetch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_fetch", [VALUES_TO_FETCH_2, VALUES_TO_FETCH_1, None,], indirect=True
)
async def test_first(mock_fetch, query):
    result = await query.auto_connection().first()
    if mock_fetch is None:
        assert result is None
    else:
        assert _to_dict(result) == mock_fetch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_fetch", [VALUES_TO_FETCH_2, VALUES_TO_FETCH_1, None,], indirect=True
)
async def test_update(mock_fetch, query):
    result = await query.auto_connection().update({"x": 1})
    assert result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_fetch,raises",
    [(VALUES_TO_FETCH_2, False), (VALUES_TO_FETCH_1, False), (None, True)],
    indirect=["mock_fetch"],
)
async def test_get(mock_fetch, query, raises):
    assert False
