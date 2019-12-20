import pytest

from sweet_sqlasync import AsyncQuery
from sweet_sqlasync.utils import (_get_key, _get_pk_filter_clause, _get_table,
                                  _instantiate, _set_key, _to_dict)
from utils import Model, compile_statement


@pytest.mark.parametrize(
    "row",
    [
        {"id": 1},
        {"id": 1, "key": "qwe"},
        {"id": 1, "value": None},
        {"id": 1, "value": None, "key": "qwe"},
    ],
)
def test___instantiate(row):
    instance = _instantiate(Model, row)
    for key, value in row.items():
        assert getattr(instance, key) == value


def test__get_pk_filter_clause():
    m = Model(id=1, sub_id=23, key="qwe", value=2)
    assert (
        compile_statement(_get_pk_filter_clause(m))
        == "models.id = 1 AND models.sub_id = 23"
    )
    m.sub_id = 999
    assert (
        compile_statement(_get_pk_filter_clause(m))
        == "models.id = 1 AND models.sub_id = 999"
    )
    m.id = 3
    assert (
        compile_statement(_get_pk_filter_clause(m))
        == "models.id = 3 AND models.sub_id = 999"
    )
    m.id = 5
    m.sub_id = 100
    assert (
        compile_statement(_get_pk_filter_clause(m))
        == "models.id = 5 AND models.sub_id = 100"
    )


def test__to_dict():
    m = Model(id=123, sub_id=543, key="qwe", value=2)
    assert _to_dict(m) == {"id": 123, "sub_id": 543, "key": "qwe", "value": 2}
    m.id = 10
    m.sub_id = 672
    m.key = "bar"
    m.value = None
    assert _to_dict(m) == {"id": 10, "sub_id": 672, "key": "bar", "value": None}


def test__get_key():
    m = Model(id=123, sub_id=543, key="qwe", value=2)
    _get_key(m) == (123, 543)
    m.sub_id = 321
    _get_key(m) == (123, 321)
    m = Model(sub_id=None, id=5, key="qwe", value=2)
    _get_key(m) == (5, None)


def test__set_key():
    m = Model(id=123, sub_id=543, key="qwe", value=2)
    _set_key(m, (10, 12))
    assert m.id == 10
    assert m.sub_id == 12
    with pytest.raises(TypeError, match="incorrect number of primary key values"):
        _set_key(m, (10,))
    with pytest.raises(TypeError, match="incorrect number of primary key values"):
        _set_key(m, (10, 11, 12))


def test___get_table():
    query = AsyncQuery([])
    with pytest.raises(ValueError, match="exactly one model supported"):
        _get_table(query)
    query = AsyncQuery([Model, Model])
    with pytest.raises(ValueError, match="exactly one model supported"):
        _get_table(query)
    query = AsyncQuery([Model])
    assert _get_table(query) == Model.__table__
