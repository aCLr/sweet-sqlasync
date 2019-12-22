import os

from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound

from sweet_sqlasync import connection_context, get_engine
from sweet_sqlasync.connections import init_db, close_db
from sweet_sqlasync.utils import _to_dict
from utils import Model, gen_create_sql, gen_drop_sql, Base

import pytest

PG_URL = os.getenv('PG_URL')

pytestmark = pytest.mark.skipif(not PG_URL, reason='pg url not specified')

PARAMS_TO_INSERT = [
    (1, 1, '1', '1'),
    (2, 2, '2', '2'),
    (3, 3, '3', '3'),
    (4, 5, '6', None),
]


@pytest.fixture(autouse=True)
async def setup_db():
    engine = await init_db(PG_URL, maxsize=3)
    create = gen_create_sql(Base.metadata)
    drop = gen_drop_sql(Base.metadata)
    async with engine.acquire() as conn:
        try:
            await conn.execute(drop)
        except Exception:
            pass
        await conn.execute(create)
        await conn.execute(Model.__table__.insert().values(PARAMS_TO_INSERT))
    yield
    async with engine.acquire() as conn:
        await conn.execute(drop)
    await close_db()


@pytest.mark.asyncio
@pytest.mark.parametrize('data', PARAMS_TO_INSERT)
async def test_get(data):
    pkey = (data[0], data[1])
    result = await Model.query.auto_connection().get(pkey)
    assert result.id == data[0]
    assert result.sub_id == data[1]
    assert result.key == data[2]
    assert result.value == data[3]


@pytest.mark.asyncio
async def test_all():
    result = await Model.query.auto_connection().order_by(Model.id.asc()).all()
    assert len(result) == len(PARAMS_TO_INSERT)
    for x, param in enumerate(PARAMS_TO_INSERT):
        assert tuple(_to_dict(result[x]).values()) == param


@pytest.mark.asyncio
async def test_update():
    result = await Model.query.auto_connection().filter(Model.id.in_([1, 2])).update({Model.value: 'updated'})
    assert result == 2
    result = await Model.query.auto_connection().order_by(Model.id.asc()).all()
    for x, model in enumerate(result):
        if model.id in [1,2]:
            assert model.value == 'updated'
        else:
           assert model.value == PARAMS_TO_INSERT[x][3]


@pytest.mark.asyncio
async def test_delete():
    result = await Model.query.auto_connection().filter(Model.id.in_([1, 2])).delete()
    assert result == 2
    result = await Model.query.auto_connection().count()
    assert result == 2


@pytest.mark.asyncio
async def test_delete_by_model():
    async with connection_context() as conn:
        init_model = await Model.query.auto_connection().first()
        async with conn.begin() as transaction:
            await init_model.delete(conn)
            with pytest.raises(NoResultFound):
                await Model.query.auto_connection().get((init_model.id, init_model.sub_id))
            await transaction.rollback()
    model = await Model.query.auto_connection().get((init_model.id, init_model.sub_id))
    assert _to_dict(model) == _to_dict(init_model)


@pytest.mark.asyncio
async def test_update_by_model():
    async with connection_context() as conn:
        init_model = await Model.query.auto_connection().first()
        init_dict = _to_dict(init_model)
        init_model.value = 'updated'
        async with conn.begin() as transaction:
            await init_model.save(conn)
            updated = await Model.query.auto_connection().get((init_model.id, init_model.sub_id))
            assert updated.value == 'updated'
            await transaction.rollback()
    model = await Model.query.auto_connection().get((init_model.id, init_model.sub_id))
    assert _to_dict(model) == init_dict


@pytest.mark.asyncio
async def test_save_new_by_model():
    async with connection_context() as conn:
        max_id = await Model.query.with_entities(func.max(Model.id)).auto_connection().scalar()
        # alter sequence manually because of manual insert id value
        await conn.execute("SELECT setval('models_id_seq', %s, true)", (max_id,))
        m = Model(sub_id=321, key='new_model_key', value='new_model_value')
        await m.save(conn)
    assert m.id == max_id + 1
    assert m.sub_id == 321
    assert m.key == 'new_model_key'
    assert m.value == 'new_model_value'


@pytest.mark.asyncio
async def test_refresh_by_model():
    async with connection_context() as conn:
        init_model = await Model.query.auto_connection().first()
        assert init_model.value != 'updated'
        async with get_engine().acquire() as another_conn:
            await Model.query.filter(Model.id == init_model.id).with_async_conn(another_conn).update({Model.value: 'updated'})
        await init_model.refresh(conn)
        assert init_model.value == 'updated'
