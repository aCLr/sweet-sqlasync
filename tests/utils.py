from psycopg2.extensions import adapt
from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base

from sweet_sqlasync.base_model import BaseModel

Base = declarative_base(cls=BaseModel)


class Model(Base):
    __tablename__ = "models"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sub_id = Column(Integer, primary_key=True)
    key = Column(String)
    value = Column(String, nullable=True)


def compile_statement(query):
    dialect = postgresql.dialect()
    compiled_query = query.compile(dialect=dialect)

    params = {}
    for k, v in compiled_query.params.items():
        if isinstance(v, str):
            params[k] = adapt(v.encode("utf-8"))
        else:
            params[k] = adapt(v)

    return compiled_query.string % params


def mock_engine():
    from sqlalchemy import create_engine as ce
    from io import StringIO
    buf = StringIO()

    def dump(sql, *multiparams, **params):
        buf.write(str(sql.compile(dialect=engine.dialect)))

    engine = ce('postgresql://', echo=True, strategy='mock', executor=dump)
    return buf, engine


def gen_create_sql(metadata):
    buf, engine = mock_engine()
    metadata.create_all(engine)
    return buf.getvalue()



def gen_drop_sql(metadata):
    buf, engine = mock_engine()
    metadata.drop_all(engine)
    return buf.getvalue()
