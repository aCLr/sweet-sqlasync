from unittest.mock import Mock, patch

import pytest

import sweet_sqlasync.connections as for_mock

assert for_mock


@pytest.fixture(autouse=True)
def setup_db():
    class ResultProxyMock:
        def close(self):
            return

        @property
        def rowcount(self):
            return 1

    class ConnMock:
        async def execute(self, query):
            return ResultProxyMock()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return

        def __await__(self):
            return self

    class EngineMock(Mock):
        def acquire(self):
            return ConnMock()

    with patch("sweet_sqlasync.connections.get_engine", return_value=EngineMock()):
        yield
