import logging
import os
import sys
from typing import AsyncIterator
from unittest.mock import patch

from httpx import ASGITransport, AsyncClient
import pytest
import pytest_asyncio
from sqlalchemy import NullPool
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.postgres import PostgresContainer

from schemas.main_base_connector import Base
from settings import TestsConfig

sys.path.append(os.path.join(sys.path[0], "..", "app"))

# Remove double logs with echo=True
# from sqlalchemy import log as sqlalchemy_log
# sqlalchemy_log._add_default_handler = lambda x: None

test_config = TestsConfig()
fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=fmt)
logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="session")
def db_url():
    if test_config.run_container_postgres_local:
        with PostgresContainer(
            username=test_config.user,
            password=test_config.db_pass,
            dbname="test_db",
            driver="asyncpg",
        ) as postgres:
            yield postgres.get_connection_url()
    else:
        yield test_config.test_database_url


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_engine(db_url) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(
        url=db_url,
        poolclass=NullPool,
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture(scope="session")
def async_session_maker(test_engine: AsyncEngine):
    return async_sessionmaker(test_engine, expire_on_commit=False)


@pytest_asyncio.fixture(loop_scope="session")
async def session(async_session_maker):
    # logger.debug("Session create")
    async with async_session_maker() as s:
        yield s
        # logger.debug("Session rollback")
        await s.rollback()
        # logger.debug("*" * 20)


@pytest_asyncio.fixture(loop_scope="session")
async def mock_grpc_response():
    with patch(
        "services.hierarchy.hierarchy_builder.utils.get_mo_links_values"
    ) as get_mo_links_val:
        yield get_mo_links_val


@pytest_asyncio.fixture(loop_scope="session")
async def mock_grpc_get_mo_ids_by_filters():
    with patch(
        "common_utils.hierarchy_filter.SearchClient.get_mo_ids_by_filters"
    ) as get_mo_ids_by_fil:
        yield get_mo_ids_by_fil


@pytest_asyncio.fixture(loop_scope="session")
async def private_client(
    session: AsyncSession, test_engine: AsyncEngine, mocker
):
    def get_session_override():
        return session

    def get_oauth2_scheme_overwrite():
        return True

    mocker.patch("settings.DATABASE_URL", new=test_engine.url)
    mocker.patch(
        "services.security.security_config.SECURITY_TYPE",
        return_value="DISABLE",
    )
    # from common_utils.security import jwt_bearer
    from main import app, v1_app

    from database import database

    # app.dependency_overrides[jwt_bearer] = get_oauth2_scheme_overwrite
    # v1_app.dependency_overrides[jwt_bearer] = get_oauth2_scheme_overwrite

    app.dependency_overrides[database.get_session] = get_session_override
    v1_app.dependency_overrides[database.get_session] = get_session_override

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
    app.dependency_overrides.clear()
    v1_app.dependency_overrides.clear()
