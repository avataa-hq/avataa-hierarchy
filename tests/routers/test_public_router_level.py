"""PUBLIC TESTS for Level router"""

import copy

from httpx import AsyncClient
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy, Level
from schemas.main_base_connector import Base

HIERARCHY_DEFAULT_DATA = {
    "name": "Test hierarchy",
    "description": "Test description",
    "author": "Test author",
}

LEVEL_DEFAULT_DATA = {
    "level": 0,
    "name": "Test name",
    "description": "Test description",
    "object_type_id": 1,
    "is_virtual": True,
    "param_type_id": 2,
    "additional_params_id": 3,
    "latitude_id": 4,
    "longitude_id": 5,
    "author": "Test Author",
}

LEVEL_DATA_TO_UPDATE = {
    "level": 1,
    "name": "Updated name",
    "description": "Updated description",
    "object_type_id": 2,
    "is_virtual": False,
    "param_type_id": 3,
    "additional_params_id": 4,
    "latitude_id": 5,
    "longitude_id": 6,
    "author": "Updated Author",
}


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession):
    h = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(h)
    await session.flush()
    yield h


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(session: AsyncSession):
    yield
    await session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


@pytest_asyncio.fixture(loop_scope="session")
async def h_id(session_fixture: Hierarchy) -> int:
    return session_fixture.id


@pytest_asyncio.fixture(loop_scope="session")
async def url(session_fixture: Hierarchy) -> str:
    return f"/api/hierarchy/v1/hierarchy/{session_fixture.id}/level"


# @pytest_asyncio.fixture(scope="function", name="client")
# async def client_fixture(session: AsyncSession):
#     def get_session_override():
#         return session
#
#     app.dependency_overrides[database.get_session] = get_session_override
#     v1_app.dependency_overrides[database.get_session] = get_session_override
#     async with AsyncClient(app=app, base_url="http://test") as client:
#         yield client
#     app.dependency_overrides.clear()
#     v1_app.dependency_overrides.clear()


@pytest.mark.asyncio(loop_scope="session")
async def test_public_get_fails(private_client: AsyncClient, url: str):
    """TEST Public GET request to the Level endpoint returns 404 error."""

    res = await private_client.get(url)

    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_public_post_fails(
    private_client: AsyncClient,
    url: str,
):
    """TEST Public POST request to the Level endpoint returns 422 error."""

    res = await private_client.post(url, json=HIERARCHY_DEFAULT_DATA)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_public_post_cant_save_data(
    session: AsyncSession,
    private_client: AsyncClient,
    url: str,
):
    """TEST Public POST request to the Level endpoint can`t save data."""
    await private_client.post(url, json=LEVEL_DEFAULT_DATA)

    statement = select(Level)
    res = await session.execute(statement)
    res = len(res.scalars().all())

    assert res == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_public_delete_fails(
    private_client: AsyncClient,
    url: str,
):
    """TEST Public DELETE request to the Level endpoint returns 404 error."""
    del_url = url + "/1"
    res = await private_client.delete(del_url)

    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_public_delete_cant_delete_data(
    session: AsyncSession,
    private_client: AsyncClient,
    h_id: int,
    url: str,
):
    """TEST Public DELETE request to the Level endpoint delete data."""
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    await session.refresh(level)

    del_url = url + "/" + str(level.id)
    await private_client.delete(del_url)

    statement = select(Level).where(Level.id == level.id)
    res = await session.execute(statement)
    res = res.scalars().all()

    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_public_put_fails(
    private_client: AsyncClient,
    url: str,
):
    """TEST Public PUT request to the Level endpoint returns 401 error."""
    put_url = url + "/1"
    res = await private_client.put(put_url, json=LEVEL_DEFAULT_DATA)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_public_put_cant_change_data(
    session: AsyncSession,
    private_client: AsyncClient,
    h_id: int,
    url: str,
):
    """TEST Public PUT request to the Level endpoint can`t change data."""
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    await session.refresh(level)
    level_before = copy.deepcopy(level)

    put_url = url + "/" + str(level.id)
    await private_client.put(put_url, json=LEVEL_DATA_TO_UPDATE)
    await session.refresh(level)
    assert level_before == level
