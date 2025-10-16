"""PUBLIC TESTS for Hierarchy router"""

import copy

from httpx import AsyncClient
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy
from schemas.main_base_connector import Base

URL = "/api/hierarchy/v1/hierarchy"
REFRESH_URL = "/api/hierarchy/v1/refresh"


HIERARCHY_DEFAULT_DATA = {
    "name": "Test hierarchy",
    "description": "Test description",
    "author": "Test author",
    "create_empty_nodes": True,
}

HIERARCHY_DATA_TO_UPDATE = {
    "name": "Updated name",
    "description": "Updated description",
    "author": "Updated author",
    "create_empty_nodes": True,
}


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(session: AsyncSession):
    yield
    await session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_public_get_fails(private_client: AsyncClient):
    """TEST Public GET request to the Hierarchy endpoint returns 404 error."""

    res = await private_client.get(URL)
    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_public_post_fails(private_client: AsyncClient):
    """TEST Public POST request to the Hierarchy endpoint returns 201."""

    res = await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)

    assert res.status_code == 201


@pytest.mark.asyncio(loop_scope="session")
async def test_public_post_cant_save_data(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Public POST request to the Hierarchy endpoint save data."""

    await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)

    statement = select(Hierarchy)
    res = await session.execute(statement)
    res = len(res.scalars().all())

    assert res == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_public_delete_fails(private_client: AsyncClient):
    """TEST Public DELETE request to the Hierarchy endpoint returns 404 error."""
    del_url = URL + "/1"
    res = await private_client.delete(del_url)

    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_public_delete_cant_delete_data(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Public DELETE request to the Hierarchy endpoint delete data."""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    await session.refresh(hierarchy)

    del_url = URL + "/" + str(hierarchy.id)
    await private_client.delete(del_url)

    statement = select(Hierarchy).where(Hierarchy.id == hierarchy.id)
    res = await session.execute(statement)
    res = res.scalars().all()

    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_public_put_fails(private_client: AsyncClient):
    """TEST Public PUT request to the Hierarchy endpoint returns 404 error."""
    put_url = URL + "/1"
    res = await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)

    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_public_put_cant_change_data(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Public PUT request to the Hierarchy endpoint change data."""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    await session.refresh(hierarchy)
    hierarchy_before = copy.deepcopy(hierarchy)

    put_url = URL + "/" + str(hierarchy.id)
    await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)
    await session.refresh(hierarchy)
    assert hierarchy_before != hierarchy
