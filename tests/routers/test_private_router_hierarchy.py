"""PRIVATE TESTS for Hierarchy router"""

import copy
from datetime import datetime
import http
import json

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
async def test_get_hierarchy_successful(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST GET request to the Hierarchy endpoint is successful."""

    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    statement = select(Hierarchy)
    hierarchy = await session.execute(statement)
    hierarchy = hierarchy.scalars().first()

    res = await private_client.get(URL)

    assert res.status_code == 200
    assert res.json() == [json.loads(hierarchy.json())]


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_successful(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Successful POST request can create Hierarchy"""

    statement = select(Hierarchy)
    count_before = await session.execute(statement)
    count_before = len(count_before.scalars().all())

    result = await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)
    assert result.status_code == 201
    count_after = await session.execute(statement)
    count_after = len(count_after.scalars().all())
    assert count_after == count_before + 1


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_successful_res_code(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Successful POST request returns 201 response."""

    res = await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)
    assert res.status_code == http.HTTPStatus.CREATED


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_successful_res_data(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Successful POST request returns created objects."""

    res = await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)

    statement = select(Hierarchy)
    hierarchy_from_db = await session.execute(statement)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    assert res.json() == json.loads(hierarchy_from_db.json())


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_with_existing_name_raises_error(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Raises error on POST request with existing hierarchy name."""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    res = await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_with_existing_name_doesnt_save_data(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Can`t save several hierarchy instances with the same name"""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    statement = select(Hierarchy)
    hierarchy_before = await session.execute(statement)
    hierarchy_before = hierarchy_before.scalars().all()

    await private_client.post(URL, json=HIERARCHY_DEFAULT_DATA)

    hierarchy_after = await session.execute(statement)
    hierarchy_after = hierarchy_after.scalars().all()

    assert hierarchy_before == hierarchy_after


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_without_author_raises_error(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Raises error on POST request without author."""
    payload = copy.deepcopy(HIERARCHY_DEFAULT_DATA)
    del payload["author"]

    res = await private_client.post(URL, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_without_author_doesnt_save_data(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Can`t save hierarchy instance without author"""
    payload = copy.deepcopy(HIERARCHY_DEFAULT_DATA)
    del payload["author"]

    statement = select(Hierarchy)
    hierarchy_before = await session.execute(statement)
    hierarchy_before = hierarchy_before.scalars().all()

    await private_client.post(URL, json=payload)

    hierarchy_after = await session.execute(statement)
    hierarchy_after = hierarchy_after.scalars().all()

    assert hierarchy_before == hierarchy_after


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_hierarchy_successful(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST DELETE request is successful. Delete hierarchy and returns 204"""

    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    await session.refresh(hierarchy)

    delete_url = URL + "/" + str(hierarchy.id)

    res = await private_client.delete(delete_url)

    assert res.status_code == http.HTTPStatus.NO_CONTENT

    statement = select(Hierarchy).where(Hierarchy.id == hierarchy.id)
    res_from_db = await session.execute(statement)
    res_from_db = len(res_from_db.scalars().all())

    assert res_from_db == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_hierarchy_fails(private_client: AsyncClient):
    """TEST Raises 404 error on Specific DELETE request
    with non-existent Hierarchy instance id in the path variable."""
    delete_url = URL + "/4"

    res = await private_client.delete(delete_url)
    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_put_hierarchy_fails(private_client: AsyncClient):
    """TEST Raises 404 error on Specific PUT request
    with non-existent Hierarchy instance id in the path variable."""

    put_url = URL + "/4"

    res = await private_client.put(put_url, json=HIERARCHY_DEFAULT_DATA)
    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_put_hierarchy_success(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Successful PUT request change data. Returns status code eq to 200"""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()

    await session.refresh(hierarchy)

    put_url = URL + "/" + str(hierarchy.id)

    res = await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)
    assert res.status_code == 200

    await session.refresh(hierarchy)
    assert hierarchy.name == HIERARCHY_DATA_TO_UPDATE["name"]
    assert hierarchy.description == HIERARCHY_DATA_TO_UPDATE["description"]


@pytest.mark.asyncio(loop_scope="session")
async def test_put_hierarchy_raises_error_on_not_uniq_name(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Cannot set the name value that eq to another existing hierarchy name"""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    hierarchy2 = Hierarchy(**HIERARCHY_DATA_TO_UPDATE)
    session.add(hierarchy)
    session.add(hierarchy2)
    await session.commit()

    await session.refresh(hierarchy)

    put_url = URL + "/" + str(hierarchy.id)

    res = await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)
    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_hierarchy_change_modified_attr(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Change hierarchy.modified on successful PUT request.
    hierarchy.modified eq to change timestamp"""
    change_time = datetime.now()
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    assert hierarchy.modified is None

    put_url = URL + "/" + str(hierarchy.id)

    await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)

    await session.refresh(hierarchy)
    assert hierarchy.modified >= change_time


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_hierarchy_change_change_author_attr(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST Change hierarchy.change_author on successful PUT request.
    hierarchy.change_author eq to author in request body"""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    assert hierarchy.change_author is None

    put_url = URL + "/" + str(hierarchy.id)

    await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)

    await session.refresh(hierarchy)
    # Change field to author instead of change_author
    assert hierarchy.author == HIERARCHY_DATA_TO_UPDATE["author"]


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_hierarchy_cant_change_author_attr(
    session: AsyncSession, private_client: AsyncClient
):
    """TEST successful PUT request doesnt change hierarchy.author."""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    assert hierarchy.change_author is None

    put_url = URL + "/" + str(hierarchy.id)

    await private_client.put(put_url, json=HIERARCHY_DATA_TO_UPDATE)

    await session.refresh(hierarchy)
    # Change field to change_author instead of author
    assert hierarchy.change_author != HIERARCHY_DATA_TO_UPDATE["author"]


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_get_list_hierarchies_all_hierarchies_have_the_create_empty_nodes_attr(
    session: AsyncSession, private_client
):
    f"""TEST All hierarchies have the create_empty_nodes attr in the results of GET request into {URL}"""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    res = await private_client.get(URL)
    res = res.json()
    assert len(res) > 0
    for h in res:
        attr = h.get("create_empty_nodes", None)
        assert attr is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_get_detail_hierarchy_url_hierarchy_has_the_create_empty_nodes_attr(
    session: AsyncSession, private_client
):
    f"""TEST Hierarchy has the create_empty_nodes attr in the results of GET request into {URL} + hierarhy.id"""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    detail_url = f"{URL}/{hierarchy.id}"
    res = await private_client.get(detail_url)

    h = res.json()
    attr = h.get("create_empty_nodes", None)
    assert attr is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_post_hierarchy_successfuly_chreate_hierarchy_with_the_create_empty_nodes_attr(
    session: AsyncSession, private_client
):
    """TEST Successful post request can create Hierarchy with not default value of the create_empty_nodes attr"""
    stmt = select(Hierarchy).where(
        Hierarchy.name == HIERARCHY_DEFAULT_DATA["name"]
    )
    h_exists = await session.execute(stmt)
    h_exists = h_exists.scalars().first()
    assert h_exists is None
    payload = dict()
    payload.update(HIERARCHY_DEFAULT_DATA)
    payload["create_empty_nodes"] = False

    await private_client.post(URL, json=payload)

    stmt = select(Hierarchy).where(
        Hierarchy.name == HIERARCHY_DEFAULT_DATA["name"]
    )
    h_exists = await session.execute(stmt)
    h_exists = h_exists.scalars().first()
    assert h_exists is not None
    assert h_exists.create_empty_nodes is False


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_detail_hierarchy_url_can_change_the_create_empty_nodes_attr(
    session: AsyncSession, private_client
):
    f"""TEST Put request into detail hierarchy url {URL} + hierarhy.id, can change the create_empty_nodes attr."""
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)
    print(hierarchy)
    assert hierarchy.create_empty_nodes is True
    payload = HIERARCHY_DEFAULT_DATA
    payload["create_empty_nodes"] = False

    detail_url = f"{URL}/{hierarchy.id}"
    res = await private_client.put(detail_url, json=payload)

    assert res.status_code == 200
    await session.refresh(hierarchy)
    assert hierarchy.create_empty_nodes is False
