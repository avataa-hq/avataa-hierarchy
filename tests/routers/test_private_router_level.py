"""PRIVATE TESTS for Level router"""

import copy
from datetime import datetime
import http
import json

from fastapi import HTTPException
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
    "key_attrs": ["2"],
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
    "key_attrs": ["3"],
    "parent_id": None,
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


@pytest_asyncio.fixture(loop_scope="session")
async def level_default_data(session_fixture: Hierarchy) -> dict:
    return {
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
        "hierarchy_id": session_fixture.id,
        "key_attrs": ["2"],
        "parent_id": None,
    }


@pytest.mark.asyncio(loop_scope="session")
async def test_get_level_successful(
    session: AsyncSession,
    private_client: AsyncClient,
    url: str,
    h_id: int,
):
    """TEST GET request to the Level endpoint is successful."""

    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)

    session.add(level)
    await session.commit()

    statement = select(Level)
    levels = await session.execute(statement)
    levels = levels.scalars().first()

    res = await private_client.get(url)

    assert res.status_code == 200
    assert res.json() == [json.loads(levels.json())]


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_successful(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
):
    """TEST Successful POST request can save Level instance."""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    statement = select(Level)
    count_before = await session.execute(statement)
    count_before = len(count_before.scalars().all())

    result = await private_client.post(url, json=level_default_data)
    assert result.status_code == 201
    count_after = await session.execute(statement)
    count_after = len(count_after.scalars().all())
    assert count_after == count_before + 1


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_successful_res_code(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
):
    """TEST Successful POST request returns 201 response."""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids", return_value=True
    )

    res = await private_client.post(url, json=level_default_data)
    assert res.status_code == http.HTTPStatus.CREATED


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_successful_res_data(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
):
    """TEST Successful POST request returns created objects."""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )

    res = await private_client.post(url, json=level_default_data)

    statement = select(Level)
    level_from_db = await session.execute(statement)
    level_from_db = level_from_db.scalars().first()

    assert res.json() == json.loads(level_from_db.json())


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_with_existing_name_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    h_id: int,
    url: str,
):
    """TEST Raises error on POST request with existing level name with the same hierarchy_id."""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )

    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    payload = copy.deepcopy(LEVEL_DATA_TO_UPDATE)
    payload["name"] = LEVEL_DEFAULT_DATA["name"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_with_existing_name_doesnt_save_data(
    session: AsyncSession,
    private_client: AsyncClient,
    h_id: int,
    url: str,
):
    """TEST Can`t save several level instances with the same name and with the same hierarchy_id"""

    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    statement = select(Level)
    level_before = await session.execute(statement)
    level_before = level_before.scalars().all()

    payload = copy.deepcopy(LEVEL_DATA_TO_UPDATE)
    payload["name"] = LEVEL_DEFAULT_DATA["name"]

    await private_client.post(url, json=payload)

    level_after = await session.execute(statement)
    level_after = level_after.scalars().all()

    assert level_before == level_after


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_without_level_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request without Level.level ."""

    payload = copy.deepcopy(level_default_data)
    del payload["level"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_without_name_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request without Level.name ."""

    payload = copy.deepcopy(level_default_data)
    del payload["name"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_without_object_type_id_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request without Level.object_type_id ."""

    payload = copy.deepcopy(level_default_data)
    del payload["object_type_id"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_without_is_virtual_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request without Level.is_virtual ."""

    payload = copy.deepcopy(level_default_data)
    del payload["is_virtual"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_without_key_attrs_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request without Level.key_attrs ."""

    payload = copy.deepcopy(level_default_data)
    del payload["key_attrs"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_without_author_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request without Level.author ."""

    payload = copy.deepcopy(level_default_data)
    del payload["author"]

    res = await private_client.post(url, json=payload)

    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_fails_if_some_params_doesnt_exists_in_inventory(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
):
    """TEST If check_hierarchy_level_inv_params_exist_by_grpc throws an error, no data is saved."""

    def raise_error(*args, **kwargs):
        raise HTTPException(
            status_code=http.HTTPStatus.UNPROCESSABLE_ENTITY, detail="Error"
        )

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        new=raise_error,
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    statement = select(Level)
    levels_before = await session.execute(statement)
    levels_before = levels_before.scalars().all()

    res = await private_client.post(url, json=level_default_data)

    levels_after = await session.execute(statement)
    levels_after = levels_after.scalars().all()

    assert res.status_code == 422
    assert levels_before == levels_after


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_level_successful(
    session: AsyncSession,
    private_client: AsyncClient,
    h_id: int,
    url: str,
):
    """TEST DELETE request is successful. Delete level and returns 204"""

    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    await session.refresh(level)

    statement = select(Level).where(Level.id == level.id)
    res_from_db_before = await session.execute(statement)
    res_from_db_before = len(res_from_db_before.scalars().all())

    assert res_from_db_before == 1

    delete_url = url + "/" + str(level.id)

    res = await private_client.delete(delete_url)

    assert res.status_code == http.HTTPStatus.NO_CONTENT

    statement = select(Level).where(Level.id == level.id)
    res_from_db = await session.execute(statement)
    res_from_db = len(res_from_db.scalars().all())

    assert res_from_db == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_level_fails(
    private_client: AsyncClient,
    url: str,
):
    """TEST Raises 404 error on Specific DELETE request
    with non-existent Level instance id in the path variable."""

    delete_url = url + "/4"

    res = await private_client.delete(delete_url)
    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_put_hierarchy_fails(
    private_client: AsyncClient,
    level_default_data: dict,
    url: str,
):
    """TEST Raises 404 error on Specific PUT request
    with non-existent Level instance id in the path variable."""

    put_url = url + "/4"

    res = await private_client.put(put_url, json=level_default_data)
    assert res.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
async def test_put_level_success(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    h_id: int,
    url: str,
):
    """TEST Successful PUT request change data. Returns status code eq to 200"""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    await session.refresh(level)

    put_url = url + "/" + str(level.id)

    res = await private_client.put(put_url, json=LEVEL_DATA_TO_UPDATE)
    print(res.json())
    assert res.status_code == 200

    await session.refresh(level)
    assert level.level == LEVEL_DATA_TO_UPDATE["level"]
    assert level.name == LEVEL_DATA_TO_UPDATE["name"]
    assert level.description == LEVEL_DATA_TO_UPDATE["description"]
    assert level.object_type_id == LEVEL_DATA_TO_UPDATE["object_type_id"]
    assert level.is_virtual == LEVEL_DATA_TO_UPDATE["is_virtual"]
    assert level.key_attrs == [
        str(x) for x in LEVEL_DATA_TO_UPDATE["key_attrs"]
    ]
    assert (
        level.additional_params_id
        == LEVEL_DATA_TO_UPDATE["additional_params_id"]
    )
    assert level.latitude_id == LEVEL_DATA_TO_UPDATE["latitude_id"]
    assert level.longitude_id == LEVEL_DATA_TO_UPDATE["longitude_id"]


@pytest.mark.asyncio(loop_scope="session")
async def test_put_level_raises_error_on_not_uniq_name(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    url: str,
    h_id: int,
):
    """TEST Cannot set the name value that eq to another existing level name with eq hierarchy_id"""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    level2 = Level(**LEVEL_DATA_TO_UPDATE, hierarchy_id=h_id)
    session.add(level)
    session.add(level2)
    await session.commit()

    await session.refresh(level)

    put_url = url + "/" + str(level.id)

    res = await private_client.put(put_url, json=LEVEL_DATA_TO_UPDATE)
    assert res.status_code == 422


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_level_change_modified_attr(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    h_id: int,
    url: str,
):
    """TEST Change level.modified on successful PUT request.
    level.modified eq to change timestamp"""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    change_time = datetime.now()
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()
    await session.refresh(level)

    assert level.modified is None

    put_url = url + "/" + str(level.id)

    await private_client.put(put_url, json=LEVEL_DATA_TO_UPDATE)

    await session.refresh(level)
    assert level.modified >= change_time


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_level_change_change_author_attr(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    h_id: int,
    url: str,
):
    """TEST Change level.change_author on successful PUT request.
    level.change_author eq to author in request body"""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()
    await session.refresh(level)

    assert level.change_author is None

    put_url = url + "/" + str(level.id)

    res = await private_client.put(put_url, json=LEVEL_DATA_TO_UPDATE)
    print(res.status_code)
    await session.refresh(level)
    assert level.change_author == LEVEL_DATA_TO_UPDATE["author"]


@pytest.mark.asyncio(loop_scope="session")
async def test_successful_put_level_cant_change_author_attr(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    h_id: int,
    url: str,
):
    """TEST successful PUT request doesnt change level.author."""

    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()
    await session.refresh(level)

    assert level.change_author is None

    put_url = url + "/" + str(level.id)

    await private_client.put(put_url, json=LEVEL_DATA_TO_UPDATE)

    await session.refresh(level)
    assert level.author != LEVEL_DATA_TO_UPDATE["author"]


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_with_not_existing_parent_id_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request with not existing level.parent_id ."""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = 255

    res = await private_client.post(url, json=payload)

    assert res.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_with_not_existing_parent_id_in_same_hierarchy_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
):
    """TEST Raises error on POST request if you can't find parent level in same hierarchy"""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    hierarchy.name = "Uniq hierarchy name"
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    level = Level(**level_default_data)
    level.hierarchy_id = hierarchy.id
    session.add(level)
    await session.commit()
    await session.refresh(level)

    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = level.id

    res = await private_client.post(url, json=payload)
    assert res.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_post_level_with_wrong_level_parameter_raise_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
    h_id: int,
):
    """TEST Raises error on POST request if parent level exists in same hierarchy, but has
    difference between parameter 'level' (child.level - parent.level) not eq to 1.
    Level parameter shows depth of level, so all children must have a 'level' greater of parent by one"""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )

    level = Level(**level_default_data)
    level.name = "Uniq level name"
    level.hierarchy_id = h_id
    session.add(level)
    await session.commit()
    await session.refresh(level)

    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = level.id

    res = await private_client.post(url, json=payload)

    assert res.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_put_level_with_not_existing_parent_id_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
    h_id: int,
):
    """TEST Raises error on PUT request with not existing level.parent_id ."""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    level = Level(**level_default_data)
    level.name = "Uniq level name"
    level.hierarchy_id = h_id
    session.add(level)
    await session.commit()
    await session.refresh(level)

    put_url = url + "/" + str(level.id)
    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = 255

    res = await private_client.put(put_url, json=payload)
    assert res.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_put_level_with_parent_id_eq_id_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
    h_id: int,
):
    """TEST Raises error on PUT request with in case level.parent_id eq level.id ."""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    level = Level(**level_default_data)
    level.name = "Uniq level name"
    level.hierarchy_id = h_id
    session.add(level)
    await session.commit()
    await session.refresh(level)

    put_url = url + "/" + str(level.id)
    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = level.id

    res = await private_client.put(put_url, json=payload)
    assert res.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_put_level_with_not_existing_parent_id_in_same_hierarchy_raises_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
    h_id: int,
):
    """TEST Raises error on PUT request if you can't find parent level in same hierarchy"""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )
    hierarchy = Hierarchy(**HIERARCHY_DEFAULT_DATA)
    hierarchy.name = "Uniq hierarchy name"
    session.add(hierarchy)
    await session.commit()
    await session.refresh(hierarchy)

    level = Level(**level_default_data)
    level.hierarchy_id = h_id
    session.add(level)
    level2 = Level(**level_default_data)
    level2.hierarchy_id = hierarchy.id
    session.add(level2)
    await session.commit()
    await session.refresh(level)
    await session.refresh(level2)

    put_url = url + "/" + str(level.id)
    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = level2.id

    res = await private_client.put(put_url, json=payload)
    assert res.status_code == 400


@pytest.mark.asyncio(loop_scope="session")
async def test_put_level_with_wrong_level_parameter_raise_error(
    session: AsyncSession,
    private_client: AsyncClient,
    mocker,
    level_default_data: dict,
    url: str,
    h_id: int,
):
    """TEST Raises error on PUT request if parent level exists in same hierarchy, but has
    difference between parameter 'level' (child.level - parent.level) not eq to 1.
    Level parameter shows depth of level, so all children must have a 'level' greater of parent by one"""
    mocker.patch(
        "routers.level_router.check_hierarchy_level_inv_params_exist_by_grpc",
        return_value={"all_valid": True},
    )
    mocker.patch(
        "routers.level_router.get_tmo_data_by_tmo_ids",
        return_value={"tmo_info": True},
    )

    level = Level(**level_default_data)
    level.name = "Uniq level name"
    level.hierarchy_id = h_id
    level2 = Level(**level_default_data)
    level2.hierarchy_id = h_id
    session.add(level)
    session.add(level2)
    await session.commit()
    await session.refresh(level)
    await session.refresh(level2)

    put_url = url + "/" + str(level2.id)

    payload = copy.deepcopy(level_default_data)
    payload["parent_id"] = level.id

    res = await private_client.put(put_url, json=payload)
    assert res.status_code == 400
