"""TESTS for Obj"""

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy, Level, Obj
from schemas.main_base_connector import Base

DEFAULT_DATA = {
    "key": "Test value",
    "object_id": 1,
    "object_type_id": 1,
    "additional_params": "1",
    "level": 1,
    "latitude": 2,
    "longitude": 3,
    "child_count": 0,
    "parent_id": None,
}

LEVEL_DEFAULT_DATA = {
    "level": 0,
    "name": "striweweneeg",
    "description": "string",
    "object_type_id": 0,
    "is_virtual": True,
    "param_type_id": 0,
    "additional_params_id": 0,
    "latitude_id": 0,
    "longitude_id": 0,
    "author": "string",
}

HIERARCHY_DEFAULT_DATA = {"name": "Test hierarchy", "author": "Test author"}


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


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_obj(session: AsyncSession, h_id):
    """TEST Creating Obj is successful"""

    inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(inst)
    await session.commit()

    await session.refresh(inst)

    assert inst is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_obj_with_not_existing_hierarchy_id(
    session: AsyncSession,
):
    """TEST Raises error on saving Obj instance with non-existent hierarchy_id"""

    inst = Obj(**DEFAULT_DATA)
    inst.hierarchy_id = 255
    session.add(inst)

    with pytest.raises(IntegrityError):
        await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_obj_child_count_by_default_0(session: AsyncSession, h_id: int):
    """TEST Obj.child_count by default eq 0"""

    inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    del inst.child_count
    session.add(inst)
    await session.commit()

    await session.refresh(inst)

    assert inst.child_count == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_obj_id_is_uuid_instance(session: AsyncSession, h_id: int):
    """TEST Obj.id is UUID instance"""

    inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(inst)
    await session.commit()

    await session.refresh(inst)
    assert isinstance(inst.id, UUID)


@pytest.mark.asyncio(loop_scope="session")
async def test_obj_parent_id_is_none_by_default(
    session: AsyncSession,
    h_id: int,
):
    """TEST Obj.parent_id is None by default"""

    inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(inst)
    await session.commit()

    await session.refresh(inst)
    assert inst.parent_id is None


@pytest.mark.asyncio(loop_scope="session")
async def test_obj_parent_id_is_uuid_instance(
    session: AsyncSession,
    h_id: int,
):
    """TEST Obj.parent_id refers to only existing Obj.id and is UUID instance"""

    inst1 = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(inst1)
    await session.flush()

    inst2 = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    inst2.parent_id = inst1.id
    session.add(inst2)
    await session.commit()

    await session.refresh(inst2)

    assert isinstance(inst2.parent_id, UUID)


@pytest.mark.asyncio(loop_scope="session")
async def test_raise_error_if_parent_id_is_not_exists(
    session: AsyncSession,
    h_id: int,
):
    """TEST Raises error on saving Obl in case if parent_id is not exists"""

    uuid_id = uuid4()
    inst1 = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    inst1.parent_id = uuid_id
    session.add(inst1)

    with pytest.raises(IntegrityError):
        await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_on_hierarchy_delete_also_delete_all_obj(
    session: AsyncSession,
    h_id: int,
):
    """TEST Deletes all obj in case of deleting a specific hierarchy"""

    inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    inst2 = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(inst)
    session.add(inst2)

    statement = select(Hierarchy).where(Hierarchy.id == h_id)
    res = (await session.execute(statement)).scalar()
    await session.delete(res)
    await session.commit()

    statement = select(Obj)
    res = (await session.execute(statement)).scalars().all()
    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_obj_with_level_id(session: AsyncSession, h_id: int):
    """TEST creating obj with existing level id is successful."""

    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.flush()
    obj_inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    obj_inst.level_id = level.id
    session.add(obj_inst)
    await session.commit()

    statement = select(Obj).where(Obj.level_id == level.id)
    res = (await session.execute(statement)).scalars().all()
    assert len(res) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_obj_with_not_existing_level_id(
    session: AsyncSession, h_id: int
):
    """TEST Raises error when creating obj with not existing level id."""
    obj_inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    obj_inst.level_id = 2
    session.add(obj_inst)

    with pytest.raises(IntegrityError):
        await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_on_level_delete_delete_all_connected_obj(
    session: AsyncSession,
    h_id: int,
):
    """TEST When deleting a level, also delete all related objects"""
    level = Level(**LEVEL_DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.flush()
    obj_inst = Obj(**DEFAULT_DATA, hierarchy_id=h_id)
    obj_inst.level_id = level.id
    session.add(obj_inst)
    await session.commit()

    statement = select(Obj)
    res = (await session.execute(statement)).scalars().all()
    assert len(res) == 1

    await session.delete(level)

    res = (await session.execute(statement)).scalars().all()
    assert len(res) == 0
