"""TESTS for Levels"""

from datetime import datetime

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy, Level
from schemas.main_base_connector import Base

DEFAULT_DATA = {
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
async def test_can_create_level(
    session: AsyncSession,
    h_id: int,
):
    """TEST Creating Level is successful"""

    inst = Level(**DEFAULT_DATA, hierarchy_id=h_id)

    session.add(inst)
    await session.commit()
    await session.refresh(inst)

    assert inst is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_on_creating_level_level_created_is_auto_generated(
    session: AsyncSession,
):
    """TEST The Level.created parameter is automatically generated when the instance is saved."""

    h = Hierarchy(name="ass", author="asdads")
    session.add(h)
    await session.commit()
    await session.refresh(h)

    inst = Level(**DEFAULT_DATA)
    inst.hierarchy_id = h.id

    session.add(inst)

    await session.commit()

    await session.refresh(inst)

    assert isinstance(inst.created, datetime)


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_level_with_not_existing_hierarchy_id(
    session: AsyncSession,
):
    """TEST Raises error on saving Level instance with non-existent hierarchy_id"""

    inst = Level(**DEFAULT_DATA)
    inst.hierarchy_id = 255
    session.add(inst)

    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_2_or_more_levels_with_eq_hierarchy_id_and_name(
    session: AsyncSession,
):
    """TEST Raises error on saving Level instance with existing eq  hierarchy_id and name"""

    inst = Level(**DEFAULT_DATA)
    inst2 = Level(**DEFAULT_DATA)
    session.add(inst)
    session.add(inst2)

    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_on_hierarchy_delete_also_delete_all_levels(
    session: AsyncSession, h_id: int
):
    """TEST Deletes all levels in case of deleting a specific hierarchy"""

    inst = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    inst2 = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    inst2.name = "Test 2"
    session.add(inst)
    session.add(inst2)

    await session.commit()
    statement = select(Hierarchy).where(Hierarchy.id == h_id)
    res = (await session.execute(statement)).scalar()
    await session.delete(res)
    await session.commit()

    statement = select(Level)
    res = (await session.execute(statement)).scalars().all()
    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_create_level_with_parent_id_successful(
    session: AsyncSession,
    h_id: int,
):
    """TEST Creating level with parent_id that refers to existing level.id is successful"""

    level1 = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level1)
    await session.flush()
    level2 = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    level2.name = "Uniq name"
    level2.parent_id = level1.id
    session.add(level2)
    await session.commit()
    await session.refresh(level2)

    statement = select(Level).where(Level.parent_id == level1.id)
    res = (await session.execute(statement)).scalars().all()

    assert len(res) == 1
    assert level2.json() == res[0].json()


@pytest.mark.asyncio(loop_scope="session")
async def test_create_level_with_not_existing_parent_id_fails(
    session: AsyncSession,
    h_id: int,
):
    """TEST Raises error when creating level with parent_id that refers to not existing level.id"""

    level1 = Level(**DEFAULT_DATA, parent_id=100, hierarchy_id=h_id)
    session.add(level1)

    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_when_delete_level_delete_all_children_successful(
    session: AsyncSession, h_id: int
):
    """TEST Raises error when creating level with parent_id that refers to not existing level.id"""

    level1 = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level1)
    await session.flush()
    level2 = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    level2.name = "Uniq name"
    level2.parent_id = level1.id
    session.add(level2)
    await session.flush()
    level3 = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    level3.name = "Uniq name 3"
    level3.parent_id = level2.id
    session.add(level3)
    await session.commit()

    await session.refresh(level1)
    await session.delete(level1)

    statement = select(Level).where(Level.parent_id == level1.id)
    res = (await session.execute(statement)).scalars().all()

    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_create_level_with_id_eq_parent_id_fails(
    session: AsyncSession,
    h_id: int,
):
    """TEST Raises IntegrityError with saving level in case than level.id eq level.parent_id."""

    level = Level(**DEFAULT_DATA, hierarchy_id=h_id)
    session.add(level)
    await session.commit()

    await session.refresh(level)

    level.parent_id = level.id
    session.add(level)
    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False
