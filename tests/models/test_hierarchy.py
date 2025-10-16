"""TESTS for Hierarchy"""

from datetime import datetime

import pytest
import pytest_asyncio
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy
from schemas.main_base_connector import Base

DEFAULT_DATA = {"name": "Test hierarchy", "author": "Test author"}


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(session: AsyncSession):
    yield
    await session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_hierarchy(session: AsyncSession):
    """TEST Creating Hierarchy is successful"""
    inst = Hierarchy(**DEFAULT_DATA)

    session.add(inst)
    await session.commit()

    await session.refresh(inst)

    assert inst is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_on_creating_hierarchy_hierarchy_created_is_auto_generated(
    session: AsyncSession,
):
    """TEST The Hierarchy.created parameter is automatically generated when the instance is
    saved."""
    inst = Hierarchy(**DEFAULT_DATA)
    session.add(inst)
    await session.commit()
    await session.refresh(inst)

    assert isinstance(inst.created, datetime)


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_hierarchy_without_name(session: AsyncSession):
    """TEST Raises error on saving Hierarchy instance without name"""
    inst = Hierarchy(**DEFAULT_DATA)
    del inst.name
    session.add(inst)

    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_hierarchy_without_author(session: AsyncSession):
    """TEST Raises error on saving Hierarchy instance without author"""
    inst = Hierarchy(**DEFAULT_DATA)
    del inst.author
    session.add(inst)

    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_cant_create_2_or_more_hierarchy_with_eq_names(
    session: AsyncSession,
):
    """TEST Raises error on saving Hierarchy instance with existing name"""
    inst = Hierarchy(**DEFAULT_DATA)
    inst2 = Hierarchy(**DEFAULT_DATA)
    session.add(inst)
    session.add(inst2)

    try:
        await session.commit()
    except IntegrityError:
        assert True
    else:
        assert False


@pytest.mark.asyncio(loop_scope="session")
async def test_the_create_empty_nodes_attr_is_optional(session: AsyncSession):
    """TEST Creating hierarchy whithout create_empty_nodes is available. create_empty_nodes by default equals True"""
    create_empty_nodes = DEFAULT_DATA.get("create_empty_nodes", None)
    assert create_empty_nodes is None

    inst = Hierarchy(**DEFAULT_DATA)

    session.add(inst)
    await session.commit()
    await session.refresh(inst)

    assert inst.create_empty_nodes is True
