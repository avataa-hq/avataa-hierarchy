# test change count on delete
import datetime

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from common_utils.hierarchy_builder import HierarchyBuilder
from grpc_config.protobuf import mo_info_pb2
from kafka_config.msg.utils import (
    refresh_child_count_for_parent_obj_with_level_delete,
)
from schemas.hier_schemas import Hierarchy, Level, Obj
from schemas.main_base_connector import Base

MO_BD = {
    1: {"mo_id": 2, "tprm_values": {1: "5", 2: "15"}, "p_id": 1},
    2: {"mo_id": 3, "tprm_values": {3: "25", 4: "35"}, "p_id": 1},
}

MO_FOR_TMO_1 = [MO_BD[1], MO_BD[2]]

INVENTORY_RESPONCE_FOR_LEVEL_1 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_1
]

INVENTORY_DB = {1: INVENTORY_RESPONCE_FOR_LEVEL_1}
THIRD_LEVEL_NAME = "Third3 real level"


async def add_hierarchy_to_session(session: AsyncSession):
    """Add hierarchy to session"""
    created = datetime.datetime.now()
    hierarchy = Hierarchy(
        name="Test Hier",
        description="Test descr",
        author="Admin",
        created=created,
    )
    hierarchy2 = Hierarchy(
        name="Test Hier2",
        description="Test descr2",
        author="Admin",
        created=created,
    )
    session.add(hierarchy)
    session.add(hierarchy2)
    await session.flush()
    return hierarchy, hierarchy2


async def add_level_to_session(
    session,
    is_virtual: bool,
    hierarchy_id: int,
    level_name: str,
    obj_type_id: int,
    level: int,
    param_type_id=1,
    additional_params_id=1,
    latitude_id=1,
    longitude_id=1,
    parent_id=None,
):
    """Add level to session"""
    created = datetime.datetime.now()
    level = Level(
        level=level,
        name=level_name,
        object_type_id=obj_type_id,
        is_virtual=is_virtual,
        hierarchy_id=hierarchy_id,
        param_type_id=param_type_id,
        created=created,
        additional_params_id=additional_params_id,
        latitude_id=latitude_id,
        longitude_id=longitude_id,
        parent_id=parent_id,
        author="AdminTest",
    )
    session.add(level)
    await session.flush()
    return level


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession, mocker):
    hierarchy = await add_hierarchy_to_session(session)
    hierarchy = hierarchy[0]
    level_1 = await add_level_to_session(
        session, True, hierarchy.id, "First real level", 1, level=0
    )
    level_2 = await add_level_to_session(
        session,
        True,
        hierarchy.id,
        "Second real level",
        1,
        level=1,
        param_type_id=2,
        parent_id=level_1.id,
    )
    await add_level_to_session(
        session,
        False,
        hierarchy.id,
        THIRD_LEVEL_NAME,
        1,
        level=2,
        additional_params_id=2,
        latitude_id=2,
        longitude_id=2,
        parent_id=level_2.id,
    )
    await session.commit()

    async def mocked_async_generator(level):
        res = INVENTORY_DB[level.object_type_id]
        for item in res:
            yield item

    mocker.patch(
        "common_utils.hierarchy_builder.HierarchyBuilder.get_data_for_level",
        side_effect=mocked_async_generator,
    )

    keeper = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy.id)
    await keeper.build_hierarchy()

    await session.flush()
    yield


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(session: AsyncSession):
    yield
    await session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_set_to_none_obj_additional_params_on_tprm_delete(
    session: AsyncSession,
):
    """TEST Recursive change child_count of parent objects if object is is_virtual = True and have child_count equal
    to 0 (Because virtual objects without child should be deleted, so we need to change child_count of this object and
    delete it, and if parent is virtual too we change it child_count and check further
    )"""

    stmt = select(Level.id).where(Level.name == THIRD_LEVEL_NAME)
    level_id = (await session.execute(stmt)).scalar()
    await refresh_child_count_for_parent_obj_with_level_delete(
        level_id, session
    )

    statement = select(Obj).order_by(Obj.level_id)
    level = await session.execute(statement)
    level = level.scalars().all()
    assert len(level) == 0
