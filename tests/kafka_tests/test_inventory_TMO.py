import datetime

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from common_utils.hierarchy_builder import HierarchyBuilder
from grpc_config.protobuf import mo_info_pb2
from kafka_config.msg.tmo_msg import on_delete_tmo
from schemas.hier_schemas import Hierarchy, Level, Obj
from schemas.main_base_connector import Base

DELETED_TMO_ID = 2
DELETE_MSG = {"objects": [{"id": DELETED_TMO_ID, "tprm_id": 1}]}


MO_BD = {
    1: {
        "mo_id": 1,
        "tprm_values": {
            1: "param_type value 1",
            2: "additional_params value 1",
            3: "1.1",
            4: "1.2",
        },
        "p_id": 0,
    },
    2: {
        "mo_id": 2,
        "tprm_values": {
            1: "param_type value 2",
            2: "additional_params value 2",
            3: "2.1",
            4: "2.2",
        },
        "p_id": 0,
    },
    3: {
        "mo_id": 3,
        "tprm_values": {
            1: "param_type value 2",
            2: "additional_params value 3",
            3: "3.1",
            4: "3.2",
        },
        "p_id": 1,
    },
    4: {
        "mo_id": 4,
        "tprm_values": {
            1: "param_type value 1",
            2: "additional_params value 1",
            3: "1.1",
            4: "1.2",
        },
        "p_id": 1,
    },
    5: {
        "mo_id": 5,
        "tprm_values": {
            1: "param_type value 2",
            2: "additional_params value 2",
            3: "2.1",
            4: "2.2",
        },
        "p_id": 1,
    },
    6: {
        "mo_id": 6,
        "tprm_values": {
            1: "param_type value 3",
            2: "additional_params value 3",
            3: "3.1",
            4: "3.2",
        },
        "p_id": 3,
    },
}

MO_FOR_TMO_1 = [MO_BD[1], MO_BD[2]]
MO_FOR_TMO_2 = [MO_BD[3], MO_BD[4]]
MO_FOR_TMO_3 = [MO_BD[5], MO_BD[6]]

INVENTORY_RESPONCE_FOR_LEVEL_1 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_1
]
INVENTORY_RESPONCE_FOR_LEVEL_2 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_2
]
INVENTORY_RESPONCE_FOR_LEVEL_3 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_3
]


INVENTORY_DB = {
    1: INVENTORY_RESPONCE_FOR_LEVEL_1,
    2: INVENTORY_RESPONCE_FOR_LEVEL_2,
    3: INVENTORY_RESPONCE_FOR_LEVEL_3,
}


async def add_hierarchy_to_session(session: AsyncSession):
    """Add hierarchy to session"""
    created = datetime.datetime.now()
    hierarchy = Hierarchy(
        name="Test Hier",
        description="Test descr",
        author="Admin",
        created=created,
    )
    session.add(hierarchy)
    await session.flush()
    return hierarchy


def add_level_to_session(
    session,
    is_virtual: bool,
    hierarchy_id: int,
    level_name: str,
    obj_type_id: int,
    level: int,
):
    """Add level to session"""
    created = datetime.datetime.now()
    level = Level(
        level=level,
        name=level_name,
        object_type_id=obj_type_id,
        is_virtual=is_virtual,
        hierarchy_id=hierarchy_id,
        param_type_id=1,
        created=created,
        additional_params_id=1,
        latitude_id=3,
        longitude_id=4,
        author="AdminTest",
    )
    session.add(level)


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session, mocker):
    hierarchy = await add_hierarchy_to_session(session)
    add_level_to_session(
        session, False, hierarchy.id, "First real level", 1, level=1
    )
    add_level_to_session(
        session,
        True,
        hierarchy.id,
        "Second real level",
        DELETED_TMO_ID,
        level=2,
    )
    add_level_to_session(
        session,
        False,
        hierarchy.id,
        "Third3 real level",
        DELETED_TMO_ID,
        level=3,
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
async def clean_test_data(session):
    yield
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


@pytest.mark.asyncio(loop_scope="session")
async def test_delete_spec_level_on_tmo_delete(session: AsyncSession, mocker):
    """TEST On receiving TMO:deleted msg - delete all levels where
    level.object_type_id == TMO.id"""

    # check if level exists
    statement = select(Level).where(Level.object_type_id == DELETED_TMO_ID)
    res = await session.execute(statement)
    res = res.scalars().all()
    assert len(res) > 0

    await on_delete_tmo(DELETE_MSG, session)

    # check if level exists after on_delete_tmo
    statement = select(Level).where(Level.object_type_id == DELETED_TMO_ID)
    res = await session.execute(statement)
    res = res.scalars().all()
    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_doesnt_rebuild_hierarchy_on_tmo_delete(session, mocker):
    """TEST On receiving TMO:deleted msg - does not rebuild hierarchy ."""

    # MOCK function
    async def mock_update_database(s):
        return True

    # MOCK
    spy = mocker.patch(
        "common_utils.hierarchy_builder.HierarchyBuilder.build_hierarchy",
        return_value=mock_update_database,
    )
    # get all Obj before on_delete_tmo call
    statement = select(Obj)
    res_before = await session.execute(statement)
    res_before = res_before.scalars().all()

    await on_delete_tmo(DELETE_MSG, session)

    res_after = await session.execute(statement)
    res_after = res_after.scalars().all()

    # check on rebuild function
    assert spy.call_count == 0
