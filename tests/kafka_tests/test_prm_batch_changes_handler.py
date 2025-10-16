import datetime

import pytest
import pytest_asyncio
from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from common_utils.hierarchy_builder import HierarchyBuilder
from grpc_config.protobuf import mo_info_pb2
from kafka_config.batch_change_handler.handler import PRMChangesHandler
from schemas.hier_schemas import Hierarchy, HierarchyRebuildOrder, Level
from schemas.main_base_connector import Base

MO_BD = {
    1: {
        "mo_id": 1,
        "tprm_values": {
            1: "1011",
            2: "102",
            3: "103",
            4: "104",
            5: "105",
            6: "106",
        },
        "p_id": 0,
    },
    2: {
        "mo_id": 2,
        "tprm_values": {
            1: "1011",
            2: "102",
            3: "103",
            4: "104",
            5: "105",
            6: "106",
        },
        "p_id": 0,
    },
    3: {
        "mo_id": 3,
        "tprm_values": {
            1: "101",
            2: "102",
            3: "103",
            4: "1044",
            5: "105",
            6: "106",
        },
        "p_id": 1,
    },
    4: {
        "mo_id": 4,
        "tprm_values": {
            1: "101",
            2: "102",
            3: "103",
            4: "1044",
            5: "105",
            6: "106",
        },
        "p_id": 1,
    },
    5: {
        "mo_id": 5,
        "tprm_values": {
            1: "1011",
            2: "102",
            3: "103",
            4: "104",
            5: "105",
            6: "106",
        },
        "p_id": 4,
    },
    6: {
        "mo_id": 6,
        "tprm_values": {
            1: "1011",
            2: "102",
            3: "103",
            4: "104",
            5: "105",
            6: "106",
        },
        "p_id": 3,
    },
    7: {
        "mo_id": 7,
        "tprm_values": {
            1: "101",
            2: "102",
            3: "103",
            4: "1044",
            5: "105",
            6: "106",
        },
        "p_id": 1,
    },
    8: {
        "mo_id": 8,
        "tprm_values": {
            1: "1011",
            2: "102",
            3: "103",
            4: "10444",
            5: "105",
            6: "106",
        },
        "p_id": 1,
    },
}

MO_FOR_TMO_1 = [MO_BD[1], MO_BD[2]]
MO_FOR_TMO_2 = [MO_BD[3], MO_BD[4], MO_BD[7], MO_BD[8]]
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

HIERARCHY_NAME_1 = "Test Hierarchy 1"
HIERARCHY_NAME_2 = "Test Hierarchy 2"


async def add_hierarchy_to_session(
    session: AsyncSession, hierarchy_name: str = "TEst name"
):
    """Add hierarchy to session"""
    created = datetime.datetime.now()
    hierarchy = Hierarchy(
        name=hierarchy_name,
        description="Test descr",
        author="Admin",
        created=created,
    )
    session.add(hierarchy)
    await session.flush()
    return hierarchy


async def add_level_to_session(
    session,
    is_virtual: bool,
    hierarchy_id: int,
    level_name: str,
    obj_type_id: int,
    level: int,
    param_type_id=1,
    parent_id=None,
    additional_params_id=2,
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
        latitude_id=5,
        longitude_id=6,
        parent_id=parent_id,
        author="AdminTest",
    )
    session.add(level)
    await session.flush()
    return level


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession, mocker):
    hierarchy = await add_hierarchy_to_session(session, HIERARCHY_NAME_1)
    level1 = await add_level_to_session(
        session,
        False,
        hierarchy.id,
        "1 - Real level",
        1,
        level=1,
        param_type_id=4,
    )
    await add_level_to_session(
        session,
        True,
        hierarchy.id,
        "2 - Grouping level",
        2,
        level=2,
        param_type_id=2,
        parent_id=level1.id,
    )

    hierarchy_2 = await add_hierarchy_to_session(session, HIERARCHY_NAME_2)
    await add_level_to_session(
        session,
        False,
        hierarchy_2.id,
        "1- Real level",
        3,
        level=1,
        param_type_id=1,
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
async def test_changes_related_to_hierarchy_less_than_handler_min_default_value_successful_case_1(
    session: AsyncSession,
):
    """Returns same dict with messages if changes related to existing hierarchy and less than
    MINIMUM_CHANGES_COUNT_TO_REBUILD of the handler"""
    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    item = {"mo_id": 1, "tprm_id": 1, "value": "TEst Created"}
    msg_as_dict = {"objects": [item]}

    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    cleared_message = await handler.get_msg_ready_for_processing()

    assert msg_as_dict == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_related_to_hierarch_less_than_handler_min_default_value_successful_case_2(
    session: AsyncSession,
):
    """Returns empty dict with empty list of messages, if changes related to existing hierarchy and less than
    MINIMUM_CHANGES_COUNT_TO_REBUILD of the handler but hierarchy in order to rebuild with status on_rebuild False"""

    stmt = select(Hierarchy).where(Hierarchy.name == HIERARCHY_NAME_1)
    hierarchy_from_db = await session.execute(stmt)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    item_in_order = HierarchyRebuildOrder(hierarchy_id=hierarchy_from_db.id)
    session.add(item_in_order)
    await session.commit()

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    item = {"mo_id": 1, "tprm_id": 4, "value": "TEst Created"}
    msg_as_dict = {"objects": [item]}

    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    cleared_message = await handler.get_msg_ready_for_processing()

    assert {"objects": list()} == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_related_to_hierarch_less_than_handler_min_default_value_successful_case_3(
    session: AsyncSession, mocker
):
    """if changes related to existing hierarchy and less than
    MINIMUM_CHANGES_COUNT_TO_REBUILD of the handler but hierarchy in order to rebuild with status on_rebuild True.
    Waits until rebuilt process will be finished. In case when the hierarchy is stuck at the recovery stage,
    trys to rebuilt it one more time."""
    spy = mocker.patch(
        "common_utils.hierarchy_builder.HierarchyBuilder.build_hierarchy"
    )

    stmt = select(Hierarchy).where(Hierarchy.name == HIERARCHY_NAME_1)
    hierarchy_from_db = await session.execute(stmt)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    item_in_order = HierarchyRebuildOrder(
        hierarchy_id=hierarchy_from_db.id, on_rebuild=True
    )
    session.add(item_in_order)
    await session.commit()

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    DEFAULT_SLEEP_TIME = 2
    item = {"mo_id": 1, "tprm_id": 4, "value": "TEst Created"}
    msg_as_dict = {"objects": [item]}

    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    handler.DEFAULT_SLEEP_TIME = DEFAULT_SLEEP_TIME
    cleared_message = await handler.get_msg_ready_for_processing()

    assert spy.call_count == 1
    assert msg_as_dict == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_if_changes_not_related_to_existing_hierarchies_successful(
    session: AsyncSession,
):
    """Returns dict {'objects': []} with empty values of 'objects' key if changes not related to existing
    hierarchies"""
    not_existing_tprm_id = 255000

    stmt = select(Level).where(
        or_(
            Level.param_type_id == not_existing_tprm_id,
            Level.additional_params_id == not_existing_tprm_id,
            Level.latitude_id == not_existing_tprm_id,
            Level.longitude_id == not_existing_tprm_id,
        )
    )
    levels_from_db = await session.execute(stmt)
    levels_from_db = levels_from_db.scalars().all()

    assert len(levels_from_db) == 0

    item_not_releated_to_hierarhy = {
        "mo_id": 1,
        "tprm_id": not_existing_tprm_id,
        "value": "TEst Created",
    }
    msg_as_dict = {
        "objects": [item_not_releated_to_hierarhy for x in range(10)]
    }

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD

    cleared_message = await handler.get_msg_ready_for_processing()

    assert {"objects": list()} == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_related_to_hierarchy_more_than_min_default_value_case_1(
    session: AsyncSession,
):
    """If count of changes more than MINIMUM_CHANGES_COUNT_TO_REBUILD of handler and changes related to one hierarchy:
    - Adds hierarchy id into rebuild order with status on_rebuild False
    - returns dict {'objects': []} with empty values of 'objects' key"""

    stmt = select(HierarchyRebuildOrder)
    rebuild_order_from_db = await session.execute(stmt)
    rebuild_order_from_db = rebuild_order_from_db.scalars().all()

    assert len(rebuild_order_from_db) == 0

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    item = {"mo_id": 1, "tprm_id": 4, "value": "TEst Created"}
    msg_as_dict = {
        "objects": [item for x in range(MINIMUM_CHANGES_COUNT_TO_REBUILD)]
    }

    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    cleared_message = await handler.get_msg_ready_for_processing()

    stmt = select(Hierarchy).where(Hierarchy.name == HIERARCHY_NAME_1)
    hierarchy_from_db = await session.execute(stmt)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    stmt = select(HierarchyRebuildOrder).where(
        HierarchyRebuildOrder.hierarchy_id == hierarchy_from_db.id
    )
    rebuild_order_from_db = await session.execute(stmt)
    rebuild_order_from_db = rebuild_order_from_db.scalars().all()

    assert len(rebuild_order_from_db) == 1
    rebuild_item = rebuild_order_from_db[0]
    assert rebuild_item.hierarchy_id == hierarchy_from_db.id
    assert rebuild_item.on_rebuild is False

    assert {"objects": list()} == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_related_to_2_hierarchies_more_than_min_default_value_case_1(
    session: AsyncSession,
):
    """If changes related to 2 hierarchies but count of changes are more than
    MINIMUM_CHANGES_COUNT_TO_REBUILD of handler only for one hierarchy:
    - Adds first hierarchy id into rebuild order with status on_rebuild False
    - returns dict {'objects': [...]} with values of 'objects' key related to second hierarchy"""
    stmt = select(HierarchyRebuildOrder)
    rebuild_order_from_db = await session.execute(stmt)
    rebuild_order_from_db = rebuild_order_from_db.scalars().all()

    assert len(rebuild_order_from_db) == 0

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    item_1 = {"mo_id": 1, "tprm_id": 4, "value": "TEst Created"}
    items_for_hierarchy_1 = [
        item_1 for x in range(MINIMUM_CHANGES_COUNT_TO_REBUILD)
    ]

    item_2 = {"mo_id": 1, "tprm_id": 1, "value": "TEst Created"}
    items_for_hierarchy_2 = [item_2]

    msg_as_dict = {"objects": items_for_hierarchy_1 + items_for_hierarchy_2}
    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    cleared_message = await handler.get_msg_ready_for_processing()
    print(items_for_hierarchy_2)
    assert {"objects": items_for_hierarchy_2} == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_related_to_hierarchy_more_than_min_default_value_case_2(
    session,
):
    """if changes related to existing hierarchy and more than
    MINIMUM_CHANGES_COUNT_TO_REBUILD of the handler but hierarchy in order to rebuild with status on_rebuild False.
    Returns empty dict with empty list of messages.
    """
    stmt = select(Hierarchy).where(Hierarchy.name == HIERARCHY_NAME_1)
    hierarchy_from_db = await session.execute(stmt)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    item_in_order = HierarchyRebuildOrder(hierarchy_id=hierarchy_from_db.id)
    session.add(item_in_order)
    await session.commit()

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    item = {"mo_id": 1, "tprm_id": 4, "value": "TEst Created"}
    msg_as_dict = {
        "objects": [item for x in range(MINIMUM_CHANGES_COUNT_TO_REBUILD)]
    }

    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    cleared_message = await handler.get_msg_ready_for_processing()

    assert {"objects": list()} == cleared_message


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_related_to_hierarchy_more_than_min_default_value_case_3(
    session: AsyncSession, mocker
):
    """if changes related to existing hierarchy and more than
    MINIMUM_CHANGES_COUNT_TO_REBUILD of the handler but hierarchy in order to rebuild with status on_rebuild True.
    Waits until rebuilt process will be finished. In case when the hierarchy is stuck at the recovery stage,
    trys to rebuilt it one more time. And add this hierarchy one more time to rebuild order
    (because count of changes more than MINIMUM_CHANGES_COUNT_TO_REBUILD)"""
    spy = mocker.patch(
        "common_utils.hierarchy_builder.HierarchyBuilder.build_hierarchy"
    )

    stmt = select(Hierarchy).where(Hierarchy.name == HIERARCHY_NAME_1)
    hierarchy_from_db = await session.execute(stmt)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    item_in_order = HierarchyRebuildOrder(
        hierarchy_id=hierarchy_from_db.id, on_rebuild=True
    )
    session.add(item_in_order)
    await session.commit()

    MINIMUM_CHANGES_COUNT_TO_REBUILD = 5
    DEFAULT_SLEEP_TIME = 2
    item = {"mo_id": 1, "tprm_id": 4, "value": "TEst Created"}
    msg_as_dict = {
        "objects": [item for x in range(MINIMUM_CHANGES_COUNT_TO_REBUILD)]
    }

    handler = PRMChangesHandler(session=session, message_as_dict=msg_as_dict)
    handler.MINIMUM_CHANGES_COUNT_TO_REBUILD = MINIMUM_CHANGES_COUNT_TO_REBUILD
    handler.DEFAULT_SLEEP_TIME = DEFAULT_SLEEP_TIME
    cleared_message = await handler.get_msg_ready_for_processing()

    assert spy.call_count == 1
    assert cleared_message == {"objects": list()}

    stmt = select(HierarchyRebuildOrder)
    rebuild_order_from_db = await session.execute(stmt)
    rebuild_order_from_db = rebuild_order_from_db.scalars().all()

    assert len(rebuild_order_from_db) == 1
    order_item = rebuild_order_from_db[0]
    assert order_item.hierarchy_id == hierarchy_from_db.id
    assert order_item.on_rebuild is False
