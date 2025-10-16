import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from common_utils.hierarchy_builder import HierarchyBuilder
from grpc_config.protobuf import mo_info_pb2
from schemas.hier_schemas import Hierarchy, Level, Obj
from schemas.main_base_connector import Base

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
        "p_id": 2,
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
    7: {
        "mo_id": 4,
        "tprm_values": {
            1: "param_type value not unique",
            2: "additional_params value 1",
            3: "1.1",
            4: "1.2",
        },
        "p_id": 1,
    },
    8: {
        "mo_id": 5,
        "tprm_values": {
            1: "param_type value not unique",
            2: "additional_params value 2",
            3: "2.1",
            4: "2.2",
        },
        "p_id": 2,
    },
    9: {
        "mo_id": 6,
        "tprm_values": {
            1: "param_type value not unique",
            2: "additional_params value 3",
            3: "3.1",
            4: "3.2",
        },
        "p_id": 3,
    },
}


MO_FOR_TMO_1 = [MO_BD[1], MO_BD[2], MO_BD[3]]
MO_FOR_TMO_2 = [MO_BD[4], MO_BD[5], MO_BD[6]]
MO_FOR_TMO_3 = [MO_BD[7], MO_BD[8], MO_BD[9]]

LEVEL_1_STAGE_1_NAME = "Level 1 stage 1"
LEVEL_2_STAGE_2_NAME = "Level 2 stage 2"
LEVEL_3_STAGE_3_NAME = "Level 3 stage 3"


INVENTORY_RESPONCE_FOR_LEVEL_1 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_1
]
INVENTORY_RESPONCE_FOR_LEVEL_2 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_2
]
INVENTORY_RESPONCE_FOR_LEVEL_3 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_3
]


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession, mocker):
    hierarchy = Hierarchy(name="Test hierarchy", author="Admin")
    session.add(hierarchy)
    await session.flush()
    level_1 = Level(
        hierarchy_id=hierarchy.id,
        name=LEVEL_1_STAGE_1_NAME,
        level=1,
        object_type_id=1,
        is_virtual=False,
        param_type_id=1,
        additional_params_id=2,
        latitude_id=3,
        longitude_id=4,
        author="Admin",
    )
    session.add(level_1)
    await session.flush()
    level_2 = Level(
        hierarchy_id=hierarchy.id,
        name=LEVEL_2_STAGE_2_NAME,
        level=2,
        object_type_id=2,
        is_virtual=False,
        param_type_id=1,
        additional_params_id=2,
        latitude_id=3,
        longitude_id=4,
        author="Admin",
        parent_id=level_1.id,
    )
    session.add(level_2)
    level_3 = Level(
        hierarchy_id=hierarchy.id,
        name=LEVEL_3_STAGE_3_NAME,
        level=3,
        object_type_id=3,
        is_virtual=True,
        param_type_id=1,
        additional_params_id=2,
        latitude_id=3,
        longitude_id=4,
        author="Admin",
        parent_id=None,
    )
    session.add(level_3)
    await session.commit()

    INVENTORY_DB = {
        level_1.object_type_id: INVENTORY_RESPONCE_FOR_LEVEL_1,
        level_2.object_type_id: INVENTORY_RESPONCE_FOR_LEVEL_2,
        level_3.object_type_id: INVENTORY_RESPONCE_FOR_LEVEL_3,
    }

    async def mocked_async_generator(level):
        res = INVENTORY_DB[level.object_type_id]
        for item in res:
            yield item

    mocker.patch(
        "common_utils.hierarchy_builder.HierarchyBuilder.get_data_for_level",
        side_effect=mocked_async_generator,
    )

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
async def test_can_create_real_nodes_for_one_level(session: AsyncSession):
    """TEST Creates real nodes for hierarchy real level if there are MO in Inventory with tmo_id == level.tmo_id.
    First stage nodes has no parents even if particular MO has."""

    stm = select(Hierarchy)
    hierarchy_from_db = await session.execute(stm)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    hierarchy = HierarchyBuilder(
        hierarchy_id=hierarchy_from_db.id, db_session=session
    )
    await hierarchy.build_hierarchy()

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level = await session.execute(stm)
    level = level.scalars().first()

    stm = select(Obj).where(
        Obj.hierarchy_id == hierarchy.hierarchy_id, Obj.level_id == level.id
    )
    res = await session.execute(stm)
    res = res.scalars().all()
    for node in res:
        mo_data = MO_BD[node.object_id]["tprm_values"]

        assert node.level == level.level
        assert node.hierarchy_id == level.hierarchy_id
        assert node.object_type_id == level.object_type_id
        assert node.key == mo_data.get(level.param_type_id, "None")
        assert node.additional_params == mo_data.get(
            level.additional_params_id, None
        )
        assert str(node.latitude) == mo_data.get(level.latitude_id, "None")
        assert str(node.longitude) == mo_data.get(level.longitude_id, "None")
        assert node.parent_id is None


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_virtual_nodes_for_one_level(session: AsyncSession):
    """TEST Creates virtual nodes for hierarchy virtual level if there are MO in Inventory with tmo_id == level.tmo_id.
    First stage virtual nodes has no parents even if particular MO has."""

    stm = select(Hierarchy)
    hierarchy_from_db = await session.execute(stm)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level = await session.execute(stm)
    level = level.scalars().first()

    level.is_virtual = True
    session.add(level)

    await session.commit()

    hierarchy = HierarchyBuilder(
        hierarchy_id=hierarchy_from_db.id, db_session=session
    )
    await hierarchy.build_hierarchy()

    stm = select(Obj).where(
        Obj.hierarchy_id == hierarchy.hierarchy_id, Obj.level_id == level.id
    )
    res = await session.execute(stm)
    res = res.scalars().all()

    for node in res:
        assert node.key is not None
        assert node.additional_params is None
        assert node.latitude is None
        assert node.longitude is None
        assert node.level == level.level
        assert node.object_id is None
        assert node.hierarchy_id == level.hierarchy_id
        assert node.object_type_id == level.object_type_id
        assert node.parent_id is None


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_virtual_nodes_with_unique_key_for_one_level(
    session: AsyncSession,
):
    """TEST Creates virtual nodes with unique key value for hierarchy virtual level if there are MO in
    Inventory with tmo_id == level.tmo_id."""
    stm = delete(Level).where(Level.name != LEVEL_3_STAGE_3_NAME)
    await session.execute(stm)

    stm = select(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    level = await session.execute(stm)
    level = level.scalars().first()

    assert level.is_virtual is True

    stm = select(Hierarchy)
    hierarchy_from_db = await session.execute(stm)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    hierarchy = HierarchyBuilder(
        hierarchy_id=hierarchy_from_db.id, db_session=session
    )
    await hierarchy.build_hierarchy()

    stm = select(Obj).where(Obj.hierarchy_id == hierarchy.hierarchy_id)
    res = await session.execute(stm)
    res = res.scalars().all()

    key_values = set(item["tprm_values"][1] for item in MO_FOR_TMO_3)

    assert len(res) == len(key_values)
    for node in res:
        assert node.key in key_values


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_nodes_from_two_levels_on_the_same_stage(
    session: AsyncSession,
):
    """TEST Creates nodes based on two levels on the same stage is successful."""
    stm = delete(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    await session.execute(stm)

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level_1 = await session.execute(stm)
    level_1 = level_1.scalars().first()

    stm = select(Level).where(Level.name == LEVEL_2_STAGE_2_NAME)
    level_2 = await session.execute(stm)
    level_2 = level_2.scalars().first()

    level_2.is_virtual = True

    session.add(level_2)
    await session.commit()

    stm = select(Hierarchy)
    hierarchy_from_db = await session.execute(stm)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    level_2_2 = Level(
        hierarchy_id=hierarchy_from_db.id,
        name="New Level 2 stage 2",
        level=2,
        object_type_id=2,
        is_virtual=True,
        param_type_id=2,
        additional_params_id=1,
        latitude_id=3,
        longitude_id=4,
        author="Admin",
        parent_id=level_1.id,
    )
    session.add(level_2_2)
    await session.commit()

    hierarchy = HierarchyBuilder(
        hierarchy_id=hierarchy_from_db.id, db_session=session
    )
    await hierarchy.build_hierarchy()

    stm = select(Obj).where(Obj.level_id == level_2.id)
    res = await session.execute(stm)
    res = res.scalars().all()
    assert len(res) > 0

    stm = select(Obj).where(Obj.level_id == level_2_2.id)
    res = await session.execute(stm)
    res = res.scalars().all()
    assert len(res) > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_not_unique_virtual_nodes_on_the_same_stage_in_different_branches(
    session: AsyncSession,
):
    """TEST Can create not unique virtual nodes on the same stage in different branches."""
    stm = delete(Level).where(Level.name != LEVEL_1_STAGE_1_NAME)
    await session.execute(stm)

    stm = select(Hierarchy)
    hierarchy_from_db = await session.execute(stm)
    hierarchy_from_db = hierarchy_from_db.scalars().first()

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level_1 = await session.execute(stm)
    level_1 = level_1.scalars().first()

    new_level_2 = Level(
        hierarchy_id=hierarchy_from_db.id,
        name="New Level 2 stage 2",
        level=2,
        object_type_id=3,
        is_virtual=True,
        param_type_id=2,
        additional_params_id=1,
        latitude_id=3,
        longitude_id=4,
        author="Admin",
        parent_id=level_1.id,
    )

    session.add(new_level_2)
    await session.commit()

    hierarchy = HierarchyBuilder(
        hierarchy_id=hierarchy_from_db.id, db_session=session
    )
    await hierarchy.build_hierarchy()

    count_of_items = len(MO_FOR_TMO_3)
    key_values = set(item["tprm_values"][1] for item in MO_FOR_TMO_3)

    assert count_of_items > len(key_values)

    stm = select(Obj).where(Obj.level_id == new_level_2.id)
    res = await session.execute(stm)
    res = res.scalars().all()

    assert len(res) == count_of_items


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_connected_nodes_based_on_real_levels(
    session: AsyncSession,
):
    """TEST Can create connected nodes based on two or more real levels."""

    stm = delete(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    await session.execute(stm)

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level_1 = await session.execute(stm)
    level_1 = level_1.scalars().first()
    level_1.is_virtual = False
    session.add(level_1)

    stm = select(Level).where(Level.name == LEVEL_2_STAGE_2_NAME)
    level_2 = await session.execute(stm)
    level_2 = level_2.scalars().first()
    level_2.is_virtual = False
    session.add(level_2)

    await session.commit()

    stm = select(Hierarchy)
    hierarchy = await session.execute(stm)
    hierarchy = hierarchy.scalars().first()

    h_builder = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy.id)
    await h_builder.build_hierarchy()

    stm = select(Obj.id).where(Obj.level_id == level_1.id)
    all_nodes_ids_of_1_level = await session.execute(stm)
    all_nodes_ids_of_1_level = all_nodes_ids_of_1_level.scalars().all()

    all_nodes_ids_of_1_level = {k: k for k in all_nodes_ids_of_1_level}

    assert len(all_nodes_ids_of_1_level) > 0

    stm = select(Obj.parent_id).where(Obj.level_id == level_2.id)
    all_nodes_parent_ids_of_2_level = await session.execute(stm)
    all_nodes_parent_ids_of_2_level = (
        all_nodes_parent_ids_of_2_level.scalars().all()
    )

    for node in all_nodes_parent_ids_of_2_level:
        assert node in all_nodes_ids_of_1_level


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_connected_nodes_based_on_virtual_levels(
    session: AsyncSession,
):
    """TEST Can create connected nodes based on two or more virtual levels."""
    stm = delete(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    await session.execute(stm)

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level_1 = await session.execute(stm)
    level_1 = level_1.scalars().first()
    level_1.is_virtual = True
    session.add(level_1)

    stm = select(Level).where(Level.name == LEVEL_2_STAGE_2_NAME)
    level_2 = await session.execute(stm)
    level_2 = level_2.scalars().first()
    level_2.is_virtual = True
    session.add(level_2)

    await session.commit()

    stm = select(Hierarchy)
    hierarchy = await session.execute(stm)
    hierarchy = hierarchy.scalars().first()

    h_builder = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy.id)
    await h_builder.build_hierarchy()

    stm = select(Obj.id).where(Obj.level_id == level_1.id)
    all_nodes_ids_of_1_level = await session.execute(stm)
    all_nodes_ids_of_1_level = all_nodes_ids_of_1_level.scalars().all()

    all_nodes_ids_of_1_level = {k: k for k in all_nodes_ids_of_1_level}

    assert len(all_nodes_ids_of_1_level) > 0

    stm = select(Obj.parent_id).where(Obj.level_id == level_2.id)
    all_nodes_parent_ids_of_2_level = await session.execute(stm)
    all_nodes_parent_ids_of_2_level = (
        all_nodes_parent_ids_of_2_level.scalars().all()
    )

    for node in all_nodes_parent_ids_of_2_level:
        assert node in all_nodes_ids_of_1_level


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_connected_nodes_based_on_virtual_and_real_levels(
    session: AsyncSession,
):
    """TEST Can create connected nodes based on first virtual and second real."""
    stm = delete(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    await session.execute(stm)

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level_1 = await session.execute(stm)
    level_1 = level_1.scalars().first()
    level_1.is_virtual = True
    session.add(level_1)

    stm = select(Level).where(Level.name == LEVEL_2_STAGE_2_NAME)
    level_2 = await session.execute(stm)
    level_2 = level_2.scalars().first()
    level_2.is_virtual = False
    session.add(level_2)

    await session.commit()

    stm = select(Hierarchy)
    hierarchy = await session.execute(stm)
    hierarchy = hierarchy.scalars().first()

    h_builder = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy.id)
    await h_builder.build_hierarchy()

    stm = select(Obj.id).where(Obj.level_id == level_1.id)
    all_nodes_ids_of_1_level = await session.execute(stm)
    all_nodes_ids_of_1_level = all_nodes_ids_of_1_level.scalars().all()

    all_nodes_ids_of_1_level = {k: k for k in all_nodes_ids_of_1_level}

    assert len(all_nodes_ids_of_1_level) > 0

    stm = select(Obj.parent_id).where(Obj.level_id == level_2.id)
    all_nodes_parent_ids_of_2_level = await session.execute(stm)
    all_nodes_parent_ids_of_2_level = (
        all_nodes_parent_ids_of_2_level.scalars().all()
    )

    for node in all_nodes_parent_ids_of_2_level:
        assert node in all_nodes_ids_of_1_level


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_connected_nodes_based_on_real_and_virtual_levels(
    session: AsyncSession,
):
    """TEST Can create connected nodes based on first real and second virtual."""
    stm = delete(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    await session.execute(stm)

    stm = select(Level).where(Level.name == LEVEL_1_STAGE_1_NAME)
    level_1 = await session.execute(stm)
    level_1 = level_1.scalars().first()
    level_1.is_virtual = False
    session.add(level_1)

    stm = select(Level).where(Level.name == LEVEL_2_STAGE_2_NAME)
    level_2 = await session.execute(stm)
    level_2 = level_2.scalars().first()
    level_2.is_virtual = True
    session.add(level_2)

    await session.commit()

    stm = select(Hierarchy)
    hierarchy = await session.execute(stm)
    hierarchy = hierarchy.scalars().first()

    h_builder = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy.id)
    await h_builder.build_hierarchy()

    stm = select(Obj.id).where(Obj.level_id == level_1.id)
    all_nodes_ids_of_1_level = await session.execute(stm)
    all_nodes_ids_of_1_level = all_nodes_ids_of_1_level.scalars().all()

    all_nodes_ids_of_1_level = {k: k for k in all_nodes_ids_of_1_level}

    assert len(all_nodes_ids_of_1_level) > 0

    stm = select(Obj.parent_id).where(Obj.level_id == level_2.id)
    all_nodes_parent_ids_of_2_level = await session.execute(stm)
    all_nodes_parent_ids_of_2_level = (
        all_nodes_parent_ids_of_2_level.scalars().all()
    )

    for node in all_nodes_parent_ids_of_2_level:
        assert node in all_nodes_ids_of_1_level


@pytest.mark.asyncio(loop_scope="session")
async def test_can_create_connected_nodes_based_on_real_virtual_real_levels(
    session: AsyncSession,
):
    """TEST Can create connected nodes based on first real level, second virtual level
    and third real level."""
    stm = select(Level).where(Level.name == LEVEL_2_STAGE_2_NAME)
    level_2 = await session.execute(stm)
    level_2 = level_2.scalars().first()
    level_2.is_virtual = True
    session.add(level_2)

    stm = select(Level).where(Level.name == LEVEL_3_STAGE_3_NAME)
    level_3 = await session.execute(stm)
    level_3 = level_3.scalars().first()
    level_3.is_virtual = True
    level_3.object_type_id = level_2.object_type_id
    level_3.parent_id = level_2.id
    session.add(level_3)

    await session.commit()

    stm = select(Hierarchy)
    hierarchy = await session.execute(stm)
    hierarchy = hierarchy.scalars().first()

    h_builder = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy.id)
    await h_builder.build_hierarchy()

    stm = select(Obj.id).where(Obj.level == 1)
    all_nodes_ids_of_1_level = await session.execute(stm)
    all_nodes_ids_of_1_level = all_nodes_ids_of_1_level.scalars().all()

    all_nodes_ids_of_1_level = {k: k for k in all_nodes_ids_of_1_level}

    assert len(all_nodes_ids_of_1_level) > 0

    stm = select(Obj).where(Obj.level_id == level_2.id)
    all_nodes_parent_ids_of_2_level = await session.execute(stm)
    all_nodes_parent_ids_of_2_level = (
        all_nodes_parent_ids_of_2_level.scalars().all()
    )

    assert len(all_nodes_parent_ids_of_2_level) > 0

    all_nodes_ids_of_2_level = {
        item.id: item for item in all_nodes_parent_ids_of_2_level
    }

    for node in all_nodes_parent_ids_of_2_level:
        assert node.parent_id in all_nodes_ids_of_1_level

    stm = select(Obj).where(Obj.level_id == level_3.id)
    all_nodes_parent_ids_of_3_level = await session.execute(stm)
    all_nodes_parent_ids_of_3_level = (
        all_nodes_parent_ids_of_3_level.scalars().all()
    )

    for node in all_nodes_parent_ids_of_3_level:
        assert node.parent_id in all_nodes_ids_of_2_level
