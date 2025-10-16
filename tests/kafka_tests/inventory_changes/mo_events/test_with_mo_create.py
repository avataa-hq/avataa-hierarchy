import logging

from kafka_tests.inventory_changes.utils_for_mo_events import (
    add_hierarchy_to_session,
    add_level_to_session,
    add_node_to_session,
    get_node_by_level_id_and_mo_id,
)
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from kafka_config.msg.mo_msg import on_create_mo
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from schemas.main_base_connector import Base
from services.hierarchy.hierarchy_builder.configs import (
    DEFAULT_KEY_OF_NULL_NODE,
)

# default hierarchy data
DEFAULT_HIERARCHY_NAME_FOR_TESTS = "Default Hierarchy"

TMO_1 = {
    1: {
        "id": 1,
        "name": "MO 1",
        "tmo_id": 1,
        "1": "node 1 level 1",
        "2": "node 3 level 2",
        "3": "node 6 level 3",
    },
    11: {
        "id": 11,
        "name": "MO 11",
        "tmo_id": 1,
        "1": "node 1 level 1",
        "2": "node 3 level 2",
        "3": "node 7 level 3",
    },
    111: {
        "id": 111,
        "name": "MO 111",
        "tmo_id": 1,
        "1": "node 1 level 1",
        "2": "node 4 level 2",
        "3": "node 8 level 3",
    },
    1111: {
        "id": 1111,
        "name": "MO 1111",
        "tmo_id": 1,
        "1": "node 2 level 1",
        "2": "node 5 level 2",
        "3": "node 9 level 3",
    },
}

TMO_2 = {
    2: {
        "id": 2,
        "name": "MO 2",
        "tmo_id": 2,
        "p_id": TMO_1[1]["id"],
        "4": "node 10 level 4",
    },
    22: {
        "id": 22,
        "name": "MO 22",
        "tmo_id": 2,
        "p_id": TMO_1[1]["id"],
        "4": "node 11 level 4",
    },
    222: {"id": 222, "name": "MO 222", "tmo_id": 2, "4": "node 12 level 4"},
}

TMO_21 = {
    21: {
        "id": 21,
        "name": "MO 21",
        "tmo_id": 21,
        "41": DEFAULT_KEY_OF_NULL_NODE,
    },
    2121: {
        "id": 2121,
        "name": "MO 2121",
        "tmo_id": 21,
        "p_id": TMO_1[1111]["id"],
        "41": "node 14 level 5",
    },
    212121: {
        "id": 212121,
        "name": "MO 212121",
        "tmo_id": 21,
        "41": "node 15 level 5",
    },
}

TMO_3 = {
    3: {
        "id": 3,
        "name": "MO 3",
        "tmo_id": 3,
        "p_id": TMO_2[2]["id"],
        "5": "node 16 level 6",
    },
    33: {
        "id": 33,
        "name": "MO 33",
        "tmo_id": 3,
        "p_id": TMO_2[2]["id"],
        "5": "node 17 level 6",
    },
    333: {
        "id": 333,
        "name": "MO 333",
        "tmo_id": 3,
        "p_id": TMO_2[2]["id"],
        "5": "node 17 level 6",
    },
}

TMO_31 = {
    31: {
        "id": 31,
        "name": "MO 31",
        "tmo_id": 31,
        "p_id": TMO_21[2121]["id"],
        "51": "node 18 level 7",
    },
    3131: {
        "id": 3131,
        "name": "MO 3131",
        "tmo_id": 31,
        "p_id": TMO_21[2121]["id"],
        "51": "node 19 level 7",
    },
}

TMO_4 = {
    4: {
        "id": 4,
        "name": "MO 4",
        "tmo_id": 4,
        "p_id": TMO_3[33]["id"],
        "6": "node 20 level 8",
    },
    44: {
        "id": 44,
        "name": "MO 44",
        "tmo_id": 4,
        "p_id": TMO_3[333]["id"],
        "6": "node 21 level 8",
    },
}

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession):
    hierarchy = await add_hierarchy_to_session(
        session, hierarchy_name=DEFAULT_HIERARCHY_NAME_FOR_TESTS
    )
    level1 = await add_level_to_session(
        session,
        level_name="TMO 1 - Virtual level - param_type_id - 1",
        is_virtual=True,
        hierarchy_id=hierarchy.id,
        obj_type_id=1,
        level=1,
        param_type_id=1,
    )

    level2 = await add_level_to_session(
        session,
        level_name="TMO 1 - Virtual level - param_type_id - 2",
        is_virtual=True,
        hierarchy_id=hierarchy.id,
        obj_type_id=1,
        level=2,
        param_type_id=2,
        parent_id=level1.id,
    )

    level3 = await add_level_to_session(
        session,
        level_name="TMO 1 - Real level - param_type_id - 3",
        is_virtual=False,
        hierarchy_id=hierarchy.id,
        obj_type_id=1,
        level=3,
        param_type_id=3,
        parent_id=level2.id,
    )

    # two levels on the same depth - start

    level4 = await add_level_to_session(
        session,
        level_name="TMO 2 - Real level - param_type_id - 4",
        is_virtual=False,
        hierarchy_id=hierarchy.id,
        obj_type_id=2,
        level=4,
        param_type_id=4,
        parent_id=level3.id,
    )

    level5 = await add_level_to_session(
        session,
        level_name="TMO 21 - Virtual level - param_type_id - 41",
        is_virtual=True,
        hierarchy_id=hierarchy.id,
        obj_type_id=21,
        level=4,
        param_type_id=41,
        parent_id=level3.id,
    )
    # two levels on the same depth - end
    # two levels on the same depth - start

    level6 = await add_level_to_session(
        session,
        level_name="TMO 3 - Virtual level - param_type_id - 5",
        is_virtual=False,
        hierarchy_id=hierarchy.id,
        obj_type_id=3,
        level=5,
        param_type_id=5,
        parent_id=level4.id,
    )

    level7 = await add_level_to_session(
        session,
        level_name="TMO 31 - Virtual level - param_type_id - 51",
        is_virtual=False,
        hierarchy_id=hierarchy.id,
        obj_type_id=31,
        level=5,
        param_type_id=51,
        parent_id=level5.id,
    )
    # two levels on the same depth - end

    level8 = await add_level_to_session(
        session,
        level_name="TMO 4 - Virtual level - param_type_id - 6",
        is_virtual=False,
        hierarchy_id=hierarchy.id,
        obj_type_id=4,
        level=6,
        param_type_id=6,
        parent_id=level6.id,
    )

    # node data for level 1
    MO_data_node_1_level_1 = [TMO_1[1], TMO_1[11], TMO_1[111]]
    node_1_level_1 = await add_node_to_session(
        session=session,
        level=level1,
        mo_data=MO_data_node_1_level_1,
        parent_node=None,
    )

    MO_data_node_2_level_1 = [TMO_1[1111]]
    node_2_level_1 = await add_node_to_session(
        session=session,
        level=level1,
        mo_data=MO_data_node_2_level_1,
        parent_node=None,
    )

    # node data for level 2
    MO_data_node_3_level_2 = [TMO_1[1], TMO_1[11]]
    node_3_level_2 = await add_node_to_session(
        session=session,
        level=level2,
        mo_data=MO_data_node_3_level_2,
        parent_node=node_1_level_1,
    )

    MO_data_node_4_level_2 = [TMO_1[111]]
    node_4_level_2 = await add_node_to_session(
        session=session,
        level=level2,
        mo_data=MO_data_node_4_level_2,
        parent_node=node_1_level_1,
    )

    MO_data_node_5_level_2 = [TMO_1[1111]]
    node_5_level_2 = await add_node_to_session(
        session=session,
        level=level2,
        mo_data=MO_data_node_5_level_2,
        parent_node=node_2_level_1,
    )

    # node data for level 3 REal level
    node_6_level_3 = await add_node_to_session(
        session=session,
        level=level3,
        mo_data=[TMO_1[1]],
        parent_node=node_3_level_2,
    )

    node_7_level_3 = await add_node_to_session(
        session=session,
        level=level3,
        mo_data=[TMO_1[11]],
        parent_node=node_3_level_2,
    )

    # node_8_level_3
    await add_node_to_session(
        session=session,
        level=level3,
        mo_data=[TMO_1[111]],
        parent_node=node_4_level_2,
    )

    node_9_level_3 = await add_node_to_session(
        session=session,
        level=level3,
        mo_data=[TMO_1[1111]],
        parent_node=node_5_level_2,
    )

    # node data for depth 4 REal level tmo 2 - start
    node_10_level_4 = await add_node_to_session(
        session=session,
        level=level4,
        mo_data=[TMO_2[2]],
        parent_node=node_6_level_3,
    )

    # node_11_level_4
    await add_node_to_session(
        session=session,
        level=level4,
        mo_data=[TMO_2[22]],
        parent_node=node_6_level_3,
    )

    # node_12_level_4
    await add_node_to_session(
        session=session,
        level=level4,
        mo_data=[TMO_2[222]],
        parent_node=node_7_level_3,
    )

    # node data for depth 4 REal level tmo 2 - end
    # node data for depth 4 Virtual level tmo 21 - start
    # node_13_level_5
    await add_node_to_session(
        session=session,
        level=level5,
        mo_data=[TMO_21[21]],
        parent_node=node_7_level_3,
    )

    node_14_level_5 = await add_node_to_session(
        session=session,
        level=level5,
        mo_data=[TMO_21[2121]],
        parent_node=node_9_level_3,
    )

    # node_15_level_5
    await add_node_to_session(
        session=session,
        level=level5,
        mo_data=[TMO_21[212121]],
        parent_node=node_9_level_3,
    )
    # node data for depth 4 REal level tmo 21- end

    # node data for depth 5 Virtual level tmo 3 - start
    # node_16_level_6
    await add_node_to_session(
        session=session,
        level=level6,
        mo_data=[TMO_3[3]],
        parent_node=node_10_level_4,
    )

    node_17_level_6 = await add_node_to_session(
        session=session,
        level=level6,
        mo_data=[TMO_3[33]],
        parent_node=node_10_level_4,
    )
    # node data for depth 5 Virtual level tmo 3 - end

    # node data for depth 5 Real level tmo 31 - start
    # node_18_level_7
    await add_node_to_session(
        session=session,
        level=level7,
        mo_data=[TMO_31[31]],
        parent_node=node_14_level_5,
    )

    # node_19_level_7
    await add_node_to_session(
        session=session,
        level=level7,
        mo_data=[TMO_31[3131]],
        parent_node=node_14_level_5,
    )
    # node data for depth 5 Real level tmo 31 - end

    # node data for depth 6 Virtual level tmo 4 - start
    # node_20_level_8
    await add_node_to_session(
        session=session,
        level=level8,
        mo_data=[TMO_4[4]],
        parent_node=node_17_level_6,
    )

    # node_21_level_8
    await add_node_to_session(
        session=session,
        level=level8,
        mo_data=[TMO_4[44]],
        parent_node=node_17_level_6,
    )

    # node data for depth 6 Real level tmo 4 - end
    await session.flush()
    yield


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(session: AsyncSession):
    yield
    await session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


async def get_default_hierarchy_by_name(session, hierarchy_name=None):
    """Returns hierarchy by hierarchy name"""
    if hierarchy_name is None:
        hierarchy_name = DEFAULT_HIERARCHY_NAME_FOR_TESTS

    stmt = select(Hierarchy).where(Hierarchy.name == hierarchy_name)
    res = await session.execute(stmt)
    return res.scalars().first()


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_1(session: AsyncSession, mock_grpc_response):
    """TEST With MO:created if MO has TMO that not uses in hierarchy levels:
    - does not create Obj
    - does not create NodeData"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    stmt = select(Obj).where(Obj.hierarchy_id == hierarchy.id)
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()

    mo_id = 9
    kafka_msg = {
        "objects": [{"id": 9, "tmo_id": 99, "p_id": None, "active": True}]
    }

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(Obj.hierarchy_id == hierarchy.id)
    nodes_after = await session.execute(stmt)
    nodes_after = nodes_after.scalars().all()
    nodes_after_cache = {node.id: node for node in nodes_after}

    assert len(nodes_after) == len(nodes_before)
    for node_before in nodes_before:
        node_after = nodes_after_cache[node_before.id]
        assert node_after is not None
        assert node_after.key == node_before.key

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 0
    # check node data after - end


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_2(session: AsyncSession):
    """TEST With MO:created. If exist real levels with created MO.tmo_id:
    - creates real node with default key
    - created NodeData
    - increase parent.child_count by 1"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_real_level = 2
    new_mo_id = 9999
    name = "Node1"
    existing_p_id = TMO_1[1]["id"]
    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id_of_real_level,
                "p_id": existing_p_id,
                "name": name,
                "active": True,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_real_level,
        Level.is_virtual == False,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.is_virtual is False

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    assert parent_node is not None

    # check if parent.child_count - before
    p_child_count_before = parent_node.child_count

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == new_mo_id
    )
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()

    assert node_before is None

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == new_mo_id
    )
    node_after = await session.execute(stmt)
    node_after = node_after.scalars().first()

    assert node_after is not None
    assert node_after.key == DEFAULT_KEY_OF_NULL_NODE
    assert node_after.object_type_id == level.object_type_id
    assert node_after.level == level.level
    assert node_after.level_id == level.id
    assert node_after.parent_id == parent_node.id

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    assert node_data_after is not None
    assert node_data_after.level_id == level.id
    assert node_data_after.node_id == node_after.id
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_tmo_id == existing_tmo_id_of_real_level
    assert node_data_after.mo_p_id == existing_p_id
    # check node data after - end

    # check if parent.child_count - before
    await session.refresh(parent_node)
    assert parent_node.child_count - p_child_count_before == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_3(session: AsyncSession):
    """TEST With MO:created. If exist real levels with created MO.tmo_id on the depth more than 0,
    and created MO has no p_id:
     - creates new node with p_id == None
     - creates NodeData"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_real_level = 2
    new_mo_id = 9999
    name = "Node1"
    not_existing_p_id = 100000000000

    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id_of_real_level,
                "p_id": not_existing_p_id,
                "active": True,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_real_level,
        Level.is_virtual == False,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.level > 0
    assert level.is_virtual is False

    no_exists_parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=not_existing_p_id
    )
    assert no_exists_parent_node is None

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == new_mo_id
    )
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    assert node_before is None

    await on_create_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == new_mo_id
    )
    node_after = await session.execute(stmt)
    node_after = node_after.scalars().first()

    assert node_after is not None
    assert node_after.key == DEFAULT_KEY_OF_NULL_NODE
    assert node_after.object_type_id == level.object_type_id
    assert node_after.level == level.level
    assert node_after.level_id == level.id
    assert node_after.parent_id is None

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    assert node_data_after is not None
    assert node_data_after.level_id == level.id
    assert node_data_after.node_id == node_after.id
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_tmo_id == existing_tmo_id_of_real_level
    assert node_data_after.mo_p_id == not_existing_p_id
    # check node data after - end


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_4(session: AsyncSession):
    """TEST With MO:created.
    If exist virtual levels with created MO.tmo_id and there are no virtual node with key == DEFAULT_KEY_OF_NULL_NODE:
    - creates virtual node with key == DEFAULT_KEY_OF_NULL_NODE
    - creates NodeData
    - increases parent.child_count"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_virtual_level = 21
    new_mo_id = 9999
    name = "Node1"
    existing_p_id = TMO_1[1111]["id"]

    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id_of_virtual_level,
                "p_id": existing_p_id,
                "active": True,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_virtual_level,
        Level.is_virtual == True,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.level > 0
    assert level.is_virtual is True

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    # check if default -node does not exists
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == parent_node.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_not_exists = await session.execute(stmt)
    default_node_not_exists = default_node_not_exists.scalars().first()
    assert default_node_not_exists is None

    assert parent_node is not None

    # check if parent.child_count - before
    p_child_count_before = parent_node.child_count

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)

    new_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=new_mo_id
    )

    assert new_node is not None
    assert new_node.key == DEFAULT_KEY_OF_NULL_NODE
    assert new_node.object_type_id == level.object_type_id
    assert new_node.level == level.level
    assert new_node.level_id == level.id
    assert new_node.parent_id == parent_node.id
    assert new_node.object_id is None

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    assert node_data_after is not None
    assert node_data_after.level_id == level.id
    assert node_data_after.node_id == new_node.id
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_tmo_id == existing_tmo_id_of_virtual_level
    assert node_data_after.mo_p_id == existing_p_id
    # check node data after - end

    # check if parent.child_count - before
    await session.refresh(parent_node)
    assert parent_node.child_count - p_child_count_before == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_5(session: AsyncSession):
    """TEST With MO:created.
    If exist virtual levels with created MO.tmo_id and there are exists virtual node with
    key == DEFAULT_KEY_OF_NULL_NODE:
     - does not create new node
     - creates NodeData
     - does not change parent.child_count"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_virtual_level = 21
    new_mo_id = 9999
    existing_p_id = TMO_1[11]["id"]
    name = "Node1"

    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id_of_virtual_level,
                "p_id": existing_p_id,
                "active": True,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_virtual_level,
        Level.is_virtual == True,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.level > 0
    assert level.is_virtual

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    assert parent_node is not None
    # check if parent.child_count - before
    p_child_count_before = parent_node.child_count

    # check if default -node exists
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == parent_node.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_before = await session.execute(stmt)
    default_node_before = default_node_before.scalars().all()
    assert len(default_node_before) == 1
    assert default_node_before[0] is not None
    default_node_before = default_node_before[0]

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == parent_node.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_after = await session.execute(stmt)
    default_node_after = default_node_after.scalars().all()
    assert len(default_node_after) == 1
    assert default_node_after[0] is not None
    default_node_after = default_node_after[0]

    assert default_node_after.id == default_node_before.id
    assert default_node_after.key == default_node_before.key
    assert default_node_after.object_id == default_node_before.object_id
    assert (
        default_node_after.object_type_id == default_node_before.object_type_id
    )
    assert (
        default_node_after.additional_params
        == default_node_before.additional_params
    )
    assert default_node_after.hierarchy_id == default_node_before.hierarchy_id
    assert default_node_after.level == default_node_before.level
    assert default_node_after.parent_id == default_node_before.parent_id
    assert default_node_after.child_count == default_node_before.child_count
    assert default_node_after.level_id == default_node_before.level_id

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    assert node_data_after is not None
    assert node_data_after.level_id == level.id
    assert node_data_after.node_id == default_node_before.id
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_tmo_id == existing_tmo_id_of_virtual_level
    assert node_data_after.mo_p_id == existing_p_id
    # check node data after - end

    # check if parent.child_count - before
    await session.refresh(parent_node)
    assert parent_node.child_count - p_child_count_before == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_6(session: AsyncSession):
    """TEST With MO:created. If exist virtual levels with created MO.tmo_id on the depth more than 0,
    and created MO has no p_id:
    - creates new node with p_id == None
    - creates NodeData"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_virtual_level = 21
    new_mo_id = 9999
    not_existing_p_id = 44444444444
    name = "Node1"

    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id_of_virtual_level,
                "p_id": not_existing_p_id,
                "active": True,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_virtual_level,
        Level.is_virtual == True,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.level > 0
    assert level.is_virtual

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=not_existing_p_id
    )
    assert parent_node is None

    # check if default -node exists
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == None,  # noqa
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_before = await session.execute(stmt)
    default_node_before = default_node_before.scalars().first()
    assert default_node_before is None

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == None,  # noqa
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_after = await session.execute(stmt)
    default_node_after = default_node_after.scalars().all()
    assert len(default_node_after) == 1
    assert default_node_after[0] is not None
    default_node_after = default_node_after[0]

    assert default_node_after.key == DEFAULT_KEY_OF_NULL_NODE
    assert default_node_after.object_id is None
    assert default_node_after.object_type_id == level.object_type_id
    assert default_node_after.level == level.level
    assert default_node_after.parent_id is None
    assert default_node_after.level_id == level.id

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    assert node_data_after is not None
    assert node_data_after.level_id == level.id
    assert node_data_after.node_id == default_node_after.id
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_tmo_id == existing_tmo_id_of_virtual_level
    assert node_data_after.mo_p_id == not_existing_p_id
    # check node data after - end


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_7(session: AsyncSession):
    """TEST With MO:created. If tmo_if of created node exists in order of levels - virtual, virtual, real -
    - creates one node at each level
    - creates one NodeData for each new node
    - first two parent nodes has child_count = 1"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id = 1
    new_mo_id = 9999
    name = "Node1"

    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id,
                "p_id": None,
                "active": True,
                "name": name,
            }
        ]
    }

    stmt = (
        select(Level)
        .where(
            Level.object_type_id == existing_tmo_id,
            Level.hierarchy_id == hierarchy.id,
        )
        .order_by(Level.level)
    )
    levels = await session.execute(stmt)
    levels = levels.scalars().all()

    assert len(levels) == 3

    level_1 = levels[0]
    level_2 = levels[1]
    level_3 = levels[2]

    assert level_1.is_virtual is True
    assert level_2.is_virtual is True
    assert level_3.is_virtual is False

    # nodes with default key for this levels do not exist
    stmt = (
        select(Obj)
        .where(
            Obj.key == DEFAULT_KEY_OF_NULL_NODE,
            Obj.level_id.in_([level_1.id, level_2.id, level_3.id]),
        )
        .order_by(Obj.level)
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()

    assert len(nodes_before) == 0

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)
    stmt = (
        select(Obj)
        .where(
            Obj.key == DEFAULT_KEY_OF_NULL_NODE,
            Obj.level_id.in_([level_1.id, level_2.id, level_3.id]),
        )
        .order_by(Obj.level)
    )
    nodes_after = await session.execute(stmt)
    nodes_after = nodes_after.scalars().all()

    assert len(nodes_after) == 3
    node_1 = nodes_after[0]
    node_2 = nodes_after[1]
    node_3 = nodes_after[2]

    assert node_1.parent_id is None
    assert node_2.parent_id == node_1.id
    assert node_3.parent_id == node_2.id

    # check child count
    assert node_1.child_count == 1
    assert node_2.child_count == 1
    assert node_3.child_count == 0

    # check node data before - start
    stmt = select(NodeData).where(
        NodeData.mo_id == new_mo_id, NodeData.level_id == node_1.level_id
    )
    node_data_1_after = await session.execute(stmt)
    node_data_1_after = node_data_1_after.scalars().first()

    assert node_data_1_after is not None
    assert node_data_1_after.node_id == node_1.id
    assert node_data_1_after.mo_id == new_mo_id
    assert node_data_1_after.mo_tmo_id == existing_tmo_id
    assert node_data_1_after.mo_p_id is None

    stmt = select(NodeData).where(
        NodeData.mo_id == new_mo_id, NodeData.level_id == node_2.level_id
    )
    node_data_2_after = await session.execute(stmt)
    node_data_2_after = node_data_2_after.scalars().first()

    assert node_data_2_after is not None
    assert node_data_2_after.node_id == node_2.id
    assert node_data_2_after.mo_id == new_mo_id
    assert node_data_2_after.mo_tmo_id == existing_tmo_id
    assert node_data_2_after.mo_p_id is None

    stmt = select(NodeData).where(
        NodeData.mo_id == new_mo_id, NodeData.level_id == node_3.level_id
    )
    node_data_3_after = await session.execute(stmt)
    node_data_3_after = node_data_3_after.scalars().first()

    assert node_data_3_after is not None
    assert node_data_3_after.node_id == node_3.id
    assert node_data_3_after.mo_id == new_mo_id
    assert node_data_3_after.mo_tmo_id == existing_tmo_id
    assert node_data_3_after.mo_p_id is None
    # check node data before - end


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_8(session: AsyncSession):
    """TEST With MO:created.
    If kafka msg consist of two MO instances and tmo_id of these instances exist in virtual levels
    and there are no virtual node with key == DEFAULT_KEY_OF_NULL_NODE:
    - creates only one virtual node with key == DEFAULT_KEY_OF_NULL_NODE
    - creates two NodeData
    - increases parent.child_count by 1"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_virtual_level = 21
    new_mo_id_1 = 9999
    new_mo_id_2 = 99999
    existing_p_id = TMO_1[1111]["id"]
    name_1 = "Node1"
    name_2 = "Node2"

    mo_1 = {
        "id": new_mo_id_1,
        "tmo_id": existing_tmo_id_of_virtual_level,
        "p_id": existing_p_id,
        "active": True,
        "name": name_1,
    }
    mo_2 = {
        "id": new_mo_id_2,
        "tmo_id": existing_tmo_id_of_virtual_level,
        "p_id": existing_p_id,
        "active": True,
        "name": name_2,
    }

    kafka_msg = {"objects": [mo_1, mo_2]}

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_virtual_level,
        Level.is_virtual == True,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.level > 0
    assert level.is_virtual is True

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    # check if default -node does not exists
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == parent_node.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_not_exists = await session.execute(stmt)
    default_node_not_exists = default_node_not_exists.scalars().first()
    assert default_node_not_exists is None

    assert parent_node is not None

    # check if parent.child_count - before
    p_child_count_before = parent_node.child_count

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id.in_([mo_1["id"], mo_2["id"]]))
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    await on_create_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == parent_node.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    default_node_not_exists = await session.execute(stmt)
    default_node_not_exists = default_node_not_exists.scalars().all()
    assert len(default_node_not_exists) == 1
    new_node = default_node_not_exists[0]

    assert new_node is not None
    assert new_node.key == DEFAULT_KEY_OF_NULL_NODE
    assert new_node.object_type_id == level.object_type_id
    assert new_node.level == level.level
    assert new_node.level_id == level.id
    assert new_node.parent_id == parent_node.id
    assert new_node.object_id is None

    # check node data after - start
    stmt = select(NodeData).where(NodeData.mo_id == mo_1["id"])
    node_data_mo_1 = await session.execute(stmt)
    node_data_mo_1 = node_data_mo_1.scalars().all()
    assert len(node_data_mo_1) == 1
    node_data_mo_1 = node_data_mo_1[0]

    assert node_data_mo_1.level_id == level.id
    assert node_data_mo_1.node_id == new_node.id
    assert node_data_mo_1.mo_id == mo_1["id"]
    assert node_data_mo_1.mo_tmo_id == existing_tmo_id_of_virtual_level
    assert node_data_mo_1.mo_p_id == existing_p_id

    stmt = select(NodeData).where(NodeData.mo_id == mo_2["id"])
    node_data_mo_1 = await session.execute(stmt)
    node_data_mo_1 = node_data_mo_1.scalars().all()
    assert len(node_data_mo_1) == 1
    node_data_mo_1 = node_data_mo_1[0]

    assert node_data_mo_1.level_id == level.id
    assert node_data_mo_1.node_id == new_node.id
    assert node_data_mo_1.mo_id == mo_2["id"]
    assert node_data_mo_1.mo_tmo_id == existing_tmo_id_of_virtual_level
    assert node_data_mo_1.mo_p_id == existing_p_id
    # check node data after - end

    # check if parent.child_count - before
    await session.refresh(parent_node)
    assert parent_node.child_count - p_child_count_before == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_9(session: AsyncSession):
    """TEST With MO:created.
    If the kafka msg consists of an MO instance with MO.active = False and the tmo_id of that instance exist
     in real level:
    - creates real node for this level
    - creates NodeData for this node
    - does not increase parent.child_count by 1"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_real_level = 2
    new_mo_id = 9999
    name = "Node1"
    existing_p_id = TMO_1[1]["id"]
    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": existing_tmo_id_of_real_level,
                "p_id": existing_p_id,
                "active": False,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_real_level,
        Level.is_virtual == False,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.is_virtual is False

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    assert parent_node is not None
    parent_child_count_before = parent_node.child_count

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == new_mo_id
    )
    before = await session.execute(stmt)
    before = before.scalars().first()

    assert before is None

    await on_create_mo(msg=kafka_msg, session=session)

    await session.refresh(parent_node)

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == new_mo_id
    )
    after = await session.execute(stmt)
    after = after.scalars().first()

    assert after is not None
    assert after.active is False
    assert after.parent_id == parent_node.id

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]
    assert node_data_after.mo_active is False
    # check node data before - end

    assert parent_child_count_before == parent_node.child_count


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_10(session: AsyncSession):
    """TEST With MO:created.
    If the kafka msg consists of an MO instance with MO.active = False and the tmo_id of that instance exist
     in virtual level, and there is virtual node with key = default key and active = True:
    - creates virtual node for this level with default key and active = False
    - creates NodeData for this node
    - does not increase parent.child_count by 1"""

    hierarchy = await get_default_hierarchy_by_name(session=session)

    tmo_id = TMO_21[2121]["tmo_id"]
    new_mo_id = 999
    existing_p_id = TMO_1[1111]["id"]
    name = "Node1"
    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": tmo_id,
                "p_id": existing_p_id,
                "active": False,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == tmo_id,
        Level.is_virtual == True,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.is_virtual is True

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    assert parent_node is not None
    parent_child_count_before = parent_node.child_count

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    # check virtual node with key = defaul key and active = False before - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.active == False,  # noqa
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    def_virtual_node_before = await session.execute(stmt)
    def_virtual_node_before = def_virtual_node_before.scalars().first()
    assert def_virtual_node_before is None
    # check virtual node with key = defaul key and active = False before - end

    # create virtual node with default key and active = true
    mo_id_of_default_node = 555
    virtual_node = Obj(
        key=DEFAULT_KEY_OF_NULL_NODE,
        object_type_id=tmo_id,
        hierarchy_id=level.hierarchy_id,
        level=level.level,
        parent_id=parent_node.id,
        child_count=0,
        path=None,
        level_id=level.id,
        active=True,
    )
    session.add(virtual_node)
    await session.flush()

    # create new node data
    def_n_data = NodeData(
        level_id=virtual_node.level_id,
        node_id=virtual_node.id,
        mo_id=mo_id_of_default_node,
        mo_name=f"{mo_id_of_default_node}",
        mo_tmo_id=virtual_node.object_type_id,
        mo_p_id=existing_p_id,
        active=True,
    )

    session.add(def_n_data)
    await session.commit()

    await on_create_mo(msg=kafka_msg, session=session)

    await session.refresh(parent_node)

    # check new node_data created - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_active is False
    # check new node_data created - end

    # check virtual node with key = defaul key and active = False after - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.active == False,  # noqa
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    def_virtual_node_after = await session.execute(stmt)
    def_virtual_node_after = def_virtual_node_after.scalars().first()
    assert def_virtual_node_after is not None
    assert def_virtual_node_after.active is False

    assert node_data_after.node_id == def_virtual_node_after.id
    # check virtual node with key = defaul key and active = False after - end

    assert parent_child_count_before == parent_node.child_count


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_mo_case_11(session: AsyncSession):
    """TEST With MO:created.
    If the kafka msg consists of an MO instance with MO.active = False and the tmo_id of that instance
    exist in virtual level, and there is virtual node with key = default key and active = False:
    - does not create virtual node for this level with default key and active = False
    - creates NodeData for this node
    - does not increase parent.child_count by 1"""

    hierarchy = await get_default_hierarchy_by_name(session=session)

    tmo_id = TMO_21[2121]["tmo_id"]
    new_mo_id = 999
    existing_p_id = TMO_1[1111]["id"]
    name = "Node1"
    kafka_msg = {
        "objects": [
            {
                "id": new_mo_id,
                "tmo_id": tmo_id,
                "p_id": existing_p_id,
                "active": False,
                "name": name,
            }
        ]
    }

    stmt = select(Level).where(
        Level.object_type_id == tmo_id,
        Level.is_virtual == True,  # noqa
        Level.hierarchy_id == hierarchy.id,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.is_virtual is True

    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=existing_p_id
    )
    assert parent_node is not None
    parent_child_count_before = parent_node.child_count

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 0
    # check node data before - end

    # check virtual node with key = defaul key and active = False before - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.active == False,  # noqa
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
    )
    def_virtual_node_before = await session.execute(stmt)
    def_virtual_node_before = def_virtual_node_before.scalars().first()
    assert def_virtual_node_before is None
    # check virtual node with key = defaul key and active = False before - end

    # create virtual node with default key and active = true
    mo_id_of_default_node = 555
    virtual_node = Obj(
        key=DEFAULT_KEY_OF_NULL_NODE,
        object_type_id=tmo_id,
        hierarchy_id=level.hierarchy_id,
        level=level.level,
        parent_id=parent_node.id,
        child_count=0,
        path=None,
        level_id=level.id,
        active=False,
    )
    session.add(virtual_node)
    await session.flush()

    # create new node data
    def_n_data = NodeData(
        level_id=virtual_node.level_id,
        node_id=virtual_node.id,
        mo_id=mo_id_of_default_node,
        mo_name=f"{mo_id_of_default_node}",
        mo_tmo_id=virtual_node.object_type_id,
        mo_p_id=existing_p_id,
        active=False,
    )

    session.add(def_n_data)
    await session.commit()

    # check default nodes before - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
        Obj.parent_id == parent_node.id,
    )
    res = await session.execute(stmt)
    res = res.scalars().all()
    assert len(res) == 1
    # check default nodes before - end

    await on_create_mo(msg=kafka_msg, session=session)

    # check default nodes after - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.key == DEFAULT_KEY_OF_NULL_NODE,
        Obj.parent_id == parent_node.id,
    )
    res = await session.execute(stmt)
    res = res.scalars().all()
    assert len(res) == 1
    # check default nodes after - end

    await session.refresh(parent_node)

    # check new node_data created - start
    stmt = select(NodeData).where(NodeData.mo_id == new_mo_id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]
    assert node_data_after.mo_id == new_mo_id
    assert node_data_after.mo_active is False
    assert node_data_after.node_id == virtual_node.id
    # check new node_data created - end

    assert parent_child_count_before == parent_node.child_count
