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

from kafka_config.msg.mo_msg import on_delete_mo
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


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession):
    hierarchy = await add_hierarchy_to_session(
        session, hierarchy_name=DEFAULT_HIERARCHY_NAME_FOR_TESTS
    )
    level1 = await add_level_to_session(
        session,
        level_name="TMO 1 - Virtual level - param_type_id -1",
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
async def test_with_delete_mo_case_1(session: AsyncSession):
    """TEST With MO:deleted if MO.id does not exist in hierarchies:
    - does not raises error
    - does not delete Obj
    - does not delete NodeData"""
    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_level = 21
    not_existing_mo_id = 9999

    mo_1 = {
        "id": not_existing_mo_id,
        "tmo_id": existing_tmo_id_of_level,
        "p_id": None,
        "active": True,
    }

    kafka_msg = {"objects": [mo_1]}

    stmt = select(Level.id).where(Level.hierarchy_id == hierarchy.id)
    all_level_ids = await session.execute(stmt)
    all_level_ids = all_level_ids.scalars().all()

    # check if level with tmo_id exists
    stmt = select(Level).where(
        Level.object_type_id == existing_tmo_id_of_level,
        Level.hierarchy_id == hierarchy.id,
    )
    levels = await session.execute(stmt)
    levels = levels.scalars().all()

    assert len(levels) > 0

    stmt = select(Obj).where(Obj.hierarchy_id == hierarchy.id)
    all_nodes = await session.execute(stmt)
    all_nodes_before = all_nodes.scalars().all()

    stmt = select(NodeData).where(NodeData.level_id.in_(all_level_ids))
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().all()

    await on_delete_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(Obj.hierarchy_id == hierarchy.id)
    all_nodes = await session.execute(stmt)
    all_nodes_after = all_nodes.scalars().all()
    assert len(all_nodes_after) == len(all_nodes_before)

    stmt = select(NodeData).where(NodeData.level_id.in_(all_level_ids))
    all_node_data_after = await session.execute(stmt)
    all_node_data_after = all_node_data_after.scalars().all()
    assert len(all_node_data_after) == len(all_node_data_before)


@pytest.mark.asyncio(loop_scope="session")
async def test_with_delete_mo_case_2(session: AsyncSession):
    """TEST With MO:deleted if real node based on delete MO exists .
    And this node has parent:
    - deletes node data
    - deletes node
    - decreases parent_node.child_count by 1"""

    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_level = 31
    existing_mo_id_of_real_node = TMO_31[3131]

    mo_1 = {
        "id": existing_mo_id_of_real_node["id"],
        "tmo_id": existing_tmo_id_of_level,
        "p_id": None,
        "active": True,
    }

    kafka_msg = {"objects": [mo_1]}

    # check if real node with existing_mo_id_of_real_node exists
    stmt = select(Obj).where(Obj.object_id == existing_mo_id_of_real_node["id"])
    real_node_before = await session.execute(stmt)
    real_node_before = real_node_before.scalars().first()

    assert real_node_before is not None

    # get parent of real node
    stmt = select(Obj).where(
        Obj.id == real_node_before.parent_id, Obj.hierarchy_id == hierarchy.id
    )
    parent_node = await session.execute(stmt)
    parent_node = parent_node.scalars().first()

    assert parent_node is not None

    parent_node_child_count_before = parent_node.child_count

    # node data before
    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id_of_real_node["id"]
    )
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().all()
    assert len(all_node_data_before) == 1

    await on_delete_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(
        Obj.object_id == existing_mo_id_of_real_node["id"],
        Obj.hierarchy_id == hierarchy.id,
    )
    real_node_after = await session.execute(stmt)
    real_node_after = real_node_after.scalars().first()

    assert real_node_after is None

    # parent.child_count after
    await session.refresh(parent_node)
    print(parent_node)
    assert parent_node_child_count_before - parent_node.child_count == 1

    # node data after
    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id_of_real_node["id"]
    )
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().all()
    assert len(all_node_data_before) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_with_delete_mo_case_3(session: AsyncSession):
    """TEST With MO:deleted if virtual node based on only deleted MO exists .
    And this node has parent:
    - deletes node data
    - deletes node
    - decreases parent_node.child_count by 1"""

    hierarchy = await get_default_hierarchy_by_name(session=session)
    existing_tmo_id_of_level = 21
    existing_mo_id_of_virtual_node = TMO_21[212121]["id"]

    mo_1 = {
        "id": existing_mo_id_of_virtual_node,
        "tmo_id": existing_tmo_id_of_level,
        "p_id": None,
        "active": True,
    }

    kafka_msg = {"objects": [mo_1]}

    # check if real node with existing_mo_id_of_real_node exists
    stmt = select(Level).where(Level.object_type_id == existing_tmo_id_of_level)
    virtual_level = await session.execute(stmt)
    virtual_level = virtual_level.scalars().first()

    assert virtual_level.is_virtual is True

    virtual_node_before = await get_node_by_level_id_and_mo_id(
        session=session,
        mo_id=existing_mo_id_of_virtual_node,
        level_id=virtual_level.id,
    )
    assert virtual_node_before is not None

    # get parent of virtual node
    stmt = select(Obj).where(
        Obj.id == virtual_node_before.parent_id,
        Obj.hierarchy_id == hierarchy.id,
    )
    parent_node = await session.execute(stmt)
    parent_node = parent_node.scalars().first()

    assert parent_node is not None

    parent_node_child_count_before = parent_node.child_count

    # node data before
    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id_of_virtual_node
    )
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().all()
    assert len(all_node_data_before) == 1

    await on_delete_mo(msg=kafka_msg, session=session)

    stmt = select(Obj).where(Obj.id == virtual_node_before.id)
    virtual_node_after = await session.execute(stmt)
    virtual_node_after = virtual_node_after.scalars().first()

    assert virtual_node_after is None

    # parent.child_count after
    await session.refresh(parent_node)
    print(parent_node)
    assert parent_node_child_count_before - parent_node.child_count == 1

    # node data after
    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id_of_virtual_node
    )
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().all()
    assert len(all_node_data_before) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_with_delete_mo_case_4(session: AsyncSession):
    """TEST With MO:deleted if virtual node based on not only deleted MO exists .
    And this node has parent:
    - deletes node data
    - does not delete node
    - does not decrease parent_node.child_count by 1"""

    existing_tmo_id_of_level = 1
    existing_mo_id_of_virtual_node = TMO_1[11]["id"]

    mo_1 = {
        "id": existing_mo_id_of_virtual_node,
        "tmo_id": existing_tmo_id_of_level,
        "p_id": None,
        "active": True,
    }

    kafka_msg = {"objects": [mo_1]}

    # check if virtual level exists
    stmt = select(Level).where(
        Level.is_virtual == True,  # noqa
        Level.object_type_id == existing_tmo_id_of_level,
        Level.level == 2,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()
    assert level is not None

    # check if node data exists
    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id_of_virtual_node,
        NodeData.level_id == level.id,
    )
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().first()

    assert node_data_before is not None

    # check that node consist of more than one MO instance
    stmt = select(NodeData).where(NodeData.node_id == node_data_before.node_id)
    node_data_of_virtual_node = await session.execute(stmt)
    node_data_of_virtual_node = node_data_of_virtual_node.scalars().all()

    assert len(node_data_of_virtual_node) > 1

    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()

    assert node_before is not None

    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_node = await session.execute(stmt)
    parent_node = parent_node.scalars().first()

    assert parent_node is not None

    parent_node_child_count_before = parent_node.child_count

    await on_delete_mo(msg=kafka_msg, session=session)

    # check that node_data for this Mo was deleted

    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id_of_virtual_node
    )
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    assert node_data_after is None

    # check that virtual node was not deleted
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_after = await session.execute(stmt)
    node_after = node_after.scalars().first()

    assert node_after is not None

    # check that parent child count was not changed

    await session.refresh(parent_node)
    assert parent_node.child_count == parent_node_child_count_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_delete_mo_case_5(session: AsyncSession):
    """TEST With MO:deleted if MO.active = False and real node based on deleted MO exists .
    And this node has parent:
    - deletes node data
    - deletes node
    - does not change parent_node.child_count"""

    hierarchy = await get_default_hierarchy_by_name(session=session)
    tmo_id = 31
    mo_id = TMO_31[3131]["id"]

    mo_1 = {"id": mo_id, "tmo_id": tmo_id, "p_id": None, "active": False}

    kafka_msg = {"objects": [mo_1]}
    # check level
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().first()
    assert level is not None
    assert level.is_virtual is False

    # check if real node with existing_mo_id_of_real_node exists
    stmt = select(Obj).where(Obj.object_id == mo_id)
    real_node_before = await session.execute(stmt)
    real_node_before = real_node_before.scalars().first()

    assert real_node_before is not None

    # node data before
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().first()
    assert all_node_data_before is not None

    assert all_node_data_before.node_id == real_node_before.id

    # change active to false
    all_node_data_before.mo_active = False
    real_node_before.active = False
    session.add(all_node_data_before)
    session.add(real_node_before)

    # get parent of real node
    stmt = select(Obj).where(
        Obj.id == real_node_before.parent_id, Obj.hierarchy_id == hierarchy.id
    )
    parent_node = await session.execute(stmt)
    parent_node = parent_node.scalars().first()

    assert parent_node is not None
    # because we made above one node active = False
    parent_node.child_count -= 1
    session.add(parent_node)
    parent_node_child_count_before = parent_node.child_count

    await session.commit()

    await on_delete_mo(msg=kafka_msg, session=session)

    # node after
    stmt = select(Obj).where(
        Obj.object_id == mo_id, Obj.hierarchy_id == hierarchy.id
    )
    real_node_after = await session.execute(stmt)
    real_node_after = real_node_after.scalars().first()

    assert real_node_after is None

    # node data after
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    all_node_data_before = await session.execute(stmt)
    all_node_data_before = all_node_data_before.scalars().all()
    assert len(all_node_data_before) == 0

    # parent.child_count after
    await session.refresh(parent_node)
    assert parent_node_child_count_before == parent_node.child_count
