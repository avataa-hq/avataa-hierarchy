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

from kafka_config.msg.mo_msg import on_update_mo
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
        is_virtual=True,
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
        is_virtual=True,
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
        mo_data=[TMO_3[33], TMO_3[333]],
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
async def test_with_mo_update_case_1(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from existing parent_id to None.
    In case there is a real Obj based on this MO:
    - changes NodeData.mo_p_id for this MO
    - changes Obj.p_id to None
    - changes parent_child count if parent exists
    - does not delete child nodes"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_2[2]["id"]
    tmo_id = TMO_2[2]["tmo_id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": None, "active": True}
        ]
    }

    # check Obj  before - start
    stmt = select(Obj).where(Obj.object_id == mo_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().all()
    assert len(node_before) == 1
    node_before = node_before[0]
    assert node_before.parent_id is not None
    # check Obj  before - end

    # check children before - start
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_before = await session.execute(stmt)
    children_before = children_before.scalars().all()
    assert len(children_before) > 0
    children_count_before = len(children_before)
    # check children before - end

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_p_id is not None
    # check node data before - end

    # check parent of Obj before - start
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    paren_node_before = await session.execute(stmt)
    paren_node_before = paren_node_before.scalars().all()
    assert len(paren_node_before) == 1
    paren_node_before = paren_node_before[0]
    parent_child_count_before = paren_node_before.child_count
    # check parent of Obj before - end

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(node_before)
    await session.refresh(node_data_before)
    await session.refresh(paren_node_before)

    assert node_before is not None
    assert node_data_before is not None
    assert paren_node_before is not None

    assert node_before.parent_id is None
    assert node_data_before.mo_p_id is None
    assert paren_node_before.child_count == parent_child_count_before - 1

    # check children after - start
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()
    assert len(children_after) > 0
    assert children_count_before == len(children_after)
    # check children after - end


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_2(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from existing parent_id to None.
    In case there is a virtual Obj based ONLY on this MO:
    - changes NodeData.mo_p_id for this MO
    - changes Obj.p_id to None
    - changes parent_child count if parent exists
    - does not delete child nodes"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_21[2121]["id"]
    tmo_id = TMO_21[2121]["tmo_id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": None, "active": True}
        ]
    }

    # check level is virtual
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # check Obj  before - start
    node_before = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_id
    )
    assert node_before.parent_id is not None
    # check Obj  before - end

    # check children before - start
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_before = await session.execute(stmt)
    children_before = children_before.scalars().all()
    assert len(children_before) > 0
    children_count_before = len(children_before)
    # check children before - end

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_p_id is not None
    # check node data before - end

    # check parent of Obj before - start
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    paren_node_before = await session.execute(stmt)
    paren_node_before = paren_node_before.scalars().all()
    assert len(paren_node_before) == 1
    paren_node_before = paren_node_before[0]
    parent_child_count_before = paren_node_before.child_count
    # check parent of Obj before - end

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(node_before)
    await session.refresh(node_data_before)
    await session.refresh(paren_node_before)

    assert node_before is not None
    assert node_data_before is not None
    assert paren_node_before is not None

    assert node_before.parent_id is None
    assert node_data_before.mo_p_id is None
    assert paren_node_before.child_count == parent_child_count_before - 1

    # check children after - start
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()
    assert len(children_after) > 0
    assert children_count_before == len(children_after)
    # check children after - end


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_3(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from existing parent_id to None.
    In case there is a virtual Obj based NOT ONLY on this MO:
    - changes NodeData.mo_p_id for this MO
    - does not change Obj.p_id
    - does not change parent_child count if parent exists
    - creates new node without parent node
    - changes parent_id for corresponding by mo_p_id children"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_3[333]["id"]
    tmo_id = TMO_3[333]["tmo_id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": None, "active": True}
        ]
    }

    # check level is virtual
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # creates new node without parent node - before - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == None,  # noqa
    )
    new_node_with_p_id_none = await session.execute(stmt)
    new_node_with_p_id_none = new_node_with_p_id_none.scalars().all()
    assert len(new_node_with_p_id_none) == 0
    # creates new node without parent node - before - end

    # check Obj  before - start
    node_before = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_id
    )
    assert node_before.parent_id is not None
    # check Obj  before - end

    # check children before - start
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_before_main_node = await session.execute(stmt)
    children_before_main_node = children_before_main_node.scalars().all()
    assert len(children_before_main_node) > 1

    # check children before - end

    # check node consist of two MO before - start
    stmt = select(NodeData).where(NodeData.node_id == node_before.id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 2
    # check node consist of two MO before - start

    # check node data before - start
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_p_id is not None
    # check node data before - end

    # check parent of Obj before - start
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    paren_node_before = await session.execute(stmt)
    paren_node_before = paren_node_before.scalars().all()
    assert len(paren_node_before) == 1
    paren_node_before = paren_node_before[0]
    parent_child_count_before = paren_node_before.child_count
    # check parent of Obj before - end

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(node_before)
    await session.refresh(node_data_before)
    await session.refresh(paren_node_before)

    assert node_before is not None
    assert node_data_before is not None
    assert paren_node_before is not None

    # does not change Obj.p_id
    assert node_before.parent_id is not None
    # changes NodeData.mo_p_id for this MO
    assert node_data_before.mo_p_id is None
    # does not change parent_child count if parent exists
    assert paren_node_before.child_count == parent_child_count_before

    # creates new node without parent node - before - start
    stmt = select(Obj).where(
        Obj.level_id == level.id,
        Obj.parent_id == None,  # noqa
    )
    new_node_with_p_id_none = await session.execute(stmt)
    new_node_with_p_id_none = new_node_with_p_id_none.scalars().all()
    assert len(new_node_with_p_id_none) == 1
    new_node_with_p_id_none = new_node_with_p_id_none[0]
    assert new_node_with_p_id_none.child_count == 1
    # creates new node without parent node - before - end

    # changes parent_id for corresponding by mo_p_id children - start
    stmt = select(Obj).where(Obj.parent_id == new_node_with_p_id_none.id)
    children_node = await session.execute(stmt)
    children_node = children_node.scalars().all()
    assert len(children_node) == 1
    # changes parent_id for corresponding by mo_p_id children - end

    # check node consist of one MO after - start
    stmt = select(NodeData).where(NodeData.node_id == node_before.id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    # check node consist of one MO after - start


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_4(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from existing parent_id to None.
    In case there is a virtual Obj based NOT ONLY on this MO and there is a virtual node p_id is None and same key:
    - does not create new node without parent node - uses existing node
    - changes NodeData.node_id of this MO to existing node"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_3[333]["id"]
    tmo_id = TMO_3[333]["tmo_id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": None, "active": True}
        ]
    }

    # check level is virtual
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    node_before = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_id
    )
    assert node_before.parent_id is not None

    # node data before
    stmt = select(NodeData).where(
        NodeData.node_id == node_before.id, NodeData.mo_id == mo_id
    )
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().first()

    # create new Obj with p_id is None
    new_node = Obj(
        key=node_before.key,
        object_type_id=node_before.object_type_id,
        additional_params=node_before.additional_params,
        hierarchy_id=node_before.hierarchy_id,
        level=node_before.level,
        parent_id=None,
        latitude=node_before.latitude,
        longitude=node_before.longitude,
        child_count=0,
        path=None,
        level_id=node_before.level_id,
    )

    session.add(new_node)
    await session.flush()

    # create new node data
    new_n_data = NodeData(
        level_id=new_node.level_id,
        node_id=new_node.id,
        mo_id=99999999,
        mo_name="999999999",
        mo_tmo_id=new_node.object_type_id,
        mo_p_id=None,
    )
    session.add(new_n_data)
    await session.commit()

    # count of level nodes before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    count_of_level_nodes_before = len(level_nodes_before)
    print(level_nodes_before)

    await on_update_mo(msg=kafka_msg, session=session)

    # count of level nodes after
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_after = await session.execute(stmt)
    level_nodes_after = level_nodes_after.scalars().all()
    assert len(level_nodes_after) == count_of_level_nodes_before

    await session.refresh(node_data_before)
    assert node_data_before.node_id == new_node.id


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_5(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from None to existing parent_id.
    In case there is a real Obj based on this MO:
    - changes NodeData.mo_p_id for this MO
    - changes Obj.p_id to existing parent
    - changes parent_child count"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = 789
    tmo_id = 2
    new_p_id = TMO_1[1]["id"]

    # check level is virtual
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is False

    # get parent node -before
    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=new_p_id
    )
    assert parent_node is not None
    parent_child_count_before = parent_node.child_count

    # check Obj  before - start
    stmt = select(Obj).where(Obj.level_id == level.id, Obj.object_id == mo_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().all()
    assert len(node_before) == 0

    # create new node with parent none
    new_node = Obj(
        key="unique key",
        object_type_id=tmo_id,
        hierarchy_id=level.hierarchy_id,
        level=level.level,
        parent_id=None,
        child_count=0,
        path=None,
        level_id=level.id,
    )
    session.add(new_node)
    await session.flush()

    # create new node data
    new_n_data = NodeData(
        level_id=new_node.level_id,
        node_id=new_node.id,
        mo_id=mo_id,
        mo_name=f"{mo_id}",
        mo_tmo_id=new_node.object_type_id,
        mo_p_id=None,
    )
    session.add(new_n_data)
    await session.commit()

    # check Obj  before - end

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(new_n_data)
    await session.refresh(new_node)
    await session.refresh(parent_node)

    # changes NodeData.mo_p_id for this MO
    assert new_n_data.mo_p_id == new_p_id
    # changes Obj.p_id to existing parent
    assert new_node.parent_id == parent_node.id
    # changes parent_child count
    assert parent_node.child_count - 1 == parent_child_count_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_6(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from None to existing parent_id.
    In case there is a virtual Obj based ONLY on this MO:
    - changes NodeData.mo_p_id for this MO
    - changes Obj.p_id to existing parent
    - changes parent_child count"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = 789
    tmo_id = 21
    new_p_id = TMO_1[11]["id"]

    # check level is virtual
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # get parent node -before
    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=new_p_id
    )
    assert parent_node is not None
    parent_child_count_before = parent_node.child_count

    # check Obj  before - start
    node_before = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_id
    )
    assert node_before is None

    # create new node with parent none
    new_node = Obj(
        key="unique key",
        object_type_id=tmo_id,
        hierarchy_id=level.hierarchy_id,
        level=level.level,
        parent_id=None,
        child_count=0,
        path=None,
        level_id=level.id,
    )
    session.add(new_node)
    await session.flush()

    # create new node data
    new_n_data = NodeData(
        level_id=new_node.level_id,
        node_id=new_node.id,
        mo_id=mo_id,
        mo_name=f"{mo_id}",
        mo_tmo_id=new_node.object_type_id,
        mo_p_id=None,
        unfolded_key={k: "unique key" for k in level.key_attrs},
    )
    session.add(new_n_data)
    await session.commit()

    # check Obj  before - end

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(new_n_data)
    await session.refresh(new_node)
    await session.refresh(parent_node)

    # changes NodeData.mo_p_id for this MO
    assert new_n_data.mo_p_id == new_p_id
    # changes Obj.p_id to existing parent
    assert new_node.parent_id == parent_node.id
    # changes parent_child count
    assert parent_node.child_count - 1 == parent_child_count_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_7(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from None to existing parent_id.
    In case there is a virtual Obj based NOT ONLY on this MO and there is no node with such key:
    - changes NodeData.mo_p_id for this MO
    - changes NodeData.node_id for this MO
    - does not change Obj.p_id
    - changes parent_child for new parent
    - create new node"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_1_id = 789
    mo_2_id = 790
    tmo_id = 21
    new_p_id = TMO_1[11]["id"]

    # check level is virtual
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # get parent node -before
    parent_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.parent_id, mo_id=new_p_id
    )
    assert parent_node is not None
    parent_child_count_before = parent_node.child_count

    # check Obj  before - start
    node_before = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_1_id
    )
    assert node_before is None
    node_before = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_2_id
    )
    assert node_before is None

    # create new node with parent none
    common_node = Obj(
        key="unique key",
        object_type_id=tmo_id,
        hierarchy_id=level.hierarchy_id,
        level=level.level,
        parent_id=None,
        child_count=0,
        path=None,
        level_id=level.id,
    )
    session.add(common_node)
    await session.flush()

    # create new node data
    new_n1_data = NodeData(
        level_id=common_node.level_id,
        node_id=common_node.id,
        mo_id=mo_1_id,
        mo_name=f"{mo_1_id}",
        mo_tmo_id=common_node.object_type_id,
        mo_p_id=None,
        unfolded_key={k: "unique key" for k in level.key_attrs},
    )
    new_n2_data = NodeData(
        level_id=common_node.level_id,
        node_id=common_node.id,
        mo_id=mo_2_id,
        mo_name=f"{mo_2_id}",
        mo_tmo_id=common_node.object_type_id,
        mo_p_id=None,
        unfolded_key={k: "unique key" for k in level.key_attrs},
    )
    session.add(new_n1_data)
    session.add(new_n2_data)
    await session.commit()

    # check Obj  before - start
    node_before_1 = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_1_id
    )
    assert node_before_1 is not None
    node_before_2 = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_2_id
    )
    assert node_before_2 is not None
    assert node_before_1.id == node_before_2.id

    # check Obj  before - end

    kafka_msg = {
        "objects": [
            {"id": mo_1_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(new_n1_data)
    await session.refresh(new_n2_data)
    await session.refresh(common_node)
    await session.refresh(parent_node)
    '''changes NodeData.mo_p_id for this MO
    - does not change Obj.p_id
    - does not change parent_child of old Obj
    - create new node"""'''
    # changes NodeData.mo_p_id for this MO
    assert new_n1_data.mo_p_id == new_p_id

    # changes NodeData.node_id for this MO
    # Change to equal
    assert new_n1_data.node_id == new_n2_data.node_id
    # does not change Obj.p_id
    # Change to is not None
    assert common_node.parent_id is not None
    # changes parent_child count
    assert parent_node.child_count - 1 == parent_child_count_before
    # create new node
    new_node = await get_node_by_level_id_and_mo_id(
        session=session, level_id=level.id, mo_id=mo_1_id
    )
    # Change to equal
    assert new_node.id == common_node.id


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_8(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from None to existing parent_id.
    In case there is a real Obj based on this MO with children:
    - changes path for children nodes"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    tmo_id = TMO_2[2]["tmo_id"]
    mo_id = TMO_2[2]["id"]
    new_p_id = TMO_1[11]["id"]

    # get level
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is False

    # get node before

    node = await get_node_by_level_id_and_mo_id(
        session=session, mo_id=mo_id, level_id=level.id
    )
    assert node is not None
    assert node.child_count > 0
    assert node.path is not None
    node_path_before = node.path

    # get children of all child level
    stmt = select(Obj).where(Obj.path.like(f"{node.path}{node.id}%"))
    children = await session.execute(stmt)
    children = children.scalars().all()
    assert len(children) > 0

    children_count_before = len(children)

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(node)

    assert node.path != node_path_before

    # get children of all child level
    stmt = select(Obj).where(Obj.path.like(f"{node.path}{node.id}%"))
    children = await session.execute(stmt)
    children = children.scalars().all()

    assert len(children) > 0

    assert children_count_before == len(children)


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_9(session: AsyncSession, mock_grpc_response):
    """TEST With MO:updated. If changed parent_id from None to existing parent_id.
    In case there is a virtual Obj based ONLY on this MO with children:
    - changes path for corresponding by mo_p_id children nodes"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    tmo_id = TMO_21[2121]["tmo_id"]
    mo_id = TMO_21[2121]["id"]
    new_p_id = TMO_1[111]["id"]

    # get level
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # get node before
    node = await get_node_by_level_id_and_mo_id(
        session=session, mo_id=mo_id, level_id=level.id
    )
    assert node is not None
    assert node.child_count > 0
    assert node.path is not None
    node_path_before = node.path

    # check node consist of one node_data
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data = await session.execute(stmt)
    node_data = node_data.scalars().all()
    assert len(node_data) == 1
    node_data = node_data[0]
    assert node_data.mo_p_id is not None

    # get children of all child level
    stmt = select(Obj).where(Obj.path.like(f"{node.path}{node.id}%"))
    children = await session.execute(stmt)
    children = children.scalars().all()
    assert len(children) > 0

    children_count_before = len(children)

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(node)

    assert node.path != node_path_before

    # get childrens of all child level
    stmt = select(Obj).where(Obj.path.like(f"{node.path}{node.id}%"))
    children = await session.execute(stmt)
    children = children.scalars().all()

    assert len(children) > 0

    assert children_count_before == len(children)


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_10(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If changed parent_id from None to existing parent_id.
    In case there is a virtual Obj based NOT ONLY on this MO with children:
    - changes path for corresponding by mo_p_id children nodes"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    tmo_id = TMO_3[333]["tmo_id"]
    mo_id = TMO_3[333]["id"]
    new_p_id = TMO_2[22]["id"]

    # get level
    stmt = select(Level).where(Level.object_type_id == tmo_id)
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # get node before
    node_before = await get_node_by_level_id_and_mo_id(
        session=session, mo_id=mo_id, level_id=level.id
    )
    assert node_before is not None
    assert node_before.child_count > 0
    assert node_before.path is not None

    # check node consist of one node_data
    stmt = select(NodeData).where(NodeData.node_id == node_before.id)
    node_data = await session.execute(stmt)
    node_data = node_data.scalars().all()
    assert len(node_data) > 1
    node_data = node_data[0]
    assert node_data.mo_p_id is not None

    # get children of all child level
    stmt = select(Obj).where(
        Obj.path.like(f"{node_before.path}{node_before.id}%")
    )
    children = await session.execute(stmt)
    children = children.scalars().all()
    assert len(children) > 0

    # get children by mo_p_id
    stmt = select(NodeData).where(NodeData.mo_p_id == mo_id)
    children_by_mo_p_id = await session.execute(stmt)
    children_by_mo_p_id = children_by_mo_p_id.scalars().all()
    assert len(children_by_mo_p_id) == 1

    children_count_before = len(children)

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    await session.refresh(node_before)

    node_after = await get_node_by_level_id_and_mo_id(
        session=session, mo_id=mo_id, level_id=level.id
    )
    # Change to equal
    assert node_before.path == node_after.path

    # get childrens of all child level
    stmt = select(Obj).where(
        Obj.path.like(f"{node_after.path}{node_after.id}%")
    )
    children = await session.execute(stmt)
    children = children.scalars().all()

    assert len(children) > 0

    # Change to equal
    assert children_count_before == len(children)
    # Change to not equal
    assert len(children_by_mo_p_id) != len(children)


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_case_11(session: AsyncSession):
    """TEST With MO:updated. If was changed MO.p_id for nodes without parent level.
    - changes node_data parent_id"""
    mo_id = TMO_1[1]["id"]
    tmo_id = TMO_1[1]["tmo_id"]
    new_p_id = 25557787

    # get levels without parent level

    stmt = select(Level).where(
        Level.object_type_id == tmo_id,
        Level.parent_id == None,  # noqa
    )
    level = await session.execute(stmt)
    level = level.scalars().all()
    assert len(level) == 1
    level = level[0]
    assert level.is_virtual is True

    # get node_data before
    stmt = select(NodeData).where(
        NodeData.mo_id == mo_id, NodeData.level_id == level.id
    )
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    node_data_before_id = node_data_before.id
    node_data_before_id_mo_p_id = node_data_before.mo_p_id

    # get node before
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    node_before_id = node_before.id
    node_before_parent_id = node_before.parent_id

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": new_p_id, "active": True}
        ]
    }

    await on_update_mo(msg=kafka_msg, session=session)

    # get node_data after
    stmt = select(NodeData).where(
        NodeData.mo_id == mo_id, NodeData.level_id == level.id
    )
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]

    # get node after
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_after = await session.execute(stmt)
    node_after = node_after.scalars().first()
    assert node_after is not None

    assert node_data_before_id == node_data_after.id
    assert node_data_before_id_mo_p_id != node_data_after.mo_p_id
    assert node_data_after.mo_p_id == new_p_id

    assert node_before_id == node_after.id
    assert node_before_parent_id == node_after.parent_id
    assert node_before.parent_id is None
