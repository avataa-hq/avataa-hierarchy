import logging

from kafka_tests.inventory_changes.utils_for_mo_events import (
    add_hierarchy_to_session,
    add_level_to_session,
    add_node_to_session,
)
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from kafka_config.msg.mo_msg import on_update_mo
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from schemas.main_base_connector import Base
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
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
    21: {"id": 21, "name": "MO 21", "tmo_id": 21, "41": "node 13 level 5"},
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
        active=False,
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
        active=False,
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

    node_8_level_3 = await add_node_to_session(
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
        active=False,
    )

    # node data for depth 4 REal level tmo 2 - start
    node_10_level_4 = await add_node_to_session(
        session=session,
        level=level4,
        mo_data=[TMO_2[2]],
        parent_node=node_6_level_3,
        active=False,
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
        parent_node=node_8_level_3,
        active=False,
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
        active=False,
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
async def test_with_mo_update_changed_active_case_1(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If changed MO.active from False to True for real level:
    - changes NodeData.mo_active to True
    - changes Obj.active to True
    - increases parent.child_count by 1"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_2[2]["id"]
    tmo_id = TMO_2[2]["tmo_id"]
    p_id = TMO_1[1]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": p_id, "active": True}
        ]
    }

    # check node before exists and it is not active
    stmt = select(Obj).where(Obj.object_id == mo_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.node_id == node_before.id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().first()
    assert node_data_before is not None
    assert node_data_before.mo_active is False

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None
    parent_before_child_count = parent_before.child_count

    await on_update_mo(msg=kafka_msg, session=session)

    # check node_becomes active
    await session.refresh(node_before)
    assert node_before.active is True

    # check node_data after - mo_active = True
    await session.refresh(node_data_before)
    assert node_data_before.mo_active is True

    # check parent after - child_count increased by 1
    await session.refresh(parent_before)
    assert parent_before.child_count == parent_before_child_count + 1


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_2(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If changed MO.active changed from False to True and it corresponding to virtual node that
    fully consist of only this MO. If there is no active virtual node with such key:
    - changes NodeData.mo_active to True
    - changes Obj.active to True
    - increases parent.child_count by 1
    - does not change child_paths"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_21[2121]["id"]
    tmo_id = TMO_21[2121]["tmo_id"]
    p_id = TMO_1[111]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": p_id, "active": True}
        ]
    }

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_active is False

    # check virtual node consist only of this Mo data
    stmt = select(NodeData).where(NodeData.node_id == node_data_before.node_id)
    count_of_node_datas = await session.execute(stmt)
    count_of_node_datas = count_of_node_datas.scalars().all()
    assert len(count_of_node_datas) == 1

    # check node before exists and it is not active
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False

    # check there is noe active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is None

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None
    parent_before_child_count = parent_before.child_count

    # get children paths before
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_before = await session.execute(stmt)
    children_before = children_before.scalars().all()
    assert len(children_before) > 0
    children_path_before = {children.path for children in children_before}

    await on_update_mo(msg=kafka_msg, session=session)

    # check node_becomes active
    await session.refresh(node_before)
    assert node_before.active is True

    # check node_data after - mo_active = True
    await session.refresh(node_data_before)
    assert node_data_before.mo_active is True

    # check parent after - child_count increased by 1
    await session.refresh(parent_before)
    assert parent_before.child_count == parent_before_child_count + 1

    # check children paths after
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()
    assert len(children_after) > 0
    for child in children_after:
        assert child.path in children_path_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_3(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If changed MO.active changed from False to True and it corresponding to virtual node that
    fully consist of only this MO. If there is active virtual node with such key:
    - changes NodeData.mo_active to True
    - changes NodeData.node_id to existing virtual node with equal key and active True
    - deletes old virtual node
    - does not increase parent.child_count by 1"""
    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_21[2121]["id"]
    tmo_id = TMO_21[2121]["tmo_id"]
    p_id = TMO_1[111]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": p_id, "active": True}
        ]
    }

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_active is False

    # check virtual node consist only of this Mo data
    stmt = select(NodeData).where(NodeData.node_id == node_data_before.node_id)
    count_of_node_datas = await session.execute(stmt)
    count_of_node_datas = count_of_node_datas.scalars().all()
    assert len(count_of_node_datas) == 1

    # check node before exists and it is not active
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False
    node_before_id = node_before.id

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None

    # create active_virtual node with key of not active virtual node
    new_mo_id = 99999555
    existing_virtual_node = Obj(
        key=node_before.key,
        object_type_id=node_before.object_type_id,
        hierarchy_id=node_before.hierarchy_id,
        level=node_before.level,
        parent_id=node_before.parent_id,
        child_count=0,
        path=create_path_for_children_node_by_parent_node(parent_before),
        level_id=node_before.level_id,
        active=True,
    )
    session.add(existing_virtual_node)
    await session.flush()

    # create new node data
    new_n1_data = NodeData(
        level_id=existing_virtual_node.level_id,
        node_id=existing_virtual_node.id,
        mo_id=new_mo_id,
        mo_name=f"{new_mo_id}",
        mo_tmo_id=existing_virtual_node.object_type_id,
        mo_p_id=p_id,
        mo_active=True,
    )
    session.add(new_n1_data)
    parent_before.child_count += 1
    session.add(parent_before)
    await session.commit()
    parent_before_child_count = parent_before.child_count

    # check there is active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is not None
    # Strange equation - Add assert
    assert active_virtual_node_with_key_exists.id == existing_virtual_node.id

    # get children by paths before
    path_before = create_path_for_children_node_by_parent_node(node_before)
    stmt = select(Obj).where(Obj.path == path_before)
    children_before = await session.execute(stmt)
    children_before = children_before.scalars().all()
    assert len(children_before) > 0
    children_ids_before = {c.id for c in children_before}

    await on_update_mo(msg=kafka_msg, session=session)

    # check - deletes old virtual node
    # check if we need really delete old node after update __rebuilding_nodes_based_on_their_data_and_level
    # Rework __rebuilding_nodes_based_on_their_data_and_level additional if on remove virtual levels
    stmt = select(Obj).where(Obj.id == node_before_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is None

    # check node_data after - mo_active = True
    await session.refresh(node_data_before)
    assert node_data_before.mo_active is True

    # check - changes NodeData.node_id to existing virtual node with equal key and active True
    # check if we need really delete old node after update __rebuilding_nodes_based_on_their_data_and_level
    # Rework __rebuilding_nodes_based_on_their_data_and_level additional if on remove virtual levels
    assert node_data_before.node_id == existing_virtual_node.id

    # check parent after - child_count increased by 1
    await session.refresh(parent_before)
    assert parent_before.child_count == parent_before_child_count

    # check children paths after
    path_after = create_path_for_children_node_by_parent_node(
        existing_virtual_node
    )
    assert path_after != path_before

    stmt = select(Obj).where(Obj.path == path_after)
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()
    assert len(children_after) > 0
    for c in children_after:
        assert c.id in children_ids_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_4(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If changed MO.active changed from False to True and it corresponding to virtual node that
    consist not only of this MO. If there is no active virtual node with such key:
    - changes NodeData.mo_active to True
    - changes NodeData.node_id to True
    - does not change Obj.active to True -> changes Obj.active to True
    - creates new virtual node
    - increases parent.child_count by 1
    - change connected children (path and parent_id)"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_3[333]["id"]
    tmo_id = TMO_3[333]["tmo_id"]
    p_id = TMO_2[2]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": p_id, "active": True}
        ]
    }

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_active is False

    # check virtual node consist not only of this Mo data
    stmt = select(NodeData).where(NodeData.node_id == node_data_before.node_id)
    count_of_node_datas = await session.execute(stmt)
    count_of_node_datas = count_of_node_datas.scalars().all()
    assert len(count_of_node_datas) > 1

    # check node before exists and it is not active
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False

    # check there is no active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is None

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None
    parent_before_child_count = parent_before.child_count

    # get children n_data by mo_p_id
    stmt = select(NodeData).where(NodeData.mo_p_id == mo_id)
    child_n_data_before = await session.execute(stmt)
    child_n_data_before = child_n_data_before.scalars().all()
    assert len(child_n_data_before) > 0
    child_node_ids = {n_data.node_id for n_data in child_n_data_before}

    # get children nodes_before
    stmt = select(Obj).where(Obj.id.in_(child_node_ids))
    children_nodes_before = await session.execute(stmt)
    children_nodes_before = children_nodes_before.scalars().all()
    assert len(children_nodes_before) > 0
    child_no_ids = (node.id for node in children_nodes_before)
    for child_node in children_nodes_before:
        assert child_node.parent_id == node_before.id

    await on_update_mo(msg=kafka_msg, session=session)

    # check node does not become active
    await session.refresh(node_before)
    assert node_before.active is True

    # check there is active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is not None

    # check node_data after - mo_active = True
    await session.refresh(node_data_before)
    assert node_data_before.mo_active is True
    # Correct to equal
    assert node_data_before.node_id == node_before.id
    assert node_data_before.node_id == active_virtual_node_with_key_exists.id

    # check parent after - child_count increased by 1
    await session.refresh(parent_before)
    assert parent_before.child_count == parent_before_child_count + 1

    stmt = select(Obj).where(Obj.id.in_(child_no_ids))
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()

    assert len(children_nodes_before) == len(children_after)
    new_parent_path = create_path_for_children_node_by_parent_node(
        active_virtual_node_with_key_exists
    )
    for c in children_after:
        assert c.parent_id == active_virtual_node_with_key_exists.id
        assert c.path == new_parent_path


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_5(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If changed Mo.active changed from False to True and it corresponding to virtual node than
    consist not only of this MO. If there is active virtual node with such key:
    - changes NodeData.mo_active to True
    - changes NodeData.node_id to existing virtual node with equal key and active True
    - does not delete old node
    - does not increase parent.child_count by 1
    - change connected children (path and parent_id)"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_3[333]["id"]
    tmo_id = TMO_3[333]["tmo_id"]
    p_id = TMO_2[2]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": p_id, "active": True}
        ]
    }

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    assert node_data_before.mo_active is False

    # check virtual node consist not only of this Mo data
    stmt = select(NodeData).where(NodeData.node_id == node_data_before.node_id)
    count_of_node_datas = await session.execute(stmt)
    count_of_node_datas = count_of_node_datas.scalars().all()
    assert len(count_of_node_datas) > 1

    # check node before exists and it is not active
    stmt = select(Obj).where(Obj.id == node_data_before.node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None

    # check there is no active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is None

    # create active_virtual node with key of not active virtual node
    new_mo_id = 99999555
    existing_virtual_node = Obj(
        key=node_before.key,
        object_type_id=node_before.object_type_id,
        hierarchy_id=node_before.hierarchy_id,
        level=node_before.level,
        parent_id=node_before.parent_id,
        child_count=0,
        path=create_path_for_children_node_by_parent_node(parent_before),
        level_id=node_before.level_id,
        active=True,
    )
    session.add(existing_virtual_node)
    await session.flush()

    # create new node data
    new_n1_data = NodeData(
        level_id=existing_virtual_node.level_id,
        node_id=existing_virtual_node.id,
        mo_id=new_mo_id,
        mo_name=f"{new_mo_id}",
        mo_tmo_id=existing_virtual_node.object_type_id,
        mo_p_id=p_id,
        mo_active=True,
    )
    session.add(new_n1_data)
    parent_before.child_count += 1
    session.add(parent_before)
    await session.commit()
    parent_before_child_count = parent_before.child_count

    # check there is active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is not None

    # get children n_data by mo_p_id
    stmt = select(NodeData).where(NodeData.mo_p_id == mo_id)
    child_n_data_before = await session.execute(stmt)
    child_n_data_before = child_n_data_before.scalars().all()
    assert len(child_n_data_before) > 0
    child_node_ids = {n_data.node_id for n_data in child_n_data_before}

    # get children nodes_before
    stmt = select(Obj).where(Obj.id.in_(child_node_ids))
    children_nodes_before = await session.execute(stmt)
    children_nodes_before = children_nodes_before.scalars().all()
    assert len(children_nodes_before) > 0
    child_no_ids = (node.id for node in children_nodes_before)
    for child_node in children_nodes_before:
        assert child_node.parent_id != active_virtual_node_with_key_exists.id
        assert child_node.parent_id == node_before.id

    await on_update_mo(msg=kafka_msg, session=session)

    # check node does not become active
    await session.refresh(node_before)
    #  -> changes Obj.active to True
    assert node_before.active is True

    # check node_data after - mo_active = True
    await session.refresh(node_data_before)
    assert node_data_before.mo_active is True
    # Correct to equal
    assert node_data_before.node_id == node_before.id
    # Correct to not equal
    assert node_data_before.node_id != active_virtual_node_with_key_exists.id

    # check parent after - child_count did not increase by 1
    await session.refresh(parent_before)
    await session.commit()
    # Current behaviour add new child
    assert parent_before.child_count == parent_before_child_count + 1

    stmt = select(Obj).where(Obj.id.in_(child_no_ids))
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()

    assert len(children_nodes_before) == len(children_after)
    # Did not work
    # new_parent_path = create_path_for_children_node_by_parent_node(
    #     active_virtual_node_with_key_exists
    # )

    # for c in children_after:
    #     assert c.parent_id == active_virtual_node_with_key_exists.id
    #     assert c.path == new_parent_path


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_6(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If the active attribute of two MOs was changed from False to True and this Mos
    corresponding to a virtual node, that consist entirely of only these MOs. If there is no active virtual
    node with such key:
    - changes NodeData.mo_active to True
    - changes Obj.active to True
    - increases parent.child_count by 1
    - does not change child_paths"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id_1 = TMO_3[33]["id"]
    mo_id_2 = TMO_3[333]["id"]
    tmo_id = TMO_3[33]["tmo_id"]
    p_id = TMO_2[2]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id_1, "tmo_id": tmo_id, "p_id": p_id, "active": True},
            {"id": mo_id_2, "tmo_id": tmo_id, "p_id": p_id, "active": True},
        ]
    }

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.mo_id.in_([mo_id_1, mo_id_2]))
    node_datas_before = await session.execute(stmt)
    node_datas_before = node_datas_before.scalars().all()
    assert len(node_datas_before) == 2
    assert node_datas_before[0].node_id == node_datas_before[1].node_id
    assert node_datas_before[0].mo_active is False
    assert node_datas_before[1].mo_active is False

    # check virtual node consist only of this Mo data
    stmt = select(NodeData).where(
        NodeData.node_id == node_datas_before[0].node_id
    )
    count_of_node_datas = await session.execute(stmt)
    count_of_node_datas = count_of_node_datas.scalars().all()
    assert len(count_of_node_datas) == 2

    # check node before exist and it is not active
    stmt = select(Obj).where(Obj.id == node_datas_before[0].node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False

    # check there is noe active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is None

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None
    parent_before_child_count = parent_before.child_count

    # get children paths before
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_before = await session.execute(stmt)
    children_before = children_before.scalars().all()
    assert len(children_before) > 0
    children_path_before = {children.path for children in children_before}

    await on_update_mo(msg=kafka_msg, session=session)

    # check node_becomes active
    await session.refresh(node_before)
    assert node_before.active is True

    # check node_data after - mo_active = True
    stmt = select(NodeData).where(NodeData.mo_id.in_([mo_id_1, mo_id_2]))
    node_datas_after = await session.execute(stmt)
    node_datas_after = node_datas_after.scalars().all()
    assert len(node_datas_after) == 2
    assert node_datas_after[0].mo_active is True
    assert node_datas_after[1].mo_active is True

    # check parent after - child_count increased by 1
    await session.refresh(parent_before)
    assert parent_before.child_count == parent_before_child_count + 1

    # check children paths after
    stmt = select(Obj).where(Obj.parent_id == node_before.id)
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()
    assert len(children_after) > 0
    for child in children_after:
        assert child.path in children_path_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_7(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. If the active attribute of two MOs was changed from False to True and this Mos
    corresponding to a virtual node, that consist entirely of only these MOs. If there is active virtual
    node with such key:
    - changes NodeData.mo_active to True
    - changes NodeData.node_id to existing virtual node with equal key and active True
    - deletes old virtual node
    - does not increase parent.child_count by 1"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id_1 = TMO_3[33]["id"]
    mo_id_2 = TMO_3[333]["id"]
    tmo_id = TMO_3[33]["tmo_id"]
    p_id = TMO_2[2]["id"]
    kafka_msg = {
        "objects": [
            {"id": mo_id_1, "tmo_id": tmo_id, "p_id": p_id, "active": True},
            {"id": mo_id_2, "tmo_id": tmo_id, "p_id": p_id, "active": True},
        ]
    }

    # check node_data before - mo_active = False
    stmt = select(NodeData).where(NodeData.mo_id.in_([mo_id_1, mo_id_2]))
    node_datas_before = await session.execute(stmt)
    node_datas_before = node_datas_before.scalars().all()
    assert len(node_datas_before) == 2
    assert node_datas_before[0].node_id == node_datas_before[1].node_id
    assert node_datas_before[0].mo_active is False
    assert node_datas_before[1].mo_active is False

    # check virtual node consist only of this Mo data
    stmt = select(NodeData).where(
        NodeData.node_id == node_datas_before[0].node_id
    )
    count_of_node_datas = await session.execute(stmt)
    count_of_node_datas = count_of_node_datas.scalars().all()
    assert len(count_of_node_datas) == 2

    # check node before exist and it is not active
    stmt = select(Obj).where(Obj.id == node_datas_before[0].node_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    assert node_before.active is False
    node_before_id = node_before.id

    # check parent-.child_count before
    stmt = select(Obj).where(Obj.id == node_before.parent_id)
    parent_before = await session.execute(stmt)
    parent_before = parent_before.scalars().first()

    assert parent_before is not None

    # create active_virtual node with key of not active virtual node
    new_mo_id = 99999555
    existing_virtual_node = Obj(
        key=node_before.key,
        object_type_id=node_before.object_type_id,
        hierarchy_id=node_before.hierarchy_id,
        level=node_before.level,
        parent_id=node_before.parent_id,
        child_count=0,
        path=create_path_for_children_node_by_parent_node(parent_before),
        level_id=node_before.level_id,
        active=True,
    )
    session.add(existing_virtual_node)
    await session.flush()

    # create new node data
    new_n1_data = NodeData(
        level_id=existing_virtual_node.level_id,
        node_id=existing_virtual_node.id,
        mo_id=new_mo_id,
        mo_name=f"{new_mo_id}",
        mo_tmo_id=existing_virtual_node.object_type_id,
        mo_p_id=p_id,
        mo_active=True,
    )
    session.add(new_n1_data)
    parent_before.child_count += 1
    session.add(parent_before)
    await session.commit()
    parent_before_child_count = parent_before.child_count

    # check there is active node with this key
    stmt = select(Obj).where(
        Obj.active == True,  # noqa
        Obj.key == node_before.key,
        Obj.parent_id == node_before.parent_id,
    )
    active_virtual_node_with_key_exists = await session.execute(stmt)
    active_virtual_node_with_key_exists = (
        active_virtual_node_with_key_exists.scalars().first()
    )
    assert active_virtual_node_with_key_exists is not None
    active_virtual_node_with_key_exists.id == existing_virtual_node.id

    # get children by paths before
    path_before = create_path_for_children_node_by_parent_node(node_before)
    stmt = select(Obj).where(Obj.path == path_before)
    children_before = await session.execute(stmt)
    children_before = children_before.scalars().all()
    assert len(children_before) > 0
    children_ids_before = {c.id for c in children_before}

    await on_update_mo(msg=kafka_msg, session=session)

    # check - deletes old virtual node
    stmt = select(Obj).where(Obj.id == node_before_id)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is None

    # check node_data after - mo_active = True
    stmt = select(NodeData).where(NodeData.mo_id.in_([mo_id_1, mo_id_2]))
    node_datas_before = await session.execute(stmt)
    node_datas_before = node_datas_before.scalars().all()
    assert len(node_datas_before) == 2
    assert node_datas_before[0].mo_active is True
    assert node_datas_before[1].mo_active is True

    # check - changes NodeData.node_id to existing virtual node with equal key and active True
    assert node_datas_before[0].node_id == existing_virtual_node.id
    assert node_datas_before[1].node_id == existing_virtual_node.id

    # check parent after - child_count increased by 1
    await session.refresh(parent_before)
    assert parent_before.child_count == parent_before_child_count

    # check children paths after
    path_after = create_path_for_children_node_by_parent_node(
        existing_virtual_node
    )
    assert path_after != path_before

    stmt = select(Obj).where(Obj.path == path_after)
    children_after = await session.execute(stmt)
    children_after = children_after.scalars().all()
    assert len(children_after) > 0
    for c in children_after:
        assert c.id in children_ids_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_mo_update_changed_active_case_8(
    session: AsyncSession, mock_grpc_response
):
    """TEST With MO:updated. Hierarchy has structure : virtual level - virtual level - real level with same tmo_id.
    If the active attribute of MO was changed from False to True and this Mos corresponding to all levels.
    all corresponding not active nodes become active"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    mo_id = TMO_1[1111]["id"]
    tmo_id = TMO_1[1111]["tmo_id"]

    kafka_msg = {
        "objects": [
            {"id": mo_id, "tmo_id": tmo_id, "p_id": None, "active": True}
        ]
    }

    # get levels for this tmo_id
    stmt = (
        select(Level)
        .where(Level.object_type_id == tmo_id)
        .order_by(Level.level)
    )
    levels = await session.execute(stmt)
    levels = levels.scalars().all()
    assert len(levels) == 3
    assert levels[0].is_virtual is True
    assert levels[1].is_virtual is True
    assert levels[2].is_virtual is False

    # get node data of this mo
    stmt = select(NodeData).where(NodeData.mo_id == mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 3
    node_data_ids_before = [n_data.id for n_data in node_data_before]
    node_ids_before = [n_data.node_id for n_data in node_data_before]
    assert node_data_before[0].mo_active is False
    assert node_data_before[1].mo_active is False
    assert node_data_before[2].mo_active is False

    # get nodes before
    stmt = select(Obj).where(Obj.id.in_(node_ids_before)).order_by(Obj.level_id)
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 3
    assert nodes_before[0].active is False
    assert nodes_before[1].active is False
    assert nodes_before[1].parent_id == nodes_before[0].id
    assert nodes_before[2].active is False
    assert nodes_before[2].parent_id == nodes_before[1].id

    await on_update_mo(msg=kafka_msg, session=session)

    # check node data become active
    stmt = select(NodeData).where(NodeData.id.in_(node_data_ids_before))
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 3
    assert node_data_after[0].mo_active is True
    assert node_data_after[1].mo_active is True
    assert node_data_after[2].mo_active is True

    # check node become active
    stmt = select(Obj).where(Obj.id.in_(node_ids_before)).order_by(Obj.level_id)
    nodes_after = await session.execute(stmt)
    nodes_after = nodes_after.scalars().all()
    assert len(nodes_before) == 3
    assert nodes_after[0].active is True
    assert nodes_after[1].active is True
    assert nodes_after[1].parent_id == nodes_after[0].id
    assert nodes_after[2].active is True
    assert nodes_after[2].parent_id == nodes_after[1].id


# TODO test from true to false also must recalc child_count
