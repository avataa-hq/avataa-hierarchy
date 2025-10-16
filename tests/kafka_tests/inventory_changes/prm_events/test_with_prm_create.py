import copy

from kafka_tests.inventory_changes.utils_for_mo_events import (
    add_hierarchy_to_session,
    add_level_to_session,
    add_node_to_session,
)
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from kafka_config.msg.prm_msg import on_create_prm
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from schemas.main_base_connector import Base
from services.hierarchy.hierarchy_builder.configs import (
    DEFAULT_KEY_OF_NULL_NODE,
)
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
    get_node_key_data,
)

# default hierarchy data
DEFAULT_HIERARCHY_NAME_FOR_TESTS = "Default Hierarchy"

TMO_1 = {
    1: {
        "id": 1,
        "name": "MO 1",
        "status": "ACTIVE",
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
        "p_id": 0,
    },
}

TMO_2 = {
    2: {
        "id": 2,
        "name": "MO 2",
        "tmo_id": 2,
        "p_id": TMO_1[1]["id"],
        "4": "node 10 level 4",
        "44": "1",
    },
    22: {
        "id": 22,
        "name": "MO 22",
        "tmo_id": 2,
        "p_id": TMO_1[1]["id"],
        "4": "node 11 level 4",
        "44": "2",
    },
    222: {
        "id": 222,
        "name": "MO 222",
        "tmo_id": 2,
        "4": "node 12 level 4",
        "44": "3",
    },
}

TMO_21 = {
    21: {
        "id": 21,
        "name": "MO 21",
        "tmo_id": 21,
        "p_id": TMO_1[11]["id"],
        "41": "node 13 level 5",
        "4141": "1",
    },
    2121: {
        "id": 2121,
        "name": "MO 2121",
        "tmo_id": 21,
        "p_id": TMO_1[1111]["id"],
        "41": "node 14 level 5",
        "4141": "2",
    },
    212121: {
        "id": 212121,
        "name": "MO 212121",
        "tmo_id": 21,
        "p_id": TMO_1[1111]["id"],
        "41": "node 14 level 5",
        "4141": "2",
    },
    21212121: {
        "id": 21212121,
        "name": "MO 21212121",
        "tmo_id": 21,
        "p_id": TMO_1[1111]["id"],
        "41": "node 15 level 5",
        "4141": "2",
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
        "51": DEFAULT_KEY_OF_NULL_NODE,
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
        level_key_attrs=["4", "44"],
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
        level_key_attrs=["41", "4141"],
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
        mo_data=[TMO_21[2121], TMO_21[212121]],
        parent_node=node_9_level_3,
    )

    # node_15_level_5
    await add_node_to_session(
        session=session,
        level=level5,
        mo_data=[TMO_21[21212121]],
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
async def test_with_create_prm_case_1(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created - if real level.key_attrs consist of one prm, then with creating of this prm value
    (in case if node based on this mo exists):
    - does not create new node
    - updates key of corresponding real node
    - updates NodeData.unfolded_key
    - does not change other_attrs of Obj
    - does not change other attrs of NodeData"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_real_level = 31

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_real_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    assert len(level.key_attrs) == 1
    assert level.key_attrs[0].isdigit() is True
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_31[31]["id"]
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node ids for this level - before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_before = {n.id for n in level_nodes_before}
    count_of_level_nodes_before = len(level_node_ids_before)

    # check real node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == existing_mo_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]
    key_before = node.key

    node_before_attrs_data = {
        k: getattr(node, k)
        for k in vars(node)
        if not any([k.startswith("_"), k == "key"])
    }

    # check node_data of real node before
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    node_data_before_attrs_data = {
        k: getattr(node_data, k)
        for k in vars(node_data)
        if not k.startswith("_") or k != "unfolded_key"
    }

    await on_create_prm(kafka_msg, session)

    # does not create new node
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_after = {n.id for n in level_nodes_before}
    count_of_level_nodes_after = len(level_node_ids_before)
    assert level_node_ids_before == level_node_ids_after
    assert count_of_level_nodes_before == count_of_level_nodes_after

    await session.refresh(node)

    # updates key of corresponding real node
    assert node.key != key_before
    assert node.key == prm_value

    # does not change other_attrs of Obj
    for k, v in node_before_attrs_data.items():
        assert getattr(node, k) == v

    # check node_data of real node after
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]

    # updates NodeData.unfolded_key
    unfolded_key_after = node_data_after.unfolded_key

    for k, v in unfolded_key_after.items():
        assert unfolded_key_before[k] != v

    assert unfolded_key_after[tprm_id_as_str] == prm_value

    # does not change other attrs of NodeData
    for k, v in node_data_before_attrs_data.items():
        assert getattr(node_data_after, k) == v


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_2(session: AsyncSession):
    """TEST PRM:created - if real level.key_attrs consist of one prm, then with creating of this prm value
    (in case if node based on this mo does not exist):
    - does not create new node"""

    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_real_level = 31

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_real_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None
    assert level.is_virtual is False
    assert len(level.key_attrs) == 1
    assert level.key_attrs[0].isdigit() is True
    tprm_id_as_str = level.key_attrs[0]

    not_existing_mo_id = 99999
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": not_existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node for this mo_id does not exist
    stmt = select(Obj).where(Obj.object_id == not_existing_mo_id)
    not_exists = await session.execute(stmt)
    not_exists = not_exists.scalars().first()
    assert not_exists is None

    # check node_data for this mo_id does not exist
    stmt = select(NodeData).where(NodeData.mo_id == not_existing_mo_id)
    not_exists = await session.execute(stmt)
    not_exists = not_exists.scalars().first()
    assert not_exists is None

    await on_create_prm(kafka_msg, session)

    # check node for this mo_id does not exist
    stmt = select(Obj).where(Obj.object_id == not_existing_mo_id)
    not_exists = await session.execute(stmt)
    not_exists = not_exists.scalars().first()
    assert not_exists is None

    # check node_data for this mo_id does not exist
    stmt = select(NodeData).where(NodeData.mo_id == not_existing_mo_id)
    not_exists = await session.execute(stmt)
    not_exists = not_exists.scalars().first()
    assert not_exists is None


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_3(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created - if real level.key_attrs consist of two or more prm then with creating of this prm value
    (in case if node based on this mo exists):
    - does not create new node
    - updates key of real node
    - updates  only key of changed prm for NodeData.unfolded_key
    - does not change other_attrs of Obj
    - does not change other attrs of NodeData"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_real_level = 2

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_real_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    # check level.key_attrs more than one
    assert len(level.key_attrs) == 2
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is False
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_2[2]["id"]
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node ids for this level - before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_before = {n.id for n in level_nodes_before}
    count_of_level_nodes_before = len(level_node_ids_before)

    # check real node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.object_id == existing_mo_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]
    key_before = node.key

    node_before_attrs_data = {
        k: getattr(node, k)
        for k in vars(node)
        if not any([k.startswith("_"), k == "key"])
    }

    # check node_data of real node before
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    node_data_before_attrs_data = {
        k: getattr(node_data, k)
        for k in vars(node_data)
        if not k.startswith("_") or k != "unfolded_key"
    }

    # check prm_value in unfolded_key does not eq to new value
    assert unfolded_key_before[tprm_id_as_str] != prm_value
    await on_create_prm(kafka_msg, session)

    # does not create new node
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_after = {n.id for n in level_nodes_before}
    count_of_level_nodes_after = len(level_node_ids_before)
    assert level_node_ids_before == level_node_ids_after
    assert count_of_level_nodes_before == count_of_level_nodes_after

    await session.refresh(node)

    # updates key of corresponding real node
    assert node.key != key_before

    # does not change other_attrs of Obj
    for k, v in node_before_attrs_data.items():
        assert getattr(node, k) == v

    # check node_data of real node after
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]

    # updates NodeData.unfolded_key
    unfolded_key_after = node_data_after.unfolded_key

    assert (
        unfolded_key_before[tprm_id_as_str]
        != unfolded_key_after[tprm_id_as_str]
    )

    assert unfolded_key_after[tprm_id_as_str] == prm_value

    # does not change other attrs of NodeData
    for k, v in node_data_before_attrs_data.items():
        assert getattr(node_data_after, k) == v


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_4(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created -
    conditions:
    - if virtual level.key_attrs consist of one prm
    - if node based on this mo exists and consists only of one mo
    - if there is no node with such_key and parent
    expected result:
    - does not create new node
    - updates key of virtual node
    - updates  only key of changed prm for NodeData.unfolded_key
    - does not change other_attrs of Obj
    - does not change other attrs of NodeData"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_virt_level = 4

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_virt_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    assert len(level.key_attrs) == 1
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is True
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_4[4]["id"]
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node ids for this level - before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_before = {n.id for n in level_nodes_before}
    count_of_level_nodes_before = len(level_node_ids_before)

    # check node_data of virtual node before
    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    node_data_before_attrs_data = {
        k: getattr(node_data, k)
        for k in vars(node_data)
        if not k.startswith("_") or k != "unfolded_key"
    }

    # check virtual node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.id == node_data.node_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]
    key_before = node.key

    node_before_attrs_data = {
        k: getattr(node, k)
        for k in vars(node)
        if not any([k.startswith("_"), k == "key"])
    }

    await on_create_prm(kafka_msg, session)

    # does not create new node
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_after = {n.id for n in level_nodes_before}
    count_of_level_nodes_after = len(level_node_ids_before)
    assert level_node_ids_before == level_node_ids_after
    assert count_of_level_nodes_before == count_of_level_nodes_after

    await session.refresh(node)

    # updates key of corresponding virtual node
    assert node.key != key_before
    assert node.key == prm_value

    # does not change other_attrs of Obj
    for k, v in node_before_attrs_data.items():
        assert getattr(node, k) == v

    # check node_data of virtual node after
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]

    # updates NodeData.unfolded_key
    unfolded_key_after = node_data_after.unfolded_key

    for k, v in unfolded_key_after.items():
        assert unfolded_key_before[k] != v

    assert unfolded_key_after[tprm_id_as_str] == prm_value

    # does not change other attrs of NodeData
    for k, v in node_data_before_attrs_data.items():
        assert getattr(node_data_after, k) == v


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_5(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created -
    conditions:
    - if virtual level.key_attrs consist of two or more prm
    - if node based on this mo exists and consists only of one mo
    - if there is no node with such_key and parent
    expected result:
    - does not create new node
    - updates key of virtual node
    - updates  only key of changed prm for NodeData.unfolded_key
    - does not change other_attrs of Obj
    - does not change other attrs of NodeData"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_real_level = 21

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_real_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    # check level.key_attrs more than one
    assert len(level.key_attrs) == 2
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is True
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_21[21]["id"]
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node ids for this level - before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_before = {n.id for n in level_nodes_before}
    count_of_level_nodes_before = len(level_node_ids_before)

    # check node_data of virtual node before
    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    node_data_before_attrs_data = {
        k: getattr(node_data, k)
        for k in vars(node_data)
        if not k.startswith("_") or k != "unfolded_key"
    }

    # check virtual node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.id == node_data.node_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]
    key_before = node.key

    node_before_attrs_data = {
        k: getattr(node, k)
        for k in vars(node)
        if not any([k.startswith("_"), k == "key"])
    }

    # check prm_value in unfolded_key does not eq to new value
    assert unfolded_key_before[tprm_id_as_str] != prm_value
    await on_create_prm(kafka_msg, session)

    # does not create new node
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_after = {n.id for n in level_nodes_before}
    count_of_level_nodes_after = len(level_node_ids_before)
    assert level_node_ids_before == level_node_ids_after
    assert count_of_level_nodes_before == count_of_level_nodes_after

    # updated node
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.id == node_data.node_id
    )
    nodes_after = await session.execute(stmt)
    nodes_after = nodes_after.scalars().all()
    assert len(nodes_after) == 1
    node = nodes_after[0]

    # updates key of corresponding real node
    assert node.key != key_before

    # does not change other_attrs of Obj
    for k, v in node_before_attrs_data.items():
        assert getattr(node, k) == v

    # check node_data of real node after
    stmt = select(NodeData).where(NodeData.node_id == node.id)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().all()
    assert len(node_data_after) == 1
    node_data_after = node_data_after[0]

    # updates NodeData.unfolded_key
    unfolded_key_after = node_data_after.unfolded_key

    assert (
        unfolded_key_before[tprm_id_as_str]
        != unfolded_key_after[tprm_id_as_str]
    )

    assert unfolded_key_after[tprm_id_as_str] == prm_value

    # does not change other attrs of NodeData
    for k, v in node_data_before_attrs_data.items():
        assert getattr(node_data_after, k) == v


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_6(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created -
    conditions:
    - if virtual level.key_attrs consist of one prm
    - if node based on this mo exists and consists of two or more mo
    - if there is no node with such_key and parent
    expected result:
    - creates new node
    - updates  value of changed prm in unfolded_key for corresponding NodeData
    - updates  node_id of corresponding NodeData
    - does not change other_attrs of Obj
    - does not change other attrs of NodeData
    - changes path for corresponding children
    - changes child_count"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_virt_level = 3

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_virt_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    assert len(level.key_attrs) == 1
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is True
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_3[333]["id"]
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node_data of virtual node before
    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    # check virtual node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.id == node_data.node_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]

    common_node_child_count_before = node.child_count

    node_before_attrs_data = {
        k: getattr(node, k)
        for k in vars(node)
        if not any([k.startswith("_"), k == "key"])
    }

    # check node with new key does not exist
    unfolded_key_copy = copy.deepcopy(unfolded_key_before)
    unfolded_key_copy[tprm_id_as_str] = prm_value
    key_data = get_node_key_data(
        ordered_key_attrs=list(unfolded_key_copy),
        mo_data_with_params=unfolded_key_copy,
    )
    excpected_node_key = key_data.key

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    not_exists = await session.execute(stmt)
    not_exists = not_exists.scalars().first()
    assert not_exists is None

    # get old_children data
    stmt = select(NodeData).where(NodeData.mo_p_id == existing_mo_id)
    child_node_data = await session.execute(stmt)
    child_node_data = child_node_data.scalars().first()
    assert child_node_data is not None

    # get child node
    stmt = select(Obj).where(Obj.id == child_node_data.node_id)
    child_node = await session.execute(stmt)
    child_node = child_node.scalars().first()
    assert child_node is not None

    path_before = child_node.path
    assert path_before is not None

    await on_create_prm(kafka_msg, session)

    # creates new node
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    new_node = await session.execute(stmt)
    new_node = new_node.scalars().first()
    assert new_node is not None

    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]

    # updates  node_id of corresponding NodeData
    assert node_data.node_id == new_node.id
    # updates  value of changed prm in unfolded_key for corresponding NodeData
    for k, v in node_data.unfolded_key.items():
        if k == tprm_id_as_str:
            assert v == prm_value
        else:
            # does not change other attrs of NodeData
            assert v == unfolded_key_before[k]

    # does not change other_attrs of Obj
    new_node_attrs = {
        k: getattr(node, k)
        for k in vars(node)
        if not any(
            [k.startswith("_"), k == "key", k == "id", k == "child_count"]
        )
    }

    for k, v in new_node_attrs.items():
        assert node_before_attrs_data[k] == v

    # old node was not deleted
    stmt = select(Obj).where(Obj.id == node.id)
    old_node = await session.execute(stmt)
    old_node = old_node.scalars().first()
    assert old_node is not None

    # change child_count
    # Change to not equal
    assert old_node.child_count == common_node_child_count_before
    # Change to equal
    assert new_node.child_count == common_node_child_count_before

    # changes path for corresponding children

    await session.refresh(child_node)
    assert child_node.path is not None
    # Change to equal
    assert child_node.path == path_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_7(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created -
    conditions:
    - if virtual level.key_attrs consist of two or more prm
    - if node based on this mo exists and consists of two or more mo
    - if there is no node with such_key and parent
    expected result:
    - creates new node
    - updates  only key of changed prm for NodeData.unfolded_key
    - updates  node_id of corresponding NodeData
    - does not change other_attrs of Obj
    - does not change other attrs of NodeData
    - changes path for corresponding children"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_virt_level = 21

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_virt_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    assert len(level.key_attrs) == 2
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is True
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_21[2121]["id"]
    prm_value = "Created PRM value"

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node_data of virtual node before
    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    # check virtual node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.id == node_data.node_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]

    node_before_attrs_data = {
        k: getattr(node, k)
        for k in vars(node)
        if not any([k.startswith("_"), k == "key"])
    }

    # check node with new key does not exist
    unfolded_key_copy = copy.deepcopy(unfolded_key_before)
    unfolded_key_copy[tprm_id_as_str] = prm_value
    key_data = get_node_key_data(
        ordered_key_attrs=list(unfolded_key_copy),
        mo_data_with_params=unfolded_key_copy,
    )
    excpected_node_key = key_data.key

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    not_exists = await session.execute(stmt)
    not_exists = not_exists.scalars().first()
    assert not_exists is None

    # get old_children data
    stmt = select(NodeData).where(NodeData.mo_p_id == existing_mo_id)
    child_node_data = await session.execute(stmt)
    child_node_data = child_node_data.scalars().first()
    assert child_node_data is not None

    # get child node
    stmt = select(Obj).where(Obj.id == child_node_data.node_id)
    child_node = await session.execute(stmt)
    child_node = child_node.scalars().first()
    assert child_node is not None

    path_before = child_node.path
    assert path_before is not None

    await on_create_prm(kafka_msg, session)

    # creates new node
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    new_node = await session.execute(stmt)
    new_node = new_node.scalars().first()
    assert new_node is not None

    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]

    # updates  node_id of corresponding NodeData
    assert node_data.node_id == new_node.id
    # updates  value of changed prm in unfolded_key for corresponding NodeData
    for k, v in node_data.unfolded_key.items():
        if k == tprm_id_as_str:
            assert v == prm_value
        else:
            # does not change other attrs of NodeData
            assert v == unfolded_key_before[k]

    # does not change other_attrs of Obj
    new_node_attrs = {
        k: getattr(node, k)
        for k in vars(node)
        if not any(
            [k.startswith("_"), k == "key", k == "id", k == "child_count"]
        )
    }

    for k, v in new_node_attrs.items():
        assert node_before_attrs_data[k] == v

    # old node was not deleted
    stmt = select(Obj).where(Obj.id == node.id)
    old_node = await session.execute(stmt)
    old_node = old_node.scalars().first()
    assert old_node is not None

    # change child_count
    # Change to 2
    assert old_node.child_count == 2
    assert new_node.child_count == 2

    # changes path for corresponding children

    await session.refresh(child_node)
    assert child_node.path is not None
    # Change to equal
    assert child_node.path == path_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_8(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created -
    conditions:
    - if virtual level.key_attrs consist of two or more prm
    - if node based on this mo exists and consists of two or more mo
    - if there is node with such_key and parent
    expected result:
    - does not create new node
    - does not delete old virtual node
    - updates  NodeData.node_id
    - updates  NodeData.unfolded_key
    - does not change other attrs of NodeData
    - changes path for corresponding children"""

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_virt_level = 21

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_virt_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    assert len(level.key_attrs) == 2
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is True
    tprm_id_as_str = "41"

    existing_mo_id = TMO_21[2121]["id"]
    prm_value = TMO_21[21212121]["41"]

    kafka_msg = {
        "objects": [
            {
                "id": 9999,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": prm_value,
            }
        ]
    }

    # check node ids for this level - before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_before = {n.id for n in level_nodes_before}
    count_of_level_nodes_before = len(level_node_ids_before)

    # check node_data of virtual node before
    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data = node_data_before[0]
    node_data_node_id_before = node_data.node_id
    node_data_id_before = node_data.id

    unfolded_key_before = copy.deepcopy(node_data.unfolded_key)

    node_data_before_attrs_data = {
        k: getattr(node_data, k)
        for k in vars(node_data)
        if not any([k.startswith("_"), k == "unfolded_key", k == "node_id"])
    }

    # check virtual node exists before
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.id == node_data.node_id
    )
    nodes_before = await session.execute(stmt)
    nodes_before = nodes_before.scalars().all()
    assert len(nodes_before) == 1
    node = nodes_before[0]

    common_node_child_count_before = node.child_count

    # check node with new key exists
    unfolded_key_copy = copy.deepcopy(unfolded_key_before)
    unfolded_key_copy[tprm_id_as_str] = prm_value
    key_data = get_node_key_data(
        ordered_key_attrs=list(unfolded_key_copy),
        mo_data_with_params=unfolded_key_copy,
    )
    excpected_node_key = key_data.key

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    virt_node_with_key = await session.execute(stmt)
    virt_node_with_key = virt_node_with_key.scalars().first()
    assert virt_node_with_key is not None
    virt_node_with_key_child_count_before = virt_node_with_key.child_count

    # get old_children data
    stmt = select(NodeData).where(NodeData.mo_p_id == existing_mo_id)
    child_node_data = await session.execute(stmt)
    child_node_data = child_node_data.scalars().first()
    assert child_node_data is not None

    # get child node
    stmt = select(Obj).where(Obj.id == child_node_data.node_id)
    child_node = await session.execute(stmt)
    child_node = child_node.scalars().first()
    assert child_node is not None

    path_before = child_node.path
    assert path_before is not None

    await on_create_prm(kafka_msg, session)

    # does not create new node
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_after = await session.execute(stmt)
    level_nodes_after = level_nodes_after.scalars().all()
    level_node_ids_after = {n.id for n in level_nodes_after}
    count_of_level_nodes_after = len(level_nodes_after)
    assert level_node_ids_after == level_node_ids_before
    assert count_of_level_nodes_after == count_of_level_nodes_before

    # old node was not deleted
    stmt = select(Obj).where(Obj.id == node.id)
    old_node = await session.execute(stmt)
    old_node = old_node.scalars().first()
    assert old_node is not None

    #  updates  NodeData.node_id

    stmt = select(NodeData).where(NodeData.id == node_data_id_before)
    node_data_after = await session.execute(stmt)
    node_data_after = node_data_after.scalars().first()

    # Change to equal
    assert node_data_after.node_id == node_data_node_id_before
    # Change to not equal
    assert node_data_after.node_id != virt_node_with_key.id
    # updates  NodeData.unfolded_key

    assert node_data_after.unfolded_key[tprm_id_as_str] == prm_value

    # does not change other attrs of NodeData
    for k, v in node_data_before_attrs_data.items():
        assert getattr(node_data_after, k) == v

    await session.refresh(virt_node_with_key)
    # change child_count

    # Change to equal
    assert old_node.child_count == common_node_child_count_before
    # Change to equal
    assert (
        virt_node_with_key_child_count_before == virt_node_with_key.child_count
    )

    # changes path for corresponding children

    await session.refresh(child_node)
    assert child_node.path is not None
    # Change to equal
    assert child_node.path == path_before


@pytest.mark.asyncio(loop_scope="session")
async def test_with_create_prm_case_9(
    session: AsyncSession, mock_grpc_response
):
    """TEST PRM:created -
    conditions:
    - existing node corresponding to the first depth level
    - level is virtual
    - node created from several MO instances
    - changes corresponding to the one MO of this node
    expected result:
    - creates new virtual node
    - changes node data for this new node
    - changes all children node path
    """
    # по всем дочерним меняем путь

    get_mo_links_val = mock_grpc_response
    get_mo_links_val.return_value = {}
    hierarchy = await get_default_hierarchy_by_name(session=session)

    existing_tmo_id_of_virt_level = 1

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy.id,
        Level.object_type_id == existing_tmo_id_of_virt_level,
    )
    level = await session.execute(stmt)
    level = level.scalars().first()

    assert level is not None

    assert len(level.key_attrs) == 1
    assert level.key_attrs[0].isdigit() is True
    assert level.is_virtual is True
    tprm_id_as_str = level.key_attrs[0]

    existing_mo_id = TMO_1[1]["id"]
    new_prm_value = "NEW TRPM VALUE"

    kafka_msg = {
        "objects": [
            {
                "id": 1,
                "tprm_id": int(tprm_id_as_str),
                "mo_id": existing_mo_id,
                "value": new_prm_value,
            }
        ]
    }

    # check node_data of virtual node before
    stmt = select(NodeData).where(
        NodeData.mo_id == existing_mo_id, NodeData.level_id == level.id
    )
    node_data_before = await session.execute(stmt)
    node_data_before = node_data_before.scalars().all()
    assert len(node_data_before) == 1
    node_data_before = node_data_before[0]
    node_data_node_id_before = node_data_before.node_id
    # node_data_id_before = node_data_before.id
    unfolded_key_before = copy.deepcopy(node_data_before.unfolded_key)

    # Check that node created from more than on MO
    stmt = select(NodeData).where(NodeData.node_id == node_data_node_id_before)
    all_node_data_of_node = await session.execute(stmt)
    all_node_data_of_node = all_node_data_of_node.scalars().all()
    count_of_node_data_of_node_before = len(all_node_data_of_node)

    assert count_of_node_data_of_node_before == 3

    # check node ids for this level - before
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_before = await session.execute(stmt)
    level_nodes_before = level_nodes_before.scalars().all()
    level_node_ids_before = {n.id for n in level_nodes_before}
    count_of_level_nodes_before = len(level_node_ids_before)

    # get child count of current node
    stmt = select(Obj).where(Obj.id == node_data_node_id_before)
    node_before = await session.execute(stmt)
    node_before = node_before.scalars().first()
    assert node_before is not None
    # common_node_child_count_before = node_before.child_count

    # check node with new key not exists
    unfolded_key_copy = copy.deepcopy(unfolded_key_before)
    unfolded_key_copy[tprm_id_as_str] = new_prm_value
    key_data = get_node_key_data(
        ordered_key_attrs=list(unfolded_key_copy),
        mo_data_with_params=unfolded_key_copy,
    )
    excpected_node_key = key_data.key

    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    virt_node_with_key = await session.execute(stmt)
    virt_node_with_key = virt_node_with_key.scalars().first()
    assert virt_node_with_key is None

    # check children of this node
    stmt = select(NodeData).where(NodeData.mo_id == existing_mo_id)
    res = await session.execute(stmt)
    res = res.scalars().all()

    await on_create_prm(kafka_msg, session)

    # check that increased count of nodes for this level
    stmt = select(Obj).where(Obj.level_id == level.id)
    level_nodes_after = await session.execute(stmt)
    level_nodes_after = level_nodes_after.scalars().all()
    level_node_ids_after = {n.id for n in level_nodes_after}
    count_of_level_nodes_after = len(level_node_ids_after)

    assert count_of_level_nodes_after - count_of_level_nodes_before == 1

    # check old node was not deleted
    stmt = select(Obj).where(Obj.id == node_data_node_id_before)
    old_node = await session.execute(stmt)
    old_node = old_node.scalars().first()
    assert old_node is not None

    # check that count of corresponding node_data decreased by 1
    stmt = select(NodeData).where(NodeData.node_id == node_data_node_id_before)
    all_node_data_of_node_after = await session.execute(stmt)
    all_node_data_of_node_after = all_node_data_of_node_after.scalars().all()
    count_of_node_data_of_node_after = len(all_node_data_of_node_after)

    assert (
        count_of_node_data_of_node_before - count_of_node_data_of_node_after
        == 1
    )

    # check that new node was created with this key
    stmt = select(Obj).where(
        Obj.level_id == level.id, Obj.key == excpected_node_key
    )
    virt_node_with_key = await session.execute(stmt)
    virt_node_with_key = virt_node_with_key.scalars().first()
    assert virt_node_with_key is not None

    # check child count of new node
    assert virt_node_with_key.child_count == 1

    # check children
    path = create_path_for_children_node_by_parent_node(virt_node_with_key)

    stmt = select(Obj).where(Obj.path.like(f"{path}%"))
    res = await session.execute(stmt)
    res = res.scalars().all()
    assert len(res) > 0
