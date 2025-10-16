"""TESTS for HierarchyFilter.get_first_depth_children_nodes"""

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.datastructures import QueryParams

from common_utils.hierarchy_builder import HierarchyBuilder
from common_utils.hierarchy_filter import HierarchyFilter
from grpc_config.protobuf import mo_info_pb2
from schemas.hier_schemas import Hierarchy, Level, Obj
from schemas.main_base_connector import Base

MO_DB = {
    # TMO 1
    1: {"mo_id": 1, "tprm_values": {1: "object 1 tmo 1"}, "p_id": 0},
    2: {"mo_id": 2, "tprm_values": {1: "object 2 tmo 1"}, "p_id": 0},
    # TMO 2
    3: {"mo_id": 3, "tprm_values": {2: "object 3 tmo 2"}, "p_id": 1},
    4: {"mo_id": 4, "tprm_values": {2: "object 4 tmo 2"}, "p_id": 1},
    5: {"mo_id": 5, "tprm_values": {2: "object 5 tmo 2"}, "p_id": 2},
    6: {"mo_id": 6, "tprm_values": {2: "object 6 tmo 2"}, "p_id": 2},
    # TMO 3
    7: {"mo_id": 7, "tprm_values": {3: "7 and 8 tmo 3", 4: "7"}, "p_id": 3},
    8: {"mo_id": 8, "tprm_values": {3: "7 and 8 tmo 3", 4: "8"}, "p_id": 3},
    9: {
        "mo_id": 9,
        "tprm_values": {3: "9 and 10  tmo 3", 4: "9 and 10"},
        "p_id": 4,
    },
    10: {
        "mo_id": 10,
        "tprm_values": {3: "9 and 10  tmo 3", 4: "9 and 10"},
        "p_id": 4,
    },
    11: {
        "mo_id": 11,
        "tprm_values": {3: "object 11 ,12 13 and tmo 3", 4: "11 and 12"},
        "p_id": 6,
    },
    12: {
        "mo_id": 12,
        "tprm_values": {3: "object 11 ,12 13 and tmo 3", 4: "11 and 12"},
        "p_id": 6,
    },
    13: {
        "mo_id": 13,
        "tprm_values": {3: "object 11 ,12 13 and tmo 3", 4: "13"},
        "p_id": 6,
    },
}


MO_FOR_TMO_1 = [MO_DB[1], MO_DB[2]]
MO_FOR_TMO_2 = [MO_DB[3], MO_DB[4], MO_DB[5], MO_DB[6]]
MO_FOR_TMO_3 = [
    MO_DB[7],
    MO_DB[8],
    MO_DB[9],
    MO_DB[10],
    MO_DB[11],
    MO_DB[12],
    MO_DB[13],
]


INVENTORY_RESPONCE_FOR_TMO_1 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_1
]
INVENTORY_RESPONCE_FOR_TMO_2 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_2
]
INVENTORY_RESPONCE_FOR_TMO_3 = [
    mo_info_pb2.ResponseWithObjInfoByTMO(**mo) for mo in MO_FOR_TMO_3
]


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def session_fixture(session: AsyncSession, mocker):
    hierarchy = Hierarchy(name="Test hierarchy", author="Admin")
    session.add(hierarchy)
    await session.flush()
    level_1 = Level(
        hierarchy_id=hierarchy.id,
        name="REAL TMO 1 DEPTH 1",
        level=1,
        object_type_id=1,
        is_virtual=False,
        param_type_id=1,
        author="Admin",
    )
    session.add(level_1)
    await session.flush()
    level_2 = Level(
        hierarchy_id=hierarchy.id,
        name="REAL TMO 2 DEPTH 2",
        level=2,
        object_type_id=2,
        is_virtual=False,
        param_type_id=2,
        author="Admin",
        parent_id=level_1.id,
    )
    session.add(level_2)
    await session.flush()
    level_3 = Level(
        hierarchy_id=hierarchy.id,
        name="VIRTUAL TMO 3 DEPTH 3",
        level=3,
        object_type_id=3,
        is_virtual=True,
        param_type_id=3,
        author="Admin",
        parent_id=level_2.id,
    )
    session.add(level_3)
    await session.flush()
    level_4 = Level(
        hierarchy_id=hierarchy.id,
        name="VIRTUAL TMO 3 DEPTH 4",
        level=4,
        object_type_id=3,
        is_virtual=True,
        param_type_id=4,
        author="Admin",
        parent_id=level_3.id,
    )
    session.add(level_4)
    await session.flush()
    level_5 = Level(
        hierarchy_id=hierarchy.id,
        name="REAL TMO 3 DEPTH 5",
        level=5,
        object_type_id=3,
        is_virtual=False,
        param_type_id=4,
        author="Admin",
        parent_id=level_4.id,
    )
    session.add(level_5)
    await session.commit()

    INVENTORY_DB = {
        level_1.object_type_id: INVENTORY_RESPONCE_FOR_TMO_1,
        level_2.object_type_id: INVENTORY_RESPONCE_FOR_TMO_2,
        level_3.object_type_id: INVENTORY_RESPONCE_FOR_TMO_3,
    }

    async def mocked_async_generator(level):
        res = INVENTORY_DB[level.object_type_id]
        for item in res:
            yield item

    mocker.patch(
        "common_utils.hierarchy_builder.HierarchyBuilder.get_data_for_level",
        side_effect=mocked_async_generator,
    )

    hierarchy = HierarchyBuilder(hierarchy_id=hierarchy.id, db_session=session)
    await hierarchy.build_hierarchy()

    await session.flush()
    yield


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clean_test_data(session: AsyncSession):
    yield
    await session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        await session.execute(table.delete())
    await session.commit()


async def get_hierarchy(session: AsyncSession):
    """Returns hierarchy instance from database"""
    stmt = select(Hierarchy)
    res = await session.execute(stmt)
    return res.scalars().first()


async def get_nodes_by_key_and_level_depth(
    node_key: str, level_depth: int, session: AsyncSession
):
    stmt = select(Obj).where(Obj.key == node_key, Obj.level == level_depth)
    res = await session.execute(stmt)
    return res.scalars().all()


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_is_none_filter_on_real_level_lower_than_parent_node(
    session: AsyncSession,
    mock_grpc_get_mo_ids_by_filters,
):
    """Returns first level nodes if parent_id is None.
    Recalculate the child_count of these nodes based on the filter.
    The filter is applied to real objects on level deeper than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    get_mo_ids_by_fil.return_value = [13]
    # async def get_filtered_data_from_inventory(**kwargs):
    #     return [13]
    #
    # mocker.patch(
    #     "common_utils.hierarchy_filter.get_mo_matched_condition",
    #     side_effect=get_filtered_data_from_inventory,
    # )

    hierarchy = await get_hierarchy(session)
    filter_condition = QueryParams("tprm_id4|equals=13")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": None,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert len(res) == 1
    res = res[0]
    assert res.level == 1
    assert res.child_count == 1
    assert res.key == MO_DB[2]["tprm_values"][1]


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_real_node_filter_on_real_level_lower_than_parent_node(
    session: AsyncSession, mock_grpc_get_mo_ids_by_filters
):
    """Returns nodes where p_id = parent_id of real node.
    Recalculate the child_count of these nodes based on the filter.
    The filter is applied to real objects on level deeper than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    get_mo_ids_by_fil.return_value = [13]
    # async def get_filtered_data_from_inventory(**kwargs):
    #     return [13]
    #
    # mocker.patch(
    #     "common_utils.hierarchy_filter.get_mo_matched_condition",
    #     side_effect=get_filtered_data_from_inventory,
    # )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[2]["tprm_values"][1], level_depth=1, session=session
    )
    real_node = real_node[0]

    filter_condition = QueryParams("tprm_id4|equals=13")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert len(res) == 1
    res = res[0]
    assert res.level == real_node.level + 1
    assert res.child_count == 1
    assert res.key == MO_DB[6]["tprm_values"][2]


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_virtual_node_filter_on_real_level_lower_than_parent_node(
    session: AsyncSession,
    mock_grpc_get_mo_ids_by_filters,
):
    """Returns nodes where p_id = parent_id of virtual node.
    Recalculate the child_count of these nodes based on the filter.
    The filter is applied to real objects on level deeper than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    get_mo_ids_by_fil.return_value = [8]
    # async def get_filtered_data_from_inventory(**kwargs):
    #     return [8]
    #
    # mocker.patch(
    #     "common_utils.hierarchy_filter.get_mo_matched_condition",
    #     side_effect=get_filtered_data_from_inventory,
    # )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][3], level_depth=3, session=session
    )
    real_node = real_node[0]

    filter_condition = QueryParams("tprm_id4|equals=13")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert len(res) == 1
    res = res[0]
    assert res.level == real_node.level + 1
    assert res.child_count == 1
    assert res.key == MO_DB[8]["tprm_values"][4]


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_is_none_filter_on_real_level_lower_than_parent_node_no_matched_mos(
    session: AsyncSession, mock_grpc_get_mo_ids_by_filters
):
    """Returns empty list, because there are no MOs matched filter conditions.
    In case when parent_id is None and the filter is applied to real objects on level deeper than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    get_mo_ids_by_fil.return_value = []
    # async def get_filtered_data_from_inventory(**kwargs):
    #     return []
    #
    # mocker.patch(
    #     "common_utils.hierarchy_filter.get_mo_matched_condition",
    #     side_effect=get_filtered_data_from_inventory,
    # )

    hierarchy = await get_hierarchy(session)
    filter_condition = QueryParams("tprm_id4|equals=13")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": None,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()
    assert res == list()


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_real_node_filter_on_real_level_lower_than_parent_node_no_matched_mos(
    session: AsyncSession, mock_grpc_get_mo_ids_by_filters
):
    """Returns empty list, because there are no MOs matched filter conditions.
    In case when parent_id = parent_id of real node and the filter is applied to real objects on level deeper
    than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    get_mo_ids_by_fil.return_value = []
    # async def get_filtered_data_from_inventory(**kwargs):
    #     return []
    #
    # mocker.patch(
    #     "common_utils.hierarchy_filter.get_mo_matched_condition",
    #     side_effect=get_filtered_data_from_inventory,
    # )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[2]["tprm_values"][1], level_depth=1, session=session
    )
    real_node = real_node[0]

    filter_condition = QueryParams("tprm_id4|equals=13")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert res == list()


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_virtual_node_filter_on_real_level_lower_than_parent_node_no_matched_mos(
    session: AsyncSession, mock_grpc_get_mo_ids_by_filters
):
    """Returns empty list, because there are no MOs matched filter conditions.
    In case when parent_id = parent_id of virtual node and the filter is applied to real objects on level deeper
    than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    get_mo_ids_by_fil.return_value = []
    # async def get_filtered_data_from_inventory(**kwargs):
    #     return []
    #
    # mocker.patch(
    #     "common_utils.hierarchy_filter.get_mo_matched_condition",
    #     side_effect=get_filtered_data_from_inventory,
    # )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][3], level_depth=3, session=session
    )
    real_node = real_node[0]

    filter_condition = QueryParams("tprm_id4|equals=13")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert res == list()


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_real_node_filter_on_real_level_higher_than_parent_node(
    session: AsyncSession,
    mocker,
):
    """Returns nodes where p_id = parent_id of real node.
    The filter is applied to real objects on level higher than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        if kwargs["mo_ids"] == [1]:
            return [{"id": 1, "p_id": None}]

        if kwargs["mo_ids"] == [3]:
            return [{"id": 3, "p_id": 1}]
        assert False

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[3]["tprm_values"][2], level_depth=2, session=session
    )
    real_node = real_node[0]
    filter_condition = QueryParams("tprm_id1|equals=object 1 tmo 1")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 1,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()
    assert len(res) == 1
    res = res[0]
    assert res.level == real_node.level + 1
    assert res.child_count == 2
    assert res.key == MO_DB[7]["tprm_values"][3]


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_real_node_filter_on_real_level_higher_than_parent_node_no_matched_mos(
    session: AsyncSession, mocker
):
    """Returns empty list, because there are no MOs matched filter conditions.
    In case when parent_id = parent_id of real node and the filter is applied to real objects on level higher
    than parent_node"""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        if kwargs["mo_ids"] == [1]:
            return [{"id": 1, "p_id": None}]

        if kwargs["mo_ids"] == [3]:
            return []
        assert False

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[3]["tprm_values"][2], level_depth=2, session=session
    )
    real_node = real_node[0]
    filter_condition = QueryParams("tprm_id1|equals=object 1 tmo 11")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 1,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()
    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_virtual_node_filter_on_real_level_higher_than_parent_node_case_1(
    session: AsyncSession, mocker
):
    """Returns nodes where p_id = parent_id of real node.
    The filter is applied to real objects on level higher than parent_node.
    When children level is real and object_type_id same as parent_node level and has 1 children"""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        if kwargs.get("tprm_ids"):
            if kwargs["tprm_ids"] == [4]:
                return [{"id": 7, "p_id": 3, 4: "7"}]

        mo_ids = kwargs.get("mo_ids")

        if mo_ids:
            if mo_ids == [1]:
                return [{"id": 1, "p_id": None}]

            if mo_ids == [3]:
                return [{"id": 3, "p_id": 1}]

            if mo_ids == [7]:
                return [{"id": 7, "p_id": 3}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][4], level_depth=4, session=session
    )
    real_node = real_node[0]
    filter_condition = QueryParams("tprm_id1|equals=object 1 tmo 1")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 1,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert len(res) == 1
    res = res[0]
    assert res.level == real_node.level + 1
    assert res.child_count == 0
    assert res.key == MO_DB[7]["tprm_values"][4]


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_id_of_virtual_node_filter_on_real_level_higher_than_parent_node_case_2(
    session: AsyncSession, mocker
):
    """Returns nodes where p_id = parent_id of real node.
    The filter is applied to real objects on level higher than parent_node.
    When children level is virtual and object_type_id same as parent_node level and has 2 children"""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        if kwargs.get("tprm_ids"):
            if kwargs["tprm_ids"] == [3]:
                return [
                    {"id": 7, "p_id": 3, 3: "7 and 8 tmo 3"},
                    {"id": 8, "p_id": 3, 3: "7 and 8 tmo 3"},
                ]
            if kwargs["tprm_ids"] == [4]:
                return [
                    {"id": 7, "p_id": 3, 4: "7"},
                    {"id": 8, "p_id": 3, 4: "8"},
                ]

        mo_ids = kwargs.get("mo_ids")

        if mo_ids:
            if mo_ids == [1]:
                return [{"id": 1, "p_id": None}]

            if mo_ids == [3]:
                return [{"id": 3, "p_id": 1}]

            if mo_ids == [7]:
                return [{"id": 7, "p_id": 3}]
            if mo_ids == [8]:
                return [{"id": 8, "p_id": 3}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][3], level_depth=3, session=session
    )
    real_node = real_node[0]
    filter_condition = QueryParams("tprm_id1|equals=object 1 tmo 1")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 1,
        "session": session,
        "parent_node": real_node,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_first_depth_children_nodes()

    assert len(res) == 2
    for node in res:
        assert node.level == real_node.level + 1
        assert node.child_count == 1
        assert node.key in [
            MO_DB[7]["tprm_values"][4],
            MO_DB[8]["tprm_values"][4],
        ]
