"""TESTS for HierarchyFilter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids"""

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
        is_virtual=True,
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
async def test_parent_node_is_none_filter_on_real_level_lower_than_parent_node(
    session: AsyncSession,
    # mock_grpc_get_mo_ids_by_filters,
    # mock_grpc_get_mo_match_condition,
    # mock_grpc_get_tprm_data,
    mocker,
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = None
    The filter is applied to real objects on level lower than parent_node."""

    # mock grpc connection GetFilteredObjSpecial
    # get_mo_ids_by_fil = mock_grpc_get_mo_ids_by_filters
    # get_mo_ids_by_fil.return_value = [7, 9, 11]
    # get_mo_match_cond = mock_grpc_get_mo_match_condition
    # get_mo_match_cond.return_value = [{'id': 7, 2: "object 6 tmo 2", 3: "7 and 8 tmo 3",4: "7",},
    #                                            {'id': 9, 2: "object 6 tmo 2", 3: "9 and 10 tmo 3", 4: "9 and 10"}]
    # get_tprm_data = mock_grpc_get_tprm_data
    # get_tprm_data.return_value = [
    #     {"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}
    # ]
    # get_first_depth_children_n.return_value = \
    #     {
    #     1: Obj(**{"level": 3, "child_count": 2, "key":"7 and 8 tmo 3"}),
    #     2: Obj(**{"level": 3, "child_count": 2, "key":"9 and 10  tmo 3"}),
    # }

    async def mock_get_tprms_data_by_tprms_ids(**kwargs):
        return [{"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}]

    async def get_mo_ids_by_fil(**kwargs):
        return [7, 9, 11]

    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")
        mo_ids = kwargs.get("mo_ids")
        tprm_ids = kwargs.get("tprm_ids")
        if object_type_id == 3 and mo_ids == [7, 8, 9, 10, 11, 12, 13]:
            return [7, 9, 11]

        if tprm_ids == [4] and object_type_id == 3:
            return [
                {"id": 7, "p_id": 3, 4: "7"},
                {"id": 9, "p_id": 4, 4: "9 and 10"},
                {"id": 11, "p_id": 6, 4: "11 and 12"},
            ]

        if tprm_ids == [3] and object_type_id == 3:
            return [
                {"id": 7, "p_id": 3, 3: "7 and 8 tmo 3"},
                {"id": 9, "p_id": 4, 3: "9 and 10 tmo 3"},
                {"id": 11, "p_id": 6, 3: "object 11 ,12 13 and tmo 3"},
            ]

        if tprm_ids == [2] and object_type_id == 2:
            return [
                {"id": 3, "p_id": 1, 2: "object 3 tmo 2"},
                {"id": 4, "p_id": 1, 2: "object 4 tmo 2"},
                {"id": 5, "p_id": 2, 2: "object 5 tmo 2"},
                {"id": 6, "p_id": 2, 2: "object 6 tmo 2"},
            ]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.SearchClient.get_mo_ids_by_filters",
        side_effect=get_mo_ids_by_fil,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_tprms_data_by_tprms_ids",
        side_effect=mock_get_tprms_data_by_tprms_ids,
    )

    hierarchy = await get_hierarchy(session)

    filter_condition = QueryParams(
        "tprm_id3|equals=7 and 8 tmo 3&tprm_id3|equals=9 and 10 tmo 3"
    )

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": None,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()

    assert len(res) == 2

    stmt = select(Obj).where(Obj.id.in_(list(res.keys())))
    nodes = await session.execute(stmt)
    nodes = nodes.scalars().all()
    for node in nodes:
        assert node.level == 1
        assert node.key in [
            MO_DB[1]["tprm_values"][1],
            MO_DB[2]["tprm_values"][1],
        ]

        if node.key == MO_DB[1]["tprm_values"][1]:
            assert res[node.id] == {1, 7, 9}
        if node.key == MO_DB[2]["tprm_values"][1]:
            assert res[node.id] == {2, 11}


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_is_none_filter_on_virtual_level_lower_than_parent_node(
    session: AsyncSession,
    mocker,
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = None
    The filter is applied to virtual objects on level lower than parent_node."""

    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")

        tprm_ids = kwargs.get("tprm_ids")
        if object_type_id == 2 and tprm_ids == [2]:
            return [
                {"id": 3, "p_id": 1, 2: "object 3 tmo 2"},
                {"id": 6, "p_id": 2, 2: "object 6 tmo 2"},
            ]

    async def mock_get_tprms_data_by_tprms_ids(**kwargs):
        return [{"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}]

    async def mock_children_mo_id_grouped_by_parent_node_id(*args):
        res_dict = {6: [11, 12, 13], 3: [7, 8]}
        result = dict()
        for item in args[0].items:
            for level_request in item.level_data:
                result[level_request.node_id] = res_dict.get(
                    level_request.mo_ids[0]
                )
        return result

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_children_mo_id_grouped_by_parent_node_id",
        side_effect=mock_children_mo_id_grouped_by_parent_node_id,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_tprms_data_by_tprms_ids",
        side_effect=mock_get_tprms_data_by_tprms_ids,
    )

    hierarchy = await get_hierarchy(session)

    filter_condition = QueryParams(
        "tprm_id2|equals=object 3 tmo 2&tprm_id2|equals=object 6 tmo 2"
    )

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 2,
        "session": session,
        "parent_node": None,
        "column_filters": [],
    }
    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()
    assert len(res) == 2

    stmt = select(Obj).where(Obj.id.in_(list(res.keys())))
    nodes = await session.execute(stmt)
    nodes = nodes.scalars().all()
    for node in nodes:
        assert node.level == 1
        assert node.key in [
            MO_DB[1]["tprm_values"][1],
            MO_DB[2]["tprm_values"][1],
        ]

        if node.key == MO_DB[1]["tprm_values"][1]:
            assert res[node.id] == {1, 7, 8}
        if node.key == MO_DB[2]["tprm_values"][1]:
            assert res[node.id] == {2, 11, 12, 13}


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_is_real_filter_on_real_level_lower_than_parent_node(
    session: AsyncSession, mocker
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = real node
    The filter is applied to virtual objects on level lower than parent_node."""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")

        tprm_ids = kwargs.get("tprm_ids")
        if object_type_id == 2 and tprm_ids == [2]:
            return [
                {"id": 3, "p_id": 1, 2: "object 3 tmo 2"},
                {"id": 6, "p_id": 2, 2: "object 6 tmo 2"},
            ]

    async def mock_children_mo_id_grouped_by_parent_node_id(*args):
        res_dict = {6: [11, 12, 13], 3: [7, 8]}
        result = dict()
        for item in args[0].items:
            for level_request in item.level_data:
                result[level_request.node_id] = res_dict.get(
                    level_request.mo_ids[0]
                )
        return result

    async def mock_get_tprms_data_by_tprms_ids(**kwargs):
        return [{"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_children_mo_id_grouped_by_parent_node_id",
        side_effect=mock_children_mo_id_grouped_by_parent_node_id,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_tprms_data_by_tprms_ids",
        side_effect=mock_get_tprms_data_by_tprms_ids,
    )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[1]["tprm_values"][1], level_depth=1, session=session
    )

    filter_condition = QueryParams(
        "tprm_id2|equals=object 3 tmo 2&tprm_id2|equals=object 6 tmo 2"
    )

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 2,
        "session": session,
        "parent_node": real_node[0],
        "column_filters": [],
    }

    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()
    assert len(res) == 1
    stmt = select(Obj).where(Obj.id.in_(list(res.keys())))
    nodes = await session.execute(stmt)
    nodes = nodes.scalars().all()

    node = nodes[0]
    assert node.level == 2
    assert node.key == MO_DB[3]["tprm_values"][2]
    assert res[node.id] == {7, 8}


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_is_virtual_filter_on_real_level_lower_than_parent_node(
    session: AsyncSession, mocker
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = virtual node
    The filter is applied to real objects on level lower than parent_node."""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")
        mo_ids = kwargs.get("mo_ids")
        tprm_ids = kwargs.get("tprm_ids")
        if object_type_id == 3 and mo_ids:
            return [7]

        if object_type_id == 3 and tprm_ids == [4]:
            return [
                {"id": 7, "p_id": 3, 4: "7"},
                {"id": 8, "p_id": 3, 4: "8"},
                {"id": 9, "p_id": 4, 4: "9"},
            ]

        if object_type_id == 3 and tprm_ids == [3]:
            return [
                {"id": 7, "p_id": 3, 3: "7 and 8 tmo 3"},
                {"id": 8, "p_id": 3, 3: "7 and 8 tmo 3"},
            ]

    async def mock_children_mo_id_grouped_by_parent_node_id(*args):
        result = dict()
        for item in args[0].items:
            for level_request in item.level_data:
                result[level_request.node_id] = [7, 8]
        return result

    async def get_mo_ids_by_fil(**kwargs):
        return [7, 9, 11]

    async def mock_get_tprms_data_by_tprms_ids(**kwargs):
        return [{"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_children_mo_id_grouped_by_parent_node_id",
        side_effect=mock_children_mo_id_grouped_by_parent_node_id,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.SearchClient.get_mo_ids_by_filters",
        side_effect=get_mo_ids_by_fil,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_tprms_data_by_tprms_ids",
        side_effect=mock_get_tprms_data_by_tprms_ids,
    )

    hierarchy = await get_hierarchy(session)
    virtual_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[3]["tprm_values"][2], level_depth=2, session=session
    )

    filter_condition = QueryParams("tprm_id4|equals=7")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 3,
        "session": session,
        "parent_node": virtual_node[0],
        "column_filters": [],
    }

    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()
    assert len(res) == 1
    stmt = select(Obj).where(Obj.id.in_(list(res.keys())))
    nodes = await session.execute(stmt)
    nodes = nodes.scalars().all()
    node = nodes[0]
    assert node.level == 3
    assert node.key == MO_DB[7]["tprm_values"][3]
    assert res[node.id] == {7}


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_is_real_filter_on_real_level_higher_than_parent_node_but_node_is_last(
    session: AsyncSession, mocker
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = real node - last level without children
    The filter is applied to real objects on level higher than parent_node."""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")
        mo_ids = kwargs.get("mo_ids")
        tprm_ids = kwargs.get("tprm_ids")
        if object_type_id == 1 and mo_ids == [1]:
            return [{"id": 1, "p_id": None}]

        if object_type_id == 2 and tprm_ids == [2]:
            return [{"id": 3, "p_id": 1, 2: "object 3 tmo 2"}]

        if object_type_id == 3 and mo_ids == [7]:
            return [{"id": 7, "p_id": 3}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )

    hierarchy = await get_hierarchy(session)
    real_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][4], level_depth=5, session=session
    )

    filter_condition = QueryParams(
        "tprm_id1|equals=object 1 tmo 1&tprm_id1|equals=object 2 tmo 1"
    )

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 1,
        "session": session,
        "parent_node": real_node[0],
        "column_filters": [],
    }

    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()
    assert len(res) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_is_virtual_filter_on_real_level_higher_than_parent_node(
    session: AsyncSession, mocker
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = virtual node
    The filter is applied to real objects on level higher than parent_node."""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")
        mo_ids = kwargs.get("mo_ids")
        tprm_ids = kwargs.get("tprm_ids")
        if object_type_id == 1 and mo_ids == [1]:
            return [{"id": 1, "p_id": None}]

        if object_type_id == 2 and tprm_ids == [2]:
            return [{"id": 3, "p_id": 1, 2: "object 3 tmo 2"}]

        if object_type_id == 3 and tprm_ids == [4]:
            return [{"id": 7, "p_id": 3, 4: "7"}]

    async def mock_get_tprms_data_by_tprms_ids(**kwargs):
        return [{"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_tprms_data_by_tprms_ids",
        side_effect=mock_get_tprms_data_by_tprms_ids,
    )

    hierarchy = await get_hierarchy(session)
    virtual_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][4], level_depth=4, session=session
    )

    filter_condition = QueryParams("tprm_id1|equals=object 1 tmo 1")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 1,
        "session": session,
        "parent_node": virtual_node[0],
        "column_filters": [],
    }

    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()

    assert len(res) == 1
    for _, value in res.items():
        assert value == [7]


@pytest.mark.asyncio(loop_scope="session")
async def test_parent_node_is_virtual_filter_on_virtual_level_higher_than_parent_node(
    session: AsyncSession, mocker
):
    """Returns dict of first depth children node ids as keys and list of mo.ids as values, where mo.ids are
     object_ids of real children nodes in the hall hierarchy branch.
     In case when:
     parent_node = virtual node
    The filter is applied to virtual objects on level higher than parent_node."""

    # mock grpc connection GetFilteredObjSpecial
    async def get_filtered_data_from_inventory(**kwargs):
        object_type_id = kwargs.get("object_type_id")

        tprm_ids = kwargs.get("tprm_ids")

        if object_type_id == 2 and tprm_ids == [2]:
            return [{"id": 3, "p_id": 1, 2: "object 3 tmo 2"}]

        if object_type_id == 3 and tprm_ids == [4]:
            return [{"id": 7, "p_id": 3}]

    async def mock_get_tprms_data_by_tprms_ids(**kwargs):
        return [{"id": 3, "val_type": "str"}, {"id": 4, "val_type": "str"}]

    mocker.patch(
        "common_utils.hierarchy_filter.get_mo_matched_condition",
        side_effect=get_filtered_data_from_inventory,
    )
    mocker.patch(
        "common_utils.hierarchy_filter.get_tprms_data_by_tprms_ids",
        side_effect=mock_get_tprms_data_by_tprms_ids,
    )

    hierarchy = await get_hierarchy(session)
    virtual_node = await get_nodes_by_key_and_level_depth(
        node_key=MO_DB[7]["tprm_values"][4], level_depth=4, session=session
    )

    filter_condition = QueryParams("tprm_id2|equals=object 3 tmo 2")

    hierarchy_filter_data = {
        "hierarchy_id": hierarchy.id,
        "filter_conditions": filter_condition,
        "tmo_id": 2,
        "session": session,
        "parent_node": virtual_node[0],
        "column_filters": [],
    }

    hierarchy_filter = HierarchyFilter(**hierarchy_filter_data)

    res = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()

    assert len(res) == 1
    for _, value in res.items():
        assert value == [7]
