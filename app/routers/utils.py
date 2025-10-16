import http
import math
import time
from typing import Iterable, List

from fastapi import HTTPException
from google.protobuf import json_format
import grpc
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common_utils.elastic_client import ElasticClient
from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from common_utils.hierarchy_filter import HierarchyFilter
from grpc_config.inventory_utils import (
    get_mo_data_by_mo_ids,
    get_mo_prm_data_by_prm_ids,
    get_tprms_data_by_tprms_ids,
)
from grpc_config.protobuf import mo_info_pb2_grpc
from grpc_config.protobuf.mo_info_pb2 import RequestTMOlifecycleByTMOidList
from schemas.hier_schemas import Hierarchy, Level, Obj
from settings import INV_HOST, INVENTORY_GRPC_PORT


async def get_hierarchy_ids_for_tmos_with_lifecycle(session):
    uniq_tmo_for_hierarchy = await session.execute(
        select(Obj.object_type_id).distinct()
    )
    tmos = uniq_tmo_for_hierarchy.scalars().all()

    async with grpc.aio.insecure_channel(
        f"{INV_HOST}:{INVENTORY_GRPC_PORT}"
    ) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        info_request = RequestTMOlifecycleByTMOidList(tmo_ids=tmos)
        tmo_with_lifecycles = await stub.GetTMOlifecycle(info_request)
        tmo_with_lifecycles_as_dict = json_format.MessageToDict(
            tmo_with_lifecycles,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )
        hierarchy_ids = await session.execute(
            select(Obj.hierarchy_id)
            .where(
                Obj.object_type_id.in_(
                    tmo_with_lifecycles_as_dict["tmo_ids_with_lifecycle"]
                )
            )
            .distinct()
        )
        hierarchy = hierarchy_ids.scalars().all()

        response = await session.execute(
            select(Hierarchy).where(Hierarchy.id.in_(hierarchy))
        )
        response = response.scalars().all()

        if not response:
            raise HTTPException(
                status_code=http.HTTPStatus.NOT_FOUND, detail="empty hierarchy"
            )

        return response


async def update_nodes_key_if_mo_link_or_prm_link(
    nodes: Iterable[Obj], session: AsyncSession
):
    """Returns nodes. Updates values of node.key for all nodes that have param_types val_type equal to
    mo_link or prm_link. Does not save changes in database"""

    # get type of level_object_type
    levels_ids = {item.level_id for item in nodes}

    param_types_by_levels = dict()

    if levels_ids:
        stmt = select(Level).where(Level.id.in_(levels_ids))
        levels = await session.execute(stmt)
        levels = levels.scalars().all()
        param_types_by_levels = dict()
        for level in levels:
            if (
                level.key_attrs
                and isinstance(level.key_attrs, list)
                and len(level.key_attrs) == 1
                and level.key_attrs[0].isdigit()
            ):
                param_types_by_levels[int(level.key_attrs[0])] = level.id
            elif not level.key_attrs:
                param_types_by_levels[level.param_type_id] = level.id
            else:
                raise ValueError(
                    "Incorrect value for level key attrs or param_type_id"
                )

    # get data of object_types from inventory
    if param_types_by_levels:
        tprms_data = await get_tprms_data_by_tprms_ids(
            param_types_by_levels.keys()
        )

        levels_with_mo_link_tprms = set()
        levels_with_prm_link_tprms = set()
        dict_of_sets = {
            "mo_link": levels_with_mo_link_tprms,
            "two-way link": levels_with_mo_link_tprms,
            "prm_link": levels_with_prm_link_tprms,
        }

        for tprm in tprms_data:
            set_of_interest = dict_of_sets.get(tprm["val_type"])
            if set_of_interest is not None:
                level_id = param_types_by_levels.get(tprm["id"])
                set_of_interest.add(level_id)

        node_change_key_to_mo_name = list()

        node_change_key_to_prm_value = list()

        dict_of_sets = {}
        for level_id in levels_with_mo_link_tprms:
            dict_of_sets[level_id] = node_change_key_to_mo_name
        for level_id in levels_with_prm_link_tprms:
            dict_of_sets[level_id] = node_change_key_to_prm_value

        for item in nodes:
            list_to_update_key = dict_of_sets.get(item.level_id)
            if list_to_update_key is not None:
                if item.key.isdigit():
                    list_to_update_key.append(item)

        if node_change_key_to_mo_name or node_change_key_to_prm_value:
            async with grpc.aio.insecure_channel(
                f"{INV_HOST}:{INVENTORY_GRPC_PORT}"
            ) as channel:
                if node_change_key_to_mo_name:
                    # get mo data
                    node_keys = [
                        int(node.key) for node in node_change_key_to_mo_name
                    ]
                    mo_link_data_gen = get_mo_data_by_mo_ids(
                        channel, mo_ids=node_keys
                    )
                    mo_link_data_dict = dict()
                    async for msg in mo_link_data_gen:
                        for mo_data in msg.list_of_mo:
                            mo_link_data_dict[str(mo_data.id)] = {
                                "name": mo_data.name,
                                "id": mo_data.id,
                                "tmo_id": mo_data.tmo_id,
                            }

                    for res_item in node_change_key_to_mo_name:
                        value_exists = mo_link_data_dict.get(res_item.key, None)
                        if value_exists is not None:
                            res_item.key = value_exists["name"]
                            res_item.object_id = value_exists["id"]
                            res_item.object_type_id = value_exists["tmo_id"]

                if node_change_key_to_prm_value:
                    # get prm data
                    node_keys = [
                        int(node.key) for node in node_change_key_to_prm_value
                    ]
                    prm_link_data_gen = get_mo_prm_data_by_prm_ids(
                        channel, prm_ids=node_keys
                    )
                    prm_link_data_dict = dict()
                    async for msg in prm_link_data_gen:
                        for prm_data in msg.list_of_prm:
                            prm_link_data_dict[str(prm_data.id)] = (
                                prm_data.value
                            )

                    for res_item in node_change_key_to_prm_value:
                        value_exists = prm_link_data_dict.get(
                            res_item.key, None
                        )
                        if value_exists is not None:
                            res_item.key = value_exists

    return nodes


async def update_nodes_key_if_mo_link_or_prm_link_as_dict(
    nodes: Iterable[dict], session: AsyncSession
):
    """Returns nodes. Updates values of node.key for all nodes that have param_types val_type equal to
    mo_link or prm_link. Does not save changes in database"""

    # get type of level_object_type
    levels_ids = {item["level_id"] for item in nodes}

    param_types_by_levels = dict()

    if levels_ids:
        stmt = select(Level).where(Level.id.in_(levels_ids))
        levels = await session.execute(stmt)
        levels = levels.scalars().all()
        param_types_by_levels = {
            level.param_type_id: level.id for level in levels
        }

    # get data of object_types from inventory
    if param_types_by_levels:
        tprms_data = await get_tprms_data_by_tprms_ids(
            param_types_by_levels.keys()
        )

        levels_with_mo_link_tprms = set()
        levels_with_prm_link_tprms = set()
        dict_of_sets = {
            "mo_link": levels_with_mo_link_tprms,
            "two-way link": levels_with_mo_link_tprms,
            "prm_link": levels_with_prm_link_tprms,
        }

        for tprm in tprms_data:
            set_of_interest = dict_of_sets.get(tprm["val_type"])
            if set_of_interest is not None:
                level_id = param_types_by_levels.get(tprm["id"])
                set_of_interest.add(level_id)

        node_change_key_to_mo_name = list()

        node_change_key_to_prm_value = list()

        dict_of_sets = {}
        for level_id in levels_with_mo_link_tprms:
            dict_of_sets[level_id] = node_change_key_to_mo_name
        for level_id in levels_with_prm_link_tprms:
            dict_of_sets[level_id] = node_change_key_to_prm_value

        for item in nodes:
            list_to_update_key = dict_of_sets.get(item["level_id"])
            if list_to_update_key is not None:
                if item["key"].isdigit():
                    list_to_update_key.append(item)

        if node_change_key_to_mo_name or node_change_key_to_prm_value:
            async with grpc.aio.insecure_channel(
                f"{INV_HOST}:{INVENTORY_GRPC_PORT}"
            ) as channel:
                if node_change_key_to_mo_name:
                    # get mo data
                    node_keys = [
                        int(node["key"]) for node in node_change_key_to_mo_name
                    ]
                    mo_link_data_gen = get_mo_data_by_mo_ids(
                        channel, mo_ids=node_keys
                    )
                    mo_link_data_dict = dict()
                    async for msg in mo_link_data_gen:
                        for mo_data in msg.list_of_mo:
                            mo_link_data_dict[str(mo_data.id)] = {
                                "name": mo_data.name,
                                "id": mo_data.id,
                                "tmo_id": mo_data.tmo_id,
                            }

                    for res_item in node_change_key_to_mo_name:
                        value_exists = mo_link_data_dict.get(
                            res_item["key"], None
                        )
                        if value_exists is not None:
                            res_item["key"] = value_exists["name"]
                            res_item["object_id"] = value_exists["id"]
                            res_item["object_type_id"] = value_exists["tmo_id"]

                if node_change_key_to_prm_value:
                    # get prm data
                    node_keys = [
                        int(node["key"])
                        for node in node_change_key_to_prm_value
                    ]
                    prm_link_data_gen = get_mo_prm_data_by_prm_ids(
                        channel, prm_ids=node_keys
                    )
                    prm_link_data_dict = dict()
                    async for msg in prm_link_data_gen:
                        for prm_data in msg.list_of_prm:
                            prm_link_data_dict[str(prm_data.id)] = (
                                prm_data.value
                            )

                    for res_item in node_change_key_to_prm_value:
                        value_exists = prm_link_data_dict.get(
                            res_item["key"], None
                        )
                        if value_exists is not None:
                            res_item["key"] = value_exists

    return nodes


def divides_list_on_parts_by_step_limit(
    list_of_data: List, items_in_step: int = 30000
) -> List[List]:
    """Divides list on parts and returns list of parts if len of list_of_data more than items_in_step"""
    res = []
    len_of_list = len(list_of_data)
    if len_of_list > items_in_step:
        steps = math.ceil(len_of_list / items_in_step)
        for step in range(steps):
            start = step * items_in_step
            end = start + items_in_step
            res.append(list_of_data[start:end])
    else:
        return [list_of_data]
    return res


async def get_filtered_result(
    session: AsyncSession,
    hierarchy_id,
    tmo_id: int,
    hierarchy_filter_data: dict,
):
    stmt = select(Level).where(
        Level.object_type_id == tmo_id, Level.hierarchy_id == hierarchy_id
    )
    levels_with_tmo = await session.execute(stmt)
    levels_with_tmo = levels_with_tmo.scalars().all()

    if not levels_with_tmo:
        raise HTTPException(
            status_code=422,
            detail=f"Levels with tmo =={tmo_id} does not exist in this hierarchy",
        )

    hierarchy_filter = HierarchyFilter(
        collect_data_cache=True, **hierarchy_filter_data
    )

    nodes = await hierarchy_filter.get_first_depth_children_nodes()
    node_real_children = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()

    return nodes, node_real_children


async def get_total_results(
    session: AsyncSession, parent_id: str, hierarchy_exist
):
    print("get total results")
    # else return results without filter
    elastic_client = ElasticClient(hierarchy_exist.id)
    st = time.time()
    # stmt = select(Obj).where(Obj.parent_id == parent_id, Obj.hierarchy_id == hierarchy_id).order_by(Obj.key)
    # nodes = await session.execute(stmt)
    # nodes = nodes.scalars().all()
    nodes = await elastic_client.get_nodes_by_parents(
        parent_id, order_by=["key"]
    )
    print(f"select objects took: {time.time() - st}")
    st = time.time()
    stmt = select(Level).where(Level.hierarchy_id == hierarchy_exist.id)
    h_levels = await session.execute(stmt)
    h_levels = h_levels.scalars().all()
    print(f"select levels took: {time.time() - st}")
    levels_param_type_ids_dict = {
        level.id: level.param_type_id for level in h_levels
    }
    tprms_data = await get_tprms_data_by_tprms_ids(
        tprm_ids=levels_param_type_ids_dict.values()
    )
    tprms_val_types = {tprm["id"]: tprm["val_type"] for tprm in tprms_data}

    level_ids_with_mo_link = {
        level_id
        for level_id, level_param_type_id in levels_param_type_ids_dict.items()
        if tprms_val_types.get(level_param_type_id, None)
        in {"mo_link", "two-way link"}
    }

    node_real_children = {}

    st = time.time()
    print("root nodes")
    print(nodes)
    for node in nodes:
        if node["level_id"] in level_ids_with_mo_link and node["key"].isdigit():
            node_real_children[node["id"]] = [int(node["key"])]
        elif node["object_id"]:
            node_real_children[node["id"]] = [node["object_id"]]
        else:
            node_real_children[node["id"]] = list()
    print(f"iterating by nodes ({len(nodes)}) took: {time.time() - st}")
    node_id_top_parent_id_cache = {node["id"]: node["id"] for node in nodes}

    # st = time.time()
    # first_step = divides_list_on_parts_by_step_limit([node["id"] for node in nodes], 30000)
    # parent_ids_order = first_step
    # print(f'splitting on parts took: {time.time() - st}')
    # st = time.time()
    # for parent_node_ids in parent_ids_order:
    #     st1 = time.time()
    #     if hierarchy_exist.create_empty_nodes:
    #         # stmt = select(Obj).where(Obj.parent_id.in_(parent_node_ids))
    #         # stmt = (select(Obj.id, Obj.parent_id, Obj.level_id, Obj.key, Obj.object_id, Obj.child_count)
    #         #         .where(Obj.parent_id.in_(parent_node_ids)))
    #         inner_nodes = await elastic_client.get_nodes_by_parents(parent_node_ids)
    #     else:
    #         # stmt = select(Obj).where(Obj.parent_id.in_(parent_node_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE)
    #         # stmt = (select(Obj.id, Obj.parent_id, Obj.level_id, Obj.key, Obj.object_id, Obj.child_count)
    #         #         .where(Obj.parent_id.in_(parent_node_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE))
    #         inner_nodes = await elastic_client.get_nodes_by_parents(parent_node_ids, include_default=False)
    #     # inner_nodes = await session.execute(stmt)
    #     # inner_nodes = inner_nodes.scalars().all()
    #     # inner_nodes = inner_nodes.mappings().all()
    #     print(len(inner_nodes))
    #     print(f'---selecting objects took: {time.time() - st1}')
    #
    #     st1 = time.time()
    #     if inner_nodes:
    #         for node in inner_nodes:
    #             node_parent = node_id_top_parent_id_cache.get(node['parent_id'], False)
    #             if node_parent:
    #                 node_id_top_parent_id_cache[node['id']] = node_parent
    #                 if node['level_id'] in level_ids_with_mo_link and node['key'].isdigit():
    #                     node_real_children[node_parent].append(int(node['key']))
    #                 elif node['object_id']:
    #                     node_real_children[node_parent].append(node['object_id'])
    #                 else:
    #                     continue
    #
    #         print(f'---iterating inner nodes took: {time.time() - st1}')
    #         next_step = [item['id'] for item in inner_nodes if item['child_count'] > 0]
    #         if len(next_step) > 30000:
    #             parent_ids_order.extend(divides_list_on_parts_by_step_limit(next_step, 30000))
    #         else:
    #             parent_ids_order.append(next_step)
    parents = [node["id"] for node in nodes]
    while True:
        st1 = time.time()
        if hierarchy_exist.create_empty_nodes:
            # stmt = select(Obj).where(Obj.parent_id.in_(parent_node_ids))
            # stmt = (select(Obj.id, Obj.parent_id, Obj.level_id, Obj.key, Obj.object_id, Obj.child_count)
            #         .where(Obj.parent_id.in_(parent_node_ids)))
            inner_nodes = await elastic_client.get_nodes_by_parents(parents)
        else:
            # stmt = select(Obj).where(Obj.parent_id.in_(parent_node_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE)
            # stmt = (select(Obj.id, Obj.parent_id, Obj.level_id, Obj.key, Obj.object_id, Obj.child_count)
            #         .where(Obj.parent_id.in_(parent_node_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE))
            inner_nodes = await elastic_client.get_nodes_by_parents(
                parents, include_default=False
            )
        # inner_nodes = await session.execute(stmt)
        # inner_nodes = inner_nodes.scalars().all()
        # inner_nodes = inner_nodes.mappings().all()
        print(len(inner_nodes))
        print(f"---selecting objects took: {time.time() - st1}")

        st1 = time.time()
        if inner_nodes:
            for node in inner_nodes:
                node_parent = node_id_top_parent_id_cache.get(
                    node["parent_id"], False
                )
                if node_parent:
                    node_id_top_parent_id_cache[node["id"]] = node_parent
                    if (
                        node["level_id"] in level_ids_with_mo_link
                        and node["key"].isdigit()
                    ):
                        node_real_children[node_parent].append(int(node["key"]))
                    elif node["object_id"]:
                        node_real_children[node_parent].append(
                            node["object_id"]
                        )
                    else:
                        continue

            print(f"---iterating inner nodes took: {time.time() - st1}")
            parents = [
                item["id"] for item in inner_nodes if item["child_count"] > 0
            ]
            # if len(next_step) > 30000:
            #     parent_ids_order.extend(divides_list_on_parts_by_step_limit(next_step, 30000))
            # else:
            #     parent_ids_order.append(next_step)
            if len(parents) == 0:
                break
        else:
            break
    print(f"iterating by parts took: {time.time() - st}")

    return nodes, node_real_children


async def get_nodes_and_real_children_for_elastic(
    session: AsyncSession, parent_id: str, hierarchy_exist
):
    print("get total results")
    # else return results without filter
    st = time.time()
    stmt = (
        select(Obj)
        .where(
            Obj.parent_id == parent_id, Obj.hierarchy_id == hierarchy_exist.id
        )
        .order_by(Obj.key)
    )
    nodes = await session.execute(stmt)
    nodes = nodes.scalars().all()
    print(f"select objects took: {time.time() - st}")
    st = time.time()
    stmt = select(Level).where(Level.hierarchy_id == hierarchy_exist.id)
    h_levels = await session.execute(stmt)
    h_levels = h_levels.scalars().all()
    print(f"select levels took: {time.time() - st}")
    levels_param_type_ids_dict = {
        level.id: level.param_type_id for level in h_levels
    }
    tprms_data = await get_tprms_data_by_tprms_ids(
        tprm_ids=levels_param_type_ids_dict.values()
    )
    tprms_val_types = {tprm["id"]: tprm["val_type"] for tprm in tprms_data}

    level_ids_with_mo_link = {
        level_id
        for level_id, level_param_type_id in levels_param_type_ids_dict.items()
        if tprms_val_types.get(level_param_type_id, None)
        in {"mo_link", "two-way link"}
    }

    node_real_children = {}

    st = time.time()
    print("root nodes")
    print(nodes)
    for node in nodes:
        if node.level_id in level_ids_with_mo_link and node.key.isdigit():
            node_real_children[node.id] = [int(node.key)]
        elif node.object_id:
            node_real_children[node.id] = [node.object_id]
        else:
            node_real_children[node.id] = list()
    print(f"iterating by nodes ({len(nodes)}) took: {time.time() - st}")
    node_id_top_parent_id_cache = {node.id: node.id for node in nodes}

    st = time.time()
    first_step = divides_list_on_parts_by_step_limit(
        [node.id for node in nodes], 30000
    )
    parent_ids_order = first_step
    print(f"splitting on parts took: {time.time() - st}")
    st = time.time()
    for parent_node_ids in parent_ids_order:
        st1 = time.time()
        if hierarchy_exist.create_empty_nodes:
            stmt = select(
                Obj.id,
                Obj.parent_id,
                Obj.level_id,
                Obj.key,
                Obj.object_id,
                Obj.child_count,
            ).where(Obj.parent_id.in_(parent_node_ids))
        else:
            stmt = select(
                Obj.id,
                Obj.parent_id,
                Obj.level_id,
                Obj.key,
                Obj.object_id,
                Obj.child_count,
            ).where(
                Obj.parent_id.in_(parent_node_ids),
                Obj.key != DEFAULT_KEY_OF_NULL_NODE,
            )
        inner_nodes = await session.execute(stmt)
        inner_nodes = inner_nodes.mappings().all()
        print(len(inner_nodes))
        print(f"---selecting objects took: {time.time() - st1}")

        st1 = time.time()
        if inner_nodes:
            for node in inner_nodes:
                node_parent = node_id_top_parent_id_cache.get(
                    node["parent_id"], False
                )
                if node_parent:
                    node_id_top_parent_id_cache[node["id"]] = node_parent
                    if (
                        node["level_id"] in level_ids_with_mo_link
                        and node["key"].isdigit()
                    ):
                        node_real_children[node_parent].append(int(node["key"]))
                    elif node["object_id"]:
                        node_real_children[node_parent].append(
                            node["object_id"]
                        )
                    else:
                        continue

            print(f"---iterating inner nodes took: {time.time() - st1}")
            next_step = [
                item["id"] for item in inner_nodes if item["child_count"] > 0
            ]
            if len(next_step) > 30000:
                parent_ids_order.extend(
                    divides_list_on_parts_by_step_limit(next_step, 30000)
                )
            else:
                parent_ids_order.append(next_step)
    print(f"iterating by parts took: {time.time() - st}")

    return nodes, node_real_children


async def get_nodes_and_real_children(
    session: AsyncSession, parent_id: str, hierarchy: Hierarchy
):
    print("get total results")
    # else return results without filter
    elastic_client = ElasticClient(hierarchy.id)
    st = time.time()
    stmt = (
        select(Obj)
        .where(Obj.parent_id == parent_id, Obj.hierarchy_id == hierarchy.id)
        .order_by(Obj.key)
    )
    nodes = await session.execute(stmt)
    nodes = nodes.scalars().all()
    print(f"select objects took: {time.time() - st}")
    # print([node.id for node in nodes])
    node_real_children = elastic_client.get_nodes_children(
        [node.id for node in nodes]
    )

    return nodes, node_real_children
