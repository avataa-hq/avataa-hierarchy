from functools import reduce
from math import ceil
from typing import List
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.datastructures import QueryParams

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from grpc_config.inventory_utils import (
    get_children_mo_id_grouped_by_parent_node_id,
    get_mo_matched_condition,
    get_tprms_data_by_tprms_ids,
)
from grpc_config.protobuf.mo_info_pb2 import (
    RequestLevel,
    RequestListLevels,
    RequestNode,
)
from grpc_config.search.client import SearchClient
from models import FilterColumn
from schemas.hier_schemas import Hierarchy, Level, Obj


async def check_if_node_match_condition_for_real_node(
    real_object: Obj, level: Level, filter_conditions: QueryParams = None
):
    """Returns additional info"""

    additional_params = dict()
    if filter_conditions:
        additional_params["query_params"] = filter_conditions

    res = await get_mo_matched_condition(
        mo_ids=[real_object.object_id],
        object_type_id=level.object_type_id,
        **additional_params,
    )
    # res = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id, mo_ids=[real_object.object_id],
    #                                                **additional_params)
    if not res:
        return dict()
    res = res[0]
    base_parent_ids = {res["p_id"]: {res["id"]}}
    return {
        "node": real_object,
        "base_mo_ids": {real_object.object_id},
        "base_parent_ids": base_parent_ids,
        "flag": "real",
    }


async def get_mo_data_for_virtual_node(
    virtual_object: Obj, level: Level, filter_conditions: QueryParams = None
):
    if filter_conditions:
        res = await get_mo_matched_condition(
            object_type_id=level.object_type_id,
            tprm_ids=[level.param_type_id],
            query_params=filter_conditions,
        )
        # res = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id, tprm_ids=[level.param_type_id],
        #                                                query_params=filter_conditions)
    else:
        filter_conditions = QueryParams(
            {f"tprm_id{level.param_type_id}|equals": virtual_object.key}
        )

        res = await get_mo_matched_condition(
            object_type_id=level.object_type_id,
            tprm_ids=[level.param_type_id],
            query_params=filter_conditions,
        )
        # res = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
        #                                                tprm_ids=[level.param_type_id], query_params=filter_conditions)

    if not res:
        return dict()
    base_parent_ids = dict()
    [
        base_parent_ids.setdefault(mo_dict["p_id"], set()).add(mo_dict["id"])
        for mo_dict in res
    ]

    return {
        "node": virtual_object,
        "base_mo_ids": {mo_dict["id"] for mo_dict in res},
        "base_parent_ids": base_parent_ids,
        "flag": "virtual",
    }


async def get_mo_data_for_node(
    obj: Obj, level: Level, filter_conditions: QueryParams = None
):
    if obj.object_id:
        return await check_if_node_match_condition_for_real_node(
            obj, level, filter_conditions
        )
    else:
        return await get_mo_data_for_virtual_node(obj, level, filter_conditions)


class HierarchyFilter:
    def __init__(
        self,
        hierarchy_id: int,
        parent_node: Obj | None,
        filter_conditions: QueryParams,
        tmo_id: int,
        session: AsyncSession,
        column_filters: list[FilterColumn],
        collect_data_cache: bool = False,
    ):
        self.hierarchy_id = hierarchy_id
        self.parent_node = parent_node
        self.filter_conditions = filter_conditions
        self.tmo_id = tmo_id
        self.session = session
        self.real_and_virtual_deepest_levels = None
        self.parent_node_id = None
        self.parent_node_level_depth = None
        self.collect_data_cache = collect_data_cache
        self.level_cache = dict()
        self.node_cache = dict()
        self.result_cache = list()
        self.cache_of_mo_ids_of_upper_nodes = dict()
        self.cache_of_upper_real_nodes = list()
        self._hierarchy = None
        self._levels_of_hierarchy = None
        self._ids_of_levels_with_mo_link = None
        self.column_filters = column_filters

    @property
    def parent_node_level_depth(self):
        return self._parent_node_level_depth

    @parent_node_level_depth.setter
    def parent_node_level_depth(self, value):
        if self.parent_node:
            self._parent_node_level_depth = self.parent_node.level
        else:
            self._parent_node_level_depth = -1

    @property
    def parent_node_id(self):
        return self._parent_node_id

    async def get_hierarchy(self):
        if self._hierarchy is None:
            stmt = select(Hierarchy).where(Hierarchy.id == self.hierarchy_id)
            res = await self.session.execute(stmt)
            self._hierarchy = res.scalars().first()
            return self._hierarchy
        if self._hierarchy.id != self.hierarchy_id:
            stmt = select(Hierarchy).where(Hierarchy.id == self.hierarchy_id)
            res = await self.session.execute(stmt)
            self._hierarchy = res.scalars().first()
            return self._hierarchy
        return self._hierarchy

    async def get_levels_of_hierarchy(self):
        if self._levels_of_hierarchy is None:
            stmt = select(Level).where(Level.hierarchy_id == self.hierarchy_id)
            res = await self.session.execute(stmt)
            self._levels_of_hierarchy = res.scalars().all()
            return self._levels_of_hierarchy
        if self._hierarchy.id != self.hierarchy_id:
            stmt = select(Level).where(Level.hierarchy_id == self.hierarchy_id)
            res = await self.session.execute(stmt)
            self._levels_of_hierarchy = res.scalars().all()
            return self._levels_of_hierarchy
        return self._levels_of_hierarchy

    async def get_ids_of_levels_with_mo_link(self):
        if self._ids_of_levels_with_mo_link is None:
            levels = await self.get_levels_of_hierarchy()
            levels_param_type_ids_dict = {
                level.id: level.param_type_id for level in levels
            }
            tprms_data = await get_tprms_data_by_tprms_ids(
                tprm_ids=levels_param_type_ids_dict.values()
            )
            tprms_val_types = {
                tprm["id"]: tprm["val_type"] for tprm in tprms_data
            }

            level_ids_with_mo_link = {
                level_id
                for level_id, level_param_type_id in levels_param_type_ids_dict.items()
                if tprms_val_types.get(level_param_type_id, None)
                in {"mo_link", "two-way link"}
            }
            self._ids_of_levels_with_mo_link = level_ids_with_mo_link
        return self._ids_of_levels_with_mo_link

    @parent_node_id.setter
    def parent_node_id(self, value):
        if self.parent_node:
            self._parent_node_id = self.parent_node.id
        else:
            self._parent_node_id = None

    async def get_levels_with_current_tmo(self):
        """Returns levels for current hierarchy with particular tmo_id"""
        levels_with_tmo = select(Level).where(
            Level.object_type_id == self.tmo_id,
            Level.hierarchy_id == self.hierarchy_id,
        )
        res = await self.session.execute(levels_with_tmo)
        res = res.scalars().all()
        return res

    async def get_real_and_virtual_deepest_levels(self):
        """Returns dict with virtual and real Levels which has max Level.level and Level.object_type_id = self.tmo_id"""
        levels = await self.get_levels_with_current_tmo()
        max_level = max(level.level for level in levels)
        virtual = []
        real = []
        [
            virtual.append(level) if level.is_virtual else real.append(level)
            for level in levels
            if level.level == max_level
        ]
        return dict(virtual_levels=virtual, real_levels=real, depth=max_level)

    async def __add_to_cache_node_id_and_based_mo_ids_for_virtual_nodes(
        self, virtual_nodes: List[Obj]
    ):
        virtual_levels_ids = {
            obj.level_id for obj in virtual_nodes if not obj.object_id
        }

        stmt = select(Level).where(Level.id.in_(virtual_levels_ids))
        virtual_levels = await self.session.execute(stmt)
        virtual_levels = virtual_levels.scalars().all()

        for level in virtual_levels:
            mo_matched_condition_by_level = await get_mo_matched_condition(
                object_type_id=level.object_type_id,
                tprm_ids=[level.param_type_id],
            )
            # mo_matched_condition_by_level = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
            #                                                                          tprm_ids=[level.param_type_id])
            temporary_dict = dict()
            [
                temporary_dict.setdefault(
                    str(data[level.param_type_id]), list()
                ).append(data["id"])
                for data in mo_matched_condition_by_level
            ]

            # node caching
            if self.collect_data_cache:
                self.node_cache.update(
                    {
                        node.id: temporary_dict.get(node.key, [0])
                        for node in virtual_nodes
                    }
                )

    async def get_parent_nodes_of_base_nodes_that_are_nearest_to_parent(
        self, base_nodes: List[Obj]
    ):
        """Returns children nodes for self.parent_node with recalculated count_child"""
        order = [base_nodes]

        for step in order:
            stmt = select(Obj).where(
                Obj.id.in_([obj.parent_id for obj in step])
            )
            res = await self.session.execute(stmt)
            res = res.scalars().all()

            if self.collect_data_cache:
                self.cache_of_upper_real_nodes.extend(
                    [node for node in res if node.object_id]
                )
                virtual_nodes = [obj for obj in res if not obj.object_id]
                await self.__add_to_cache_node_id_and_based_mo_ids_for_virtual_nodes(
                    virtual_nodes
                )

            is_parent = any(obj.parent_id == self.parent_node_id for obj in res)

            if is_parent:
                res_dict = {
                    obj.id: obj
                    for obj in res
                    if obj.parent_id == self.parent_node_id
                }
                stmt = (
                    select(Obj.parent_id, func.count(Obj.id).label("count"))
                    .where(Obj.id.in_([obj.id for obj in step]))
                    .group_by(Obj.parent_id)
                )
                aggregate_data = await self.session.execute(stmt)
                aggregate_data = aggregate_data.all()

                def change_count(obj: Obj, count: int):
                    obj.child_count = count

                [
                    change_count(obj_from_dict, agr_data.count)
                    for agr_data in aggregate_data
                    if (obj_from_dict := res_dict.get(agr_data.parent_id))
                ]

                return res_dict.values()

            else:
                if res:
                    order.append(res)
                else:
                    return []

    async def get_nodes_of_deepest_real_levels(
        self, real_levels: List[Level], p_ids=None
    ):
        """Returns nodes which match filter_conditions and belong to founded real_levels"""
        if real_levels:
            deepest_real_levels_ids = [level.id for level in real_levels]
            stmt = select(Obj.object_id).where(
                Obj.level_id.in_(deepest_real_levels_ids),
                Obj.object_id is not None,
            )
            mo_ids = await self.session.execute(stmt)
            mo_ids = mo_ids.scalars().all()

            mo_ids_matched_condition = []
            for level in real_levels:
                # mo_ids_matched_condition_by_level = await get_mo_matched_condition(
                #     object_type_id=level.object_type_id,
                #     query_params=self.filter_conditions,
                #     mo_ids=mo_ids,
                #     only_ids=True,
                #     p_ids=p_ids if p_ids else None
                # )
                mo_ids_matched_condition_by_level = (
                    await SearchClient.get_mo_ids_by_filters(
                        tmo_id=level.object_type_id,
                        query_params=self.filter_conditions,
                        column_filters=self.column_filters,
                        mo_ids=mo_ids,
                        only_ids=True,
                        p_ids=p_ids if p_ids else None,
                    )
                )

                mo_ids_matched_condition.extend(
                    mo_ids_matched_condition_by_level
                )

            stmt = select(Obj).where(
                Obj.level_id.in_(deepest_real_levels_ids),
                Obj.object_id.in_(mo_ids_matched_condition),
            )
            res = await self.session.execute(stmt)
            res = res.scalars().all()

            # node caching
            if self.collect_data_cache:
                self.node_cache.update({obj.id: [obj.object_id] for obj in res})

            return res
        else:
            return []

    async def get_nodes_of_deepest_virtual_levels(
        self, virtual_levels: List[Level]
    ):
        """Returns nodes which match filter_conditions and belong to founded virtual_levels"""
        node_keys_matched_condition = []
        for level in virtual_levels:
            mo_matched_condition_by_level = await get_mo_matched_condition(
                object_type_id=level.object_type_id,
                query_params=self.filter_conditions,
                tprm_ids=[level.param_type_id],
            )
            # mo_matched_condition_by_level = await SearchClient.get_mo_ids_by_filters(
            #     tmo_id=level.object_type_id,
            #     query_params=self.filter_conditions,
            #     tprm_ids=[level.param_type_id]
            # )

            temporary_dict = dict()
            [
                temporary_dict.setdefault(
                    str(data[level.param_type_id]), list()
                ).append(data["id"])
                for data in mo_matched_condition_by_level
            ]

            stmt = select(Obj).where(
                Obj.level_id == level.id, Obj.key.in_(temporary_dict)
            )
            res = await self.session.execute(stmt)
            res = res.scalars().all()

            # node caching
            if self.collect_data_cache:
                self.node_cache.update(
                    {node.id: temporary_dict.get(node.key, [0]) for node in res}
                )

            node_keys_matched_condition.extend(res)

        return node_keys_matched_condition

    async def get_node_path_from_filtered_levels_node_to_current_node(
        self, levels: List[Level]
    ):
        """Returns path of nodes with unique tmo_id and max depth on the way to node of level with filter conditions"""
        level_ids = {level.id for level in levels}

        if self.parent_node.level_id in level_ids:
            return [self.parent_node]

        order = [self.parent_node]
        for node in order:
            stmt = select(Obj).where(Obj.id == node.parent_id)
            node = await self.session.execute(stmt)
            node = node.scalars().first()
            if node:
                order.append(node)
                if node.level_id in level_ids:
                    break

        if order[-1].level_id not in level_ids:
            return {}

        return order[::-1]

    async def check_path(self, path: List[Obj]):
        """Returns mo_ids of node if node is match conditions, otherwise returns empty list"""

        # get node levels
        stmt = select(Level).where(
            Level.id.in_({node.level_id for node in path})
        )
        levels_cache = await self.session.execute(stmt)
        levels_cache = {
            level.id: level for level in levels_cache.scalars().all()
        }

        cache_parents = set()

        first = True
        for node in path:
            if first:
                node_data = await get_mo_data_for_node(
                    node, levels_cache[node.level_id], self.filter_conditions
                )
                if not node_data:
                    cache_parents = set()

                    break
                first = False
                cache_parents = node_data["base_mo_ids"]
                continue

            node_data = await get_mo_data_for_node(
                node, levels_cache[node.level_id]
            )

            if not node_data:
                cache_parents = set()
                break
            parent_child_intersection = cache_parents.intersection(
                set(node_data["base_parent_ids"])
            )
            if not parent_child_intersection:
                cache_parents = set()
                break
            cache_parents = set()
            [
                cache_parents.update(node_data["base_parent_ids"][x])
                for x in parent_child_intersection
            ]
        return cache_parents

    async def add_levels_to_cache(self, levels: List[Level]):
        self.level_cache.update({level.id: level for level in levels})

    async def get_path_with_unique_tmos_as_siblings_in_path(
        self, path: List[Obj]
    ):
        """Returns a chain of nodes with a unique node object_type_id between siblings."""

        def return_only_unique_sibling(x, y):
            if not isinstance(x, list):
                if x.object_type_id != y.object_type_id:
                    return [x, y]
                return [y]
            if x[-1].object_type_id != y.object_type_id:
                x.append(y)
            else:
                x[-1] = y
            return x

        res = reduce(return_only_unique_sibling, path)
        return res

    async def __get_nodes_if_filtered_node_upper_or_on_the_same_depth_with_parent_node(
        self,
    ):
        """Returns first depth children nodes of the self.parent_node which match filter_conditions in the case
        when filtered level situated upper or on the same depth with parent node"""
        deepest_levels = []
        deepest_levels.extend(
            self.real_and_virtual_deepest_levels["virtual_levels"]
        )
        deepest_levels.extend(
            self.real_and_virtual_deepest_levels["real_levels"]
        )
        path = (
            await self.get_node_path_from_filtered_levels_node_to_current_node(
                deepest_levels
            )
        )
        path = await self.get_path_with_unique_tmos_as_siblings_in_path(path)

        based_mo_ids_of_parent_node = await self.check_path(path)

        # if parent node match conditions returns mo_ids which the parent node is based
        if not based_mo_ids_of_parent_node:
            return []

        if self.parent_node.object_id:
            # If real return all children
            stmt = select(Obj).where(Obj.parent_id == self.parent_node.id)
            children_nodes = await self.session.execute(stmt)
            children_nodes = children_nodes.scalars().all()
            return children_nodes

        stmt = select(Level).where(Level.parent_id == self.parent_node.level_id)
        first_child_levels = await self.session.execute(stmt)
        first_child_levels = first_child_levels.scalars().all()

        # level caching
        if self.collect_data_cache:
            await self.add_levels_to_cache(first_child_levels)

        based_mo_ids_of_parent_node = list(based_mo_ids_of_parent_node)

        result = []
        real_object_object_ids = []

        virtual_1depth_levels = []
        real_1depth_levels = []

        [
            virtual_1depth_levels.append(level)
            if level.is_virtual
            else real_1depth_levels.append(level)
            for level in first_child_levels
        ]

        for level in real_1depth_levels:
            if level.object_type_id == self.parent_node.object_type_id:
                real_object_object_ids.extend(based_mo_ids_of_parent_node)
            else:
                real_object_object_ids.extend(
                    await get_mo_matched_condition(
                        object_type_id=level.object_type_id,
                        p_ids=based_mo_ids_of_parent_node,
                        only_ids=True,
                    )
                )
                # real_object_object_ids.extend(await SearchClient.get_mo_ids_by_filters(
                #     tmo_id=level.object_type_id,
                #     p_ids=based_mo_ids_of_parent_node,
                #     only_ids=True
                # ))

        if real_object_object_ids:
            stmt = select(Obj).where(
                Obj.object_id.in_(real_object_object_ids),
                Obj.parent_id == self.parent_node_id,
            )
            real_objs = await self.session.execute(stmt)
            real_objs = real_objs.scalars().all()
            result.extend(real_objs)

            # node caching
            if self.collect_data_cache:
                self.node_cache.update(
                    {obj.id: [obj.object_id] for obj in real_objs}
                )

        for level in virtual_1depth_levels:
            if level.object_type_id == self.parent_node.object_type_id:
                level_data = await get_mo_matched_condition(
                    object_type_id=level.object_type_id,
                    tprm_ids=[level.param_type_id],
                    mo_ids=based_mo_ids_of_parent_node,
                )
                # level_data = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
                #                                                       tprm_ids=[level.param_type_id],
                #                                                       mo_ids=based_mo_ids_of_parent_node)
            else:
                level_data = await get_mo_matched_condition(
                    object_type_id=level.object_type_id,
                    tprm_ids=[level.param_type_id],
                    p_ids=based_mo_ids_of_parent_node,
                )
                # level_data = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
                #                                                       tprm_ids=[level.param_type_id],
                #                                                       p_ids=based_mo_ids_of_parent_node)

            virtual_object_values = {
                str(data[level.param_type_id]) for data in level_data
            }
            mo_ids_of_first_level = {data["id"] for data in level_data}

            stmt = select(Obj).where(
                Obj.level_id == level.id, Obj.key.in_(virtual_object_values)
            )
            level_nodes = await self.session.execute(stmt)
            level_nodes = level_nodes.scalars().all()

            # node caching
            if self.collect_data_cache:
                temporary_dict = dict()
                [
                    temporary_dict.setdefault(
                        str(data[level.param_type_id]), list()
                    ).append(data["id"])
                    for data in level_data
                ]
                self.node_cache.update(
                    {
                        node.id: temporary_dict.get(node.key, [0])
                        for node in level_nodes
                    }
                )

            stmt = select(Level).where(Level.parent_id == level.id)
            child_levels = await self.session.execute(stmt)
            child_levels = child_levels.scalars().all()

            result.extend(level_nodes)

            if not child_levels:
                continue
            parent_level = level

            def set_count_to_zero(node: Obj):
                node.child_count = 0

            [set_count_to_zero(virtual_node) for virtual_node in level_nodes]

            node_cache = {node.id: node for node in level_nodes}

            def add_node_count_by_node_id(node_id, count):
                node = node_cache.get(node_id)
                if node:
                    node.child_count += count

            virtual_2depth_levels = []
            real_2depth_levels = []
            [
                virtual_2depth_levels.append(level)
                if level.is_virtual
                else real_2depth_levels.append(level)
                for level in child_levels
            ]

            for level in real_2depth_levels:
                if level.object_type_id == parent_level.object_type_id:
                    object_ids_2depth = list(mo_ids_of_first_level)
                else:
                    object_ids_2depth = await get_mo_matched_condition(
                        object_type_id=level.object_type_id,
                        p_ids=list(mo_ids_of_first_level),
                        only_ids=True,
                    )
                    # object_ids_2depth = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
                    #                                                              p_ids=list(mo_ids_of_first_level),
                    #                                                              only_ids=True)

                if object_ids_2depth:
                    stmt = (
                        select(Obj.parent_id, func.count(Obj.id).label("count"))
                        .where(
                            Obj.object_id.in_(object_ids_2depth),
                            Obj.level_id == level.id,
                        )
                        .group_by(Obj.parent_id)
                    )
                    aggregate_data = await self.session.execute(stmt)
                    aggregate_data = aggregate_data.all()

                [
                    add_node_count_by_node_id(data.parent_id, data.count)
                    for data in aggregate_data
                ]

            for level in virtual_2depth_levels:
                if level.object_type_id == parent_level.object_type_id:
                    object_data_2depth = await get_mo_matched_condition(
                        object_type_id=level.object_type_id,
                        mo_ids=list(mo_ids_of_first_level),
                        tprm_ids=[level.param_type_id],
                    )
                    # object_data_2depth = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
                    #                                                               mo_ids=list(mo_ids_of_first_level),
                    #                                                               tprm_ids=[level.param_type_id])
                else:
                    object_data_2depth = await get_mo_matched_condition(
                        object_type_id=level.object_type_id,
                        p_ids=list(mo_ids_of_first_level),
                        tprm_ids=[level.param_type_id],
                    )
                    # object_data_2depth = await SearchClient.get_mo_ids_by_filters(tmo_id=level.object_type_id,
                    #                                                               p_ids=list(mo_ids_of_first_level),
                    #                                                               tprm_ids=[level.param_type_id])

                if object_data_2depth:
                    object_data_2depth = {
                        mo_dict[level.param_type_id]
                        for mo_dict in object_data_2depth
                    }

                    stmt = (
                        select(Obj.parent_id, func.count(Obj.id).label("count"))
                        .where(
                            Obj.key.in_(object_data_2depth),
                            Obj.level_id == level.id,
                        )
                        .group_by(Obj.parent_id)
                    )
                    aggregate_data = await self.session.execute(stmt)
                    aggregate_data = aggregate_data.all()

                    [
                        add_node_count_by_node_id(data.parent_id, data.count)
                        for data in aggregate_data
                    ]

        return result

    async def get_first_depth_children_nodes(self) -> List[Obj]:
        """Returns first depth children nodes of the self.parent_node which match filter_conditions"""

        self.real_and_virtual_deepest_levels = (
            await self.get_real_and_virtual_deepest_levels()
        )
        result = []
        max_depth = self.real_and_virtual_deepest_levels["depth"]

        if max_depth <= self.parent_node_level_depth:
            # parent_node must be Obj instance in this case
            result = await self.__get_nodes_if_filtered_node_upper_or_on_the_same_depth_with_parent_node()

            if self.collect_data_cache:
                self.result_cache = result
            return result

        # collect data for real levels
        deepest_real_levels = self.real_and_virtual_deepest_levels[
            "real_levels"
        ]

        # level caching
        if self.collect_data_cache:
            await self.add_levels_to_cache(deepest_real_levels)

        if deepest_real_levels:
            nodes_based_on_real_levels = (
                await self.get_nodes_of_deepest_real_levels(deepest_real_levels)
            )
            if nodes_based_on_real_levels:
                is_parent = any(
                    obj.parent_id == self.parent_node_id
                    for obj in nodes_based_on_real_levels
                )

                if is_parent:
                    result.extend(
                        [
                            obj
                            for obj in nodes_based_on_real_levels
                            if obj.parent_id == self.parent_node_id
                        ]
                    )

                else:
                    result.extend(
                        await self.get_parent_nodes_of_base_nodes_that_are_nearest_to_parent(
                            nodes_based_on_real_levels
                        )
                    )

        # collect data for virtual levels
        deepest_virtual_levels = self.real_and_virtual_deepest_levels[
            "virtual_levels"
        ]

        # level caching
        if self.collect_data_cache:
            await self.add_levels_to_cache(deepest_virtual_levels)

        if deepest_virtual_levels:
            nodes_based_on_virtual_levels = (
                await self.get_nodes_of_deepest_virtual_levels(
                    deepest_virtual_levels
                )
            )

            if nodes_based_on_virtual_levels:
                is_parent = any(
                    obj.parent_id == self.parent_node_id
                    for obj in nodes_based_on_virtual_levels
                )

                if is_parent:
                    result.extend(
                        [
                            obj
                            for obj in nodes_based_on_virtual_levels
                            if obj.parent_id == self.parent_node_id
                        ]
                    )

                else:
                    result.extend(
                        await self.get_parent_nodes_of_base_nodes_that_are_nearest_to_parent(
                            nodes_based_on_virtual_levels
                        )
                    )
        # result caching
        if self.collect_data_cache:
            self.result_cache = result
        return result

    async def group_children_real_nodes_by_parents_real_modes(
        self, parent_nodes: List[Obj]
    ):
        parent_nodes_cache = {node.id: node.id for node in parent_nodes}
        level_ids_with_mo_link = await self.get_ids_of_levels_with_mo_link()

        parent_nodes_ids = dict()
        for node in parent_nodes:
            if node.level_id in level_ids_with_mo_link and node.key.isdigit():
                parent_nodes_ids[node.id] = [int(node.key)]
            elif node.object_id:
                parent_nodes_ids[node.id] = [node.object_id]
            else:
                parent_nodes_ids[node.id] = list()

        def add_mo_id_to_main_parent(parent_id, mo_id, obj_id):
            main_parent_id = parent_nodes_cache.get(parent_id)
            parent_nodes_cache[obj_id] = main_parent_id
            if mo_id:
                parent_nodes_ids[main_parent_id].append(mo_id)

        order = [list(parent_nodes_ids)]
        hierarchy = await self.get_hierarchy()

        for step in order:
            if len(step) > 30000:
                steps_divider = ceil(len(step) / 30000)
                for step_number in range(steps_divider):
                    start = step_number * 30000
                    end = start + 30000
                    order.append(step[start:end])
                continue

            if hierarchy.create_empty_nodes:
                stmt = select(Obj).where(Obj.parent_id.in_(step))
            else:
                stmt = select(Obj).where(
                    Obj.parent_id.in_(step), Obj.key != DEFAULT_KEY_OF_NULL_NODE
                )

            res = await self.session.execute(stmt)
            res = res.scalars().all()

            if res:
                for item in res:
                    if (
                        item.level_id in level_ids_with_mo_link
                        and item.key.isdigit()
                    ):
                        add_mo_id_to_main_parent(
                            item.parent_id, int(item.key), item.id
                        )
                    else:
                        add_mo_id_to_main_parent(
                            item.parent_id, item.object_id, item.id
                        )

                next_step = [item.id for item in res if item.child_count > 0]
                if next_step:
                    order.append(next_step)
        return parent_nodes_ids

    async def group_children_real_nodes_by_parents_virtual_modes(
        self, parent_nodes: List[Obj]
    ):
        parent_nodes_cache = {node.id: node.id for node in parent_nodes}
        level_ids_with_mo_link = await self.get_ids_of_levels_with_mo_link()

        parent_nodes_ids = dict()
        for node in parent_nodes:
            if node.level_id in level_ids_with_mo_link and node.key.isdigit():
                parent_nodes_ids[node.id] = [int(node.key)]
            else:
                parent_nodes_ids[node.id] = list()

        def add_mo_id_to_main_parent(parent_id, mo_id, obj_id):
            main_parent_id = parent_nodes_cache.get(parent_id)
            parent_nodes_cache[obj_id] = main_parent_id
            if mo_id:
                parent_nodes_ids[main_parent_id].append(mo_id)

        order = [list(parent_nodes_ids)]

        hierarchy = await self.get_hierarchy()

        for step in order:
            if len(step) > 30000:
                steps_divider = ceil(len(step) / 30000)
                for step_number in range(steps_divider):
                    start = step_number * 30000
                    end = start + 30000
                    order.append(step[start:end])
                continue

            if hierarchy.create_empty_nodes:
                stmt = select(Obj).where(Obj.parent_id.in_(step))
            else:
                stmt = select(Obj).where(
                    Obj.parent_id.in_(step), Obj.key != DEFAULT_KEY_OF_NULL_NODE
                )

            res = await self.session.execute(stmt)
            res = res.scalars().all()

            if res:
                for item in res:
                    if (
                        item.level_id in level_ids_with_mo_link
                        and item.key.isdigit()
                    ):
                        add_mo_id_to_main_parent(
                            item.parent_id, int(item.key), item.id
                        )
                    else:
                        add_mo_id_to_main_parent(
                            item.parent_id, item.object_id, item.id
                        )

                next_step = [item.id for item in res if item.child_count > 0]
                if next_step:
                    order.append(next_step)
        return parent_nodes_ids

    async def return_levels_with_children_real_levels(
        self, levels: List[Level]
    ):
        level_pre_results = {
            level.id: {
                "order_of_children_tmos": list(),
                "collect_data_of_tmos": list(),
            }
            for level in levels
        }
        level_parent_cache = {level.id: level.id for level in levels}

        def add_child_level_tmo_id_to_main_level(level: Level):
            main_level_id = level_parent_cache.get(level.parent_id)
            level_parent_cache[level.id] = main_level_id
            order_of_children_tmos = level_pre_results[main_level_id][
                "order_of_children_tmos"
            ]

            if len(order_of_children_tmos) > 0:
                if level.object_type_id != order_of_children_tmos[-1]:
                    order_of_children_tmos.append(level.object_type_id)
            else:
                order_of_children_tmos.append(level.object_type_id)

            if not level.is_virtual:
                level_pre_results[main_level_id]["collect_data_of_tmos"].append(
                    level.object_type_id
                )

        order = []
        if level_pre_results:
            order.append(list(level_pre_results))
        for step in order:
            stmt = select(Level).where(Level.parent_id.in_(step))
            res = await self.session.execute(stmt)
            res = res.scalars().all()

            if res:
                [add_child_level_tmo_id_to_main_level(level) for level in res]
                order.append([level.id for level in res])

        level_pre_results = {
            level_id: level_data
            for level_id, level_data in level_pre_results.items()
            if level_data["collect_data_of_tmos"]
        }

        return level_pre_results

    async def __get_mo_ids_of_real_obj_grouped_by_node_id(
        self, nodes: List[Obj]
    ):
        """Works only if cache was used"""
        level_node_dict = {}
        [
            level_node_dict.setdefault(obj.level_id, list()).append(obj)
            for obj in nodes
        ]

        # virual levels which has one or more real children levels.

        stmt = select(Level).where(Level.id.in_(level_node_dict))
        virtual_levels = await self.session.execute(stmt)
        virtual_levels = virtual_levels.scalars().all()

        self.level_cache.update({level.id: level for level in virtual_levels})

        virt_l_with_real_children_l = (
            await self.return_levels_with_children_real_levels(virtual_levels)
        )

        data_for_message = list()
        for level_id, level_data in virt_l_with_real_children_l.items():
            level_nodes = level_node_dict[level_id]
            level_tmo_id = self.level_cache.get(level_id).object_type_id
            nodes_and_base_mo_ids = [
                RequestNode(node_id=str(node.id), mo_ids=mo_ids)
                for node in level_nodes
                if (mo_ids := self.node_cache.get(node.id))
            ]
            path_of_children_tmos = level_data["order_of_children_tmos"]
            collect_data_for_tmos = level_data["collect_data_of_tmos"]
            if all(
                [
                    level_tmo_id,
                    nodes_and_base_mo_ids,
                    path_of_children_tmos,
                    collect_data_for_tmos,
                ]
            ):
                data_for_message.append(
                    RequestLevel(
                        level_data=nodes_and_base_mo_ids,
                        level_tmo_id=level_tmo_id,
                        path_of_children_tmos=path_of_children_tmos,
                        collect_data_for_tmos=collect_data_for_tmos,
                    )
                )

        res = []
        if data_for_message:
            msg = RequestListLevels(items=data_for_message)
            res = await get_children_mo_id_grouped_by_parent_node_id(msg)
            res = {UUID(k): v for k, v in res.items()}
        return res

    async def get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids(
        self,
    ):
        """Returns dict"""
        result = dict()

        if not self.collect_data_cache:
            self.collect_data_cache = True
            await self.get_first_depth_children_nodes()

        if not self.result_cache:
            return {}

        # Now we have all data in cache
        # level_cache, node_cache, result_cache, cache_of_mo_ids_of_upper_nodes

        max_depth = self.real_and_virtual_deepest_levels["depth"]

        if max_depth <= self.parent_node_level_depth:
            if self.parent_node.object_id:
                # get all real children nodes
                result.update(
                    await self.group_children_real_nodes_by_parents_real_modes(
                        self.result_cache
                    )
                )

            else:
                real_objects = [
                    obj for obj in self.result_cache if obj.object_id
                ]
                result.update(
                    await self.group_children_real_nodes_by_parents_real_modes(
                        real_objects
                    )
                )

                virtual_objects = [
                    obj for obj in self.result_cache if not obj.object_id
                ]
                result.update(
                    await self.__get_mo_ids_of_real_obj_grouped_by_node_id(
                        virtual_objects
                    )
                )

        else:
            main_node_dependencies = {}
            real_objects = [obj for obj in self.result_cache if obj.object_id]
            virtual_objects = [
                obj for obj in self.result_cache if not obj.object_id
            ]

            main_node_dependencies.update(
                await self.group_children_real_nodes_by_parents_real_modes(
                    real_objects
                )
            )

            children_mo_ids = set()
            if self.cache_of_upper_real_nodes:
                children_mo_ids.update(
                    [node.object_id for node in self.cache_of_upper_real_nodes]
                )

            # collect -lower results
            deepest_real_levels = self.real_and_virtual_deepest_levels[
                "real_levels"
            ]
            deepest_real_levels_len = len(deepest_real_levels)
            virtual_objects_len = len(virtual_objects)
            same_levels = False
            if all([deepest_real_levels_len, virtual_objects_len]):
                same_levels = (
                    deepest_real_levels[0].object_type_id
                    == virtual_objects[0].object_type_id
                )

            if same_levels:
                main_node_dependencies.update(
                    await self.group_children_real_nodes_by_parents_virtual_modes(
                        virtual_objects
                    )
                )
                pass
            else:
                main_node_dependencies.update(
                    await self.__get_mo_ids_of_real_obj_grouped_by_node_id(
                        virtual_objects
                    )
                )

            if deepest_real_levels:
                nodes_based_on_real_levels = (
                    await self.get_nodes_of_deepest_real_levels(
                        deepest_real_levels
                    )
                )
                if nodes_based_on_real_levels:
                    lower_result = await self.group_children_real_nodes_by_parents_real_modes(
                        nodes_based_on_real_levels
                    )
                    lower_result_mo_ids = []
                    [
                        lower_result_mo_ids.extend(mo_ids)
                        for mo_ids in lower_result.values()
                    ]
                    children_mo_ids.update(lower_result_mo_ids)

            deepest_virtual_levels = self.real_and_virtual_deepest_levels[
                "virtual_levels"
            ]

            if deepest_virtual_levels:
                nodes_based_on_virtual_levels = (
                    await self.get_nodes_of_deepest_virtual_levels(
                        deepest_virtual_levels
                    )
                )
                if nodes_based_on_virtual_levels:
                    lower_result = (
                        await self.__get_mo_ids_of_real_obj_grouped_by_node_id(
                            nodes_based_on_virtual_levels
                        )
                    )

                    lower_result_mo_ids = []
                    [
                        lower_result_mo_ids.extend(mo_ids)
                        for mo_ids in lower_result.values()
                    ]
                    children_mo_ids.update(lower_result_mo_ids)

            result = {
                node_id: set(children_real_mo_ids).intersection(children_mo_ids)
                for node_id, children_real_mo_ids in main_node_dependencies.items()
            }

        return result
