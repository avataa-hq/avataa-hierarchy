from collections import defaultdict
import math
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from schemas.hier_schemas import Level, NodeData, Obj
from services.hierarchy.hierarchy_builder.configs import (
    DEFAULT_KEY_OF_NULL_NODE,
)
from services.hierarchy.hierarchy_builder.dto_models import KeyData
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
    get_node_key_data,
)
from settings import (
    LIMIT_OF_POSTGRES_RESULTS_PER_STEP,
    POSTGRES_ITEMS_LIMIT_IN_QUERY,
)


class SpecialHierarchyMOCreateHandler:
    """Algorithm for changing hierarchies when creating a new MO"""

    def __init__(
        self, mo_data: list[dict], session: AsyncSession, hierarchy_id: int
    ):
        self.base_mo_list = mo_data
        self.hierarchy_id = hierarchy_id
        self.session = session
        # cache data for mo items
        self._mo_ids = list()
        self._tmo_ids = set()
        self._mo_items_grouped_by_tmo_ids = defaultdict(list)
        self._mo_ids_grouped_by_tmo_ids = defaultdict(list)
        self._mo_data_cache = dict()
        # cache data for level items
        self._all_levels = list()
        self._level_items_grouped_by_tmo_ids = defaultdict(list)
        self._level_data_cache = dict()
        self._ids_of_parent_levels_by_child_level_ids = dict()
        # cache data for level parents
        self._parent_level_items_by_parent_id = dict()
        # special level cache start
        self.__mos_data_for_spec_level: list[dict] = list()
        self.__mo_ids_for_spec_level: list[int] = list()
        # if level has parent we mast create cache of parent nodes, where key will be id (int)
        # of new created mo and value will be parent node data - (Obj)
        self.__cache_of_parent_nodes_by_child_mo_id: dict[int, Obj] = dict()
        # special level cache end

    def _step_1_create_cache_data_for_inputed_mo(self):
        """Creates cache data for inputed mo"""

        for item in self.base_mo_list:
            mo_id = item["id"]
            tmo_id = item["tmo_id"]
            self._mo_ids.append(mo_id)
            self._tmo_ids.add(tmo_id)

            self._mo_items_grouped_by_tmo_ids[tmo_id].append(item)
            self._mo_ids_grouped_by_tmo_ids[tmo_id].append(mo_id)
            self._mo_data_cache[mo_id] = item

    async def _step_1_create_cache_data_for_hierarchy_levels(self):
        """Creates cache data for hierarchy levels"""

        # cache data for level items
        all_levels_stmt = (
            select(Level)
            .where(
                Level.object_type_id.in_(list(self._tmo_ids)),
                Level.hierarchy_id == self.hierarchy_id,
            )
            .order_by(Level.object_type_id, Level.level)
        )
        all_levels = await self.session.execute(all_levels_stmt)
        self._all_levels = all_levels.scalars().all()

        # get cache data for level items

        for level in self._all_levels:
            self._level_items_grouped_by_tmo_ids[level.object_type_id].append(
                level
            )
            self._level_data_cache[level.id] = level
            if level.parent_id:
                self._ids_of_parent_levels_by_child_level_ids[level.id] = (
                    level.parent_id
                )

    async def _step_1_create_cache_data_for_level_parents(self):
        """Creates cache data for level parents"""

        if self._ids_of_parent_levels_by_child_level_ids:
            level_parent_ids = list(
                self._ids_of_parent_levels_by_child_level_ids.values()
            )

            stmt = select(Level).where(Level.id.in_(level_parent_ids))
            parent_levels = await self.session.execute(stmt)
            parent_levels = parent_levels.scalars().all()
            self._parent_level_items_by_parent_id = {
                p_level.id: p_level for p_level in parent_levels
            }

    async def create_cache_of_main_data(self):
        self._step_1_create_cache_data_for_inputed_mo()
        await self._step_1_create_cache_data_for_hierarchy_levels()
        await self._step_1_create_cache_data_for_level_parents()

    async def __create_cache_data_for_special_level(self, level: Level):
        self.__mos_data_for_spec_level = self._mo_items_grouped_by_tmo_ids.get(
            level.object_type_id
        )
        self.__mo_ids_for_spec_level = self._mo_ids_grouped_by_tmo_ids.get(
            level.object_type_id
        )

        if not self.__mo_ids_for_spec_level:
            return

        # check if mo_ids already exists
        stmt = select(NodeData.mo_id).where(
            NodeData.level_id == level.id,
            NodeData.mo_id.in_(self.__mo_ids_for_spec_level),
        )
        res = await self.session.execute(stmt)
        res = set(res.scalars().all())

        not_created = set(self.__mo_ids_for_spec_level).difference(res)
        if len(not_created) < len(self.__mo_ids_for_spec_level):
            self.__mo_ids_for_spec_level = list(not_created)
            self.__mos_data_for_spec_level = [
                self._mo_data_cache.get(mo_id)
                for mo_id in self.__mo_ids_for_spec_level
            ]

        self.__cache_of_parent_nodes_by_child_mo_id = dict()
        if level.parent_id:
            parent_level_data = self._parent_level_items_by_parent_id.get(
                level.parent_id
            )
            if parent_level_data:
                if parent_level_data.object_type_id == level.object_type_id:
                    # get parent and connect py mo _id
                    stmt = select(NodeData).where(
                        NodeData.level_id == parent_level_data.id,
                        NodeData.mo_id.in_(self.__mo_ids_for_spec_level),
                    )
                    paren_node_data = await self.session.execute(stmt)
                    paren_node_data = {
                        p_n_d.mo_id: p_n_d.node_id
                        for p_n_d in paren_node_data.scalars().all()
                    }

                    parent_node_ids = set(paren_node_data.values())

                    parent_nodes = dict()
                    if parent_node_ids:
                        stmt = select(Obj).where(
                            Obj.id.in_(parent_node_ids),
                            Obj.hierarchy_id == self.hierarchy_id,
                        )
                        parent_nodes = await self.session.execute(stmt)
                        parent_nodes = {
                            p_node.id: p_node
                            for p_node in parent_nodes.scalars().all()
                        }

                    for mo_data in self.__mos_data_for_spec_level:
                        parent_node_id = paren_node_data.get(mo_data["id"])

                        if not parent_node_id:
                            continue

                        parent_node = parent_nodes.get(parent_node_id)
                        if not parent_node:
                            continue
                        self.__cache_of_parent_nodes_by_child_mo_id[
                            mo_data["id"]
                        ] = parent_node

                else:
                    mo_data_parent_ids = [
                        mo_p_id
                        for mo in self.__mos_data_for_spec_level
                        if (mo_p_id := mo.get("p_id"))
                    ]

                    if mo_data_parent_ids:
                        stmt = select(NodeData).where(
                            NodeData.level_id == parent_level_data.id,
                            NodeData.mo_id.in_(mo_data_parent_ids),
                        )
                        paren_node_data = await self.session.execute(stmt)
                        paren_node_data = {
                            p_n_d.mo_id: p_n_d.node_id
                            for p_n_d in paren_node_data.scalars().all()
                        }

                        parent_node_ids = set(paren_node_data.values())

                        parent_nodes = dict()
                        if parent_node_ids:
                            stmt = select(Obj).where(
                                Obj.id.in_(parent_node_ids),
                                Obj.hierarchy_id == self.hierarchy_id,
                            )
                            parent_nodes = await self.session.execute(stmt)
                            parent_nodes = {
                                p_node.id: p_node
                                for p_node in parent_nodes.scalars().all()
                            }

                        for mo_data in self.__mos_data_for_spec_level:
                            mo_p_id = mo_data.get("p_id")
                            if not mo_p_id:
                                continue

                            parent_node_id = paren_node_data.get(mo_p_id)

                            if not parent_node_id:
                                continue

                            parent_node = parent_nodes.get(parent_node_id)
                            if not parent_node:
                                continue
                            self.__cache_of_parent_nodes_by_child_mo_id[
                                mo_data["id"]
                            ] = parent_node

    async def __create_nodes_for_virtual_hierarchical_level(self, level: Level):
        async def get_level_parents(_attr_str: str) -> list[Obj]:
            _p_ids = [
                i[_attr_str]
                for i in self.__mos_data_for_spec_level
                if _attr_str in i
            ]
            _query = select(Obj).where(
                Obj.level_id == level.id, Obj.object_id.in_(_p_ids)
            )
            _results = await self.session.execute(_query)
            return _results.scalars().all()

        attr_str = str(level.attr_as_parent)
        parents_in_level = {
            i.object_id: i for i in await get_level_parents(attr_str)
        }
        for mo_dict in self.__mos_data_for_spec_level:
            parent_id_by_attr = mo_dict.get(attr_str, None)
            if parent_id_by_attr:
                parent_node = parents_in_level.get(parent_id_by_attr, None)
            else:
                parent_node = self.__cache_of_parent_nodes_by_child_mo_id.get(
                    mo_dict["id"], None
                )
            parent_node_id = parent_node.id if parent_node else None
            is_active = mo_dict.get("active", False)
            key_data = get_node_key_data(
                ordered_key_attrs=level.key_attrs, mo_data_with_params=mo_dict
            )
            if level.key_attrs:
                unfolded_key = {k: mo_dict.get(k) for k in level.key_attrs}
            else:
                unfolded_key = {str(level.param_type_id): None}
            path = (
                create_path_for_children_node_by_parent_node(parent_node)
                if parent_node
                else None
            )

            node = Obj(
                key=key_data.key,
                object_id=None,
                object_type_id=level.object_type_id,
                additional_params=None,
                hierarchy_id=level.hierarchy_id,
                level=level.level,
                latitude=None,
                longitude=None,
                child_count=0,
                level_id=level.id,
                parent_id=parent_node_id,
                path=path,
                active=is_active,
                key_is_empty=key_data.key_is_empty,
            )
            self.session.add(node)
            if parent_node:
                if is_active:
                    parent_node.child_count += 1
                    self.session.add(parent_node)

            await self.session.flush()

            node_data = NodeData(
                node_id=node.id,
                level_id=level.id,
                mo_id=mo_dict["id"],
                mo_name=mo_dict.get("name"),
                mo_latitude=mo_dict.get("latitude"),
                mo_longitude=mo_dict.get("longitude"),
                mo_status=mo_dict.get("status"),
                mo_tmo_id=mo_dict.get("tmo_id"),
                mo_p_id=mo_dict.get("p_id"),
                mo_active=is_active,
                unfolded_key=unfolded_key,
            )
            self.session.add(node_data)

            await self.session.flush()
        await self.session.commit()

    async def __create_nodes_for_virtual_level(self, level: Level):
        # get default parent_nodes
        # parent_node_ids = set()
        # if level.parent_id:
        #     parent_node_ids = {node.id for node in self.__cache_of_parent_nodes_by_child_mo_id.values()}

        # get default_keys
        default_keys = set()
        level_key_mo_attrs = [
            attr for attr in level.key_attrs if not attr.isdigit()
        ]
        for mo in self.__mos_data_for_spec_level:
            key = DEFAULT_KEY_OF_NULL_NODE
            if level_key_mo_attrs:
                key_data = get_node_key_data(
                    ordered_key_attrs=level_key_mo_attrs, mo_data_with_params=mo
                )
                key = key_data.key
            default_keys.add(key)

        where_cond = [Obj.level_id == level.id, Obj.key.in_(default_keys)]

        # if parent_node_ids:
        #     where_cond.append(Obj.parent_id.in_(parent_node_ids))

        stmt = select(Obj).where(*where_cond)
        default_nodes = await self.session.execute(stmt)
        default_nodes = default_nodes.scalars().all()

        # parent_id_active_default_node = {def_n.parent_id: def_n for def_n in default_nodes if def_n.active}
        # parent_id_not_active_default_node = {def_n.parent_id: def_n for def_n in default_nodes if not def_n.active}

        parent_id_default_node_key_active_default_node = defaultdict(dict)
        parent_id_default_node_key_not_active_default_node = defaultdict(dict)

        for def_n in default_nodes:
            if def_n.active:
                parent_id_default_node_key_active_default_node[def_n.parent_id][
                    def_n.key
                ] = def_n
            else:
                parent_id_default_node_key_not_active_default_node[
                    def_n.parent_id
                ][def_n.key] = def_n

        # group new mos by parent_ids

        for mo in self.__mos_data_for_spec_level:
            is_active = mo.get("active", False)

            parent_node = self.__cache_of_parent_nodes_by_child_mo_id.get(
                mo["id"], None
            )
            parent_node_id = None
            if parent_node:
                parent_node_id = parent_node.id

            # key = DEFAULT_KEY_OF_NULL_NODE
            key_data = KeyData(key=DEFAULT_KEY_OF_NULL_NODE, key_is_empty=True)
            if level_key_mo_attrs:
                key_data = get_node_key_data(
                    ordered_key_attrs=level_key_mo_attrs, mo_data_with_params=mo
                )

            if level.key_attrs:
                unfolded_key = {k: mo.get(k) for k in level_key_mo_attrs}
            else:
                unfolded_key = {str(level.param_type_id): None}

            default_node = None

            if is_active:
                default_keys_nodes = (
                    parent_id_default_node_key_active_default_node.get(
                        parent_node_id, None
                    )
                )
            else:
                default_keys_nodes = (
                    parent_id_default_node_key_not_active_default_node.get(
                        parent_node_id, None
                    )
                )

            if default_keys_nodes:
                default_node = default_keys_nodes.get(key_data.key, None)

            if not default_node:
                default_node = Obj(
                    key=key_data.key,
                    object_id=None,
                    object_type_id=level.object_type_id,
                    additional_params=None,
                    hierarchy_id=level.hierarchy_id,
                    level=level.level,
                    latitude=None,
                    longitude=None,
                    child_count=0,
                    level_id=level.id,
                    parent_id=parent_node_id,
                    path=None,
                    active=is_active,
                    key_is_empty=key_data.key_is_empty,
                )
                self.session.add(default_node)
                await self.session.flush()
                if parent_node:
                    if is_active:
                        parent_node.child_count += 1
                        self.session.add(parent_node)

                if is_active:
                    parent_id_default_node_key_active_default_node[
                        parent_node_id
                    ][key_data.key] = default_node
                else:
                    parent_id_default_node_key_not_active_default_node[
                        parent_node_id
                    ][key_data.key] = default_node

            node_data = NodeData(
                node_id=default_node.id,
                level_id=level.id,
                mo_id=mo["id"],
                mo_name=mo.get("name"),
                mo_latitude=mo.get("latitude"),
                mo_longitude=mo.get("longitude"),
                mo_status=mo.get("status"),
                mo_tmo_id=mo.get("tmo_id"),
                mo_p_id=mo.get("p_id"),
                mo_active=is_active,
                unfolded_key=unfolded_key,
            )
            self.session.add(node_data)

        await self.session.commit()

    async def __create_nodes_for_real_level(self, level: Level):
        """Creates nodes and node_data items for real level"""
        mo_id_obj_data = dict()
        mo_id_node_data = dict()

        MAX_COUNT_TO_FLUSH = 25_000
        item_counter_to_flush = 0

        level_key_mo_attrs = [
            attr for attr in level.key_attrs if not attr.isdigit()
        ]

        for mo in self.__mos_data_for_spec_level:
            parent_node = None
            is_active = mo.get("active", False)

            if level.parent_id:
                parent_node = self.__cache_of_parent_nodes_by_child_mo_id.get(
                    mo["id"], None
                )

            path = None
            if parent_node:
                path = create_path_for_children_node_by_parent_node(parent_node)

            key_data = KeyData(key=DEFAULT_KEY_OF_NULL_NODE, key_is_empty=True)
            if level_key_mo_attrs:
                key_data = get_node_key_data(
                    ordered_key_attrs=level_key_mo_attrs, mo_data_with_params=mo
                )

            if level.key_attrs:
                unfolded_key = {k: mo.get(k) for k in level_key_mo_attrs}
            else:
                unfolded_key = {str(level.param_type_id): None}

            new_node = Obj(
                key=key_data.key,
                object_id=mo["id"],
                object_type_id=level.object_type_id,
                additional_params=None,
                hierarchy_id=level.hierarchy_id,
                level=level.level,
                latitude=mo.get("latitude"),
                longitude=mo.get("longitude"),
                child_count=0,
                level_id=level.id,
                parent_id=parent_node.id if parent_node is not None else None,
                active=is_active,
                path=path,
                key_is_empty=key_data.key_is_empty,
            )
            item_counter_to_flush += 1
            self.session.add(new_node)
            if parent_node:
                if is_active:
                    parent_node.child_count += 1
                    self.session.add(parent_node)

            mo_id_obj_data[mo["id"]] = new_node
            mo_id_node_data[mo["id"]] = dict(
                level_id=level.id,
                mo_id=mo["id"],
                mo_name=mo.get("name"),
                mo_latitude=mo.get("latitude"),
                mo_longitude=mo.get("longitude"),
                mo_status=mo.get("status"),
                mo_tmo_id=mo.get("tmo_id"),
                mo_p_id=mo.get("p_id"),
                mo_active=is_active,
                unfolded_key=unfolded_key,
            )

            if item_counter_to_flush >= MAX_COUNT_TO_FLUSH:
                item_counter_to_flush = 0
                await self.session.flush()

                for mo_id, node_data in mo_id_node_data.items():
                    connected_obj = mo_id_obj_data[mo_id]
                    node_data["node_id"] = connected_obj.id
                    node_data = NodeData(**node_data)
                    self.session.add(node_data)
                await self.session.flush()
                mo_id_obj_data = dict()
                mo_id_node_data = dict()

        if mo_id_node_data:
            await self.session.flush()
            for mo_id, node_data in mo_id_node_data.items():
                connected_obj = mo_id_obj_data[mo_id]
                node_data["node_id"] = connected_obj.id
                node_data = NodeData(**node_data)
                self.session.add(node_data)
            await self.session.flush()

        await self.session.commit()

    async def make_changes(self):
        await self.create_cache_of_main_data()

        for (
            tmo_id,
            list_of_levels,
        ) in self._level_items_grouped_by_tmo_ids.items():
            for level in list_of_levels:
                await self.__create_cache_data_for_special_level(level=level)
                if not self.__mos_data_for_spec_level:
                    continue

                if level.is_virtual:
                    if level.attr_as_parent:
                        await (
                            self.__create_nodes_for_virtual_hierarchical_level(
                                level=level
                            )
                        )
                    else:
                        await self.__create_nodes_for_virtual_level(level=level)
                else:
                    await self.__create_nodes_for_real_level(level=level)


async def __get_parent_level(
    level: Level, session: AsyncSession
) -> Level | None:
    parent_level = None
    if level.parent_id:
        stmt = select(Level).where(Level.id == level.parent_id)
        parent_level = await session.execute(stmt)
        parent_level = parent_level.scalar_one()
    return parent_level


async def __group_by_new_parents(
    parent_level: Level | None,
    level: Level,
    node_data_with_new_parents: list[NodeData],
    session: AsyncSession,
) -> dict[str, str]:
    new_mo_p_id_list_of_node_ids = defaultdict(list)
    # get new parents

    if parent_level:
        if level.object_type_id != parent_level.object_type_id:
            for n_data in node_data_with_new_parents:
                if n_data.mo_p_id:
                    new_mo_p_id_list_of_node_ids[n_data.mo_p_id].append(
                        n_data.node_id
                    )
        else:
            for n_data in node_data_with_new_parents:
                new_mo_p_id_list_of_node_ids[n_data.mo_id].append(
                    n_data.node_id
                )

    new_mo_p_ids = list(new_mo_p_id_list_of_node_ids.keys())

    node_id_new_p_node_id = dict()
    # new parents id cache start
    if new_mo_p_ids:
        steps = math.ceil(len(new_mo_p_ids) / POSTGRES_ITEMS_LIMIT_IN_QUERY)

        for step in range(steps):
            start = step * POSTGRES_ITEMS_LIMIT_IN_QUERY
            end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

            stmt = select(NodeData).where(
                NodeData.level_id == level.parent_id,
                NodeData.mo_id.in_(new_mo_p_ids[start:end]),
            )
            new_parent = await session.execute(stmt)

            for n_data in new_parent.scalars().all():
                child_node_ids = new_mo_p_id_list_of_node_ids.get(n_data.mo_id)
                if child_node_ids:
                    for child_node_id in child_node_ids:
                        node_id_new_p_node_id[child_node_id] = n_data.node_id
    return node_id_new_p_node_id


async def __rebuild_real(
    session: AsyncSession,
    node_data_with_new_parents: list[NodeData],
    node_id_new_p_node_id: dict[str, str],
) -> set[str | UUID]:
    # if level is real
    node_ids_to_recalc_child_count: set[UUID | str] = set()

    node_data_group_by_node_id = {
        n_data.node_id: n_data for n_data in node_data_with_new_parents
    }
    list_node_ids = list(node_data_group_by_node_id)

    node_ids_to_recalc_child_count.update(node_data_group_by_node_id)

    steps = math.ceil(len(list_node_ids) / POSTGRES_ITEMS_LIMIT_IN_QUERY)

    for step in range(steps):
        start = step * POSTGRES_ITEMS_LIMIT_IN_QUERY
        end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

        stmt = select(Obj).where(Obj.id.in_(list_node_ids[start:end]))
        partition = await session.execute(stmt)
        partition = partition.scalars().all()
        # get old parent ids and new parent ids
        old_parent_ids = set()
        new_parent_ids = set()

        for child_node in partition:
            if child_node.parent_id:
                old_parent_ids.add(child_node.parent_id)

            new_parent_node_id = node_id_new_p_node_id.get(child_node.id)
            if new_parent_node_id:
                new_parent_ids.add(new_parent_node_id)

        node_ids_to_recalc_child_count.update(old_parent_ids, new_parent_ids)
        # get new parents
        parents = dict()

        if new_parent_ids:
            stmt = select(Obj).where(Obj.id.in_(new_parent_ids))
            parents = await session.execute(stmt)
            parents = {parent.id: parent for parent in parents.scalars().all()}

        # cache change path for child
        replace_path_cache = dict()

        # change node parent, path and path for children new parents
        for child_node in partition:
            based_n_data = node_data_group_by_node_id.get(child_node.id)

            new_parent_node_id = node_id_new_p_node_id.get(child_node.id)
            new_parent = None

            if new_parent_node_id:
                new_parent = parents.get(new_parent_node_id)

            child_node.parent_id = (
                new_parent.id if new_parent is not None else None
            )
            # add new data from node_data
            child_node.active = based_n_data.mo_active
            # add new key from node_data
            key_data = get_node_key_data(
                ordered_key_attrs=list(based_n_data.unfolded_key),
                mo_data_with_params=based_n_data.unfolded_key,
            )
            child_node.key = key_data.key
            child_node.key_is_empty = key_data.key_is_empty

            new_path = create_path_for_children_node_by_parent_node(new_parent)

            if child_node.path != new_path:
                old_path_to_change_for_children = (
                    create_path_for_children_node_by_parent_node(child_node)
                )
                child_node.path = new_path
                new_path_replace_for_children = (
                    create_path_for_children_node_by_parent_node(child_node)
                )
                if (
                    old_path_to_change_for_children
                    != new_path_replace_for_children
                ):
                    replace_path_cache[old_path_to_change_for_children] = (
                        new_path_replace_for_children
                    )

                child_node.path = new_path

            session.add(child_node)

        await session.flush()
        for old_path_prefix, new_path_prefix in replace_path_cache.items():
            query = select(Obj).where(Obj.path.like(f"{old_path_prefix}%"))
            result_generator = await session.stream_scalars(query)
            async for partition in result_generator.yield_per(
                LIMIT_OF_POSTGRES_RESULTS_PER_STEP
            ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
                for child_node in partition:
                    child_node.path = child_node.path.replace(
                        old_path_prefix, new_path_prefix
                    )
                    session.add(child_node)

            await session.flush()

    await session.flush()
    return node_ids_to_recalc_child_count


async def __rebuild_hierarchical_data(
    session: AsyncSession,
    node_data_with_new_parents: list[NodeData],
    level: Level,
    node_id_new_p_node_id: dict[str, str],
):
    node_ids_to_recalc_child_count = set()

    node_data_group_by_node_id = {
        n_data.node_id: n_data for n_data in node_data_with_new_parents
    }
    list_node_ids = list(node_data_group_by_node_id)
    part_node_ids_iterator = (
        list_node_ids[i : i + POSTGRES_ITEMS_LIMIT_IN_QUERY]
        for i in range(0, len(list_node_ids), POSTGRES_ITEMS_LIMIT_IN_QUERY)
    )
    for part_node_ids in part_node_ids_iterator:
        stmt = select(Obj).where(Obj.id.in_(part_node_ids))
        chunk = await session.execute(stmt)
        chunk = chunk.scalars().all()

        # left only highest objects
        obj_parent_ids = set([i.parent_id for i in chunk if i.parent_id])
        if obj_parent_ids:
            # ignore same level
            stmt = select(Obj.id).where(
                Obj.id.in_(obj_parent_ids), Obj.level_id == level.id
            )
            chunk_parent_ids = await session.execute(stmt)
            chunk_parent_ids = set(chunk_parent_ids.scalars().all())
            chunk = [i for i in chunk if i.parent_id not in chunk_parent_ids]

        # get old parent ids and new parent ids
        old_parent_ids = set()
        new_parent_ids = set()

        for child_node in chunk:
            if child_node.parent_id:
                old_parent_ids.add(child_node.parent_id)

            new_parent_node_id = node_id_new_p_node_id.get(child_node.id)
            if new_parent_node_id:
                new_parent_ids.add(new_parent_node_id)

        node_ids_to_recalc_child_count.update(old_parent_ids, new_parent_ids)

        if new_parent_ids:
            stmt = select(Obj).where(Obj.id.in_(new_parent_ids))
            parents = await session.execute(stmt)
            parents = {parent.id: parent for parent in parents.scalars().all()}

            # cache change path for child
            replace_path_cache = dict()

            # change node parent, path and path for children new parents
            for child_node in chunk:
                based_n_data = node_data_group_by_node_id.get(child_node.id)

                new_parent_node_id = node_id_new_p_node_id.get(child_node.id)
                new_parent = None

                if new_parent_node_id:
                    new_parent = parents.get(new_parent_node_id)

                child_node.parent_id = (
                    new_parent.id if new_parent is not None else None
                )
                # add new data from node_data
                child_node.active = based_n_data.mo_active
                # add new key from node_data
                key_data = get_node_key_data(
                    ordered_key_attrs=list(based_n_data.unfolded_key),
                    mo_data_with_params=based_n_data.unfolded_key,
                )
                child_node.key = key_data.key
                child_node.key_is_empty = key_data.key_is_empty

                new_path = create_path_for_children_node_by_parent_node(
                    new_parent
                )

                if child_node.path != new_path:
                    old_path_to_change_for_children = (
                        create_path_for_children_node_by_parent_node(child_node)
                    )
                    child_node.path = new_path
                    new_path_replace_for_children = (
                        create_path_for_children_node_by_parent_node(child_node)
                    )
                    if (
                        old_path_to_change_for_children
                        != new_path_replace_for_children
                    ):
                        replace_path_cache[old_path_to_change_for_children] = (
                            new_path_replace_for_children
                        )

                    child_node.path = new_path

                session.add(child_node)

            await session.flush()
            for old_path_prefix, new_path_prefix in replace_path_cache.items():
                query = select(Obj).where(Obj.path.like(f"{old_path_prefix}%"))
                result_generator = await session.stream_scalars(query)
                async for partition in result_generator.yield_per(
                    LIMIT_OF_POSTGRES_RESULTS_PER_STEP
                ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
                    for child_node in partition:
                        child_node.path = child_node.path.replace(
                            old_path_prefix, new_path_prefix
                        )
                        session.add(child_node)

                await session.flush()

    await session.flush()
    return node_ids_to_recalc_child_count


async def __rebuild_virtual(
    session: AsyncSession,
    node_data_with_new_parents: list[NodeData],
    node_id_new_p_node_id: dict[str, str],
    level: Level,
    parent_level: Level | None,
):
    # if level is virtual
    node_ids_to_recalc_child_count = set()
    # if level is virtual look if node fully consist of node_data with new parent
    get_children_for_mo_ids = set()
    n_data_list_grouped_by_node_id = defaultdict(list)
    for node_data in node_data_with_new_parents:
        n_data_list_grouped_by_node_id[node_data.node_id].append(node_data)
        get_children_for_mo_ids.add(node_data.mo_id)

    virtual_node_ids_to_change_parents = set()
    virtual_node_data_to_change_parent = list()

    # get count of node_data grouped by node id
    stmt = (
        select(NodeData.node_id, func.count(NodeData.id).label("consist_of"))
        .where(NodeData.node_id.in_(n_data_list_grouped_by_node_id))
        .group_by(NodeData.node_id)
    )
    res = await session.execute(stmt)
    res = {data.node_id: data.consist_of for data in res.all()}

    # if count of node_data is equals to changed same p_ids  -change parent of node -
    # change child count for new and old parent
    for node_id, list_of_node_data in n_data_list_grouped_by_node_id.items():
        first_n_data = list_of_node_data[0]
        first_item_parent = first_n_data.mo_p_id
        first_item_key_data = get_node_key_data(
            ordered_key_attrs=list(first_n_data.unfolded_key),
            mo_data_with_params=first_n_data.unfolded_key,
        )
        first_item_key = first_item_key_data.key
        all_n_data_active = first_n_data.mo_active

        all_same = True

        for node_data in list_of_node_data:
            node_key_data = get_node_key_data(
                ordered_key_attrs=list(node_data.unfolded_key),
                mo_data_with_params=node_data.unfolded_key,
            )
            node_key_based_on_n_data = node_key_data.key

            if any(
                [
                    node_data.mo_p_id != first_item_parent,
                    node_data.mo_active != all_n_data_active,
                    first_item_key != node_key_based_on_n_data,
                ]
            ):
                all_same = False
                break

        count_of_node_data_to_change_parent = res.get(node_id)

        if (
            count_of_node_data_to_change_parent == len(list_of_node_data)
            and all_same
        ):
            virtual_node_ids_to_change_parents.add(node_id)
        else:
            virtual_node_data_to_change_parent.extend(list_of_node_data)

    # case 1 change parent for node and node_data
    list_of_virtual_node_ids_to_change_parents = list(
        virtual_node_ids_to_change_parents
    )
    if virtual_node_ids_to_change_parents:
        node_ids_to_recalc_child_count.update(
            virtual_node_ids_to_change_parents
        )

        steps = math.ceil(
            len(list_of_virtual_node_ids_to_change_parents)
            / POSTGRES_ITEMS_LIMIT_IN_QUERY
        )

        for step in range(steps):
            start = step * POSTGRES_ITEMS_LIMIT_IN_QUERY
            end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

            stmt = select(Obj).where(
                Obj.id.in_(
                    list_of_virtual_node_ids_to_change_parents[start:end]
                )
            )
            nodes = await session.execute(stmt)
            nodes = nodes.scalars().all()

            # add their node_id and old parent to recalc
            # get old parent ids and new parent ids
            old_parent_ids = set()
            new_parent_ids = set()

            for node in nodes:
                if node.parent_id:
                    old_parent_ids.add(node.parent_id)

                new_parent_node_id = node_id_new_p_node_id.get(node.id)
                if new_parent_node_id:
                    new_parent_ids.add(new_parent_node_id)

            node_ids_to_recalc_child_count.update(
                old_parent_ids, new_parent_ids
            )

            # get new parents
            parents = dict()

            if new_parent_ids:
                stmt = select(Obj).where(Obj.id.in_(new_parent_ids))
                parents = await session.execute(stmt)
                parents = {
                    parent.id: parent for parent in parents.scalars().all()
                }

            # cache change path for child
            replace_path_cache = dict()
            old_node_id_new_node_id_for_children = dict()

            # change node paren, path and path for children new parents
            for node in nodes:
                based_n_data = n_data_list_grouped_by_node_id.get(node.id)[0]

                new_parent_node_id = node_id_new_p_node_id.get(node.id)
                new_parent = None

                if new_parent_node_id:
                    new_parent = parents.get(new_parent_node_id)

                # check if exists node with same key and same parent on this level
                node_key_data = get_node_key_data(
                    ordered_key_attrs=list(based_n_data.unfolded_key),
                    mo_data_with_params=based_n_data.unfolded_key,
                )
                node_key = node_key_data.key
                stmt = select(Obj).where(
                    Obj.level_id == level.id,
                    Obj.parent_id == new_parent_node_id,
                    Obj.key == node_key,
                    Obj.active == based_n_data.mo_active,
                    Obj.id != node.id,
                )
                default_node = await session.execute(stmt)
                default_node = default_node.scalars().first()

                if not default_node:
                    node.parent_id = (
                        new_parent.id if new_parent is not None else None
                    )
                    # add new data from node_data
                    node.active = based_n_data.mo_active
                    node.key = node_key
                    node.key_is_empty = node_key_data.key_is_empty

                    new_path = create_path_for_children_node_by_parent_node(
                        new_parent
                    )
                    if node.path != new_path:
                        old_path_to_change_for_children = (
                            create_path_for_children_node_by_parent_node(node)
                        )
                        node.path = new_path
                        new_path_replace_for_children = (
                            create_path_for_children_node_by_parent_node(node)
                        )
                        if (
                            old_path_to_change_for_children
                            != new_path_replace_for_children
                        ):
                            replace_path_cache[
                                old_path_to_change_for_children
                            ] = new_path_replace_for_children

                        node.path = new_path
                        session.add(node)
                else:
                    old_node_id_new_node_id_for_children[node.id] = (
                        default_node.id
                    )
                    old_path = create_path_for_children_node_by_parent_node(
                        node
                    )
                    new_path_for_children = (
                        create_path_for_children_node_by_parent_node(
                            default_node
                        )
                    )
                    replace_path_cache[old_path] = new_path_for_children

                    for n_data in n_data_list_grouped_by_node_id.get(node.id):
                        n_data.node_id = default_node.id

            if old_node_id_new_node_id_for_children:
                old_node_ids = list(old_node_id_new_node_id_for_children)
                inner_steps = math.ceil(len(old_node_ids) / 30_000)

                # update node id for children
                for inner_step in range(inner_steps):
                    inner_start = inner_step * 30_000
                    inner_end = inner_start + 30_000
                    step_old_node_ids = old_node_ids[inner_start:inner_end]

                    stmt = select(Obj).where(
                        Obj.parent_id.in_(step_old_node_ids)
                    )
                    objs_with_old_parents = await session.execute(stmt)
                    objs_with_old_parents = (
                        objs_with_old_parents.scalars().all()
                    )

                    for obj_with_old_parent in objs_with_old_parents:
                        new_p = old_node_id_new_node_id_for_children.get(
                            obj_with_old_parent.parent_id
                        )
                        obj_with_old_parent.parent_id = new_p
                        session.add(obj_with_old_parent)
                    await session.commit()

                # delete node id for children
                for inner_step in range(inner_steps):
                    inner_start = inner_step * 30_000
                    inner_end = inner_start + 30_000
                    step_old_node_ids = old_node_ids[inner_start:inner_end]

                    stmt = select(Obj).where(Obj.id.in_(step_old_node_ids))
                    old_parents = await session.execute(stmt)
                    old_parents = old_parents.scalars().all()
                    for old_parent in old_parents:
                        await session.delete(old_parent)

                    await session.commit()

            # update paths
            for old_path_prefix, new_path_prefix in replace_path_cache.items():
                query = select(Obj).where(Obj.path.like(f"{old_path_prefix}%"))
                result_generator = await session.stream_scalars(query)
                async for partition in result_generator.yield_per(
                    LIMIT_OF_POSTGRES_RESULTS_PER_STEP
                ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
                    for child_node in partition:
                        child_node.path = child_node.path.replace(
                            old_path_prefix, new_path_prefix
                        )
                        session.add(child_node)

                await session.flush()
            await session.flush()

    # case 2 change parent for only for node_data of virtual nodes

    if virtual_node_data_to_change_parent:
        # get new parents ids
        mo_p_ids = set()

        if parent_level:
            if level.object_type_id != parent_level.object_type_id:
                mo_p_ids = {
                    n_data.mo_p_id
                    for n_data in virtual_node_data_to_change_parent
                    if n_data.mo_p_id
                }
            else:
                mo_p_ids = {
                    n_data.mo_id
                    for n_data in virtual_node_data_to_change_parent
                    if n_data.mo_id
                }

        new_parent_node_data_by_p_id = dict()
        new_parent_node_ids = dict()
        if mo_p_ids:
            stmt = select(NodeData).where(
                NodeData.mo_id.in_(mo_p_ids),
                NodeData.level_id == level.parent_id,
            )
            new_parent_node_data = await session.execute(stmt)
            new_parent_node_data = new_parent_node_data.scalars().all()
            new_parent_node_data_by_p_id = {
                n_data.mo_id: n_data for n_data in new_parent_node_data
            }

            new_parent_node_ids = {
                n_data.node_id: n_data for n_data in new_parent_node_data
            }

            # update to recalt for new parents
            node_ids_to_recalc_child_count.update(new_parent_node_ids)

        # get new parents
        new_parent_nodes_by_id = dict()
        if new_parent_node_ids:
            stmt = select(Obj).where(Obj.id.in_(new_parent_node_ids))
            new_parent_nodes_by_id = await session.execute(stmt)
            new_parent_nodes_by_id = new_parent_nodes_by_id.scalars().all()
            new_parent_nodes_by_id = {
                node.id: node for node in new_parent_nodes_by_id
            }

        n_data_grouped_by_key_node_p_id_and_active = defaultdict(list)
        for n_data in virtual_node_data_to_change_parent:
            key_data = get_node_key_data(
                ordered_key_attrs=list(n_data.unfolded_key),
                mo_data_with_params=n_data.unfolded_key,
            )
            new_parent_node_id = None
            if parent_level:
                if level.object_type_id != parent_level.object_type_id:
                    new_parent_node_id = new_parent_node_data_by_p_id.get(
                        n_data.mo_p_id
                    )
                else:
                    new_parent_node_id = new_parent_node_data_by_p_id.get(
                        n_data.mo_id
                    )

            p_id = None
            if new_parent_node_id:
                p_id = new_parent_node_id.node_id

            is_active = n_data.mo_active
            n_data_grouped_by_key_node_p_id_and_active[
                (key_data.key, key_data.key_is_empty, p_id, is_active)
            ].append(n_data)
        # todo: optimize
        for (
            key_parent_id_active_tuple,
            list_of_n_data,
        ) in n_data_grouped_by_key_node_p_id_and_active.items():
            # check if node with such key exists for this parent node
            key, key_is_empty, new_parent_node_id, is_active = (
                key_parent_id_active_tuple
            )
            stmt = select(Obj).where(
                Obj.key == key,
                Obj.parent_id == new_parent_node_id,
                Obj.level_id == level.id,
                Obj.active == is_active,
            )
            existing_v_node = await session.execute(stmt)
            existing_v_node = existing_v_node.scalars().first()

            if existing_v_node:
                for n_data in list_of_n_data:
                    n_data.node_id = existing_v_node.id
                    session.add(n_data)
                    get_children_for_mo_ids.add(n_data.mo_id)
            else:
                parent = None
                if new_parent_node_id:
                    parent = new_parent_nodes_by_id.get(new_parent_node_id)

                path = create_path_for_children_node_by_parent_node(parent)
                if parent:
                    parent.child_count += 1
                    session.add(parent)

                new_node = Obj(
                    key=key,
                    object_id=None,
                    object_type_id=level.object_type_id,
                    additional_params=None,
                    hierarchy_id=level.hierarchy_id,
                    level=level.level,
                    latitude=None,
                    longitude=None,
                    child_count=0,
                    level_id=level.id,
                    parent_id=parent.id if parent is not None else None,
                    path=path,
                    active=is_active,
                    key_is_empty=key_is_empty,
                )

                session.add(new_node)
                await session.flush()

                for node_data in list_of_n_data:
                    node_data.node_id = new_node.id
                    session.add(node_data)
                    get_children_for_mo_ids.add(node_data.mo_id)

            await session.flush()

    # change parent for node data
    # if was created some nodes or node_data after active was changed, changes node_id,
    # we need to change parents for children nodes
    return_node_data_for_one_more_iteration = []

    if get_children_for_mo_ids:
        get_children_for_mo_ids = list(get_children_for_mo_ids)
        # get child levels
        stmt = select(Level).where(Level.parent_id == level.id)
        child_levels = await session.execute(stmt)
        child_levels = child_levels.scalars().all()

        steps = math.ceil(
            len(get_children_for_mo_ids) / POSTGRES_ITEMS_LIMIT_IN_QUERY
        )

        for child_level in child_levels:
            level_node_data = list()

            for step in range(steps):
                start = step * POSTGRES_ITEMS_LIMIT_IN_QUERY
                end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

                step_mo_ids = get_children_for_mo_ids[start:end]

                if child_level.object_type_id == level.object_type_id:
                    # if child level is real and match parent level by object type id
                    stmt = select(NodeData).where(
                        NodeData.level_id == child_level.id,
                        NodeData.mo_id.in_(step_mo_ids),
                    )
                    child_nodes = await session.execute(stmt)
                    child_nodes = child_nodes.scalars().all()
                    level_node_data.extend(child_nodes)

                else:
                    stmt = select(NodeData).where(
                        NodeData.level_id == child_level.id,
                        NodeData.mo_p_id.in_(step_mo_ids),
                    )
                    child_nodes = await session.execute(stmt)
                    child_nodes = child_nodes.scalars().all()
                    level_node_data.extend(child_nodes)

            if level_node_data:
                return_node_data_for_one_more_iteration.append(
                    (child_level, level_node_data)
                )
    return (
        node_ids_to_recalc_child_count,
        return_node_data_for_one_more_iteration,
    )


async def __recalc_child_count(
    node_ids_to_recalc_child_count: set,
    session: AsyncSession,
):
    steps = math.ceil(
        len(node_ids_to_recalc_child_count) / POSTGRES_ITEMS_LIMIT_IN_QUERY
    )
    list_of_node_id = list(node_ids_to_recalc_child_count)

    for step in range(steps):
        start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
        end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

        step_list = list_of_node_id[start:end]

        subquery = (
            select(Obj.parent_id, func.count(Obj.id).label("new_count"))
            .where(Obj.parent_id.in_(step_list), Obj.active == True)  # noqa: E712
            .group_by(Obj.parent_id)
            .subquery()
        )
        aliased_table = aliased(subquery)

        stmt = (
            select(Obj)
            .where(Obj.id.in_(step_list))
            .outerjoin(aliased_table, Obj.id == aliased_table.c.parent_id)
            .add_columns(aliased_table.c.new_count)
        )

        res = await session.execute(stmt)
        res = res.all()
        for item in res:
            if item.new_count is None:
                item.Obj.child_count = 0
            else:
                item.Obj.child_count = item.new_count
            session.add(item.Obj)

        await session.flush()


async def __rebuilding_nodes_based_on_their_data_and_level(
    node_data_with_new_parents: list[NodeData],
    session: AsyncSession,
    level: Level,
):
    """Use to rebuild nodes"""

    if not node_data_with_new_parents:
        return
    parent_level = await __get_parent_level(level=level, session=session)

    node_id_new_p_node_id = await __group_by_new_parents(
        level=level,
        parent_level=parent_level,
        session=session,
        node_data_with_new_parents=node_data_with_new_parents,
    )

    return_node_data_for_one_more_iteration = []

    if not level.is_virtual:
        node_ids_to_recalc_child_count = await __rebuild_real(
            session=session,
            node_data_with_new_parents=node_data_with_new_parents,
            node_id_new_p_node_id=node_id_new_p_node_id,
        )
    else:
        if level.key_attrs:
            node_ids_to_recalc_child_count = await __rebuild_hierarchical_data(
                session=session,
                node_id_new_p_node_id=node_id_new_p_node_id,
                node_data_with_new_parents=node_data_with_new_parents,
                level=level,
            )
        # For correct work with test test_with_mo_update_changed_active_case_3
        if level.is_virtual:
            (
                node_ids_to_recalc_child_count,
                return_node_data_for_one_more_iteration,
            ) = await __rebuild_virtual(
                session=session,
                node_id_new_p_node_id=node_id_new_p_node_id,
                node_data_with_new_parents=node_data_with_new_parents,
                level=level,
                parent_level=parent_level,
            )

    if node_ids_to_recalc_child_count:
        await __recalc_child_count(
            node_ids_to_recalc_child_count=node_ids_to_recalc_child_count,
            session=session,
        )
    return return_node_data_for_one_more_iteration


async def rebuilding_nodes_based_on_their_data(
    node_data_with_new_parents: list[NodeData],
    session: AsyncSession,
    level: Level,
):
    """Rebuilds nodes based on their NodeData"""

    order = [(level, node_data_with_new_parents)]

    counter = 0
    for level, node_data_with_new_parents in order:  # type: Level, list[NodeData]
        res = await __rebuilding_nodes_based_on_their_data_and_level(
            node_data_with_new_parents=node_data_with_new_parents,
            session=session,
            level=level,
        )

        order[counter] = (level, [])
        if res:
            counter += 1
            order.extend(res)
