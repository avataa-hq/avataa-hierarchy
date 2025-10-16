from collections import defaultdict
import math
from uuid import UUID

from sqlalchemy import delete, func, select, update
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
from services.updater.event_handlers.common.msg_utils import (
    rebuilding_nodes_based_on_their_data,
)
from settings import (
    LIMIT_OF_POSTGRES_RESULTS_PER_STEP,
    POSTGRES_ITEMS_LIMIT_IN_QUERY,
)


async def delete_node_and_recalculate_child_count_for_parent(
    nodes_ids_must_be_deleted: set[UUID], session: AsyncSession
):
    """New version of function that deletes nodes (Use only if Node_data was implemented)"""

    # get parents of this nodes
    parent_ids_to_recalculate = set()
    if nodes_ids_must_be_deleted:
        steps = math.ceil(
            len(nodes_ids_must_be_deleted) / POSTGRES_ITEMS_LIMIT_IN_QUERY
        )
        list_of_node_id = list(nodes_ids_must_be_deleted)

        for step in range(steps):
            start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
            end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

            step_list = list_of_node_id[start:end]

            # get parents of this nodes
            stmt = (
                select(Obj.parent_id)
                .where(
                    Obj.parent_id.is_not(None),
                    Obj.id.in_(nodes_ids_must_be_deleted),
                )
                .distinct()
            )
            res = await session.execute(stmt)
            parent_ids_to_recalculate.update(set(res.scalars().all()))

            # delete node
            stmt = delete(Obj).where(Obj.id.in_(step_list))
            await session.execute(stmt)

    if parent_ids_to_recalculate:
        steps = math.ceil(
            len(parent_ids_to_recalculate) / POSTGRES_ITEMS_LIMIT_IN_QUERY
        )
        list_of_node_id = list(parent_ids_to_recalculate)

        for step in range(steps):
            start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
            end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

            step_list = list_of_node_id[start:end]

            subquery = (
                select(Obj.parent_id, func.count(Obj.id).label("new_count"))
                .where(Obj.parent_id.in_(step_list))
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

            await session.commit()

    stmt = delete(NodeData).where(
        NodeData.node_id.in_(list(nodes_ids_must_be_deleted))
    )
    await session.execute(stmt)
    await session.commit()


class HierarchyMOCreateHandler:
    """Algorithm for changing hierarchies when creating a new MO"""

    def __init__(self, mo_data: list[dict], session: AsyncSession):
        self.base_mo_list = mo_data
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
            .where(Level.object_type_id.in_(list(self._tmo_ids)))
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
                        stmt = select(Obj).where(Obj.id.in_(parent_node_ids))
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
                                Obj.id.in_(parent_node_ids)
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
        # We shouldn't use commit
        # await self.session.flush()

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
        # We shouldn't use commit
        # await self.session.flush()

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
                    await self.__create_nodes_for_virtual_level(level=level)
                else:
                    await self.__create_nodes_for_real_level(level=level)


async def rebuilding_all_for_specific_level(
    session: AsyncSession, level: Level
):
    """Rebuilds nodes based on their NodeData for specific level"""

    stmt = select(NodeData).where(NodeData.level_id == level.id)
    result_generator = await session.stream_scalars(stmt)
    async for partition in result_generator.yield_per(
        POSTGRES_ITEMS_LIMIT_IN_QUERY
    ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
        await rebuilding_nodes_based_on_their_data(
            node_data_with_new_parents=partition, session=session, level=level
        )


async def recalculate_child_count_of_nodes_for_specific_level(
    session: AsyncSession, level_id: int
):
    """Recalculates nodes child_count for special level. Flushes session (does not commit)"""

    stmt = select(Level.id).where(Level.parent_id == level_id)
    child_levels = await session.execute(stmt)
    child_levels = child_levels.scalars().all()

    subquery = (
        select(Obj.parent_id, func.count(Obj.id).label("new_count"))
        .where(Obj.level_id.in_(child_levels))
        .group_by(Obj.parent_id)
        .subquery()
    )
    aliased_table = aliased(subquery)

    stmt = (
        select(Obj)
        .where(Obj.level_id == level_id)
        .outerjoin(aliased_table, Obj.id == aliased_table.c.parent_id)
        .add_columns(aliased_table.c.new_count)
    )
    result_generator = await session.stream(stmt)
    async for partition in result_generator.yield_per(
        LIMIT_OF_POSTGRES_RESULTS_PER_STEP
    ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
        for item in partition:
            if item.new_count is None:
                item.Obj.child_count = 0
            else:
                item.Obj.child_count = item.new_count
            session.add(item.Obj)

        await session.flush()


async def change_status_for_nodes_data_and_nodes(
    mo_data_list: list[dict], session: AsyncSession
):
    """Changes status for node_data and corresponding nodes and recalc their child_count"""
    tmo_ids = set()
    mo_ids = set()
    # become active
    active_mo_ids_group_by_tmo_id = defaultdict(list)

    # become not active
    not_active_mo_ids_group_by_tmo_id = defaultdict(list)
    for mo_item in mo_data_list:
        tmo_id = mo_item.get("tmo_id")
        mo_id = mo_item.get("id")

        tmo_ids.add(tmo_id)
        mo_ids.add(mo_id)
        is_active = mo_item.get("active")

        if is_active:
            active_mo_ids_group_by_tmo_id[tmo_id].append(mo_id)
        else:
            not_active_mo_ids_group_by_tmo_id[tmo_id].append(mo_id)

    # get levels of all corresponding hierarchies
    stmt = (
        select(Level)
        .where(Level.object_type_id.in_(tmo_ids))
        .order_by(Level.hierarchy_id.asc(), Level.level.asc())
    )

    levels = await session.execute(stmt)
    levels = levels.scalars().all()

    top_level_by_hierarchy = dict()
    real_levels = list()
    virtual_levels = list()
    for level in levels:
        if level.is_virtual:
            virtual_levels.append(level)
        else:
            real_levels.append(level)
        level_in_cache = top_level_by_hierarchy.get(level.hierarchy_id)
        if not level_in_cache:
            top_level_by_hierarchy[level.hierarchy_id] = level

    for level in real_levels:
        become_active_mo_ids = active_mo_ids_group_by_tmo_id.get(
            level.object_type_id
        )
        become_not_active_mo_ids = not_active_mo_ids_group_by_tmo_id.get(
            level.object_type_id
        )

        if become_active_mo_ids:
            stmt = (
                update(Obj)
                .where(Obj.object_id.in_(mo_ids), Obj.level_id == level.id)
                .values(active=True)
            )
            await session.execute(stmt)

            stmt = (
                update(NodeData)
                .where(
                    NodeData.mo_id.in_(mo_ids), NodeData.level_id == level.id
                )
                .values(mo_active=True)
            )
            await session.execute(stmt)
        if become_not_active_mo_ids:
            stmt = (
                update(Obj)
                .where(Obj.object_id.in_(mo_ids), Obj.level_id == level.id)
                .values(active=False)
            )
            await session.execute(stmt)

            stmt = (
                update(NodeData)
                .where(
                    NodeData.mo_id.in_(mo_ids), NodeData.level_id == level.id
                )
                .values(mo_active=False)
            )
            await session.execute(stmt)

    for level in virtual_levels:
        become_active_mo_ids = active_mo_ids_group_by_tmo_id.get(
            level.object_type_id
        )
        become_not_active_mo_ids = not_active_mo_ids_group_by_tmo_id.get(
            level.object_type_id
        )
        if become_active_mo_ids:
            stmt = (
                update(NodeData)
                .where(
                    NodeData.mo_id.in_(mo_ids), NodeData.level_id == level.id
                )
                .values(mo_active=True)
            )
            await session.execute(stmt)

        if become_not_active_mo_ids:
            stmt = (
                update(NodeData)
                .where(
                    NodeData.mo_id.in_(mo_ids), NodeData.level_id == level.id
                )
                .values(mo_active=False)
            )
            await session.execute(stmt)
    await session.commit()

    for _, level in top_level_by_hierarchy.items():
        stmt = select(NodeData).where(
            NodeData.mo_id.in_(mo_ids), NodeData.level_id == level.id
        )
        node_data_list = await session.execute(stmt)
        node_data_list = node_data_list.scalars().all()

        await rebuilding_nodes_based_on_their_data(
            node_data_with_new_parents=node_data_list,
            session=session,
            level=level,
        )
