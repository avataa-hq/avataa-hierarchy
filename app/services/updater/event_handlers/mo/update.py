from collections import defaultdict
import copy
import math

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from schemas.hier_schemas import Level, NodeData
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)
from services.updater.event_handlers.common.msg_utils import (
    rebuilding_nodes_based_on_their_data,
)
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


async def with_mo_update_event(msg, session: AsyncSession, hierarchy_id: int):
    """If MO.p_id has been changed - makes changes to a specific hierarchy without full rebuild."""

    mo_cache = dict()
    mo_ids_with_active_false = set()
    mo_ids_with_active_true = set()
    tmo_ids = set()
    set_of_keys = set()
    for item in msg["objects"]:
        set_of_keys.update(item)
        mo_id = item["id"]
        mo_cache[mo_id] = item
        tmo_ids.add(item["tmo_id"])
        if item["active"]:
            mo_ids_with_active_true.add(mo_id)
        else:
            mo_ids_with_active_false.add(mo_id)

    # get all levels with mo_attrs in key_attr and create level cache data
    stmt = select(Level).where(
        Level.object_type_id.in_(tmo_ids),
        Level.key_attrs.overlap(set_of_keys),
        Level.hierarchy_id == hierarchy_id,
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    if not all_levels:
        return

    levels_id_mo_attr_keys = {
        level.id: mo_attrs_key
        for level in all_levels
        if (
            mo_attrs_key := [
                attr for attr in level.key_attrs if not attr.isdigit()
            ]
        )
    }

    # get all NodeData and update their data
    node_data_ids_with_changed_p_id_gr_by_tmo_id = defaultdict(set)
    node_data_ids_with_changed_active_attr_gr_by_tmo_id = defaultdict(set)
    node_data_ids_with_changed_key_grouped_by_tmo_id = defaultdict(set)

    stmt = select(NodeData).where(
        NodeData.mo_id.in_(mo_cache),
        NodeData.level_id.in_(list(levels_id_mo_attr_keys)),
    )
    result_generator = await session.stream_scalars(stmt)
    async for partition in result_generator.yield_per(
        POSTGRES_ITEMS_LIMIT_IN_QUERY
    ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
        for node_data in partition:
            new_unfolded_key_data = dict()

            mo_data_from_msg = mo_cache.get(node_data.mo_id)

            n_data_changed = False

            if mo_data_from_msg:
                if "p_id" in mo_data_from_msg:
                    if mo_data_from_msg["p_id"] != node_data.mo_p_id:
                        node_data_ids_with_changed_p_id_gr_by_tmo_id[
                            node_data.mo_tmo_id
                        ].add(node_data.id)

                        # change node_data
                        node_data.mo_p_id = mo_data_from_msg["p_id"]
                        n_data_changed = True

                if "active" in mo_data_from_msg:
                    if mo_data_from_msg["active"] != node_data.mo_active:
                        node_data_ids_with_changed_active_attr_gr_by_tmo_id[
                            node_data.mo_tmo_id
                        ].add(node_data.id)

                        # change node_data
                        node_data.mo_active = mo_data_from_msg["active"]
                        n_data_changed = True

                level_mo_attrs_as_keys = levels_id_mo_attr_keys.get(
                    node_data.level_id
                )
                if level_mo_attrs_as_keys:
                    # then key may be changed
                    for key_attr in level_mo_attrs_as_keys:
                        if key_attr in mo_data_from_msg:
                            new_unfolded_key_data = copy.deepcopy(
                                node_data.unfolded_key
                            )
                            attr_value_from_msg = mo_data_from_msg[key_attr]
                            if (
                                attr_value_from_msg
                                != node_data.unfolded_key.get(key_attr)
                            ):
                                node_data_ids_with_changed_key_grouped_by_tmo_id[
                                    node_data.mo_tmo_id
                                ].add(node_data.id)
                                # change node_data
                                new_unfolded_key_data[key_attr] = (
                                    attr_value_from_msg
                                )
                                n_data_changed = True
            if n_data_changed:
                node_data.unfolded_key = new_unfolded_key_data
                session.add(node_data)
        await session.flush()
    await session.flush()

    all_tmo_ids = list(tmo_ids)

    for tmo_id in all_tmo_ids:
        node_data_ids_with_changed_key = (
            node_data_ids_with_changed_key_grouped_by_tmo_id.get(tmo_id, set())
        )
        node_data_ids_with_changed_active_attr = (
            node_data_ids_with_changed_active_attr_gr_by_tmo_id.get(
                tmo_id, set()
            )
        )
        node_data_ids_with_changed_p_id = (
            node_data_ids_with_changed_p_id_gr_by_tmo_id.get(tmo_id, set())
        )

        set_of_n_data_to_refresh_branch = set()
        set_of_n_data_to_refresh_branch.update(
            node_data_ids_with_changed_key,
            node_data_ids_with_changed_active_attr,
        )

        list_of_node_data_ids_to_refresh_branch = list(
            set_of_n_data_to_refresh_branch
        )
        # if there are nodes to refresh
        if list_of_node_data_ids_to_refresh_branch:
            # get levels of all corresponding hierarchies
            stmt = (
                select(Level)
                .where(Level.object_type_id == tmo_id)
                .order_by(Level.hierarchy_id.desc(), Level.level.asc())
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

            for _, level in top_level_by_hierarchy.items():
                node_data_list = list()

                steps = math.ceil(
                    len(list_of_node_data_ids_to_refresh_branch)
                    / POSTGRES_ITEMS_LIMIT_IN_QUERY
                )

                for step in range(steps):
                    start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
                    end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

                    step_list = list_of_node_data_ids_to_refresh_branch[
                        start:end
                    ]

                    stmt = select(NodeData).where(
                        NodeData.id.in_(step_list),
                        NodeData.level_id == level.id,
                    )
                    step_node_data_list = await session.execute(stmt)
                    step_node_data_list = step_node_data_list.scalars().all()
                    node_data_list.extend(step_node_data_list)

                if node_data_list:
                    await rebuilding_nodes_based_on_their_data(
                        node_data_with_new_parents=node_data_list,
                        session=session,
                        level=level,
                    )

        # get node_ids that was not changed with key ar active
        n_data_ids_changed_parent = node_data_ids_with_changed_p_id.difference(
            set_of_n_data_to_refresh_branch
        )
        if n_data_ids_changed_parent:
            print("With parent changing")
            # get levels with parent - in this case we don`t need levels with paren_id is None

            subquery = (
                select(Level)
                .where(
                    Level.object_type_id != tmo_id,
                    Level.hierarchy_id == hierarchy_id,
                )
                .subquery()
            )
            aliased_table = aliased(subquery)

            stmt = (
                select(Level, aliased_table)
                .where(
                    Level.object_type_id == tmo_id,
                    Level.parent_id.is_not(None),
                    Level.hierarchy_id == hierarchy_id,
                )
                .join(
                    aliased_table,
                    Level.parent_id == aliased_table.c.id,
                    isouter=False,
                )
                .add_columns(aliased_table.c.id)
                .order_by(Level.level)
            )
            cache_all_levels_data = await session.execute(stmt)
            cache_all_levels_data = cache_all_levels_data.scalars().all()

            for level in cache_all_levels_data:
                n_data_ids_changed_parent_list = list(n_data_ids_changed_parent)

                steps = math.ceil(
                    len(n_data_ids_changed_parent_list)
                    / POSTGRES_ITEMS_LIMIT_IN_QUERY
                )
                node_to_change_parent = list()

                for step in range(steps):
                    start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
                    end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

                    step_list = n_data_ids_changed_parent_list[start:end]

                    # get node data
                    stmt = select(NodeData).where(
                        NodeData.id.in_(step_list),
                        NodeData.level_id == level.id,
                    )
                    step_node_to_change_parent = await session.execute(stmt)
                    step_node_to_change_parent = (
                        step_node_to_change_parent.scalars().all()
                    )
                    node_to_change_parent.extend(step_node_to_change_parent)

                    # get node_data of new parents
                if node_to_change_parent:
                    await rebuilding_nodes_based_on_their_data(
                        node_data_with_new_parents=node_to_change_parent,
                        session=session,
                        level=level,
                    )

    await session.commit()


class MOUpdateHandler(HierarchyChangeInterface):
    async def make_changes(self):
        await with_mo_update_event(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
