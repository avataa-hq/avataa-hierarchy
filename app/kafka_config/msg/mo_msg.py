from collections import defaultdict
import math
import time

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from kafka_config.msg.msg_utils import (
    HierarchyMOCreateHandler,
    rebuilding_nodes_based_on_their_data,
)
from schemas.hier_schemas import Level, NodeData, Obj
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


async def on_delete_mo(msg, session: AsyncSession):
    """Delete objects and does not rebuild hierarchy"""
    # log info start
    start_time = time.time()
    count_of_obj = len(msg["objects"])
    # log info end
    ids = []
    tmo_ids = set()

    for item in msg["objects"]:
        ids.append(item["id"])
        tmo_ids.add(item["tmo_id"])

    if len(ids) == 0:
        return

    # node_data part start

    # node identifiers to be checked during deletion
    node_id_check_on_delete = set()

    query = select(NodeData).where(NodeData.mo_id.in_(ids))
    result_generator = await session.stream_scalars(query)
    async for partition in result_generator.yield_per(
        POSTGRES_ITEMS_LIMIT_IN_QUERY
    ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
        node_data_id_to_delete = list()

        for node_data in partition:
            node_data_id_to_delete.append(node_data.id)
            node_id_check_on_delete.add(node_data.node_id)

        delete_stm = delete(NodeData).where(
            NodeData.id.in_(node_data_id_to_delete)
        )
        await session.execute(delete_stm)

    steps = math.ceil(
        len(node_id_check_on_delete) / POSTGRES_ITEMS_LIMIT_IN_QUERY
    )
    list_of_node_id = list(node_id_check_on_delete)

    nodes_ids_must_be_deleted = set()
    for step in range(steps):
        start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
        end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

        step_list = list_of_node_id[start:end]
        step_set = set(step_list)

        stmt = (
            select(NodeData.node_id)
            .where(NodeData.node_id.in_(step_list))
            .distinct()
        )

        res = await session.execute(stmt)
        res = set(res.scalars().all())

        nodes_ids_must_be_deleted.update(step_set.difference(res))

    if nodes_ids_must_be_deleted:
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

                # untie children nodes ! necessary
                stmt = (
                    update(Obj)
                    .where(Obj.parent_id.in_(step_list))
                    .values(parent_id=None)
                )
                await session.execute(stmt)

                # delete node
                stmt = delete(Obj).where(Obj.id.in_(step_list))
                await session.execute(stmt)
                await session.commit()

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
                    .where(Obj.parent_id.in_(step_list), Obj.active == True)  # noqa: E712
                    .group_by(Obj.parent_id)
                    .subquery()
                )
                aliased_table = aliased(subquery)

                stmt = (
                    select(Obj)
                    .where(Obj.id.in_(step_list))
                    .outerjoin(
                        aliased_table, Obj.id == aliased_table.c.parent_id
                    )
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
    # log info start
    end_time = time.time()
    print(f"Deleted {count_of_obj} MOs in {end_time - start_time} seconds.")
    # log info end


async def on_create_mo(msg, session: AsyncSession):
    """If new MO has been created - makes changes to a specific hierarchy without full rebuild."""
    # log info start
    start_time = time.time()
    count_of_obj = len(msg["objects"])
    # log info end
    create_handler = HierarchyMOCreateHandler(
        mo_data=msg["objects"], session=session
    )
    await create_handler.make_changes()
    # log info start
    end_time = time.time()
    print(f"Created {count_of_obj} MOs in {end_time - start_time} seconds.")
    # log info end


async def on_update_mo(msg, session: AsyncSession):
    """If MO.p_id has been changed - makes changes to a specific hierarchy without full rebuild."""
    # log info start
    start_time = time.time()
    count_of_obj = len(msg["objects"])
    # log info end
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
        Level.object_type_id.in_(tmo_ids), Level.key_attrs.overlap(set_of_keys)
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

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

    stmt = select(NodeData).where(NodeData.mo_id.in_(mo_cache))
    result_generator = await session.stream_scalars(stmt)
    async for partition in result_generator.yield_per(
        POSTGRES_ITEMS_LIMIT_IN_QUERY
    ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
        for node_data in partition:
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
                            attr_value_from_msg = mo_data_from_msg[key_attr]
                            if (
                                attr_value_from_msg
                                != node_data.unfolded_key.get(key_attr)
                            ):
                                node_data_ids_with_changed_key_grouped_by_tmo_id[
                                    node_data.mo_tmo_id
                                ].add(node_data.id)

                                # change node_data
                                node_data.unfolded_key[key_attr] = (
                                    attr_value_from_msg
                                )
                                n_data_changed = True
            if n_data_changed:
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
                select(Level).where(Level.object_type_id != tmo_id).subquery()
            )
            aliased_table = aliased(subquery)

            stmt = (
                select(Level, aliased_table)
                .where(
                    Level.object_type_id == tmo_id, Level.parent_id.is_not(None)
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
    # log info start
    end_time = time.time()
    print(f"Updated {count_of_obj} MOs in {end_time - start_time} seconds.")
    # log info end


#
# async def on_update_mo_old(msg, session: AsyncSession):
#     """If MO.p_id has been changed - makes changes to a specific hierarchy without full rebuild."""
#     mo_cache = dict()
#     mo_ids_with_active_false = set()
#     mo_ids_with_active_true = set()
#
#     for item in msg['objects']:
#         mo_id = item['id']
#         mo_cache[mo_id] = item
#         if item['active']:
#             mo_ids_with_active_true.add(mo_id)
#         else:
#             mo_ids_with_active_false.add(mo_id)
#
#     # get all Nodes of updated objects
#     print(msg)
#     print(list(mo_cache))
#     stm = select(NodeData.mo_id, NodeData.mo_p_id, NodeData.mo_name, NodeData.mo_active).where(
#         NodeData.mo_id.in_(mo_cache)).distinct()
#     all_node_data = await session.execute(stm)
#     all_node_data = all_node_data.all()
#
#     # create cache data for data with updated p_id and data of existing mo
#     existing_nodes_mo_ids = set()
#     # get mo with changed parent_id grouped by tmo_id
#     changed_mo_grouped_by_tmo_id = defaultdict(list)
#     mo_cache_with_changed_p_id_gr_by_tmo_id = defaultdict(dict)
#     mo_list_with_changed_active_attr = list()
#     for n_d in all_node_data:
#         existing_nodes_mo_ids.add(n_d.mo_id)
#         mo_item_from_cache = mo_cache.get(n_d.mo_id)
#         if mo_item_from_cache:
#             if mo_item_from_cache['p_id'] != n_d.mo_p_id:
#                 tmo_id = mo_item_from_cache['tmo_id']
#
#                 mo_cache_with_changed_p_id_gr_by_tmo_id[tmo_id][n_d.mo_id] = mo_item_from_cache
#                 changed_mo_grouped_by_tmo_id[tmo_id].append(mo_item_from_cache)
#             if mo_item_from_cache['active'] != n_d.mo_active:
#                 mo_list_with_changed_active_attr.append(mo_item_from_cache)
#
#     # case - mo changed active attr - start
#     if mo_list_with_changed_active_attr:
#         await change_status_for_nodes_data_and_nodes(mo_data_list=mo_list_with_changed_active_attr, session=session)
#     # case - mo changed active attr - end
#
#     # if not changed_mo_grouped_by_tmo_id:
#     #     return
#
#     # case p_id was changed - start
#     # if there are mo with changed p_id
#
#     for tmo_id, mo_data_cache in mo_cache_with_changed_p_id_gr_by_tmo_id.items():
#
#         # get levels with parent - in this case we don`t need levels with paren_id is None
#
#         subquery = select(Level).where(Level.object_type_id != tmo_id).subquery()
#         aliased_table = aliased(subquery)
#
#         stmt = select(Level, aliased_table).where(
#             Level.object_type_id == tmo_id,
#             Level.parent_id.is_not(None)).join(
#             aliased_table, Level.parent_id == aliased_table.c.id, isouter=False).add_columns(
#             aliased_table.c.id).order_by(Level.level)
#         cache_all_levels_data = await session.execute(stmt)
#         cache_all_levels_data = cache_all_levels_data.scalars().all()
#
#         mo_data_with_new_parent = dict()
#         new_parent_mo_ids = set()
#
#         for mo_data in mo_data_cache.values():
#             p_id = mo_data.get('p_id')
#             mo_data_with_new_parent[mo_data['id']] = mo_data
#             if p_id:
#                 new_parent_mo_ids.add(p_id)
#
#         for level in cache_all_levels_data:
#
#             node_to_change_parent_cache = list()
#
#             # get node data and parent node data for specific level
#
#             stmt = select(NodeData).where(
#                 NodeData.mo_id.in_(mo_data_cache),
#                 NodeData.level_id == level.id)
#             node_data_and_p_node_data = await session.execute(stmt)
#             node_data_and_p_node_data = node_data_and_p_node_data.scalars().all()
#
#             for node_data in node_data_and_p_node_data:
#                 new_mo_p_id = mo_data_cache.get(node_data.mo_id).get('p_id')
#                 node_data.mo_p_id = new_mo_p_id
#                 node_to_change_parent_cache.append(node_data)
#
#             # get node_data of new parents
#             if node_to_change_parent_cache:
#                 await change_parents_of_node_data_for_specific_level(
#                     node_data_with_new_parents=node_to_change_parent_cache,
#                     session=session,
#                     level=level)
#
#         # change Node data p_id for all not changed levels
#         levels_was_changed = [level.id for level in cache_all_levels_data]
#         query = select(NodeData).where(NodeData.mo_id.in_(mo_data_cache),
#                                        NodeData.level_id.not_in(levels_was_changed))
#         result_generator = await session.stream_scalars(query)
#         async for partition in result_generator.yield_per(POSTGRES_ITEMS_LIMIT_IN_QUERY).partitions(
#                 POSTGRES_ITEMS_LIMIT_IN_QUERY):
#             for node_data in partition:
#                 mo_data = mo_data_cache[node_data.mo_id]
#                 node_data.mo_p_id = mo_data.get('p_id')
#                 session.add(node_data)
#
#             await session.flush()
#
#         await session.commit()
#
#     # case p_id was changed - End
#
