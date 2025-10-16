from collections import defaultdict
import time

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from kafka_config.msg.msg_utils import rebuilding_nodes_based_on_their_data
from schemas.hier_schemas import Level, NodeData


async def on_delete_prm(msg, session: AsyncSession):
    """Makes changes to the hierarchy with event 'PRM:deleted'."""
    # log info start
    start_time = time.time()
    count_of_obj = len(msg["objects"])
    # log info end
    unique_tprm_ids_as_str = set()
    prm_data_grouped_by_mo_ids = defaultdict(dict)
    for item in msg["objects"]:
        unique_tprm_ids_as_str.add(str(item["tprm_id"]))
        item_tprm_id_as_str = str(item["tprm_id"])
        item_value = item["value"] if item["value"] != "None" else None
        item_mo_id = item["mo_id"]
        prm_data_grouped_by_mo_ids[item_mo_id][item_tprm_id_as_str] = item_value

    stmt = (
        select(Level)
        .where(Level.key_attrs.overlap(unique_tprm_ids_as_str))
        .order_by(
            Level.hierarchy_id.desc(),
            Level.object_type_id.desc(),
            Level.level.asc(),
        )
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    top_level_by_hierarchy_and_tmo = defaultdict(dict)

    for level in all_levels:
        level_in_cache = top_level_by_hierarchy_and_tmo.get(level.hierarchy_id)
        if not level_in_cache:
            top_level_by_hierarchy_and_tmo[level.hierarchy_id][
                level.object_type_id
            ] = level
        else:
            if not level_in_cache.get(level.object_type_id):
                top_level_by_hierarchy_and_tmo[level.hierarchy_id][
                    level.object_type_id
                ] = level

    for _, level_grouped_by_tmo in top_level_by_hierarchy_and_tmo.items():
        for tmo_id, level in level_grouped_by_tmo.items():
            stmt = select(NodeData).where(
                NodeData.level_id == level.id,
                NodeData.mo_id.in_(prm_data_grouped_by_mo_ids),
            )
            level_node_data = await session.execute(stmt)
            level_node_data = level_node_data.scalars().all()

            prm_ids_as_str_for_node_key = {
                key_attr for key_attr in level.key_attrs if key_attr.isdigit()
            }
            changed_node_datas = list()
            for node_data in level_node_data:
                dict_of_new_prm_data = prm_data_grouped_by_mo_ids.get(
                    node_data.mo_id
                )
                if dict_of_new_prm_data:
                    shared_keys = (
                        prm_ids_as_str_for_node_key
                        & dict_of_new_prm_data.keys()
                    )
                    dict_intersection = {
                        k: dict_of_new_prm_data[k] for k in shared_keys
                    }
                    if dict_intersection:
                        for k, v in dict_intersection.items():
                            node_data.unfolded_key[k] = None

                        changed_node_datas.append(node_data)
                        session.add(node_data)

            if changed_node_datas:
                await session.flush()
                print("With nodes rebuilding")
                await rebuilding_nodes_based_on_their_data(
                    node_data_with_new_parents=changed_node_datas,
                    session=session,
                    level=level,
                )
    await session.commit()
    # log info start
    end_time = time.time()
    print(f"Deleted {count_of_obj} PRMs in {end_time - start_time} seconds.")
    # log info end


async def on_update_prm(msg, session: AsyncSession):
    """Makes changes to the hierarchy with event 'PRM:updated'."""
    # log info start
    start_time = time.time()
    count_of_obj = len(msg["objects"])
    # log info end
    unique_tprm_ids_as_str = set()
    prm_data_grouped_by_mo_ids = defaultdict(dict)
    for item in msg["objects"]:
        unique_tprm_ids_as_str.add(str(item["tprm_id"]))
        item_tprm_id_as_str = str(item["tprm_id"])
        item_value = item["value"] if item["value"] != "None" else None
        item_mo_id = item["mo_id"]
        prm_data_grouped_by_mo_ids[item_mo_id][item_tprm_id_as_str] = item_value

    stmt = (
        select(Level)
        .where(Level.key_attrs.overlap(unique_tprm_ids_as_str))
        .order_by(
            Level.hierarchy_id.desc(),
            Level.object_type_id.desc(),
            Level.level.asc(),
        )
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    top_level_by_hierarchy_and_tmo = defaultdict(dict)

    for level in all_levels:
        level_in_cache = top_level_by_hierarchy_and_tmo.get(level.hierarchy_id)
        if not level_in_cache:
            top_level_by_hierarchy_and_tmo[level.hierarchy_id][
                level.object_type_id
            ] = level
        else:
            if not level_in_cache.get(level.object_type_id):
                top_level_by_hierarchy_and_tmo[level.hierarchy_id][
                    level.object_type_id
                ] = level

    for _, level_grouped_by_tmo in top_level_by_hierarchy_and_tmo.items():
        for tmo_id, level in level_grouped_by_tmo.items():
            stmt = select(NodeData).where(
                NodeData.level_id == level.id,
                NodeData.mo_id.in_(prm_data_grouped_by_mo_ids),
            )
            level_node_data = await session.execute(stmt)
            level_node_data = level_node_data.scalars().all()

            prm_ids_as_str_for_node_key = {
                key_attr for key_attr in level.key_attrs if key_attr.isdigit()
            }
            changed_node_datas = list()
            for node_data in level_node_data:
                dict_of_new_prm_data = prm_data_grouped_by_mo_ids.get(
                    node_data.mo_id
                )
                if dict_of_new_prm_data:
                    shared_keys = (
                        prm_ids_as_str_for_node_key
                        & dict_of_new_prm_data.keys()
                    )
                    dict_intersection = {
                        k: dict_of_new_prm_data[k] for k in shared_keys
                    }
                    if dict_intersection:
                        for k, v in dict_intersection.items():
                            node_data.unfolded_key[k] = v

                        changed_node_datas.append(node_data)
                        session.add(node_data)

            if changed_node_datas:
                await session.flush()
                print("With nodes rebuilding")
                await rebuilding_nodes_based_on_their_data(
                    node_data_with_new_parents=changed_node_datas,
                    session=session,
                    level=level,
                )
    await session.commit()
    # log info start
    end_time = time.time()
    print(f"Updated {count_of_obj} PRMs in {end_time - start_time} seconds.")
    # log info end


async def on_create_prm(msg, session: AsyncSession):
    """Makes changes to the hierarchy with event 'PRM:created'."""
    # log info start
    start_time = time.time()
    count_of_obj = len(msg["objects"])
    # log info end
    unique_tprm_ids_as_str = set()
    prm_data_grouped_by_mo_ids = defaultdict(dict)
    for item in msg["objects"]:
        unique_tprm_ids_as_str.add(str(item["tprm_id"]))
        item_tprm_id_as_str = str(item["tprm_id"])
        item_value = item["value"] if item["value"] != "None" else None
        item_mo_id = item["mo_id"]
        prm_data_grouped_by_mo_ids[item_mo_id][item_tprm_id_as_str] = item_value

    stmt = (
        select(Level)
        .where(Level.key_attrs.overlap(unique_tprm_ids_as_str))
        .order_by(
            Level.hierarchy_id.desc(),
            Level.object_type_id.desc(),
            Level.level.asc(),
        )
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    top_level_by_hierarchy_and_tmo = defaultdict(dict)
    print("all_levels")
    print(all_levels)
    for level in all_levels:
        level_in_cache = top_level_by_hierarchy_and_tmo.get(level.hierarchy_id)
        if not level_in_cache:
            top_level_by_hierarchy_and_tmo[level.hierarchy_id][
                level.object_type_id
            ] = level
        else:
            if not level_in_cache.get(level.object_type_id):
                top_level_by_hierarchy_and_tmo[level.hierarchy_id][
                    level.object_type_id
                ] = level

    for _, level_grouped_by_tmo in top_level_by_hierarchy_and_tmo.items():
        for tmo_id, level in level_grouped_by_tmo.items():
            stmt = select(NodeData).where(
                NodeData.level_id == level.id,
                NodeData.mo_id.in_(prm_data_grouped_by_mo_ids),
            )
            level_node_data = await session.execute(stmt)
            level_node_data = level_node_data.scalars().all()

            prm_ids_as_str_for_node_key = {
                key_attr for key_attr in level.key_attrs if key_attr.isdigit()
            }
            changed_node_datas = list()
            for node_data in level_node_data:
                dict_of_new_prm_data = prm_data_grouped_by_mo_ids.get(
                    node_data.mo_id
                )
                if dict_of_new_prm_data:
                    shared_keys = (
                        prm_ids_as_str_for_node_key
                        & dict_of_new_prm_data.keys()
                    )
                    dict_intersection = {
                        k: dict_of_new_prm_data[k] for k in shared_keys
                    }
                    if dict_intersection:
                        for k, v in dict_intersection.items():
                            node_data.unfolded_key[k] = v

                        changed_node_datas.append(node_data)
                        session.add(node_data)

            if changed_node_datas:
                await session.flush()
                print("With nodes rebuilding")
                print(level)
                print(changed_node_datas)
                await rebuilding_nodes_based_on_their_data(
                    node_data_with_new_parents=changed_node_datas,
                    session=session,
                    level=level,
                )
    await session.commit()
    # log info start
    end_time = time.time()
    print(f"Created {count_of_obj} PRMs in {end_time - start_time} seconds.")
    # log info end
