from collections import defaultdict

from sqlalchemy import delete, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from kafka_config.msg.msg_utils import (
    recalculate_child_count_of_nodes_for_specific_level,
)
from kafka_config.msg.utils import (
    refresh_child_count_for_parent_obj_with_level_delete,
)
from schemas.hier_schemas import Level, Obj


async def on_delete_tprm(msg, session: AsyncSession):
    """Deletes level if level.param_type_id == TPRM.id. Cleans up the levels attrs ( additional_params_id,
    latitude_id, longitude_id) if they are eq to deleted TPRM.id and does not rebuild hierarchy"""
    ids = set()
    ids_as_str = set()

    for item in msg["objects"]:
        ids.add(item["id"])
        ids_as_str.add(str(item["id"]))

    # get all levels where deleted parameter id in key_attrs
    stmt = select(Level).where(Level.key_attrs.overlap(ids_as_str))
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    levels_to_delete_ids = set()
    levels_to_rebuild_ids = set()
    parent_ids_of_deleted_levels_to_recalc = set()

    for level in all_levels:
        level.key_attrs = [
            attr for attr in level.key_attrs if attr not in ids_as_str
        ]

        if level.key_attrs:
            session.add(level)
            levels_to_rebuild_ids.add(level.id)
        else:
            levels_to_delete_ids.add(level.id)

            if level.parent_id:
                parent_ids_of_deleted_levels_to_recalc.add(level.parent_id)

    # delete levels to levels_to_delete

    stmt = delete(Level).where(Level.id.in_(levels_to_delete_ids))
    await session.execute(stmt)
    await session.flush()

    # get all levels to rebuild
    stmt = (
        select(Level)
        .where(Level.id.in_(levels_to_rebuild_ids))
        .order_by(
            Level.hierarchy_id.desc(),
            Level.object_type_id.desc(),
            Level.level.asc(),
        )
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    top_level_by_hierarchy_and_tmo_to_rebuild = defaultdict(dict)

    for level in all_levels:
        level_in_cache = top_level_by_hierarchy_and_tmo_to_rebuild.get(
            level.hierarchy_id
        )
        if not level_in_cache:
            top_level_by_hierarchy_and_tmo_to_rebuild[level.hierarchy_id][
                level.object_type_id
            ] = level
        else:
            if not level_in_cache.get(level.object_type_id):
                top_level_by_hierarchy_and_tmo_to_rebuild[level.hierarchy_id][
                    level.object_type_id
                ] = level

        if level.id in parent_ids_of_deleted_levels_to_recalc:
            parent_ids_of_deleted_levels_to_recalc.remove(level.id)

    # rebuild nodes for levels

    for (
        h_id,
        grouped_by_tmo_levels,
    ) in top_level_by_hierarchy_and_tmo_to_rebuild.items():
        for tmo_id, level in grouped_by_tmo_levels.items():
            await refresh_child_count_for_parent_obj_with_level_delete(
                level, session
            )
            await session.flush()

    # recalculate node children for levels
    for leve_id in parent_ids_of_deleted_levels_to_recalc:
        await recalculate_child_count_of_nodes_for_specific_level(
            session=session, level_id=leve_id
        )

    # Update all Levels where Level.additional_params_id == TMO.id
    # Update Obj.additional_params of this levels

    stm_check = select(Level).where(Level.additional_params_id.in_(ids))
    res = await session.execute(stm_check)
    res = res.scalars().all()

    if len(res) > 0:
        stm = (
            update(Level)
            .where(Level.additional_params_id.in_(ids))
            .values(additional_params_id=None)
        )
        await session.execute(stm)
        await session.flush()

        level_ids = [level.id for level in res]

        stm = (
            update(Obj)
            .where(Obj.level_id.in_(level_ids))
            .values(additional_params=None)
        )
        await session.execute(stm)
        await session.flush()

    # Update all Levels where Level.latitude_id == TMO.id
    # Update Obj.latitude of this levels
    stm_check = select(Level).where(Level.latitude_id.in_(ids))
    res = await session.execute(stm_check)
    res = res.scalars().all()

    if len(res) > 0:
        stm = (
            update(Level)
            .where(Level.latitude_id.in_(ids))
            .values(latitude_id=None)
        )
        await session.execute(stm)
        await session.flush()

        level_ids = [level.id for level in res]

        stm = (
            update(Obj).where(Obj.level_id.in_(level_ids)).values(latitude=None)
        )
        await session.execute(stm)
        await session.flush()

    # Update all Levels where Level.longitude_id == TMO.id
    # Update Obj.longitude of this levels
    stm_check = select(Level).where(Level.longitude_id.in_(ids))
    res = await session.execute(stm_check)
    res = res.scalars().all()

    if len(res) > 0:
        stm = (
            update(Level)
            .where(Level.longitude_id.in_(ids))
            .values(longitude_id=None)
        )
        await session.execute(stm)
        await session.flush()

        level_ids = [level.id for level in res]

        stm = (
            update(Obj)
            .where(Obj.level_id.in_(level_ids))
            .values(longitude=None)
        )
        await session.execute(stm)
        await session.flush()

    await session.commit()
