import math
from typing import List
import uuid

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from schemas.hier_schemas import Level, Obj


async def get_node_or_raise_error(
    node_id: uuid.UUID, session: AsyncSession
) -> Obj:
    """Returns node or raise error."""
    stmt = select(Obj).where(Obj.id == node_id)
    res = await session.execute(stmt)
    res = res.scalars().first()

    if not res:
        raise HTTPException(
            status_code=404, detail=f"Node with id = {node_id} does not exist"
        )
    return res


async def get_first_depth_child_levels(
    parent_level_ids: List[int], session: AsyncSession
) -> List[Level]:
    """Returns list of first depth child levels"""
    stmt = select(Level).where(Level.parent_id.in_(parent_level_ids))
    res = await session.execute(stmt)
    res = res.scalars().all()

    return res


async def get_nodes_by_node_ids(node_ids: List, session: AsyncSession):
    """Returns list of Obj instances"""
    stmt = select(Obj).where(Obj.id.in_(node_ids))
    res = await session.execute(stmt)
    res = res.scalars().all()
    return res


async def get_object_type_ids_of_all_child_levels(
    nodes: List[Obj], session: AsyncSession
):
    """Returns all object_types ids of all child level for particular nodes"""

    real_levels_object_types = []
    levels_ids_to_search = []

    level_ids = [node.level_id for node in nodes]
    stmt = select(Level).where(Level.id.in_(level_ids))
    first_levels = await session.execute(stmt)
    first_levels = first_levels.scalars().all()

    if not first_levels:
        return real_levels_object_types

    real_levels_object_types += [
        level.object_type_id for level in first_levels if not level.is_virtual
    ]

    levels_ids_to_search.append([level.id for level in first_levels])

    for levels_ids in levels_ids_to_search:
        stmt = select(Level).where(Level.parent_id.in_(levels_ids))
        levels = await session.execute(stmt)
        levels = levels.scalars().all()

        if levels:
            levels_ids_to_search.append([level.id for level in levels])
            real_levels_object_types += [
                level.object_type_id for level in levels if not level.is_virtual
            ]

    return real_levels_object_types


async def get_child_mo_ids_with_particular_object_type_for_nodes_ids(
    node_ids: List,
    allowed_object_type_ids: List[int],
    session: AsyncSession,
    consider_nodes_with_default_key: bool = True,
):
    """Returns dict with node.id as key and list of children object_id as value if children.object_type
    in allowed_object_type_ids"""
    if not node_ids:
        return {}

    where_condition = []
    if allowed_object_type_ids:
        where_condition.append(Obj.object_type_id.in_(allowed_object_type_ids))

    dict_result = {x: dict() for x in node_ids}

    dict_order = [{x: [x]} for x in node_ids]

    for dict_in_order in dict_order:
        main_node_id = list(dict_in_order.keys())[0]
        node_ids_from_order = dict_in_order[main_node_id]

        stmt = select(Obj.id).where(Obj.parent_id.in_(node_ids_from_order))

        if not consider_nodes_with_default_key:
            stmt = stmt.where(Obj.key != DEFAULT_KEY_OF_NULL_NODE)

        res = await session.execute(stmt)
        res = res.scalars().all()

        if res:
            stmt = select(Obj.object_id, Obj.object_type_id).where(
                Obj.parent_id.in_(node_ids_from_order),
                Obj.object_id != None,  # noqa
                *where_condition,
            )

            if not consider_nodes_with_default_key:
                stmt = stmt.where(Obj.key != DEFAULT_KEY_OF_NULL_NODE)

            real_obj_ids = await session.execute(stmt)
            real_obj_ids = real_obj_ids.all()

            res_sep_by_object_type_id = dict.fromkeys(
                allowed_object_type_ids, list()
            )
            [
                res_sep_by_object_type_id[node.object_type_id].append(
                    node.object_id
                )
                for node in real_obj_ids
            ]
            if real_obj_ids:
                dict_result[main_node_id].update(res_sep_by_object_type_id)

            max_count = 30000
            slices = 1
            if len(res) > max_count:
                slices = math.ceil(len(res) / max_count)

            for slice_iter in range(slices):
                start = max_count * slice_iter
                end = start + max_count
                dict_order.append({main_node_id: res[start:end]})

    return dict_result


async def get_child_mo_ids_for_nodes_ids(node_ids: List, session: AsyncSession):
    """Returns dict with node.id as key and list of children object_id as value"""
    if not node_ids:
        return {}
    dict_result = {x: list() for x in node_ids}
    dict_order = [{x: [x]} for x in node_ids]
    for dict_in_order in dict_order:
        main_node_id = list(dict_in_order.keys())[0]
        node_ids_from_order = dict_in_order[main_node_id]

        stmt = select(Obj.id).where(Obj.parent_id.in_(node_ids_from_order))
        res = await session.execute(stmt)
        res = res.scalars().all()
        if res:
            stmt = (
                select(Obj.object_id)
                .where(
                    Obj.parent_id.in_(node_ids_from_order),
                    Obj.object_id != None,  # noqa: E711
                )
                .distinct()
            )

            real_obj_ids = await session.execute(stmt)
            real_obj_ids = real_obj_ids.scalars().all()
            if real_obj_ids:
                dict_result[main_node_id].extend(real_obj_ids)

            max_count = 30000
            slices = 1
            if len(res) > max_count:
                slices = math.ceil(len(res) / max_count)

            for slice_iter in range(slices):
                start = max_count * slice_iter
                end = start + max_count
                dict_order.append({main_node_id: res[start:end]})
    return dict_result


async def get_child_mo_ids_for_nodes_ids_consider_default_key(
    node_ids: List[uuid.UUID],
    session: AsyncSession,
    consider_nodes_with_default_key: bool = True,
):
    """Returns dict with node.id as key and list of children object_id as value. If consider_nodes_with_default_key is
    False searches for nodes whose key is not equal to DEFAULT_KEY_OF_NULL_NODE"""
    if not node_ids:
        return {}
    dict_result = {x: list() for x in node_ids}
    dict_order = [{x: [x]} for x in node_ids]
    for dict_in_order in dict_order:
        main_node_id = list(dict_in_order.keys())[0]
        node_ids_from_order = dict_in_order[main_node_id]

        if consider_nodes_with_default_key:
            stmt = select(Obj.id).where(Obj.parent_id.in_(node_ids_from_order))
        else:
            stmt = select(Obj.id).where(
                Obj.parent_id.in_(node_ids_from_order),
                Obj.key != DEFAULT_KEY_OF_NULL_NODE,
            )

        res = await session.execute(stmt)
        res = res.scalars().all()
        if res:
            if consider_nodes_with_default_key:
                stmt = (
                    select(Obj.object_id)
                    .where(
                        Obj.parent_id.in_(node_ids_from_order),
                        Obj.object_id != None,  # noqa: E711
                    )
                    .distinct()
                )
            else:
                stmt = (
                    select(Obj.object_id)
                    .where(
                        Obj.parent_id.in_(node_ids_from_order),
                        Obj.key != DEFAULT_KEY_OF_NULL_NODE,
                        Obj.object_id != None,  # noqa: E711
                    )
                    .distinct()
                )

            real_obj_ids = await session.execute(stmt)
            real_obj_ids = real_obj_ids.scalars().all()
            if real_obj_ids:
                dict_result[main_node_id].extend(real_obj_ids)

            max_count = 30000
            slices = 1
            if len(res) > max_count:
                slices = math.ceil(len(res) / max_count)

            for slice_iter in range(slices):
                start = max_count * slice_iter
                end = start + max_count
                dict_order.append({main_node_id: res[start:end]})
    return dict_result


async def get_real_and_virtual_deepest_levels(levels: List[Level]) -> dict:
    """Returns dict with virtual and real Levels which has max Level.level"""
    max_level = max(level.level for level in levels)
    virtual = []
    real = []
    [
        virtual.append(level) if level.is_virtual else real.append(level)
        for level in levels
        if level.level == max_level
    ]
    return dict(virtual_levels=virtual, real_levels=real, deep=max_level)


async def get_count_of_children_nodes_with_not_default_key(
    p_ids: List[uuid.UUID], session: AsyncSession
) -> dict:
    """Returns a dict with the parent_id as the key and the number of child nodes with an unequal key attribute to .
    DEFAULT_KEY_OF_NULL_NODE as value"""
    children_cache = list()
    count_per_step = 30000

    steps = math.ceil(len(p_ids) / count_per_step)
    for step in range(steps):
        start = step * count_per_step
        end = start + count_per_step
        step_ids = p_ids[start:end]
        stmt = (
            select(Obj.parent_id, func.count(Obj.id).label("count"))
            .where(
                Obj.parent_id.in_(step_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE
            )
            .group_by(Obj.parent_id)
        )
        children_count = await session.execute(stmt)
        children_count = children_count.all()
        children_cache.append(children_count)

    children_count = {item.parent_id: item.count for item in children_count}
    return children_count


async def get_object_ids_of_real_nodes_with_existing_key(
    object_ids: List[int], session: AsyncSession
) -> set:
    """Returns a set of real nodes object_ids whose key attribute is not equal to DEFAULT_KEY_OF_NULL_NODE."""
    res = set()
    count_per_step = 30000

    steps = math.ceil(len(object_ids) / count_per_step)
    for step in range(steps):
        start = step * count_per_step
        end = start + count_per_step
        step_ids = object_ids[start:end]
        stmt = select(Obj.object_id).where(
            Obj.object_id.in_(step_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE
        )
        obj_ids = await session.execute(stmt)
        obj_ids = obj_ids.scalars().all()
        res.update(obj_ids)
    return res
