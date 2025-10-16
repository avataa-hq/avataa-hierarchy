import math
from typing import List
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from common_utils.node_manipulator import NodeManipulator
from database import database
from grpc_config.inventory_utils import (
    check_if_tmo_has_lifecycle,
    get_max_severity_for_mo_ids_with_particular_tmo,
    get_tprms_data_by_tprms_ids,
)
from routers.hierarchy_object.utills.utils import (
    get_child_mo_ids_for_nodes_ids_consider_default_key,
    get_child_mo_ids_with_particular_object_type_for_nodes_ids,
    get_first_depth_child_levels,
    get_node_or_raise_error,
    get_nodes_by_node_ids,
    get_object_type_ids_of_all_child_levels,
)
from routers.utility_checks import check_hierarchy_exist

# from routers.utils import update_nodes_key_if_mo_link_or_prm_link
from schemas.hier_schemas import Hierarchy, Level, Obj, ObjResponseNew

router = APIRouter(prefix="/hierarchy_object", tags=["Node"])


@router.get("/count_children_with_lifecycle_and_max_severity", status_code=200)
async def get_count_children_with_lifecycle_and_max_severity_by_node_ids(
    node_ids: List[uuid.UUID] = Query(),
    session: AsyncSession = Depends(database.get_session),
):
    if not node_ids:
        raise HTTPException(
            status_code=422, detail="node_ids must contains at least one id"
        )

    nodes = await get_nodes_by_node_ids(node_ids, session)

    res = {}

    if not nodes:
        return res

    object_type_ids = await get_object_type_ids_of_all_child_levels(
        nodes, session
    )
    object_type_ids_with_lifecycle = await check_if_tmo_has_lifecycle(
        object_type_ids
    )

    # get nodes hierarchies
    hierarchies_ids = {node.hierarchy_id for node in nodes}
    stmt = select(Hierarchy).where(Hierarchy.id.in_(hierarchies_ids))
    hierarchies = await session.execute(stmt)
    hierarchies = hierarchies.scalars().all()

    hierarchies_data_cache = {h.id: h.create_empty_nodes for h in hierarchies}

    node_ids_uses_default_values_of_key = list()
    node_ids_not_uses_default_values_of_key = list()
    for node in nodes:
        create_empty_nodes = hierarchies_data_cache.get(node.hierarchy_id)
        if create_empty_nodes:
            node_ids_uses_default_values_of_key.append(node.id)
        else:
            node_ids_not_uses_default_values_of_key.append(node.id)

    if object_type_ids_with_lifecycle:
        data = dict()

        if node_ids_uses_default_values_of_key:
            data_with_default_keys = await get_child_mo_ids_with_particular_object_type_for_nodes_ids(
                node_ids=node_ids_uses_default_values_of_key,
                allowed_object_type_ids=object_type_ids_with_lifecycle,
                session=session,
                consider_nodes_with_default_key=True,
            )
            data.update(data_with_default_keys)

        if node_ids_not_uses_default_values_of_key:
            data_without_default_keys = await get_child_mo_ids_with_particular_object_type_for_nodes_ids(
                node_ids=node_ids_not_uses_default_values_of_key,
                allowed_object_type_ids=object_type_ids_with_lifecycle,
                session=session,
                consider_nodes_with_default_key=False,
            )
            data.update(data_without_default_keys)

        # append id of parent node into results if parent node is real
        for obj in nodes:
            if (
                obj.object_type_id in object_type_ids_with_lifecycle
                and obj.object_id
            ):
                inner_object_type = data[obj.id].get(obj.object_type_id)
                if inner_object_type:
                    inner_object_type.append(obj.object_id)
                else:
                    data[obj.id][obj.object_type_id] = [obj.object_id]

        for node_id, node_data in data.items():
            severities = []
            count = 0
            for tmo_id, mo_ids in node_data.items():
                count += len(mo_ids)
                severities.append(
                    await get_max_severity_for_mo_ids_with_particular_tmo(
                        tmo_id, mo_ids
                    )
                )
            res[node_id] = {"count": count, "severity": max(severities)}

    return res


@router.get("/children_mo_ids_of_particular_nodes", status_code=200)
async def get_children_mo_ids_of_particular_nodes(
    node_ids: List[uuid.UUID] = Query(),
    session: AsyncSession = Depends(database.get_session),
):
    if not node_ids:
        raise HTTPException(
            status_code=422, detail="node_ids must contains at least one id"
        )

    nodes = await get_nodes_by_node_ids(node_ids, session)

    res = {}

    if not nodes:
        return res
    nodes_hierarchy_ids = {node.hierarchy_id for node in nodes}
    stmt = select(Hierarchy).where(Hierarchy.id.in_(nodes_hierarchy_ids))
    hierarchies = await session.execute(stmt)
    hierarchies = hierarchies.scalars().all()

    hierarchies_data = {h.id: h.create_empty_nodes for h in hierarchies}

    nodes_uses_default_key_value = []
    nodes_not_uses_default_key_value = []
    for node in nodes:
        h_uses_key_default_values = hierarchies_data.get(node.hierarchy_id)
        if h_uses_key_default_values:
            nodes_uses_default_key_value.append(node.id)
        else:
            nodes_not_uses_default_key_value.append(node.id)

    if nodes_uses_default_key_value:
        data = await get_child_mo_ids_for_nodes_ids_consider_default_key(
            node_ids=nodes_uses_default_key_value,
            session=session,
            consider_nodes_with_default_key=True,
        )
        res = data

    if nodes_not_uses_default_key_value:
        data = await get_child_mo_ids_for_nodes_ids_consider_default_key(
            node_ids=nodes_not_uses_default_key_value,
            session=session,
            consider_nodes_with_default_key=False,
        )
        res.update(data)

    # append id of parent node into results if parent node is real
    [res[obj.id].append(obj.object_id) for obj in nodes if obj.object_id]

    return res


@router.get("/{object_id}/breadcrumbs", response_model=list[ObjResponseNew])
async def get_hierarchy_object_breadcrumbs(
    object_id: uuid.UUID, session: AsyncSession = Depends(database.get_session)
):
    stmt = select(Obj).where(Obj.id == object_id)
    object_from_db = await session.execute(stmt)
    object_from_db = object_from_db.scalars().first()

    node = NodeManipulator(object_from_db, session)
    nodes = await node.get_full_chain_of_parent_nodes()
    # TODO Change to level key attrs
    # nodes = await update_nodes_key_if_mo_link_or_prm_link(nodes, session)
    return nodes


@router.get(
    "/{object_id}/mo_ids_of_first_real_children_nodes", response_model=list[int]
)
async def get_mo_ids_of_first_real_children_nodes(
    object_id: uuid.UUID, session: AsyncSession = Depends(database.get_session)
):
    node = await get_node_or_raise_error(node_id=object_id, session=session)

    hierarchy_exist = await check_hierarchy_exist(node.hierarchy_id, session)

    child_levels = await get_first_depth_child_levels(
        parent_level_ids=[node.level_id], session=session
    )

    order = [dict(node_ids=[node.id], levels=child_levels)]

    stmt = select(Level).where(Level.hierarchy_id == hierarchy_exist.id)
    hierarchy_level_param_type_ids = await session.execute(stmt)
    hierarchy_level_param_type_ids = (
        hierarchy_level_param_type_ids.scalars().all()
    )

    levels_param_type_ids_dict = {
        level.id: level.param_type_id
        for level in hierarchy_level_param_type_ids
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

    res = []
    for interation in order:
        mo_link_levels = [
            level.id
            for level in interation["levels"]
            if level.id in level_ids_with_mo_link
        ]
        real_levels = [
            level.id for level in interation["levels"] if not level.is_virtual
        ]

        if real_levels:
            if hierarchy_exist.create_empty_nodes:
                stmt = (
                    select(Obj.object_id)
                    .where(
                        Obj.parent_id.in_(interation["node_ids"]),
                        Obj.level_id.in_(real_levels),
                    )
                    .distinct()
                )
            else:
                stmt = (
                    select(Obj.object_id)
                    .where(
                        Obj.parent_id.in_(interation["node_ids"]),
                        Obj.key != DEFAULT_KEY_OF_NULL_NODE,
                        Obj.level_id.in_(real_levels),
                    )
                    .distinct()
                )
            res = await session.execute(stmt)
            res = res.scalars().all()
            return res

        elif mo_link_levels:
            stmt = select(Obj.key).where(Obj.level_id.in_(mo_link_levels))
            res = await session.execute(stmt)
            res = res.scalars().all()
            res = [int(item) for item in res if item.isdigit()]
            return res

        else:
            if hierarchy_exist.create_empty_nodes:
                stmt = select(Obj.id).where(
                    Obj.parent_id.in_(interation["node_ids"])
                )
            else:
                stmt = select(Obj.id).where(
                    Obj.parent_id.in_(interation["node_ids"]),
                    Obj.key != DEFAULT_KEY_OF_NULL_NODE,
                )
            # get node_ids
            node_ids = await session.execute(stmt)
            node_ids = node_ids.scalars().all()

            parent_level_ids = [level.id for level in interation["levels"]]
            stmt = select(Level).where(Level.parent_id.in_(parent_level_ids))
            child_levels = await session.execute(stmt)
            child_levels = child_levels.scalars().all()

            if node_ids and child_levels:
                step_count = 30000
                if len(node_ids) >= step_count:
                    steps = math.ceil(len(node_ids) / step_count)
                    for step in range(steps):
                        start = step * step_count
                        end = start + step_count
                        order.append(
                            dict(
                                node_ids=node_ids[start:end],
                                levels=child_levels,
                            )
                        )
                else:
                    order.append(dict(node_ids=node_ids, levels=child_levels))

    return res
