import datetime
import http
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import Response

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from database import database
from grpc_config.inventory_utils import (
    check_if_tmo_has_lifecycle,
    get_max_severity_for_mo_ids_with_particular_tmo,
)
from routers.utility_checks import (
    check_hierarchy_exist,
    check_hierarchy_exist_with_lock,
    check_hierarchy_name_exist,
)
from routers.utils import get_hierarchy_ids_for_tmos_with_lifecycle
from schemas.hier_schemas import Hierarchy, HierarchyCreate, Level, Obj
from services.hierarchy.common.delete.delete_handler import (
    HierarchyDeleteHandler,
)

router = APIRouter()


@router.get(
    "/hierarchy",
    response_model=list[Hierarchy],
    tags=["Create/change hierarchy methods"],
)
async def get_hierarchies(
    with_lifecycle: bool = False,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Returns the hierarchical structure of the object as a list of objects.  <br>
    Use when *creating/deleting/changing* a structure.
    """
    if with_lifecycle:
        return await get_hierarchy_ids_for_tmos_with_lifecycle(session=session)
    hierarchy = await session.execute(select(Hierarchy))
    hierarchy = hierarchy.scalars().all()
    if not hierarchy:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND, detail="empty hierarchy"
        )
    return hierarchy


@router.get(
    "/hierarchy/{hierarchy_id}",
    response_model=Hierarchy,
    tags=["Create/change hierarchy methods"],
    deprecated=True,
)
async def get_hierarchy(
    hierarchy_id: int, session: AsyncSession = Depends(database.get_session)
):
    """
    Returns the hierarchical structure of the object.  <br>
    Use when *creating/deleting/changing* a structure.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**
    """
    hierarchy = await check_hierarchy_exist(
        hierarchy_id=hierarchy_id, session=session
    )
    return hierarchy


@router.post(
    "/hierarchy",
    response_model=Hierarchy,
    status_code=http.HTTPStatus.CREATED,
    tags=["Create/change hierarchy methods"],
)
async def add_hierarchy_node(
    hierarchy: HierarchyCreate,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Adds a new element to the hierarchy.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**  <br>
    The next fields are optional:  <br>
    - description
    """
    # checks
    await check_hierarchy_name_exist(
        hierarchy_name=hierarchy.name, session=session
    )
    # data
    created = datetime.datetime.now()
    hierarchy = Hierarchy(
        **hierarchy.model_dump(), modified=created, created=created
    )
    # insert and refresh
    try:
        session.add(hierarchy)
        await session.flush()
        await session.refresh(hierarchy)
        await session.commit()
    except Exception:
        raise HTTPException(
            status_code=http.HTTPStatus.UNPROCESSABLE_ENTITY,
            detail="Check the hierarchy name.",
        )
    return hierarchy


@router.put(
    "/hierarchy/{hierarchy_id}",
    response_model=Hierarchy,
    tags=["Create/change hierarchy methods"],
)
async def change_hierarchy_node(
    hierarchy_id: int,
    hierarchy: HierarchyCreate,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Modifies an **existing** member of a hierarchy.  <br>
    The **name** field must be unique, **hierarchy_id** must exist in the database.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**  <br>
    The next fields are optional:
    - description  <br>
    """
    # checks
    hierarchy_exist = await check_hierarchy_exist_with_lock(
        hierarchy_id=hierarchy_id, session=session
    )
    if hierarchy_exist.name != hierarchy.name:
        await check_hierarchy_name_exist(
            hierarchy_name=hierarchy.name, session=session
        )
    # data
    update_data = hierarchy.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(hierarchy_exist, field, value)
    hierarchy_exist.modified = datetime.datetime.now()
    # update
    await session.flush()
    await session.refresh(hierarchy_exist)
    await session.commit()
    return hierarchy_exist


@router.delete(
    "/hierarchy/{hierarchy_id}",
    status_code=http.HTTPStatus.NO_CONTENT,
    response_class=Response,
    tags=["Create/change hierarchy methods"],
)
async def delete_hierarchy_node(
    hierarchy_id: int, session: AsyncSession = Depends(database.get_session)
):
    """
    Removes an **existing** member of a hierarchy.  <br>
    **hierarchy_id** must exist in the database.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**
    """
    hierarchy = await check_hierarchy_exist_with_lock(
        hierarchy_id=hierarchy_id, session=session
    )
    hierarchy_delete_handler = HierarchyDeleteHandler(
        session=session, hierarchies_to_delete=[hierarchy]
    )
    await hierarchy_delete_handler.delete_and_commit()


@router.get(
    "/hierarchy-info/count_children_with_lifecycle_and_max_severity",
    status_code=200,
    tags=["Hierarchy-info"],
)
async def get_count_children_with_lifecycle_and_max_severity_by_hierarchy_ids(
    hierarchy_ids: List[int] = Query(),
    session: AsyncSession = Depends(database.get_session),
):
    if not hierarchy_ids:
        raise HTTPException(
            status_code=422,
            detail="hierarchy_ids must contains at least one id",
        )

    # get hierarchies data
    stmt = select(Hierarchy).where(Hierarchy.id.in_(hierarchy_ids))
    hierarchies = await session.execute(stmt)
    hierarchies = hierarchies.scalars().all()
    hierarchies_data_cache = {h.id: h.create_empty_nodes for h in hierarchies}

    level_where_condition = [
        Level.hierarchy_id == hierarchy_id for hierarchy_id in hierarchy_ids
    ]
    stmt = (
        select(Level.object_type_id, Level.hierarchy_id)
        .where(
            Level.is_virtual == False,  # noqa
            or_(*level_where_condition),
        )
        .distinct()
    )
    level_data = await session.execute(stmt)
    level_data = level_data.all()

    h_levels = dict().fromkeys(hierarchy_ids, list())
    [
        h_levels[level.hierarchy_id].append(level.object_type_id)
        for level in level_data
    ]
    h_levels = {
        hierarchy_id: set(hierarchy_levels)
        for hierarchy_id, hierarchy_levels in h_levels.items()
    }

    object_type_ids_with_lifecycle = set()
    if level_data:
        object_type_ids = [level.object_type_id for level in level_data]
        object_type_ids_with_lifecycle = set(
            await check_if_tmo_has_lifecycle(object_type_ids)
        )

    h_levels = {
        hierarchy_id: object_type_ids_with_lifecycle.intersection(
            hierarchy_levels
        )
        for hierarchy_id, hierarchy_levels in h_levels.items()
    }

    res = dict()
    if h_levels:
        for hierarchy_id, object_type_ids in h_levels.items():
            severities = [0]
            count = 0

            for object_type_id in object_type_ids:
                stmt = select(Obj.object_id).where(
                    Obj.hierarchy_id == hierarchy_id,
                    Obj.object_type_id == object_type_id,
                    Obj.object_id != None,  # noqa: E711
                )
                if not hierarchies_data_cache.get(hierarchy_id):
                    stmt = stmt.where(Obj.key != DEFAULT_KEY_OF_NULL_NODE)
                object_ids_with_lifecycle = await session.execute(stmt)
                object_ids_with_lifecycle = (
                    object_ids_with_lifecycle.scalars().all()
                )
                if object_ids_with_lifecycle:
                    count += len(object_ids_with_lifecycle)
                    severities.append(
                        await get_max_severity_for_mo_ids_with_particular_tmo(
                            object_type_id, object_ids_with_lifecycle
                        )
                    )

            res[hierarchy_id] = {"count": count, "severity": max(severities)}

    return res


@router.get(
    "/hierarchy-info/children_mo_ids_of_particular_hierarchy",
    status_code=200,
    tags=["Hierarchy-info"],
)
async def get_children_mo_ids_of_particular_hierarchy(
    hierarchy_ids: List[int] = Query(),
    session: AsyncSession = Depends(database.get_session),
):
    if not hierarchy_ids:
        raise HTTPException(
            status_code=422,
            detail="hierarchy_ids must contains at least one id",
        )

    res = dict()

    # get_hierarhies
    stmt = select(Hierarchy).where(Hierarchy.id.in_(hierarchy_ids))
    hierarchies = await session.execute(stmt)
    hierarchies = hierarchies.scalars().all()

    for hierarchy in hierarchies:
        stmt = (
            select(Obj.object_id)
            .where(Obj.hierarchy_id == hierarchy.id, Obj.object_id != None)  # noqa: E711
            .distinct()
        )  # noqa
        if not hierarchy.create_empty_nodes:
            stmt = stmt.where(Obj.key != DEFAULT_KEY_OF_NULL_NODE)
        data = await session.execute(stmt)
        res[hierarchy.id] = data.scalars().all()

    return res
