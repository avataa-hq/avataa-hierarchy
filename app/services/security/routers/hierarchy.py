from fastapi import APIRouter, Body, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession

from database import database
from schemas.hier_schemas import Hierarchy
from services.security.data.permissions.hierarchy import HierarchyPermission
from services.security.data.utils import PREFIX
from services.security.routers.models.request_models import (
    CreatePermission,
    CreatePermissions,
    UpdatePermission,
)
from services.security.routers.models.response_models import PermissionResponse
from services.security.routers.utils.functions import (
    create_permission,
    create_permissions,
    delete_object,
    delete_objects,
    get_all_permissions,
    get_permissions,
    update_permission,
)
from services.security.routers.utils.utils import transform

router = APIRouter(
    prefix=f"{PREFIX}/hierarchy", tags=["Permissions: Hierarchies"]
)

MAIN_TABLE = Hierarchy
PERMISSION_TABLE = HierarchyPermission


@router.get("/", response_model=list[PermissionResponse])
async def get_all_hierarchies_permissions(
    session: AsyncSession = Depends(database.get_session),
):
    raw_objects = await get_all_permissions(
        session=session, permission_table=PERMISSION_TABLE
    )
    return transform(raw_objects)


@router.get("/{hierarchy_id}", response_model=list[PermissionResponse])
async def get_hierarchy_permissions(
    hierarchy_id: int = Path(...),
    session: AsyncSession = Depends(database.get_session),
):
    raw_objects = await get_permissions(
        session=session,
        permission_table=PERMISSION_TABLE,
        parent_id=hierarchy_id,
    )
    return transform(raw_objects)


@router.post("/", status_code=201)
async def create_hierarchy_permission(
    item: CreatePermission,
    session: AsyncSession = Depends(database.get_session),
):
    return await create_permission(
        session=session,
        permission_table=PERMISSION_TABLE,
        item=item,
        main_table=MAIN_TABLE,
    )


@router.post("/multiple", status_code=201)
async def create_hierarchies_permissions(
    items: CreatePermissions,
    session: AsyncSession = Depends(database.get_session),
):
    return await create_permissions(
        session=session,
        permission_table=PERMISSION_TABLE,
        items=items,
        main_table=MAIN_TABLE,
    )


@router.patch("/{id}", status_code=204)
async def update_hierarchy_permission(
    id_: int = Path(..., alias="id"),
    item: UpdatePermission = Body(...),
    session: AsyncSession = Depends(database.get_session),
):
    return await update_permission(
        session=session,
        permission_table=PERMISSION_TABLE,
        item=item,
        item_id=id_,
        main_table=MAIN_TABLE,
    )


@router.delete("/multiple", status_code=204)
async def delete_hierarchies_permissions(
    id_: list[int] = Body(..., alias="ids", min_items=1),
    session: AsyncSession = Depends(database.get_session),
):
    return await delete_objects(
        session=session,
        permission_table=PERMISSION_TABLE,
        item_ids=id_,
        main_table=MAIN_TABLE,
    )


@router.delete("/{id}", status_code=204)
async def delete_hierarchy_permission(
    id_: int = Path(..., alias="id"),
    session: AsyncSession = Depends(database.get_session),
):
    return await delete_object(
        session=session,
        permission_table=PERMISSION_TABLE,
        item_id=id_,
        main_table=MAIN_TABLE,
    )
