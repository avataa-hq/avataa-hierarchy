import sys
from typing import Type, TypeVar

from fastapi import HTTPException
from sqlalchemy import select, true
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from services.security.data.permission import db_admins
from services.security.data.permissions.permission_template import (
    PermissionTemplate,
)
from services.security.data.utils import get_user_permissions, role_prefix
from services.security.routers.models.request_models import (
    CreatePermission,
    CreatePermissions,
    UpdatePermission,
)
from services.security.security_data_models import UserData

T = TypeVar("T")


def get_permission_name(raw_permission_name: str):
    prefix = f"realm_access.{role_prefix}"
    permission_name = raw_permission_name.replace(prefix, "", 1)
    if permission_name.startswith(role_prefix):
        permission_name = permission_name.replace(role_prefix, "", 1)
    return permission_name


async def get_all_permissions(
    session: AsyncSession, permission_table: Type[T]
) -> list[T]:
    query = select(permission_table)
    permissions = await session.execute(query)
    permissions = permissions.scalars().all()

    return permissions


async def _check_object_exists(session: AsyncSession, main_table, item_id):
    item = await session.get(main_table, item_id)
    if not item:
        raise HTTPException(
            status_code=422,
            detail="Object with this ID not found or not available",
        )


def _get_user_permissions(jwt: UserData | None):
    if not jwt:
        raise HTTPException(status_code=403, detail="Access denied")
    user_permissions = get_user_permissions(jwt)
    user_permissions.append("default")
    return user_permissions


def _get_query_available_objects(
    permission_table: Type[T],
    user_permissions: list[str],
    must_be_admin: bool = True,
):
    if not user_permissions:
        raise HTTPException(
            status_code=403, detail="The user does not have access"
        )
    query = select(permission_table.parent_id)
    if not db_admins.intersection(user_permissions):
        query = query.where(permission_table.permission.in_(user_permissions))
        if must_be_admin:
            query = query.where(permission_table.admin == true())
    return query


async def get_permissions(
    session: AsyncSession, permission_table: Type[T], parent_id: int
) -> list[T]:
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table,
        user_permissions=user_permissions,
        must_be_admin=False,
    )
    query = select(permission_table).where(
        permission_table.parent_id == parent_id,
        permission_table.parent_id.in_(subquery),
    )
    permissions = await session.execute(query)
    permissions = permissions.scalars().all()
    # if not permissions:
    #     raise HTTPException(status_code=404, detail='Not found or access denied')
    return permissions


async def create_permission(
    session: AsyncSession,
    permission_table: Type[T],
    item: CreatePermission,
    main_table,
):
    # check
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    is_admin = len(db_admins.intersection(user_permissions)) > 0
    if not is_admin and item.permission not in user_permissions:
        raise HTTPException(
            status_code=404,
            detail="You can only assign roles from the list of roles available to you",
        )

    query = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    ).where(permission_table.parent_id == item.parent_id)
    available_objects = await session.execute(query)
    available_objects = available_objects.scalars().all()
    if not available_objects and not is_admin:
        raise HTTPException(
            status_code=404, detail="Parent element not found or access denied"
        )

    await _check_object_exists(session, main_table, item.parent_id)

    permission_name = get_permission_name(item.permission)

    # add
    db_item = permission_table(**item.dict(), permission_name=permission_name)
    session.add(db_item)
    print(db_item.__dict__)
    try:
        await session.flush()
        await session.refresh(db_item)
        item_id = db_item.id

        await session.commit()
    except IntegrityError as e:
        print(e, file=sys.stderr)
        error_msgs = {
            "23503": "Object with this ID not found or not available",  # ForeignKeyViolation
            "23505": "An entry already exists for the given permission and object.",  # 'UniqueViolation'
        }
        default_msg = "An unexpected error occurred in the database. Please notify the system administrator"
        error_msg = error_msgs.get(e.orig.pgcode, default_msg)
        raise HTTPException(status_code=422, detail=error_msg)

    return item_id


async def delete_object(
    session: AsyncSession, permission_table: Type[T], item_id: int, main_table
):
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    )
    query = select(permission_table).where(
        permission_table.id == item_id, permission_table.parent_id.in_(subquery)
    )
    db_item = await session.execute(query)
    db_item = db_item.scalar_one_or_none()
    if not db_item:
        raise HTTPException(
            status_code=404, detail="Element not found or access denied"
        )
    if db_item.root_permission_id:
        raise HTTPException(
            status_code=422,
            detail=f"For editing, use the main element of the rule with ID {db_item.root_permission_id}",
        )

    await session.delete(db_item)
    await session.commit()


async def create_permissions(
    session: AsyncSession,
    permission_table: Type[T],
    items: CreatePermissions,
    main_table,
):
    ids = []
    try:
        item_main_data = items.dict(exclude={"permission"})
        for permission in items.permission:
            item_main_data["permission"] = permission
            item = CreatePermission(**item_main_data)
            item_id = await create_permission(
                session=session,
                permission_table=permission_table,
                item=item,
                main_table=main_table,
            )
            ids.append(item_id)
    except HTTPException as e:
        print(e, file=sys.stderr)
        for item_id in ids:
            await delete_object(
                session=session,
                permission_table=permission_table,
                item_id=item_id,
                main_table=main_table,
            )
        raise e
    return ids


async def delete_objects(
    session: AsyncSession,
    permission_table: Type[T],
    item_ids: list[int],
    main_table,
):
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    )
    query = select(permission_table).where(
        permission_table.id.in_(item_ids),
        permission_table.parent_id.in_(subquery),
    )
    db_items = await session.execute(query)
    db_items = db_items.scalars().all()
    if len(db_items) != len(item_ids):
        raise HTTPException(
            status_code=404, detail="Elements not found or access denied"
        )
    # do not combine the next 2 cycles into one. Since this will affect the consistency of the removal
    for db_item in db_items:
        if db_item.root_permission_id:
            raise HTTPException(
                status_code=422,
                detail="For editing, use the main element of the rule",
            )
    for item_id in item_ids:
        await delete_object(
            session=session,
            permission_table=permission_table,
            item_id=item_id,
            main_table=main_table,
        )


async def update_permission(
    session: AsyncSession,
    permission_table: Type[T],
    item: UpdatePermission,
    item_id: int,
    main_table,
):
    if len(item.get_actions()) == 0:
        raise HTTPException(status_code=422, detail="No field changed")
    user_permissions = _get_user_permissions(session.info.get("jwt"))
    subquery = _get_query_available_objects(
        permission_table=permission_table, user_permissions=user_permissions
    )
    query = select(permission_table).where(
        permission_table.id == item_id, permission_table.parent_id.in_(subquery)
    )
    db_item = await session.execute(query)
    db_item: PermissionTemplate = db_item.scalar_one_or_none()

    if not db_item:
        raise HTTPException(
            status_code=404, detail="Element not found or access denied"
        )
    if db_item.root_permission_id:
        raise HTTPException(
            status_code=422,
            detail=f"For editing, use the main element of the rule with ID {db_item.root_permission_id}",
        )

    db_item.update_from_dict(item.dict(exclude_unset=True))
    session.add(db_item)
    await session.commit()
    await session.refresh(db_item)
    return item_id
