import http
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from grpc_config.inventory_utils import get_tprms_data_by_tprms_ids
from routers.utils import update_nodes_key_if_mo_link_or_prm_link
from schemas.enum_models import SET_OF_AVAILABLE_MO_ATTRS_FOR_LEVEL_KEY_ATTRS
from schemas.hier_schemas import (
    Hierarchy,
    Level,
    LevelCreate,
    Obj,
    ObjResponseNew,
)


# HIERARCHY
async def check_hierarchy_exist(hierarchy_id: int, session: AsyncSession):
    hierarchy = await session.get(Hierarchy, hierarchy_id)
    if not hierarchy:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND,
            detail="hierarchy_id with this number was not found",
        )
    return hierarchy


async def check_hierarchy_exist_with_lock(
    hierarchy_id: int, session: AsyncSession
):
    hierarchy = await session.get(Hierarchy, hierarchy_id, with_for_update=True)
    if not hierarchy:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND,
            detail="hierarchy_id with this number was not found",
        )
    return hierarchy


async def check_hierarchy_name_exist(
    hierarchy_name: str, session: AsyncSession
):
    query = select(Hierarchy).where(Hierarchy.name == hierarchy_name)
    hierarchy_exist = await session.execute(query)
    hierarchy_exist = hierarchy_exist.first()
    if hierarchy_exist:
        raise HTTPException(
            status_code=http.HTTPStatus.UNPROCESSABLE_ENTITY,
            detail="A hierarchy with the same name already exists",
        )
    return hierarchy_exist


async def create_tree_from_parent(parent_id: UUID, session: AsyncSession):
    objects = await session.execute(
        select(Obj).where(Obj.parent_id == parent_id)
    )
    objects: list[Obj] = objects.scalars().all()
    result = []
    for o in objects:
        child = await create_tree_from_parent(o.id, session)
        if len(child) == 0:
            child = None
        result.append(ObjResponseNew(**o.dict(), child=child))

    return result


async def create_tree_from_parent_node(
    parent_node: Obj,
    session: AsyncSession,
    consider_nodes_with_default_key: bool = True,
):
    objects = await session.execute(
        select(Obj).where(Obj.parent_id == parent_node.id)
    )
    objects: list[Obj] = objects.scalars().all()

    objects = await update_nodes_key_if_mo_link_or_prm_link(objects, session)

    result = []
    if not consider_nodes_with_default_key:
        for o in objects:
            if o.key == DEFAULT_KEY_OF_NULL_NODE:
                parent_node.child_count -= 1
                continue
            child = await create_tree_from_parent_node(
                o, session, consider_nodes_with_default_key
            )
            if len(child) == 0:
                child = None
            result.append(ObjResponseNew(**o.dict(), child=child))
    else:
        for o in objects:
            child = await create_tree_from_parent_node(o, session)
            if len(child) == 0:
                child = None
            result.append(ObjResponseNew(**o.dict(), child=child))

    return result


# LEVEL
async def check_hierarchy_level_exist(
    hierarchy_id: int, level_id: int, session: AsyncSession
):
    query = select(Level).where(
        and_(Level.id == level_id, Level.hierarchy_id == hierarchy_id)
    )
    level = await session.execute(query)
    level = level.scalar_one_or_none()
    if not level:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND,
            detail="level_id with this number was not found",
        )
    return level


async def check_hierarchy_level_name_exist(
    hierarchy_id: int,
    level_name: str,
    session: AsyncSession,
    exclude_level_id: int = None,
):
    query = select(Level).where(
        and_(Level.name == level_name, Level.hierarchy_id == hierarchy_id)
    )
    level_exist = await session.execute(query)
    level_exist = level_exist.scalars().first()

    if level_exist and (
        exclude_level_id is None or exclude_level_id != level_exist.id
    ):
        raise HTTPException(
            status_code=http.HTTPStatus.UNPROCESSABLE_ENTITY,
            detail="A level with the same name already exists",
        )
    return level_exist


# INVENTORY


async def check_hierarchy_level_inv_attr_as_parent_by_grpc(
    level: LevelCreate,
) -> str | None:
    if not level.attr_as_parent:
        return

    errors = []
    tprms_data = await get_tprms_data_by_tprms_ids(
        tprm_ids=[level.attr_as_parent]
    )
    if not tprms_data:
        errors.append(
            f"Unavailable {level.attr_as_parent.__name__}: {level.attr_as_parent}."
        )
    elif not level.is_virtual:
        errors.append(
            f" Level with {level.attr_as_parent.__name__} must be virtual."
        )
    elif len(tprms_data) != 1:
        errors.append("Found more than one tprm with this tprm_id.")
    else:
        tprm_data = tprms_data[0]
        if tprm_data.get("val_type") != "mo_link":
            errors.append(
                f'Tprm with name {tprms_data["name"]} (id: {tprms_data["id"]}) must be type "mo_link"'
            )
        elif tprm_data.get("multiple"):
            errors.append(
                f'Tprm with name {tprms_data["name"]} (id: {tprms_data["id"]}) cannot be type "multiple"'
            )
        elif tprm_data.get("constraint", "") != str(level.object_type_id):
            errors.append(
                f"Tprm with name {tprms_data['name']} (id: {tprms_data['id']}) "
                f"must have same constraint as level object type id ({level.object_type_id})"
            )
    if not errors:
        return
    return "\n".join(errors)


async def check_hierarchy_level_inv_params_exist_by_grpc(
    level: LevelCreate,
) -> dict:
    all_tprm_ids = set()
    all_mo_attr_names = set()

    simple_attrs = ["additional_params_id", "latitude_id", "longitude_id"]

    for attr_name in simple_attrs:
        attr_value = getattr(level, attr_name, None)
        if attr_value:
            all_tprm_ids.add(attr_value)

    for attr in level.key_attrs:
        if attr.isdigit():
            all_tprm_ids.add(int(attr))
        else:
            all_mo_attr_names.add(attr)

    # check mo attrs
    mo_attr_details = ""
    if all_mo_attr_names:
        not_available_mo_attrs = all_mo_attr_names.difference(
            SET_OF_AVAILABLE_MO_ATTRS_FOR_LEVEL_KEY_ATTRS
        )
        if not_available_mo_attrs:
            mo_attr_details += (
                f"Unavailable key_attrs: {not_available_mo_attrs}. "
            )

    # check tprms
    tprm_details = ""
    if all_tprm_ids:
        tprms_data = await get_tprms_data_by_tprms_ids(
            tprm_ids=list(all_tprm_ids)
        )

        tprms_does_not_matched_tmo_id = []
        tprm_ids_tprm_name = set()

        for tprm_data in tprms_data:
            tmo_id = tprm_data.get("tmo_id")
            tprm_ids_tprm_name.add(tprm_data["id"])
            if tmo_id != level.object_type_id:
                tprms_does_not_matched_tmo_id.append(tprm_data["name"])

        tprm_ids_was_not_founded = list(
            all_tprm_ids.difference(tprm_ids_tprm_name)
        )

        if tprms_does_not_matched_tmo_id:
            tprm_details = (
                f"Some TPRMs can`t be used as key_attrs "
                f"parameters for this level: {tprms_does_not_matched_tmo_id}. "
            )
        if tprm_ids_was_not_founded:
            tprm_details += f"Not founded TPRMs: {tprm_ids_was_not_founded}. "

    all_valid = True
    if mo_attr_details or tprm_details:
        all_valid = False

    result = {
        "all_valid": all_valid,
        "mo_attr_details": mo_attr_details,
        "tprm_details": tprm_details,
    }

    return result


async def check_level_parent_id(
    level: LevelCreate,
    hierarchy_id: int,
    session: AsyncSession,
    level_id: int = None,
):
    """Check if level.parent_id exists is same hierarchy and not eq to level.id, otherwise raise error.
    Check that parent level.level one less than a child level.level"""
    parent_id = getattr(level, "parent_id", None)
    if level_id is not None and parent_id is not None:
        if level_id == parent_id:
            raise HTTPException(
                status_code=http.HTTPStatus.BAD_REQUEST,
                detail="level.parent_id can`t be equal to level.id.",
            )

    if parent_id is not None:
        statement = select(Level).where(
            Level.hierarchy_id == hierarchy_id, Level.id == parent_id
        )
        res = await session.execute(statement)
        parent = res.scalars().first()
        if parent is None:
            raise HTTPException(
                status_code=http.HTTPStatus.BAD_REQUEST,
                detail=f"parent_id - doesn`t exist. Parent level doesn`t exist in hierarchy with id ="
                f" {hierarchy_id}.",
            )

        else:
            if level.level - parent.level != 1:
                raise HTTPException(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    detail=f"Child should be on the next level after parent. (difference "
                    f"(child.level - parent.level) should be eq to 1). "
                    f"Parent.level = {parent.level} but child.level = {level.level}",
                )

    return True
