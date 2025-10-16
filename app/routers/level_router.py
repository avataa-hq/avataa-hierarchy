import datetime
import http

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from starlette.responses import Response

from database import database
from grpc_config.inventory_utils import get_tmo_data_by_tmo_ids
from routers.utility_checks import (
    check_hierarchy_exist,
    check_hierarchy_level_exist,
    check_hierarchy_level_inv_attr_as_parent_by_grpc,
    check_hierarchy_level_inv_params_exist_by_grpc,
    check_hierarchy_level_name_exist,
    check_level_parent_id,
)
from schemas.hier_schemas import Level, LevelCreate
from services.level.common.delete.delete_handler import LevelDeleteHandler

router = APIRouter(tags=["Level"])


@router.get("/level/{level_id}", status_code=200, response_model=Level)
async def read_level(
    level_id: int, session: AsyncSession = Depends(database.get_session)
):
    """Returns level by level id, otherwise raises 404 error"""

    stmt = select(Level).where(Level.id == level_id)
    res = await session.execute(stmt)
    res = res.scalars().first()

    if not res:
        raise HTTPException(
            status_code=404, detail=f"Level with id {level_id} doest not exist"
        )
    return res


@router.get("/hierarchy/{hierarchy_id}/level", response_model=list[Level])
async def get_levels(
    hierarchy_id: int, session: AsyncSession = Depends(database.get_session)
):
    """
    Returns the elements of the levels that are related to the element in the hierarchy.  <br>
    Use when *creating/deleting/changing* a structure.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**
    """
    query = select(Level).filter(Level.hierarchy_id == hierarchy_id)
    level = await session.execute(query)
    level = level.scalars().all()
    if len(level) == 0:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND, detail="levels was not found"
        )
    return level


@router.get(
    "/hierarchy/{hierarchy_id}/level/{level_id}",
    response_model=Level,
    deprecated=True,
)
async def get_level(
    hierarchy_id: int,
    level_id: int,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Returns a level element from a hierarchical structure.  <br>
    Use when *creating/deleting/changing* a structure.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**  <br>
    The value **level_id** can be obtained from the endpoint **/hierarchy/{hierarchy_id}/level**
    """
    # checks
    await check_hierarchy_exist(hierarchy_id=hierarchy_id, session=session)
    level = await check_hierarchy_level_exist(
        hierarchy_id=hierarchy_id, level_id=level_id, session=session
    )
    return level


@router.post(
    "/hierarchy/{hierarchy_id}/level",
    response_model=Level,
    status_code=http.HTTPStatus.CREATED,
)
async def add_level(
    hierarchy_id: int,
    level: LevelCreate,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Adds a new level to the hierarchy.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**  <br>
    The value **level_id** can be obtained from the endpoint **/hierarchy/{hierarchy_id}/level**  <br>
    The next fields are optional:  <br>
    - description  <br>
    - additional_params_id <br>
    - latitude_id <br>
    - longitude_id <br>
    - parent_id <br>
    """
    # checks
    await check_hierarchy_exist(hierarchy_id=hierarchy_id, session=session)
    await check_hierarchy_level_name_exist(
        hierarchy_id=hierarchy_id, level_name=level.name, session=session
    )

    # check tmo_id
    tmo_data = await get_tmo_data_by_tmo_ids(tmo_ids=[level.object_type_id])

    if not tmo_data:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND, detail="TMO data not found"
        )

    mo_attr_trpm_check = await check_hierarchy_level_inv_params_exist_by_grpc(
        level=level
    )
    if mo_attr_trpm_check["all_valid"] is False:
        details = (
            mo_attr_trpm_check["mo_attr_details"]
            + mo_attr_trpm_check["tprm_details"]
        )
        raise HTTPException(
            status_code=http.HTTPStatus.BAD_REQUEST, detail=details
        )
    mo_attr_as_parent_check = (
        await check_hierarchy_level_inv_attr_as_parent_by_grpc(level=level)
    )
    if mo_attr_as_parent_check:
        raise HTTPException(
            status_code=http.HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=mo_attr_as_parent_check,
        )
    await check_level_parent_id(
        level=level, hierarchy_id=hierarchy_id, session=session
    )
    # append data
    created = datetime.datetime.now()

    level = Level(
        **level.model_dump(), created=created, hierarchy_id=hierarchy_id
    )

    # insert and refresh
    session.add(level)
    await session.flush()
    await session.refresh(level)
    await session.commit()
    return level


@router.put("/hierarchy/{hierarchy_id}/level/{level_id}", response_model=Level)
async def change_level(
    hierarchy_id: int,
    level_id: int,
    level: LevelCreate,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Modifies an **existing** level from the hierarchy.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**  <br>
    The value **level_id** can be obtained from the endpoint **/hierarchy/{hierarchy_id}/level**  <br>
    The next fields are optional:  <br>
    - description  <br>
    - additional_params_id <br>
    - latitude_id <br>
    - longitude_id <br>
    - parent_id <br>
    """
    # checks
    await check_hierarchy_exist(hierarchy_id=hierarchy_id, session=session)
    level_exist = await check_hierarchy_level_exist(
        hierarchy_id=hierarchy_id, level_id=level_id, session=session
    )
    await check_hierarchy_level_name_exist(
        hierarchy_id=hierarchy_id,
        level_name=level.name,
        session=session,
        exclude_level_id=level_id,
    )
    # check tmo_id
    tmo_data = await get_tmo_data_by_tmo_ids(tmo_ids=[level.object_type_id])
    if not tmo_data:
        raise HTTPException(
            status_code=http.HTTPStatus.NOT_FOUND, detail="TMO data not found"
        )

    mo_attr_trpm_check = await check_hierarchy_level_inv_params_exist_by_grpc(
        level=level
    )
    if mo_attr_trpm_check["all_valid"] is False:
        details = (
            mo_attr_trpm_check["mo_attr_details"]
            + mo_attr_trpm_check["tprm_details"]
        )
        raise HTTPException(
            status_code=http.HTTPStatus.BAD_REQUEST, detail=details
        )
    mo_attr_as_parent_check = (
        await check_hierarchy_level_inv_attr_as_parent_by_grpc(level=level)
    )
    if mo_attr_as_parent_check:
        raise HTTPException(
            status_code=http.HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=mo_attr_as_parent_check,
        )
    await check_level_parent_id(
        level=level,
        hierarchy_id=hierarchy_id,
        session=session,
        level_id=level_id,
    )
    # append data
    level_dict = level.model_dump(exclude_unset=True)
    level_dict["level"] = level.level
    level_dict["change_author"] = level_dict.pop("author")
    level_dict["author"] = level_exist.author
    level_dict["modified"] = datetime.datetime.now()

    # update and refresh
    query = update(Level).where(Level.id == level_id).values(**level_dict)
    await session.execute(query)
    await session.commit()
    await session.refresh(level_exist)
    return level_exist


@router.delete(
    "/hierarchy/{hierarchy_id}/level/{level_id}",
    status_code=http.HTTPStatus.NO_CONTENT,
    response_class=Response,
)
async def delete_level(
    hierarchy_id: int,
    level_id: int,
    session: AsyncSession = Depends(database.get_session),
):
    """
    Removes an **existing** level of a hierarchy.  <br>
    **hierarchy_id** must exist in the database.  <br>
    **level_id** must exist in the hierarchy.  <br>
    The value **hierarchy_id** can be obtained from the endpoint **/hierarchy**  <br>
    The value **level_id** can be obtained from the endpoint **/hierarchy/{hierarchy_id}/level**  <br>
    """
    # checks
    level_exist = await check_hierarchy_level_exist(
        hierarchy_id=hierarchy_id, level_id=level_id, session=session
    )
    level_delete_handler = LevelDeleteHandler(
        session=session, levels_to_delete=[level_exist]
    )
    await level_delete_handler.delete_and_commit()
