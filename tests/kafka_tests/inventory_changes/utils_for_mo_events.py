import datetime
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
)


async def add_hierarchy_to_session(session: AsyncSession, hierarchy_name: str):
    """Add hierarchy to session"""
    created = datetime.datetime.now()
    hierarchy = Hierarchy(
        name=hierarchy_name,
        description="Test descr",
        author="Admin",
        created=created,
    )
    session.add(hierarchy)
    await session.flush()
    return hierarchy


async def add_level_to_session(
    session: AsyncSession,
    is_virtual: bool,
    hierarchy_id: int,
    level_name: str,
    obj_type_id: int,
    level: int,
    param_type_id=1,
    parent_id=None,
    level_key_attrs: List[str] = None,
):
    """Add level to session"""
    if not level_key_attrs:
        level_key_attrs = [str(param_type_id)]

    created = datetime.datetime.now()
    level = Level(
        level=level,
        name=level_name,
        object_type_id=obj_type_id,
        is_virtual=is_virtual,
        hierarchy_id=hierarchy_id,
        param_type_id=param_type_id,
        created=created,
        additional_params_id=None,
        latitude_id=None,
        longitude_id=None,
        parent_id=parent_id,
        author="AdminTest",
        key_attrs=level_key_attrs,
    )
    session.add(level)
    await session.flush()
    return level


class MODataFromInventory:
    def __init__(
        self,
        mo_id,
        mo_name,
        mo_tmo_id,
        mo_latitude=None,
        mo_longitude=None,
        mo_status="ACTIVE",
        mo_p_id: int = None,
        mo_active: bool = None,
    ):
        self.mo_id = mo_id
        self.mo_name = mo_name
        self.mo_latitude = mo_latitude
        self.mo_longitude = mo_longitude
        self.mo_status = mo_status
        self.mo_tmo_id = mo_tmo_id
        self.mo_p_id = mo_p_id


async def add_node_to_session(
    session: AsyncSession,
    level: Level,
    mo_data: List[dict],
    active: bool = True,
    parent_node: Obj = None,
):
    if parent_node and active:
        parent_node.child_count += 1
        session.add(parent_node)
    path = create_path_for_children_node_by_parent_node(parent_node=parent_node)

    key = "-".join([mo_data[0].get(key) for key in level.key_attrs])

    # check key for all must be same:
    for item_mo_data in mo_data:
        item_key = "-".join([item_mo_data.get(key) for key in level.key_attrs])
        if item_key != key:
            raise Exception(
                f"Key must be the same for all items in mo_data: {item_key}!= {key}"
            )

    node_dict = {
        "key": key,
        "object_type_id": mo_data[0].get("tmo_id"),
        "hierarchy_id": level.hierarchy_id,
        "level": level.level,
        "level_id": level.id,
        "parent_id": parent_node.id if parent_node else None,
        "child_count": 0,
        "path": path,
        "active": active,
    }

    if not level.is_virtual:
        node_dict["object_id"] = mo_data[0]["id"]

    new_node = Obj(**node_dict)

    session.add(new_node)
    await session.flush()

    for mo_item_data in mo_data:
        node_data = NodeData(
            node_id=new_node.id,
            mo_id=mo_item_data.get("id"),
            level_id=level.id,
            mo_name=mo_item_data.get("name"),
            mo_latitude=mo_item_data.get("latitude"),
            mo_longitude=mo_item_data.get("longitude"),
            mo_status=mo_item_data.get("status"),
            mo_tmo_id=mo_item_data.get("tmo_id"),
            mo_p_id=mo_item_data.get("p_id"),
            mo_active=active,
            unfolded_key={k: mo_item_data.get(k) for k in level.key_attrs},
        )
        session.add(node_data)

    await session.flush()
    return new_node


async def get_node_by_level_id_and_mo_id(
    session: AsyncSession, mo_id: int, level_id: int = None
) -> Obj | None:
    if not level_id:
        return None

    stmt = select(NodeData.node_id).where(
        NodeData.level_id == level_id, NodeData.mo_id == mo_id
    )
    node_id = await session.execute(stmt)
    node_id = node_id.scalars().first()
    if not node_id:
        return None

    stmt = select(Obj).where(Obj.id == node_id)
    node = await session.execute(stmt)
    return node.scalars().first()
