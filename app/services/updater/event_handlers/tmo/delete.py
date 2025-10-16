from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Level
from services.level.common.delete.delete_handler import LevelDeleteHandler
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)


async def with_tmo_delete_event(msg, session: AsyncSession, hierarchy_id: int):
    """Delete levels and does not rebuild hierarchy"""

    ids = []

    for item in msg["objects"]:
        ids.append(item["id"])

    # Before deleting a level, we need to change the number of children of
    # the parents of the level objects that we will be deleting.
    stm = select(Level).where(
        Level.object_type_id.in_(ids), Level.hierarchy_id == hierarchy_id
    )
    levels_to_delete = await session.execute(stm)
    levels_to_delete = levels_to_delete.scalars().all()

    if not levels_to_delete:
        return

    level_del_handler = LevelDeleteHandler(
        session=session, levels_to_delete=levels_to_delete
    )
    await level_del_handler.delete_and_commit()


class TMODeleteHandler(HierarchyChangeInterface):
    async def make_changes(self):
        await with_tmo_delete_event(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
