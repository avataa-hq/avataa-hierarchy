from sqlalchemy.ext.asyncio import AsyncSession

from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)
from services.updater.event_handlers.common.msg_utils import (
    SpecialHierarchyMOCreateHandler,
)


async def with_mo_create_event(msg, session: AsyncSession, hierarchy_id: int):
    """If new MO has been created - makes changes to a specific hierarchy without full rebuild."""
    create_handler = SpecialHierarchyMOCreateHandler(
        mo_data=msg["objects"], session=session, hierarchy_id=hierarchy_id
    )
    await create_handler.make_changes()


class MOCreateHandler(HierarchyChangeInterface):
    async def make_changes(self):
        await with_mo_create_event(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
