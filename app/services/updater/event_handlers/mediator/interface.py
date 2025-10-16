from abc import ABC, abstractmethod

from sqlalchemy.ext.asyncio import AsyncSession

from schemas.enum_models import InventoryClassNames
from services.obj_events.status import ObjEventStatus
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)


class UpdaterEventMediator(ABC):
    @abstractmethod
    def get_handler(
        self,
        msg: dict,
        class_name: InventoryClassNames,
        event: ObjEventStatus,
        session: AsyncSession,
        hierarchy_id: int,
    ) -> HierarchyChangeInterface | None:
        pass

    @abstractmethod
    async def handle_the_message(
        self,
        msg: dict,
        class_name: InventoryClassNames,
        event: ObjEventStatus,
        session: AsyncSession,
        hierarchy_id: int,
    ):
        pass
