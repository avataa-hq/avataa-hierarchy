from sqlalchemy.ext.asyncio import AsyncSession

from schemas.enum_models import InventoryClassNames
from services.obj_events.status import ObjEventStatus
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)
from services.updater.event_handlers.mediator.interface import (
    UpdaterEventMediator,
)
from services.updater.event_handlers.mo.create import MOCreateHandler
from services.updater.event_handlers.mo.delete import MODeleteHandler
from services.updater.event_handlers.mo.update import MOUpdateHandler
from services.updater.event_handlers.prm.create import PRMCreateHandler
from services.updater.event_handlers.prm.delete import PRMDeleteHandler
from services.updater.event_handlers.prm.update import PRMUpdateHandler
from services.updater.event_handlers.tmo.delete import TMODeleteHandler
from services.updater.event_handlers.tprm.delete import TPRMDeleteHandler


class UpdaterEventMediatorImpl(UpdaterEventMediator):
    TMO_HANDLERS = {ObjEventStatus.DELETED: TMODeleteHandler}

    MO_HANDLERS = {
        ObjEventStatus.CREATED: MOCreateHandler,
        ObjEventStatus.UPDATED: MOUpdateHandler,
        ObjEventStatus.DELETED: MODeleteHandler,
    }
    TPRM_HANDLERS = {ObjEventStatus.DELETED: TPRMDeleteHandler}

    PRM_HANDLERS = {
        ObjEventStatus.CREATED: PRMCreateHandler,
        ObjEventStatus.UPDATED: PRMUpdateHandler,
        ObjEventStatus.DELETED: PRMDeleteHandler,
    }

    CLASS_NAME_HANDLERS = {
        InventoryClassNames.TMO: TMO_HANDLERS,
        InventoryClassNames.MO: MO_HANDLERS,
        InventoryClassNames.TPRM: TPRM_HANDLERS,
        InventoryClassNames.PRM: PRM_HANDLERS,
    }

    def get_handler(
        self,
        msg: dict,
        class_name: InventoryClassNames,
        event: ObjEventStatus,
        session: AsyncSession,
        hierarchy_id: int,
    ) -> HierarchyChangeInterface | None:
        dict_of_handlers = self.CLASS_NAME_HANDLERS.get(class_name)
        if not dict_of_handlers:
            return None

        event_handler_class = dict_of_handlers.get(event)
        if not event_handler_class:
            return None

        return event_handler_class(
            msg=msg, session=session, hierarchy_id=hierarchy_id
        )

    async def handle_the_message(
        self,
        msg: dict,
        class_name: InventoryClassNames,
        event: ObjEventStatus,
        session: AsyncSession,
        hierarchy_id: int,
    ):
        handler = self.get_handler(
            msg=msg,
            class_name=class_name,
            event=event,
            session=session,
            hierarchy_id=hierarchy_id,
        )

        if not handler:
            return
        await handler.make_changes()
