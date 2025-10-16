from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from kafka_config.msg.prm_msg import on_create_prm, on_delete_prm, on_update_prm
from services.obj_events.status import ObjEventStatus
import settings

from .msg.mo_msg import on_create_mo, on_delete_mo, on_update_mo
from .msg.tmo_msg import on_delete_tmo
from .msg.tprm_msg import on_delete_tprm

# from .batch_change_handler.handler import PRMChangesHandler, MOChangesHandler
from .utils import ObjClassNames


async def adapter_function(msg_class_name, msg_event, message_as_dict):
    engine = create_async_engine(
        settings.DATABASE_URL, echo=False, future=True, pool_pre_ping=True
    )
    async_session = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )
    async with async_session() as db_session:
        match msg_class_name:
            # Inventory PRM cases
            case ObjClassNames.PRM.value:
                # handler = PRMChangesHandler(session=db_session, message_as_dict=message_as_dict)
                # message_as_dict = await handler.get_msg_ready_for_processing()

                match msg_event:
                    case ObjEventStatus.DELETED.value:
                        await on_delete_prm(message_as_dict, session=db_session)
                    case ObjEventStatus.UPDATED.value:
                        await on_update_prm(message_as_dict, session=db_session)
                    case ObjEventStatus.CREATED.value:
                        await on_create_prm(message_as_dict, session=db_session)

            # Inventory TMO cases
            case ObjClassNames.TMO.value:
                match msg_event:
                    case ObjEventStatus.DELETED.value:
                        await on_delete_tmo(message_as_dict, session=db_session)

            # Inventory TPRM cases
            case ObjClassNames.TPRM.value:
                match msg_event:
                    case ObjEventStatus.DELETED.value:
                        await on_delete_tprm(
                            message_as_dict, session=db_session
                        )

            # Inventory MO cases
            case ObjClassNames.MO.value:
                # handler = MOChangesHandler(session=db_session, message_as_dict=message_as_dict)
                # message_as_dict = await handler.get_msg_ready_for_processing()
                match msg_event:
                    case ObjEventStatus.DELETED.value:
                        await on_delete_mo(message_as_dict, session=db_session)

                    case ObjEventStatus.UPDATED.value:
                        await on_update_mo(message_as_dict, session=db_session)

                    case ObjEventStatus.CREATED.value:
                        await on_create_mo(message_as_dict, session=db_session)
