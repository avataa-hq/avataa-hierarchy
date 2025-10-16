from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from kafka_config.msg.msg_utils import (
    recalculate_child_count_of_nodes_for_specific_level,
)
from schemas.hier_schemas import Level


async def on_delete_tmo(msg, session: AsyncSession):
    """Delete levels and does not rebuild hierarchy"""

    ids = []

    for item in msg["objects"]:
        ids.append(item["id"])

    # Before deleting a level, we need to change the number of children of
    # the parents of the level objects that we will be deleting.
    stm = select(Level).where(Level.object_type_id.in_(ids))
    levels_to_delete = await session.execute(stm)
    levels_to_delete = levels_to_delete.scalars().all()

    level_ids_to_delete = set(level.id for level in levels_to_delete)

    parent_ids = {
        level.parent_id
        for level in levels_to_delete
        if all(
            [
                level.parent_id is not None,
                level.parent_id not in level_ids_to_delete,
            ]
        )
    }

    # Delete all Levels where Level.object_type_id == TMO.id
    statement = delete(Level).where(Level.object_type_id.in_(ids))
    await session.execute(statement)

    # recalculate node children for parent levels
    for leve_id in parent_ids:
        await recalculate_child_count_of_nodes_for_specific_level(
            session=session, level_id=leve_id
        )

    await session.commit()
