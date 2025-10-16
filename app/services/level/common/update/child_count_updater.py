from sqlalchemy import func, select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from schemas.hier_schemas import Level, Obj
from settings import LIMIT_OF_POSTGRES_RESULTS_PER_STEP


class LevelChildCounterUpdater:
    """Updates child count for nodes of special level"""

    def __init__(self, session: AsyncSession, level_id: int):
        self.level_id = level_id
        self.session = session

    async def __set_transaction_isolation_level(self):
        await self.session.connection(
            execution_options={"isolation_level": "SERIALIZABLE"}
        )

    async def __get_child_level_ids(self):
        stmt = select(Level.id).where(Level.parent_id == self.level_id)
        child_levels = await self.session.execute(stmt)
        return child_levels.scalars().all()

    async def update_without_commit(self):
        """Updates child count for obj of special level and marks them as updated in the session without commit
        Used to read events from the session and send all related data to kafka"""
        await self.__set_transaction_isolation_level()

        child_levels = await self.__get_child_level_ids()
        if child_levels:
            subquery = (
                select(Obj.parent_id, func.count(Obj.id).label("new_count"))
                .where(Obj.level_id.in_(child_levels), Obj.active == true())
                .group_by(Obj.parent_id)
                .subquery()
            )
            aliased_table = aliased(subquery)

            stmt = (
                select(Obj)
                .where(Obj.level_id == self.level_id)
                .outerjoin(aliased_table, Obj.id == aliased_table.c.parent_id)
                .add_columns(aliased_table.c.new_count)
            )

            result_generator = await self.session.stream(stmt)
            async for partition in result_generator.yield_per(
                LIMIT_OF_POSTGRES_RESULTS_PER_STEP
            ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
                for item in partition:
                    if item.new_count is None:
                        item.Obj.child_count = 0
                    else:
                        item.Obj.child_count = item.new_count
                    self.session.add(item.Obj)

                await self.session.flush()

    async def update_and_commit(self):
        """Updates child count for obj of special level and marks them as updated in the session with saving data in DB
        Used to read events from the session and send all related data to kafka"""
        await self.update_without_commit()
        await self.session.commit()
