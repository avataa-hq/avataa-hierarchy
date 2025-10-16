import math
from typing import Generator, List

from sqlalchemy import func, select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from schemas.hier_schemas import Obj
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


class NodeChildCounterUpdater:
    """Updates child count for nodes of special level"""

    def __init__(self, session: AsyncSession, node_ids: List[str]):
        self.node_ids = node_ids
        self.session = session

    async def __set_transaction_isolation_level(self):
        await self.session.connection(
            execution_options={"isolation_level": "SERIALIZABLE"}
        )

    def __get_generator_of_nodes_to_update_divided_on_parts(self) -> Generator:
        """Divides nodes_to_delete on parts and returns as generator"""
        size_per_step = POSTGRES_ITEMS_LIMIT_IN_QUERY
        steps = math.ceil(len(self.node_ids) / POSTGRES_ITEMS_LIMIT_IN_QUERY)
        return (
            self.node_ids[
                (start := step * size_per_step) : start + size_per_step
            ]
            for step in range(steps)
        )

    async def update_without_commit(self):
        """Updates child count for obj of special level and marks them as updated in the session without commit
        Used to read events from the session and send all related data to kafka"""
        await self.__set_transaction_isolation_level()

        for (
            step_node_ids
        ) in self.__get_generator_of_nodes_to_update_divided_on_parts():
            subquery = (
                select(Obj.parent_id, func.count(Obj.id).label("new_count"))
                .where(Obj.parent_id.in_(step_node_ids), Obj.active == true())
                .group_by(Obj.parent_id)
                .subquery()
            )
            aliased_table = aliased(subquery)

            stmt = (
                select(Obj)
                .where(Obj.id.in_(step_node_ids))
                .outerjoin(aliased_table, Obj.id == aliased_table.c.parent_id)
                .add_columns(aliased_table.c.new_count)
            )

            res = await self.session.execute(stmt)
            res = res.all()

            for item in res:
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
