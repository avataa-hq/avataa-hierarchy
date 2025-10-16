from typing import AsyncGenerator, List

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


class HierarchyDeleteHandler:
    def __init__(
        self, session: AsyncSession, hierarchies_to_delete: List[Hierarchy]
    ):
        self.session = session
        self.hierarchies_to_delete = hierarchies_to_delete

    async def __set_transaction_isolation_level(self):
        await self.session.connection(
            execution_options={"isolation_level": "SERIALIZABLE"}
        )

    async def __get_sorted_desc_levels_for_hierarchy(
        self, hierarchy: Hierarchy
    ) -> List[Level]:
        """Returns hierarchy level order by depth DESC"""
        stmt = (
            select(Level)
            .where(Level.hierarchy_id == hierarchy.id)
            .order_by(desc(Level.level))
        )
        levels = await self.session.execute(stmt)
        return levels.scalars().all()

    async def __get_nodes_for_level(self, level: Level) -> AsyncGenerator:
        """Returns async generator of lists of Obj"""
        stmt = select(Obj).where(Obj.level_id == level.id)
        result_generator = await self.session.stream_scalars(stmt)
        async for partition in result_generator.yield_per(
            POSTGRES_ITEMS_LIMIT_IN_QUERY
        ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
            yield partition

    async def __get_node_data_for_level(self, level: Level) -> AsyncGenerator:
        """Returns async generator of lists of NodeData"""
        stmt = select(NodeData).where(NodeData.level_id == level.id)
        result_generator = await self.session.stream_scalars(stmt)
        async for partition in result_generator.yield_per(
            POSTGRES_ITEMS_LIMIT_IN_QUERY
        ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
            yield partition

    async def __delete_hierarchy_and_all_related_objects_add_all_data_into_session(
        self,
    ):
        await self.__set_transaction_isolation_level()

        for hierarchy in self.hierarchies_to_delete:
            all_levels = await self.__get_sorted_desc_levels_for_hierarchy(
                hierarchy=hierarchy
            )

            for level in all_levels:
                async for node_data_list in self.__get_node_data_for_level(
                    level=level
                ):
                    for node_data in node_data_list:
                        await self.session.delete(node_data)
                    await self.session.flush()

                async for nodes_list in self.__get_nodes_for_level(level=level):
                    for node in nodes_list:
                        await self.session.delete(node)
                    await self.session.flush()

                await self.session.delete(level)
            await self.session.delete(hierarchy)

    async def __delete_hierarchy_and_all_related_objects_add_only_hierarchy_into_session(
        self,
    ):
        # sys:1: SAWarning: Connection is already established for the given bind; execution_options ignored
        # await self.__set_transaction_isolation_level()

        for hierarchy in self.hierarchies_to_delete:
            await self.session.delete(hierarchy)

    async def delete_without_commit(self):
        """Marks the hierarchy and all associated levels, obj, node_data for deletion in the session without commit
        Used to read events from the session and send all related data to kafka"""
        await self.__delete_hierarchy_and_all_related_objects_add_only_hierarchy_into_session()

    async def delete_and_commit(self):
        """Marks the hierarchy and all associated levels, obj, node_data for deletion in the session and deletes whose
        objects from DB. Used to read events from the session and send all related data to kafka"""
        await self.delete_without_commit()
        await self.session.commit()
