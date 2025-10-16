from collections import defaultdict
from typing import AsyncGenerator, Dict, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from schemas.hier_schemas import Level, NodeData, Obj
from services.level.common.update.child_count_updater import (
    LevelChildCounterUpdater,
)
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


class LevelDeleteHandler:
    def __init__(self, session: AsyncSession, levels_to_delete: List[Level]):
        self.session = session
        self.levels_to_delete = levels_to_delete
        # cache
        self.__level_ids_to_update_child_count = set()

    async def __set_transaction_isolation_level(self):
        await self.session.connection(
            execution_options={"isolation_level": "SERIALIZABLE"}
        )

    def __get_grouped_levels_by_hierarchy_id(self) -> Dict[int, List[Level]]:
        """Returns levels grouped by hierarchy"""
        res = defaultdict(list)
        for level in self.levels_to_delete:
            res[level.hierarchy_id].append(level)
        return res

    @staticmethod
    def __get_levels_sorted_by_depth_asc(levels: List[Level]) -> List[Level]:
        """Returns list of Levels sorted by depth (ASC)"""
        return sorted(levels, key=lambda level_item: level_item.level)

    @staticmethod
    def __get_levels_sorted_by_depth_desc(levels: List[Level]) -> List[Level]:
        """Returns list of Levels sorted by depth (DESC)"""
        return sorted(
            levels, key=lambda level_item: level_item.level, reverse=True
        )

    async def __get_all_child_levels(self, parent_level: Level) -> List[Level]:
        """Returns list of child levels for special parent_level"""
        child_levels = dict()
        level_p_ids = [parent_level.id]
        aliased_child_level = aliased(Level)

        while True:
            stmt = (
                select(Level, aliased_child_level)
                .where(Level.parent_id.in_(level_p_ids))
                .outerjoin(
                    aliased_child_level,
                    aliased_child_level.parent_id == Level.id,
                )
            )
            res = await self.session.execute(stmt)
            res = res.all()
            level_p_ids = list()

            for level, child_level in res:
                child_levels[level.id] = level
                if child_level:
                    level_p_ids.append(child_level.id)
                    child_levels[child_level.id] = child_level
            if not level_p_ids:
                break
        return list(child_levels.values())

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

    async def __levels_delete_handler_for_special_hierarchy_adds_all_related_data_into_session(
        self, levels: List[Level]
    ):
        """Deletes levels for special hierarchy"""
        l_sorted_by_depth = {
            level.id: level
            for level in self.__get_levels_sorted_by_depth_asc(levels)
        }
        already_processed_level_ids = set()

        for level_id, level_item in l_sorted_by_depth.items():
            print(level_item)
            if level_id in already_processed_level_ids:
                continue

            if level_item.parent_id:
                self.__level_ids_to_update_child_count.add(level_item.parent_id)

            child_levels = await self.__get_all_child_levels(
                parent_level=level_item
            )

            if child_levels:
                # start to delete from bottom to top
                levels_to_process = self.__get_levels_sorted_by_depth_desc(
                    child_levels
                ) + [level_item]

                for level in levels_to_process:
                    already_processed_level_ids.add(level.id)
                    async for node_data_list in self.__get_node_data_for_level(
                        level=level
                    ):
                        for node_data in node_data_list:
                            await self.session.delete(node_data)
                        await self.session.flush()

                    async for nodes_list in self.__get_nodes_for_level(
                        level=level
                    ):
                        for node in nodes_list:
                            await self.session.delete(node)
                        await self.session.flush()

                    await self.session.delete(level)
            else:
                await self.session.delete(level_item)

        await self.session.flush()

    async def __levels_delete_handler_for_special_hierarchy_adds_only_level_and_related_levels_into_session(
        self, levels: List[Level]
    ):
        """Deletes levels for special hierarchy"""
        l_sorted_by_depth = {
            level.id: level
            for level in self.__get_levels_sorted_by_depth_asc(levels)
        }
        already_processed_level_ids = set()

        for level_id, level_item in l_sorted_by_depth.items():
            print(level_item)
            if level_id in already_processed_level_ids:
                continue

            if level_item.parent_id:
                self.__level_ids_to_update_child_count.add(level_item.parent_id)

            child_levels = await self.__get_all_child_levels(
                parent_level=level_item
            )

            if child_levels:
                # start to delete from bottom to top
                for level in child_levels:
                    await self.session.delete(level)

            await self.session.delete(level_item)

        await self.session.flush()

    async def __update_child_count_for_parent_levels(self):
        for level_id in self.__level_ids_to_update_child_count:
            child_count_updater = LevelChildCounterUpdater(
                session=self.session, level_id=level_id
            )
            await child_count_updater.update_without_commit()

    async def delete_without_commit(self):
        """Marks all levels_to_delete and all associated child levels, obj, node_data for deletion in the session
        without commit. Used to read events from the session and send all related data to kafka"""
        await self.__set_transaction_isolation_level()
        levels_grouped_by_hierarchy_id = (
            self.__get_grouped_levels_by_hierarchy_id()
        )
        for _, levels in levels_grouped_by_hierarchy_id.items():
            await self.__levels_delete_handler_for_special_hierarchy_adds_only_level_and_related_levels_into_session(
                levels
            )

        await self.__update_child_count_for_parent_levels()

    async def delete_and_commit(self):
        """Marks the levels_to_delete and all associated child levels, obj, node_data for deletion in the session
        and deletes whose items from DB. Used to read events from the session and send all related data to kafka"""
        await self.delete_without_commit()
        await self.session.commit()
