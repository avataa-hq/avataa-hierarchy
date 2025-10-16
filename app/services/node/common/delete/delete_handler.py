import math
from typing import Generator, List

from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Obj
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
)
from services.node.common.update.child_count_updater import (
    NodeChildCounterUpdater,
)
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


class NodeDeleteHandler:
    def __init__(self, session: AsyncSession, nodes_to_delete: List[Obj]):
        self.session = session
        self.nodes_to_delete = nodes_to_delete
        # cache
        self.__node_ids_to_update_child_count = None

    def __get_generator_of_nodes_to_delete_divided_on_parts(self) -> Generator:
        """Divides nodes_to_delete on parts and returns as generator"""
        size_per_step = POSTGRES_ITEMS_LIMIT_IN_QUERY
        steps = math.ceil(
            len(self.nodes_to_delete) / POSTGRES_ITEMS_LIMIT_IN_QUERY
        )
        return (
            self.nodes_to_delete[
                (start := step * size_per_step) : start + size_per_step
            ]
            for step in range(steps)
        )

    def __create_default_params(self):
        self.__node_ids_to_update_child_count = set()

    @staticmethod
    async def __get_unique_paths_of_children_startswith_by_parent_nodes(
        parent_nodes: List[Obj],
    ) -> List[str]:
        """Returns list of unique parent paths"""
        return list(
            {
                create_path_for_children_node_by_parent_node(node)
                for node in parent_nodes
            }
        )

    @staticmethod
    async def __get_generator_of_paths_divided_ot_parts(
        paths: List[str],
    ) -> Generator:
        """Returns generator of lists of paths"""
        unique_paths_per_step = 10
        steps = math.ceil(len(paths) / unique_paths_per_step)
        return (
            paths[
                (start := step * unique_paths_per_step) : start
                + unique_paths_per_step
            ]
            for step in range(steps)
        )

    async def __process_one_part_of_nodes_to_delete_generator(
        self, nodes_to_delete: List[Obj]
    ):
        for node in nodes_to_delete:
            if node.parent_id:
                self.__node_ids_to_update_child_count.add(node.parent_id)

            await self.session.delete(node)

    async def __recalculate_node_child_count(self):
        if self.__node_ids_to_update_child_count:
            node_child_count_updater = NodeChildCounterUpdater(
                session=self.session,
                node_ids=list(self.__node_ids_to_update_child_count),
            )
            await node_child_count_updater.update_without_commit()

    async def delete_without_commit(self):
        self.__create_default_params()
        for part in self.__get_generator_of_nodes_to_delete_divided_on_parts():
            await self.__process_one_part_of_nodes_to_delete_generator(part)

        await self.__recalculate_node_child_count()

    async def delete_with_commit(self):
        await self.delete_without_commit()
        await self.session.commit()
