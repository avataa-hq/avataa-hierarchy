import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from kafka_config.batch_change_handler.utils.utils import (
    get_count_of_hierarchy_nodes,
    rebuild_all_hierarchies_from_order,
    rebuild_hierarchy_and_change_item_of_hierarchy_rebuild_order,
)
from schemas.hier_schemas import HierarchyRebuildOrder
import settings


class HierarchyRebuildOrderChecker:
    DEFAULT_SLEEP_TIME = 5

    @staticmethod
    async def get_hierarchy_rebuild_order_with_on_rebuild_true(
        session: AsyncSession,
    ):
        """Returns HierarchyRebuildOrder items in rebuild order with status on_rebuild True"""
        stmt = select(HierarchyRebuildOrder).order_by(HierarchyRebuildOrder.id)
        hierarchies_in_rebuild_stage = await session.execute(stmt)
        hierarchies_in_rebuild_stage = (
            hierarchies_in_rebuild_stage.scalars().all()
        )
        return hierarchies_in_rebuild_stage

    async def check_order_and_rebuild_hierarchy(self):
        """IF there are some hierarchies in rebuild order with status on_rebuild False - rebuilds this hierarchies.
        If there are some hierarchies in rebuild order with status on_rebuild True - checks to see if they are stuck.
        If they are stuck - rebuilds them too"""
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
            # rebuild all hierarchies with stage on_rebuild = False
            print("Search for hierarchies with on rebuild False Start")
            await rebuild_all_hierarchies_from_order(session=db_session)
            print("Search for hierarchies with on rebuild False End")

            h_on_rebuild_true = (
                await self.get_hierarchy_rebuild_order_with_on_rebuild_true(
                    db_session
                )
            )
            print("Search for hierarchies with on rebuild True Start")
            print(h_on_rebuild_true)
            print("Search for hierarchies with on rebuild End Start")

            if h_on_rebuild_true:
                h_ids_on_rebuild_true = [
                    item.hierarchy_id for item in h_on_rebuild_true
                ]
                count_hierarchies_nodes = await get_count_of_hierarchy_nodes(
                    session=db_session, hierarchy_ids=h_ids_on_rebuild_true
                )
                time.sleep(self.DEFAULT_SLEEP_TIME)

                h_on_rebuild_true = (
                    await self.get_hierarchy_rebuild_order_with_on_rebuild_true(
                        db_session
                    )
                )
                h_ids_on_rebuild_true = [
                    item.hierarchy_id for item in h_on_rebuild_true
                ]
                control_data = await get_count_of_hierarchy_nodes(
                    session=db_session, hierarchy_ids=h_ids_on_rebuild_true
                )

                cache_hierarchy_rebuild_item_by_h_id = {
                    h.hierarchy_id: h for h in h_on_rebuild_true
                }

                intersection_ids = set(count_hierarchies_nodes).intersection(
                    control_data
                )

                if intersection_ids:
                    # find hierarchies ids count nodes of which was not changed
                    not_changed = {
                        x
                        for x in intersection_ids
                        if control_data[x] == count_hierarchies_nodes[x]
                    }
                    if not_changed:
                        for not_changed_hierarchy_id in not_changed:
                            h_order_item = (
                                cache_hierarchy_rebuild_item_by_h_id.get(
                                    not_changed_hierarchy_id
                                )
                            )
                            if h_order_item:
                                await rebuild_hierarchy_and_change_item_of_hierarchy_rebuild_order(
                                    session=db_session,
                                    item_to_rebuild=h_order_item,
                                )
