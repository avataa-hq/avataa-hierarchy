import math

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Level, NodeData, Obj
from services.node.common.delete.delete_handler import NodeDeleteHandler
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)
from settings import POSTGRES_ITEMS_LIMIT_IN_QUERY


async def with_mo_delete_event(msg, session: AsyncSession, hierarchy_id: int):
    """Delete objects and does not rebuild hierarchy"""
    ids = []
    tmo_ids = set()

    for item in msg["objects"]:
        ids.append(item["id"])
        tmo_ids.add(item["tmo_id"])

    if len(ids) == 0:
        return

    # get all levels for spec hierarchy
    stmt = select(Level.id).where(
        Level.object_type_id.in_(list(tmo_ids)),
        Level.hierarchy_id == hierarchy_id,
    )
    all_level_ids = await session.execute(stmt)
    all_level_ids = all_level_ids.scalars().all()
    # node_data part start
    if not all_level_ids:
        return

    # node identifiers to be checked during deletion
    node_id_check_on_delete = set()

    query = select(NodeData).where(
        NodeData.mo_id.in_(ids), NodeData.level_id.in_(all_level_ids)
    )
    result_generator = await session.stream_scalars(query)
    async for partition in result_generator.yield_per(
        POSTGRES_ITEMS_LIMIT_IN_QUERY
    ).partitions(POSTGRES_ITEMS_LIMIT_IN_QUERY):
        node_data_id_to_delete = list()

        for node_data in partition:
            node_data_id_to_delete.append(node_data.id)
            node_id_check_on_delete.add(node_data.node_id)

        delete_stm = delete(NodeData).where(
            NodeData.id.in_(node_data_id_to_delete)
        )
        await session.execute(delete_stm)

    steps = math.ceil(
        len(node_id_check_on_delete) / POSTGRES_ITEMS_LIMIT_IN_QUERY
    )
    list_of_node_id = list(node_id_check_on_delete)

    nodes_ids_must_be_deleted = set()
    for step in range(steps):
        start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
        end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

        step_list = list_of_node_id[start:end]
        step_set = set(step_list)

        stmt = (
            select(NodeData.node_id)
            .where(NodeData.node_id.in_(step_list))
            .distinct()
        )

        res = await session.execute(stmt)
        res = set(res.scalars().all())

        nodes_ids_must_be_deleted.update(step_set.difference(res))

    if nodes_ids_must_be_deleted:
        steps = math.ceil(
            len(nodes_ids_must_be_deleted) / POSTGRES_ITEMS_LIMIT_IN_QUERY
        )
        list_of_node_id = list(nodes_ids_must_be_deleted)

        for step in range(steps):
            start = POSTGRES_ITEMS_LIMIT_IN_QUERY * step
            end = start + POSTGRES_ITEMS_LIMIT_IN_QUERY

            step_list = list_of_node_id[start:end]

            # get nodes
            stmt = select(Obj).where(
                Obj.id.in_(step_list), Obj.hierarchy_id == hierarchy_id
            )
            nodes_to_delete = await session.execute(stmt)
            nodes_to_delete = nodes_to_delete.scalars().all()

            delete_handler = NodeDeleteHandler(
                session=session, nodes_to_delete=nodes_to_delete
            )
            await delete_handler.delete_with_commit()


class MODeleteHandler(HierarchyChangeInterface):
    async def make_changes(self):
        await with_mo_delete_event(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
