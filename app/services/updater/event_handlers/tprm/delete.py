import copy

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Level, NodeData, Obj
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
)
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)
from services.updater.event_handlers.common.msg_utils import (
    __recalc_child_count,
    rebuilding_nodes_based_on_their_data,
)
from settings import LIMIT_OF_POSTGRES_RESULTS_PER_STEP


async def with_tprm_delete_event(msg, session: AsyncSession, hierarchy_id: int):
    """Deletes level if level.param_type_id == TPRM.id. Cleans up the levels attrs ( additional_params_id,
    latitude_id, longitude_id) if they are eq to deleted TPRM.id and does not rebuild hierarchy"""
    ids = set()
    tprm_ids_as_str = set()

    for item in msg["objects"]:
        ids.add(item["id"])
        tprm_ids_as_str.add(str(item["id"]))

    # get all levels where deleted parameter id in key_attrs
    stmt = (
        select(Level)
        .where(
            Level.hierarchy_id == hierarchy_id,
            Level.key_attrs.overlap(tprm_ids_as_str),
        )
        .order_by(Level.object_type_id.desc(), Level.level.asc())
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    # change key_attrs
    for level in all_levels:
        level.key_attrs = [
            attr for attr in level.key_attrs if attr not in tprm_ids_as_str
        ]

    top_level_by_tmo = dict()

    for level in all_levels:
        level_in_cache = top_level_by_tmo.get(level.object_type_id)
        if not level_in_cache:
            top_level_by_tmo[level.object_type_id] = level

    for level in top_level_by_tmo.values():
        stmt = select(NodeData).where(NodeData.level_id == level.id)
        level_node_data = await session.execute(stmt)
        level_node_data = level_node_data.scalars().all()

        changed_node_datas = list()
        for node_data in level_node_data:
            keys_intersection = node_data.unfolded_key.keys() & tprm_ids_as_str

            if keys_intersection:
                new_data = copy.deepcopy(node_data.unfolded_key)
                for key in keys_intersection:
                    del new_data[key]
                node_data.unfolded_key = new_data
                changed_node_datas.append(node_data)

        if changed_node_datas:
            await session.flush()
            print("With nodes rebuilding")
            await rebuilding_nodes_based_on_their_data(
                node_data_with_new_parents=changed_node_datas,
                session=session,
                level=level,
            )
    await session.commit()


async def with_tprm_delete_event_in_parent_attr(
    msg: dict, session: AsyncSession, hierarchy_id: int
):
    tprm_ids = [i["id"] for i in msg["objects"]]

    # find levels where attr_as_parent equals to deleted tprm_id
    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy_id, Level.attr_as_parent.in_(tprm_ids)
    )
    response = await session.execute(stmt)
    levels_by_id: dict[int, Level] = {}
    for level in response.scalars():
        levels_by_id[level.id] = level
    level_ids = list(levels_by_id.keys())
    if not level_ids:
        return

    # delete level nodes
    stmt = delete(NodeData).where(NodeData.level_id.in_(level_ids))
    await session.execute(stmt)

    # change level objects
    p_ids_for_recalc_child_count = set()
    for level_id in level_ids:
        stmt = select(Obj).where(Obj.level_id == level_id)
        response = await session.execute(stmt)
        level_objects_by_ids = {i.id: i for i in response.scalars()}
        for obj in level_objects_by_ids.values():
            if obj.parent_id:
                p_ids_for_recalc_child_count.add(obj.parent_id)

            # find children and change parent
            stmt = select(Obj).where(Obj.parent_id == obj.id)
            response = await session.execute(stmt)
            for child in response.scalars():  # type: Obj
                child.parent_id = obj.parent_id

            # replace child path
            path = create_path_for_children_node_by_parent_node(parent_node=obj)
            stmt = select(Obj).where(
                Obj.hierarchy_id == hierarchy_id, Obj.path.like(f"{path}%")
            )
            result_generator = await session.stream_scalars(stmt)
            async for partition in result_generator.yield_per(
                LIMIT_OF_POSTGRES_RESULTS_PER_STEP
            ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
                for child_node in partition:
                    child_node.path = child_node.path.replace(path, obj.path)
            await session.delete(obj)

    # recalculate child_count
    if p_ids_for_recalc_child_count:
        await __recalc_child_count(
            node_ids_to_recalc_child_count=p_ids_for_recalc_child_count,
            session=session,
        )

    # change child level parents
    stmt = select(Level).where(Level.parent_id.in_(level_ids))
    response = await session.execute(stmt)
    for child_level in response.scalars():  # type: Level
        level = levels_by_id[child_level.parent_id]
        child_level.parent_id = level.parent_id

    # delete levels
    for level in levels_by_id.values():
        await session.delete(level)

    await session.commit()


class TPRMDeleteHandler(HierarchyChangeInterface):
    async def make_changes(self):
        await with_tprm_delete_event_in_parent_attr(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
        await with_tprm_delete_event(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
