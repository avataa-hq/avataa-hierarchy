from collections import defaultdict
import copy

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from grpc_config.inventory_utils import get_mo_links_tprms
from schemas.hier_schemas import Level, NodeData, Obj
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
    get_node_key_data,
)
from services.updater.event_handlers.common.handler_interface import (
    HierarchyChangeInterface,
)
from services.updater.event_handlers.common.msg_utils import (
    __recalc_child_count,
    rebuilding_nodes_based_on_their_data,
)
from settings import LIMIT_OF_POSTGRES_RESULTS_PER_STEP


async def with_prm_create_event(msg, session: AsyncSession, hierarchy_id: int):
    """Makes changes to the hierarchy with event 'PRM:created'."""
    unique_tprm_ids_as_str = set()
    prm_data_grouped_by_mo_ids = defaultdict(dict)
    for item in msg["objects"]:
        item_tprm_id_as_str = str(item["tprm_id"])
        unique_tprm_ids_as_str.add(item_tprm_id_as_str)
        item_value = item["value"] if item["value"] != "None" else None
        item_mo_id = item["mo_id"]
        prm_data_grouped_by_mo_ids[item_mo_id][item_tprm_id_as_str] = item_value

    stmt = (
        select(Level)
        .where(
            Level.hierarchy_id == hierarchy_id,
            Level.key_attrs.overlap(unique_tprm_ids_as_str),
        )
        .order_by(Level.object_type_id.desc(), Level.level.asc())
    )
    all_levels = await session.execute(stmt)
    all_levels = all_levels.scalars().all()

    top_level_by_tmo = dict()

    for level in all_levels:
        level_in_cache = top_level_by_tmo.get(level.object_type_id)
        if not level_in_cache:
            top_level_by_tmo[level.object_type_id] = level

    for _, level in top_level_by_tmo.items():
        stmt = select(NodeData).where(
            NodeData.level_id == level.id,
            NodeData.mo_id.in_(prm_data_grouped_by_mo_ids),
        )
        level_node_data = await session.execute(stmt)
        level_node_data = level_node_data.scalars().all()

        prm_ids_as_str_for_node_key = {
            key_attr for key_attr in level.key_attrs if key_attr.isdigit()
        }
        changed_node_datas = list()
        for node_data in level_node_data:
            dict_of_new_prm_data = prm_data_grouped_by_mo_ids.get(
                node_data.mo_id
            )
            if dict_of_new_prm_data:
                shared_keys = (
                    prm_ids_as_str_for_node_key & dict_of_new_prm_data.keys()
                )

                if shared_keys:
                    new_data = copy.deepcopy(node_data.unfolded_key)
                    mo_links_tprms = await get_mo_links_tprms(
                        tmo_id=level.object_type_id
                    )
                    for k in shared_keys:
                        if int(k) in mo_links_tprms:
                            key_data = get_node_key_data(
                                ordered_key_attrs=list(shared_keys),
                                mo_data_with_params=dict_of_new_prm_data,
                                mo_links_attrs=mo_links_tprms,
                            )
                            new_data[k] = key_data.key
                        else:
                            new_data[k] = dict_of_new_prm_data[k]
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


async def with_prm_create_event_parent_attr(
    msg, session: AsyncSession, hierarchy_id: int
):
    tprm_ids = set()
    prms_by_mo_ids = defaultdict(list)
    for msg_obj in msg["objects"]:
        tprm_ids.add(msg_obj["tprm_id"])
        prms_by_mo_ids[msg_obj["mo_id"]].append(msg_obj)
    mo_ids = list(prms_by_mo_ids.keys())

    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy_id, Level.attr_as_parent.in_(tprm_ids)
    )
    response = await session.execute(stmt)
    levels_by_id = {}
    for level in response.scalars():
        levels_by_id[level.id] = level

    level_ids = list(levels_by_id.keys())
    if not level_ids:
        return

    p_ids_for_recalc_child_count = set()

    stmt = select(Obj).where(
        Obj.object_id.in_(mo_ids), Obj.level_id.in_(level_ids)
    )
    response = await session.execute(stmt)
    for obj in response.scalars():  # type: Obj
        level = levels_by_id[obj.level_id]
        if not level:
            continue

        prm = None
        for prm_data in prms_by_mo_ids[obj.object_id]:
            if prm_data["tprm_id"] != level.attr_as_parent:
                continue
            prm = prm_data
            break
        if not prm:
            continue

        # find new parent obj
        stmt = select(Obj).where(
            Obj.level_id == level.id, Obj.object_id == prm["value"]
        )
        response = await session.execute(stmt)
        parent_obj: Obj | None = response.scalar_one_or_none()
        if not parent_obj:
            continue

        if obj.parent_id:
            p_ids_for_recalc_child_count.add(obj.parent_id)
        p_ids_for_recalc_child_count.add(parent_obj.id)

        obj.parent_id = parent_obj.id
        old_path = create_path_for_children_node_by_parent_node(parent_node=obj)
        obj.path = create_path_for_children_node_by_parent_node(
            parent_node=parent_obj
        )
        new_path = create_path_for_children_node_by_parent_node(parent_node=obj)

        # replace child path
        stmt = select(Obj).where(
            Obj.hierarchy_id == hierarchy_id, Obj.path.like(f"{old_path}%")
        )
        result_generator = await session.stream_scalars(stmt)
        async for partition in result_generator.yield_per(
            LIMIT_OF_POSTGRES_RESULTS_PER_STEP
        ).partitions(LIMIT_OF_POSTGRES_RESULTS_PER_STEP):
            for child_node in partition:
                child_node.path = child_node.path.replace(old_path, new_path, 1)

    # recalculate child_count
    if p_ids_for_recalc_child_count:
        await __recalc_child_count(
            node_ids_to_recalc_child_count=p_ids_for_recalc_child_count,
            session=session,
        )

    await session.flush()


class PRMCreateHandler(HierarchyChangeInterface):
    async def make_changes(self):
        await with_prm_create_event_parent_attr(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
        await with_prm_create_event(
            msg=self.msg, session=self.session, hierarchy_id=self.hierarchy_id
        )
