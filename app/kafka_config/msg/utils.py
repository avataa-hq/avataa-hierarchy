"""Utils for kafka msg handlers"""

from typing import Any, Union

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from common_utils.hierarchy_builder import (
    DEFAULT_KEY_OF_NULL_NODE,
    HierarchyBuilder,
)
from kafka_config.connection_handler.handler import KafkaConnectionHandler
from schemas.hier_schemas import Level, Obj


async def get_parent_change_child_count_and_check(
    parent_id, session: AsyncSession
):
    """Recursive function to check parent object. And if parent object is virtual and have child_count
    equal to 0 - delete this parent object and make same check for parent of deleted parent objects"""
    if parent_id is None:
        return
    stm = select(Obj).where(Obj.id == parent_id)
    res = await session.execute(stm)
    parent = res.scalars().first()
    if parent is None:
        return

    parent.child_count -= 1

    if parent.object_id is None and parent.child_count == 0:
        change_and_check = parent.parent_id
        await session.delete(parent)
        await get_parent_change_child_count_and_check(change_and_check, session)
    else:
        session.add(parent)
    return


async def refresh_child_count_for_parent_obj_with_level_delete(
    level_id: int, session: AsyncSession
):
    """Gets all child elements of the remote level.
    Finds their parents and changes the parent parameter child_count.
    If virtual object has no children - deletes it.
    Call before level is deleted!!!."""

    stm = (
        select(Obj.parent_id, func.count(Obj.parent_id).label("count"))
        .where(Obj.level_id == level_id)
        .group_by(Obj.parent_id)
    )
    res = await session.execute(stm)
    res = res.fetchall()

    all_parent_dict = {x[0]: x[1] for x in res}

    stm = select(Obj).where(Obj.id.in_(all_parent_dict.keys()))
    all_parents = await session.execute(stm)
    all_parents = all_parents.scalars().all()

    for parent in all_parents:
        minus = all_parent_dict[parent.id]
        parent.child_count = parent.child_count - minus
        if parent.object_id is None and parent.child_count == 0:
            change_and_check = parent.parent_id
            await session.delete(parent)
            await get_parent_change_child_count_and_check(
                change_and_check, session
            )
        else:
            session.add(parent)
    await session.commit()


async def get_all_children_levels_by_parent_level_id_and_tmo(
    parent_level_id, tmo_id, session: AsyncSession
):
    """Returns chain of children levels with object_type_id = tmo_id for level with id = parent_level_id."""
    stm = select(Level).where(
        Level.parent_id == parent_level_id, Level.object_type_id == tmo_id
    )
    children_levels = await session.execute(stm)
    children_levels = children_levels.scalars().all()
    if len(children_levels) == 0:
        return []

    else:
        list_for_level = children_levels
        for level in children_levels:
            list_for_level += (
                await get_all_children_levels_by_parent_level_id_and_tmo(
                    level.id, tmo_id, session
                )
            )
        return list_for_level


async def recursive_find_short_chaine_of_parents_levels(
    level: Level, session: AsyncSession
) -> list[Level]:
    """Looks for a path to the nearest parent with a different object_type_id (tmo)
    and returns ordered list of parent levels for this level."""
    if level.parent_id is None:
        return []
    stm = select(Level).where(Level.id == level.parent_id)
    parent = await session.execute(stm)
    parent = parent.scalars().first()
    if parent.object_type_id != level.object_type_id:
        return [parent]
    else:
        return [parent] + await recursive_find_short_chaine_of_parents_levels(
            parent, session
        )


async def recursive_find_full_chaine_of_parents_levels(
    level: Level, session: AsyncSession
) -> list[Level]:
    """Looks for a path to the highest parent level
    and returns ordered list of parent levels for this level."""
    if level.parent_id is None:
        return []
    stm = select(Level).where(Level.id == level.parent_id)
    parent = await session.execute(stm)
    parent = parent.scalars().first()
    return [parent] + await recursive_find_full_chaine_of_parents_levels(
        parent, session
    )


async def recursive_find_parent_nodes_and_levels(
    node_obj: Obj, session: AsyncSession
):
    """Looks for a path to the nearest parent with a different object_type_id (tmo)
    and returns ordered list of parent levels for this level."""
    if node_obj.parent_id is None:
        return []
    node = aliased(Obj, name="node")
    level = aliased(Level, name="level")
    stm = (
        select(node, level)
        .join(level, node.level_id == level.id)
        .where(node.id == node_obj.parent_id)
    )
    tuple_node_level = await session.execute(stm)
    tuple_node_level = tuple_node_level.first()
    if tuple_node_level["node"].object_id is not None:
        return [tuple_node_level]
    else:
        return [
            tuple_node_level
        ] + await recursive_find_parent_nodes_and_levels(
            tuple_node_level["node"], session
        )


async def add_values_to_existing_note_instance(
    tprm_id: int, level_inst: Level, node_inst: Obj, new_value: Any
) -> Obj:
    """
    Adds new_value for parameter of existing node instance (node_inst) and returns changed node.
    If new value == None, and changed parameter is key - set key to DEFAULT_KEY_OF_NULL_NODE.
    If new value == None, and changed parameter is additional_params or
    latitude or longitude - set additional_params or latitude or longitude  to None
    :param tprm_id: id of tprm which must value be changed
    :param level_inst: Level instance - level of node_inst
    :param node_inst: Obj instance - node, which parameter values must be changed
    :param new_value: new value for node parameter
    :returns: changed node: Obj
    """

    level_attrs_mo_attrs_int_float_can_be_null = {
        "latitude_id": "latitude",
        "longitude_id": "longitude",
    }

    level_attrs_mo_attrs_str_can_be_null = {
        "additional_params_id": "additional_params"
    }

    for (
        level_attr,
        node_attr,
    ) in level_attrs_mo_attrs_int_float_can_be_null.items():
        level_attr_id = getattr(level_inst, level_attr, None)
        if level_attr_id == tprm_id:
            if new_value:
                new_value = float(new_value)
            setattr(node_inst, node_attr, new_value)

    for level_attr, node_attr in level_attrs_mo_attrs_str_can_be_null.items():
        level_attr_id = getattr(level_inst, level_attr, None)
        if level_attr_id == tprm_id:
            setattr(node_inst, node_attr, str(new_value))

    if level_inst.param_type_id == tprm_id:
        node_inst.key = (
            str(new_value)
            if new_value is not None
            else DEFAULT_KEY_OF_NULL_NODE
        )

    return node_inst


async def rebuild_branch_for_real_node_when_mo_attr_was_updated(
    node_inst: Obj,
    row_parent_node: Union[Obj, None],
    level_inst: Level,
    mo_new_prm_value: Any,
    item_tprm: int,
    session: AsyncSession,
):
    """
    Based on the new mo attribute value, rebuilds a branch of parents for existing real node

    :param node_inst: real Node
    :param row_parent_node: parent Node of node_inst
    :param level_inst: level of node_inst
    :param mo_new_prm_value: new attribute value of MO
    :param item_tprm: tprm id of MO which value we need to change
    :param session: AsyncSession instance
    :return: None
    """
    if row_parent_node is None or row_parent_node.object_id is not None:
        # if parent is not virtual or not exist

        node_inst = await add_values_to_existing_note_instance(
            tprm_id=item_tprm,
            level_inst=level_inst,
            node_inst=node_inst,
            new_value=mo_new_prm_value,
        )
        session.add(node_inst)

    else:
        # if parent is virtual
        # get short chaine of parent levels
        parent_nodes_and_levels = await recursive_find_parent_nodes_and_levels(
            node_inst, session
        )

        # is any parent level has param_type_id eq to tprm_id
        is_any_level = any(
            item["level"].param_type_id == item_tprm
            for item in parent_nodes_and_levels
        )

        if is_any_level:
            reverse_order_of_p_nodes_and_levels = parent_nodes_and_levels[::-1]

            cache_of_nodes = dict()

            if row_parent_node.parent_id is not None:
                cache_of_nodes[row_parent_node.level_id] = row_parent_node

            last_row_level_id = ""
            is_new_virtual_node_created = False

            for tuple_node_and_level in reverse_order_of_p_nodes_and_levels:
                row_node = tuple_node_and_level["node"]
                row_level = tuple_node_and_level["level"]
                last_row_level_id = row_level.id
                parent_node = cache_of_nodes.get(row_level.parent_id, None)

                if row_node.object_id is not None:
                    cache_of_nodes[row_level.id] = row_node
                    continue

                if row_level.param_type_id != item_tprm:
                    if (
                        is_new_virtual_node_created
                        and row_node.object_id is None
                    ):
                        new_node = dict(row_node)
                        del new_node["id"]
                        new_node = Obj.parse_obj(new_node)
                        new_node.child_count = 0
                        new_node.parent_id = (
                            parent_node.id if parent_node is not None else None
                        )
                        session.add(new_node)
                        await session.flush()
                        cache_of_nodes[row_level.id] = new_node

                        if parent_node:
                            parent_node.child_count += 1
                            session.add(parent_node)

                    else:
                        stm_p_id = (
                            parent_node.id if parent_node is not None else None
                        )
                        stm = select(Obj).where(
                            Obj.level_id == row_level.id,
                            Obj.key == row_node.key,
                            Obj.parent_id == stm_p_id,
                        )

                        virtual_node = await session.execute(stm)
                        virtual_node = virtual_node.scalars().first()

                        if virtual_node is not None:
                            if is_new_virtual_node_created:
                                new_node = dict(virtual_node)
                                del new_node["id"]
                                virtual_node = Obj.parse_obj(new_node)
                                virtual_node.child_count = 0
                                virtual_node.parent_id = (
                                    parent_node.id
                                    if parent_node is not None
                                    else None
                                )
                                session.add(virtual_node)

                                await session.flush()
                        else:
                            new_node = dict(row_node)
                            del new_node["id"]
                            virtual_node = Obj.parse_obj(new_node)
                            virtual_node.child_count = 0
                            virtual_node.parent_id = (
                                parent_node.id
                                if parent_node is not None
                                else None
                            )
                            session.add(virtual_node)

                            if parent_node is not None:
                                parent_node.child_count += 1
                                session.add(parent_node)
                            await session.flush()

                        cache_of_nodes[row_level.id] = virtual_node

                        # end
                    continue

                if (
                    row_level.param_type_id == item_tprm
                    and row_node.object_id is None
                ):
                    stm_p_id = (
                        parent_node.id if parent_node is not None else None
                    )
                    stm = select(Obj).where(
                        Obj.level_id == row_level.id,
                        Obj.key == str(mo_new_prm_value),
                        Obj.parent_id == stm_p_id,
                    )

                    virtual_node = await session.execute(stm)
                    virtual_node = virtual_node.scalars().first()

                    if virtual_node is not None:
                        if is_new_virtual_node_created:
                            new_node = dict(virtual_node)
                            del new_node["id"]
                            virtual_node = Obj.parse_obj(new_node)
                            virtual_node.child_count = 0
                            virtual_node.parent_id = (
                                parent_node.id
                                if parent_node is not None
                                else None
                            )
                            session.add(virtual_node)

                            await session.flush()

                        cache_of_nodes[row_level.id] = virtual_node

                    else:
                        key_val = (
                            mo_new_prm_value
                            if mo_new_prm_value is not None
                            else DEFAULT_KEY_OF_NULL_NODE
                        )
                        # create new
                        new_virtual_node = Obj(
                            key=str(key_val),
                            object_id=row_node.object_id,
                            object_type_id=row_node.object_type_id,
                            additional_params=row_node.additional_params,
                            hierarchy_id=row_node.hierarchy_id,
                            level=row_node.level,
                            latitude=row_node.latitude,
                            longitude=row_node.longitude,
                            child_count=0,
                            level_id=row_node.level_id,
                            parent_id=parent_node.id
                            if parent_node is not None
                            else None,
                        )

                        session.add(new_virtual_node)
                        is_new_virtual_node_created = True

                        if parent_node is not None:
                            parent_node.child_count += 1
                            session.add(parent_node)
                        await session.flush()
                        cache_of_nodes[row_level.id] = new_virtual_node

                    continue

            new_parent_id = cache_of_nodes[last_row_level_id].id
            old_parent_id = node_inst.parent_id
            node_inst.parent_id = new_parent_id

            new_parent = cache_of_nodes[last_row_level_id]
            new_parent.child_count += 1
            session.add(new_parent)

            node_inst = await add_values_to_existing_note_instance(
                tprm_id=item_tprm,
                level_inst=level_inst,
                node_inst=node_inst,
                new_value=mo_new_prm_value,
            )

            session.add(node_inst)
            await session.flush()

            if new_parent_id != old_parent_id and old_parent_id is not None:
                await get_parent_change_child_count_and_check(
                    old_parent_id, session
                )

        else:
            node_inst = await add_values_to_existing_note_instance(
                tprm_id=item_tprm,
                level_inst=level_inst,
                node_inst=node_inst,
                new_value=mo_new_prm_value,
            )

            session.add(node_inst)

    await session.commit()


async def rebuild_hierarchy_if_level_condition(
    session: AsyncSession, tmo_ids: iter
):
    """For each tmo_id tries to find virtual levels which has no child levels or their child has different tmo_id
    and if such levels exist rebuild hierarchy with id = level.hierarchy_id"""
    hierarchy_to_update = set()

    for tmo_id in tmo_ids:
        # get all virtual levels which has no child levels or their child has different tmo_id
        parent_level = aliased(Level, name="parent_level")
        child_level = aliased(Level, name="child_level")
        stmt = (
            select(
                parent_level.id.label("parent_level_id"),
                parent_level.hierarchy_id.label("parent_level_h_id"),
                child_level.object_type_id.label("child_level_object_type_id"),
            )
            .join(
                child_level,
                child_level.parent_id == parent_level.id,
                isouter=True,
            )
            .where(
                parent_level.object_type_id == tmo_id,
                parent_level.is_virtual == True,  # noqa: E712
            )
        )

        subq = stmt.subquery()
        stmt = (
            select(subq.c.parent_level_h_id)
            .where(
                or_(
                    subq.c.child_level_object_type_id == None,  # noqa
                    subq.c.child_level_object_type_id != tmo_id,
                )
            )
            .distinct()
        )

        res = await session.execute(stmt)
        res = set(res.scalars().all())
        hierarchy_to_update.update(res)

    if hierarchy_to_update:
        kafka_connection_handler = KafkaConnectionHandler()
        if kafka_connection_handler.consumer is not None:
            kafka_connection_handler.consumer.commit()
        kafka_connection_handler.disconnect_from_kafka_topic()
        for hierarchy_id in hierarchy_to_update:
            h = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy_id)
            await h.build_hierarchy()
        kafka_connection_handler.connect_to_kafka_topic()


async def rebuild_hierarchy_if_level_condition_by_tprm(
    session: AsyncSession, tprm_ids: iter
):
    """For tries to find virtual (by tprm id in params) levels which has no child levels or their child has different
    tmo_id and if such levels exist rebuild hierarchy with id = level.hierarchy_id"""
    hierarchy_to_update = set()

    # get all virtual levels which has no child levels or their child has different tmo_id
    parent_level = aliased(Level, name="parent_level")
    child_level = aliased(Level, name="child_level")
    stmt = (
        select(
            parent_level.id.label("parent_level_id"),
            parent_level.object_type_id.label("parent_level_object_type_id"),
            parent_level.hierarchy_id.label("parent_level_h_id"),
            child_level.object_type_id.label("child_level_object_type_id"),
        )
        .join(
            child_level, child_level.parent_id == parent_level.id, isouter=True
        )
        .where(
            parent_level.is_virtual == True,  # noqa
            parent_level.param_type_id.in_(tprm_ids),
        )
    )

    subq = stmt.subquery()
    stmt = (
        select(subq.c.parent_level_h_id)
        .where(
            or_(
                subq.c.child_level_object_type_id == None,  # noqa
                subq.c.child_level_object_type_id
                != subq.c.parent_level_object_type_id,
            )
        )
        .distinct()
    )

    res = await session.execute(stmt)
    res = set(res.scalars().all())

    hierarchy_to_update.update(res)

    if hierarchy_to_update:
        kafka_connection_handler = KafkaConnectionHandler()
        if kafka_connection_handler.consumer is not None:
            kafka_connection_handler.consumer.commit()
        kafka_connection_handler.disconnect_from_kafka_topic()
        for hierarchy_id in hierarchy_to_update:
            h = HierarchyBuilder(db_session=session, hierarchy_id=hierarchy_id)
            await h.build_hierarchy()
        kafka_connection_handler.connect_to_kafka_topic()


def create_node_data_unfolded_key(
    ordered_key_attrs: list[str], mo_data_with_params: dict[str, Any]
):
    return {k: mo_data_with_params.get(k) for k in ordered_key_attrs}
