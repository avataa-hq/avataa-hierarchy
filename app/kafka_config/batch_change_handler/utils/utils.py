from typing import Iterable, List, Union

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common_utils.hierarchy_builder import HierarchyBuilder
from schemas.hier_schemas import HierarchyRebuildOrder, Obj


async def get_list_of_hierarchies_on_rebuild_stage(
    session: AsyncSession,
) -> bool:
    """Returns HierarchyRebuildOrder with ids of hierarchies and their stage"""
    stmt = select(HierarchyRebuildOrder)
    res = await session.session.execute(stmt)
    res = res.scalars().first()
    return res


async def get_item_from_hierarchy_rebuild_order(
    session: AsyncSession,
) -> Union[HierarchyRebuildOrder, None]:
    """Returns item from hierarchy rebuild order otherwise None"""
    stmt = select(HierarchyRebuildOrder).where(
        HierarchyRebuildOrder.on_rebuild == False  # noqa: E712
    )
    item_to_rebuild = await session.execute(stmt)
    item_to_rebuild = item_to_rebuild.scalars().first()
    return item_to_rebuild


async def get_items_from_hierarchy_rebuild_order_by_hierarchy_ids(
    session: AsyncSession, hierarchy_ids: Iterable[int], on_rebuild_status: bool
) -> List[HierarchyRebuildOrder]:
    """Returns list of HierarchyRebuildOrder"""
    stmt = select(HierarchyRebuildOrder).where(
        HierarchyRebuildOrder.hierarchy_id.in_(hierarchy_ids),
        HierarchyRebuildOrder.on_rebuild == on_rebuild_status,
    )
    item_to_rebuild = await session.execute(stmt)
    item_to_rebuild = item_to_rebuild.scalars().first()
    return item_to_rebuild


async def rebuild_hierarchy_and_change_item_of_hierarchy_rebuild_order(
    session: AsyncSession, item_to_rebuild: HierarchyRebuildOrder
) -> None:
    """Changes status of HierarchyRebuildOrder instance, rebuilds hierarchy and delete instance
    of HierarchyRebuildOrder from order"""
    print(f"Hierarchy to rebuild {item_to_rebuild}")
    if not item_to_rebuild.on_rebuild:
        item_to_rebuild.on_rebuild = True
        try:
            session.add(item_to_rebuild)
            print(f"Status was changed {item_to_rebuild}")
            await session.commit()
        except Exception:
            print(str(Exception))
            return

    keeper = HierarchyBuilder(
        db_session=session, hierarchy_id=item_to_rebuild.hierarchy_id
    )

    try:
        await keeper.build_hierarchy()
        print("Hierarchy successfully rebuilt")
    except IntegrityError as e:
        print(e)

    try:
        print("Hierarchy to delete from order")
        print(item_to_rebuild)
        await session.delete(item_to_rebuild)
        print("Hierarchy was deleted from order")
    except Exception:
        print(str(Exception))
        pass
    finally:
        await session.commit()


async def rebuild_all_hierarchies_from_order(
    session: AsyncSession, item_to_rebuild: HierarchyRebuildOrder = None
) -> None:
    """Rebuilds all hierarchies with attr on_rebuild = False - one hierarchy per iteration"""
    if item_to_rebuild is None:
        item_to_rebuild = await get_item_from_hierarchy_rebuild_order(session)

    if item_to_rebuild:
        await rebuild_hierarchy_and_change_item_of_hierarchy_rebuild_order(
            session, item_to_rebuild
        )
    else:
        return

    item_to_rebuild = await get_item_from_hierarchy_rebuild_order(session)
    if item_to_rebuild:
        await rebuild_all_hierarchies_from_order(session, item_to_rebuild)


async def get_count_of_hierarchy_nodes(
    session: AsyncSession, hierarchy_ids: Iterable[int]
) -> dict:
    """Returns count of hierarchy nodes grouped by hierarchy id"""
    stmt = (
        select(Obj.hierarchy_id, func.count(Obj.id).label("count"))
        .where(Obj.hierarchy_id.in_(hierarchy_ids))
        .group_by(Obj.hierarchy_id)
    )
    res = await session.execute(stmt)
    res = res.all()
    return {item.hierarchy_id: item.count for item in res}
