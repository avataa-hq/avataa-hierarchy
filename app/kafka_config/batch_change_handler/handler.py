from abc import abstractmethod
from collections import defaultdict
import time
from typing import List

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from kafka_config.batch_change_handler.utils.utils import (
    get_count_of_hierarchy_nodes,
    rebuild_all_hierarchies_from_order,
    rebuild_hierarchy_and_change_item_of_hierarchy_rebuild_order,
)
from schemas.hier_schemas import HierarchyRebuildOrder, Level


class BatchChangesHandler:
    MINIMUM_CHANGES_COUNT_TO_REBUILD = 100
    # MINIMUM_CHANGES_COUNT_TO_REBUILD - number of changes that can be defined as batch changes.
    DEFAULT_SLEEP_TIME = 30

    # DEFAULT_SLEEP_TIME is used when we need to wait until some hierarchy is rebuilt.

    def __init__(self, session: AsyncSession, message_as_dict: dict):
        self.session = session
        self.message_as_dict = message_as_dict

    @property
    def message_as_dict(self):
        return self._message_as_dict

    @message_as_dict.setter
    def message_as_dict(self, value: dict):
        if not isinstance(value, dict):
            raise TypeError("message_as_dict must be instance of dict")
        self._message_as_dict = value

        self.grouped_message = self.__group_message(value)

    @staticmethod
    @abstractmethod
    def __group_message(message_as_dict: dict):
        """Returns a dict with messages grouped by a specific parameter that can be used in searching for levels.
        Example: tmo_id or tprm_id"""
        pass

    @abstractmethod
    async def get_levels_data(self):
        """Returns levels, nodes of which will be changed by this msg"""
        # stmt = select(Level).where(Level.object_type_id.in_(self.grouped_message))
        pass

    @abstractmethod
    async def get_count_changes_by_hierarchies(self, levels: List[Level]):
        """Returns dict with hierarchy_id as key and dict with count of changes and with hierarchy tmo_ids ot tprm_ids
         as value
        Example: {1: {'changes_count': 1, 'tmo_ids': [1,2,3]}} or {1: {'changes_count': 1, 'tprm_ids': [1,2,3]}}"""
        pass

    @abstractmethod
    async def _separator_for_msg_which_not_connected_to_hierarchies_in_rebuild_order(
        self, changes_by_hierarchy_id: dict
    ):
        """Returns dict with 'objects' as key and list of msg from kafka msg which can be processed
        Examle: {'objects': [...]}"""
        pass

    async def get_msg_ready_for_processing(self):
        """Returns msg which ready to further processing"""
        levels_with_tmos = await self.get_levels_data()
        if not levels_with_tmos:
            return dict(objects=list())

        # get all hierarchies ids from order to rebuild
        stmt = select(HierarchyRebuildOrder)
        hierarchies_in_rebuild_stage = await self.session.execute(stmt)
        hierarchies_in_rebuild_stage = (
            hierarchies_in_rebuild_stage.scalars().all()
        )
        hierarchies_in_rebuild_stage = {
            item.hierarchy_id: item.on_rebuild
            for item in hierarchies_in_rebuild_stage
        }
        changes_by_hierarchy_id = await self.get_count_changes_by_hierarchies(
            levels_with_tmos
        )

        if not hierarchies_in_rebuild_stage:
            res = await self._separator_for_msg_which_not_connected_to_hierarchies_in_rebuild_order(
                changes_by_hierarchy_id
            )
        else:
            intersection = set(changes_by_hierarchy_id).intersection(
                hierarchies_in_rebuild_stage
            )
            if not intersection:
                # if there are hierarchies in rebuild order, and they are not intersected with hierarchies which will
                # be changed by msg - we take hierarchies from order to rebuild with attr on_rebuild = False and
                # rebuild them
                await rebuild_all_hierarchies_from_order(session=self.session)
                res = self.message_as_dict
            else:
                # check if all hierarchies in intersection not at work yet
                stmt = select(HierarchyRebuildOrder).where(
                    HierarchyRebuildOrder.hierarchy_id.in_(intersection),
                    HierarchyRebuildOrder.on_rebuild == True,  # noqa
                )
                at_work = await self.session.execute(stmt)
                at_work = at_work.scalars().all()

                if not at_work:
                    # send all hierarchies changes which not in intersection to separator
                    changes_to_separator = {
                        hierarchy_id: changes
                        for hierarchy_id, changes in changes_by_hierarchy_id.items()
                        if hierarchy_id not in intersection
                    }

                    res = await self._separator_for_msg_which_not_connected_to_hierarchies_in_rebuild_order(
                        changes_to_separator
                    )
                else:
                    # if we use several workers, we need to extract the logic for rebuilding frozen hierarchies
                    # from here
                    # if there are some hierarchies from intersection with on_rebuild = True - we can`t commit
                    # this message and can`t go further, so we need to wait while they rebuilt
                    ids_at_work = {item.hierarchy_id for item in at_work}
                    count_hierarchies_nodes = (
                        await get_count_of_hierarchy_nodes(
                            session=self.session, hierarchy_ids=ids_at_work
                        )
                    )
                    time.sleep(self.DEFAULT_SLEEP_TIME)

                    # control stage
                    stmt = select(HierarchyRebuildOrder).where(
                        HierarchyRebuildOrder.hierarchy_id.in_(intersection),
                        HierarchyRebuildOrder.on_rebuild == True,  # noqa
                    )
                    at_work = await self.session.execute(stmt)
                    at_work = at_work.scalars().all()
                    at_work = {item.hierarchy_id: item for item in at_work}

                    control_data = await get_count_of_hierarchy_nodes(
                        session=self.session, hierarchy_ids=at_work
                    )
                    intersection_ids = set(
                        count_hierarchies_nodes
                    ).intersection(control_data)
                    if intersection_ids:
                        # find hierarchies ids count nodes of which was not changed
                        not_changed = {
                            x
                            for x in intersection_ids
                            if control_data[x] == count_hierarchies_nodes[x]
                        }
                        if not_changed:
                            order_item_to_rebuild = at_work.get(
                                not_changed.pop()
                            )
                            await rebuild_hierarchy_and_change_item_of_hierarchy_rebuild_order(
                                session=self.session,
                                item_to_rebuild=order_item_to_rebuild,
                            )
                        else:
                            time.sleep(self.DEFAULT_SLEEP_TIME)
                        res = await self.get_msg_ready_for_processing()
                    else:
                        res = self.message_as_dict
        return res


class MOChangesHandler(BatchChangesHandler):
    @property
    def message_as_dict(self):
        return self._message_as_dict

    @message_as_dict.setter
    def message_as_dict(self, value: dict):
        if not isinstance(value, dict):
            raise TypeError("message_as_dict must be instance of dict")
        self._message_as_dict = value

        self.grouped_message = self.__group_message(value)

    @staticmethod
    def __group_message(message_as_dict: dict):
        """Returns dict with grouped messages by tmo_id"""
        # group mo_data by tmo_id
        mo_data_by_tmo_ids = defaultdict(list)
        for mo_data in message_as_dict["objects"]:
            mo_data_by_tmo_ids[mo_data["tmo_id"]].append(mo_data)
        return mo_data_by_tmo_ids

    async def get_levels_data(self):
        """Returns levels, nodes of which will be changed by this msg"""
        if not self.grouped_message:
            return []
        stmt = select(Level).where(
            Level.object_type_id.in_(self.grouped_message)
        )
        levels_with_tmos = await self.session.execute(stmt)
        levels_with_tmos = levels_with_tmos.scalars().all()
        return levels_with_tmos

    async def get_count_changes_by_hierarchies(self, levels: List[Level]):
        """Returns dict with hierarchy_id as key and dict with count of changes and with hierarchy tmo_ids as value
        Example: {1: {'changes_count': 1, 'tmo_ids': [1,2,3]}}"""
        changes_by_hierarchy_id = defaultdict(
            lambda: dict(changes_count=0, tmo_ids=set())
        )
        for leve_data in levels:
            hierarchy_data = changes_by_hierarchy_id[leve_data.hierarchy_id]
            hierarchy_data["changes_count"] += len(
                self.grouped_message.get(leve_data.object_type_id)
            )
            hierarchy_data["tmo_ids"].add(leve_data.object_type_id)

        return changes_by_hierarchy_id

    async def _separator_for_msg_which_not_connected_to_hierarchies_in_rebuild_order(
        self, changes_by_hierarchy_id: dict
    ):
        # if hierarchy with small changes have no tmo_id which also present in hierarchies with big changes AND
        # their ids not specified in the rebuild order - we can push data for that hierarchies further
        if not changes_by_hierarchy_id:
            return {"objects": list()}
        # find hierarchies may not need to be rebuilt:
        hierarchy_with_less_changes = {
            k: v
            for k, v in changes_by_hierarchy_id.items()
            if v["changes_count"] < self.MINIMUM_CHANGES_COUNT_TO_REBUILD
        }

        hierarchy_ids_with_more_changes = {
            k: v
            for k, v in changes_by_hierarchy_id.items()
            if v["changes_count"] >= self.MINIMUM_CHANGES_COUNT_TO_REBUILD
        }

        tmos_of_hierarchys_with_big_changes = set()
        for _, hierarchy_data in hierarchy_ids_with_more_changes.items():
            tmos_of_hierarchys_with_big_changes.update(
                hierarchy_data["tmo_ids"]
            )

        msg_can_go_further = []
        if tmos_of_hierarchys_with_big_changes:
            for (
                hierarchy_id,
                hierarchy_data,
            ) in hierarchy_with_less_changes.items():
                hierarchy_tmo_ids = hierarchy_data["tmo_ids"]
                if not hierarchy_tmo_ids.intersection(
                    tmos_of_hierarchys_with_big_changes
                ):
                    for tmo_id in hierarchy_tmo_ids:
                        msg_can_go_further.extend(self.grouped_message[tmo_id])
                else:
                    hierarchy_ids_with_more_changes.add(hierarchy_id)
        else:
            msg_can_go_further = self.message_as_dict["objects"]

        # if there are new hierarchies with big changes - add to order

        for hierarchy_id in hierarchy_ids_with_more_changes:
            self.session.add(HierarchyRebuildOrder(hierarchy_id=hierarchy_id))

        await self.session.commit()

        return {"objects": msg_can_go_further}


class PRMChangesHandler(BatchChangesHandler):
    @property
    def message_as_dict(self):
        return self._message_as_dict

    @message_as_dict.setter
    def message_as_dict(self, value: dict):
        if not isinstance(value, dict):
            raise TypeError("message_as_dict must be instance of dict")
        self._message_as_dict = value

        self.grouped_message = self.__group_message(value)

    @staticmethod
    def __group_message(message_as_dict: dict):
        """Returns dict with grouped messages by TPRM"""
        # group mo_data by tprm_id
        prm_data_by_tmo_ids = defaultdict(list)
        for prm_data in message_as_dict["objects"]:
            prm_data_by_tmo_ids[prm_data["tprm_id"]].append(prm_data)

        return prm_data_by_tmo_ids

    async def get_levels_data(self):
        """Returns levels, nodes of which will be changed by this msg"""
        if not self.grouped_message:
            return []
        tprm_ids = list(self.grouped_message)
        stmt = select(Level).where(
            or_(
                Level.param_type_id.in_(tprm_ids),
                Level.additional_params_id.in_(tprm_ids),
                Level.latitude_id.in_(tprm_ids),
                Level.longitude_id.in_(tprm_ids),
            )
        )
        levels = await self.session.execute(stmt)
        levels = levels.scalars().all()
        return levels

    async def get_count_changes_by_hierarchies(self, levels: List[Level]):
        """Returns dict with hierarchy_id as key and dict with count of changes and with hierarchy tmo_ids as value
        Example: {1: {'changes_count': 1, 'tmo_ids': [1,2,3]}}"""
        changes_by_hierarchy_id = defaultdict(
            lambda: dict(changes_count=0, tprm_ids=set())
        )
        for leve_data in levels:
            tprm_attrs = {
                leve_data.param_type_id,
                leve_data.additional_params_id,
                leve_data.latitude_id,
                leve_data.longitude_id,
            }
            level_tprms = {
                tprm_id for tprm_id in tprm_attrs if tprm_id is not None
            }
            hierarchy_data = changes_by_hierarchy_id[leve_data.hierarchy_id]

            for tprm_id in level_tprms:
                msg_list = self.grouped_message.get(tprm_id, None)
                if isinstance(msg_list, list):
                    hierarchy_data["tprm_ids"].add(tprm_id)
                    count = len(msg_list)
                else:
                    count = 0
                hierarchy_data["changes_count"] += count

        return changes_by_hierarchy_id

    async def _separator_for_msg_which_not_connected_to_hierarchies_in_rebuild_order(
        self, changes_by_hierarchy_id: dict
    ):
        # if hierarchy with small changes have no tmo_id which also present in hierarchies with big changes AND
        # their ids not specified in the rebuild order - we can push data for that hierarchies further

        if not changes_by_hierarchy_id:
            return {"objects": list()}
        # find hierarchies may not need to be rebuilt:
        hierarchy_with_less_changes = {
            k: v
            for k, v in changes_by_hierarchy_id.items()
            if v["changes_count"] < self.MINIMUM_CHANGES_COUNT_TO_REBUILD
        }

        hierarchy_ids_with_more_changes = {
            k: v
            for k, v in changes_by_hierarchy_id.items()
            if v["changes_count"] >= self.MINIMUM_CHANGES_COUNT_TO_REBUILD
        }

        tprms_of_hierarchys_with_big_changes = set()
        for _, hierarchy_data in hierarchy_ids_with_more_changes.items():
            tprms_of_hierarchys_with_big_changes.update(
                hierarchy_data["tprm_ids"]
            )

        msg_can_go_further = []
        if tprms_of_hierarchys_with_big_changes:
            for (
                hierarchy_id,
                hierarchy_data,
            ) in hierarchy_with_less_changes.items():
                hierarchy_tmo_ids = hierarchy_data["tprm_ids"]
                if not hierarchy_tmo_ids.intersection(
                    tprms_of_hierarchys_with_big_changes
                ):
                    for tmo_id in hierarchy_tmo_ids:
                        msg_can_go_further.extend(self.grouped_message[tmo_id])
                else:
                    hierarchy_ids_with_more_changes[hierarchy_id] = (
                        hierarchy_data
                    )
        else:
            msg_can_go_further = self.message_as_dict["objects"]

        # if there are new hierarchies with big changes - add to order

        for hierarchy_id in hierarchy_ids_with_more_changes:
            self.session.add(HierarchyRebuildOrder(hierarchy_id=hierarchy_id))

        await self.session.commit()

        return {"objects": msg_can_go_further}
