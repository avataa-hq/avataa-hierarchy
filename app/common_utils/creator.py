import copy
import json
import warnings

from aiohttp import ClientSession
from pydantic import parse_obj_as
import sqlalchemy
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from common_utils.node import Node
from common_utils.notifier import Notifier
from schemas.hier_schemas import Hierarchy, Level, Obj
from schemas.invetory_schemas import ObjectWithParams

changes_list = []


class HierarchyKeeper:
    def __init__(
        self,
        db_session: AsyncSession,
        request_session: ClientSession,
        token: dict,
        limit=50,
        url="/api/inventory/v1/objects/",
    ):
        self._db_session = db_session
        self._inv_objects_cache = dict()
        self._request_session = request_session
        self.__limit = limit
        self.__url_inv_objects = url
        self.__token = token

    async def create_from_levels(self, levels: list[Level]) -> set[Node]:
        is_one_hierarchy = len(set([x.hierarchy_id for x in levels])) == 1
        if not is_one_hierarchy:
            raise ValueError("Levels must belong to the same hierarchy")
        levels = copy.deepcopy(levels)
        root_nodes: set[Node] | None = None
        previous_level = None
        previous_level_nodes: list[Node] | None = None
        while len(levels) > 0:
            level = self._get_next_level(levels)
            levels.remove(level)

            object_type_id = level.object_type_id
            objects: list[ObjectWithParams] = await self._get_objects(
                object_type_id
            )
            nodes: list[Node] = await self._get_nodes(objects, level)
            if root_nodes is None:
                root_nodes = set(nodes)

            self._connect_nodes(previous_level_nodes, nodes)

            if previous_level == level.level:
                previous_level_nodes.extend(nodes)
            else:
                previous_level_nodes = nodes
            previous_level = level.level
        return root_nodes

    async def get_from_db(self, hierarchy_id: int | None = None) -> set[Node]:
        nodes = await self._get_nodes_list_from_db_records(hierarchy_id)
        nodes = self._structure_nodes(nodes)
        return set(nodes)

    async def _get_nodes_list_from_db_records(
        self, hierarchy_id: int | None = None
    ) -> list[Node]:
        query = select(Obj)
        if hierarchy_id is not None:
            query = query.where(Obj.hierarchy_id == hierarchy_id)
        records = await self._db_session.execute(query)
        records = records.scalars().all()
        nodes = []
        for record in records:
            data = {
                "key": record.key,
                "object_type_id": record.object_type_id,
                "latitude": record.latitude,
                "longitude": record.longitude,
                "additional_params": record.additional_params,
                "hierarchy_id": record.hierarchy_id,
                "level": record.level,
                "level_id": record.level_id,
            }
            data = {k: v for k, v in data.items() if v}
            node = Node(
                object_ids=record.object_id,
                data=data,
                parent_ids=record.parent_id,
                node_id=record.id,
                is_virtual=record.object_id is None,
            )
            nodes.append(node)
        return nodes

    @staticmethod
    def _structure_nodes(nodes: list[Node]) -> list[Node]:
        nodes_by_id = {}
        for node in nodes:
            nodes_by_id[node.id] = node
        for node in nodes[:]:
            if node.parent_ids is None:
                continue
            parent_id = next(iter(node.parent_ids))
            if parent_id not in nodes_by_id:
                warnings.warn(f"Element  with id={node.id} has no parent")
            else:
                nodes.remove(node)
                nodes_by_id[parent_id].add_child(node)
        return nodes

    @staticmethod
    def _get_next_level(levels: list[Level]) -> Level:
        levels_dict = {}
        for level in levels:
            if level.level not in levels_dict:
                levels_dict[level.level] = []
            levels_dict[level.level].append(level)
        min_level = min(levels_dict.keys())
        levels_list = levels_dict[min_level]
        next_level = levels_list[0]
        return next_level

    async def _get_objects(self, object_type_id: int) -> list[ObjectWithParams]:
        # check "cache"
        if object_type_id in self._inv_objects_cache:
            return self._inv_objects_cache[object_type_id]

        page = 0
        all_objects = []
        while True:
            headers = {
                "Authorization": f"""Bearer {self.__token["credentials"]}"""
            }
            params = {
                "object_type_id": object_type_id,
                "limit": self.__limit,
                "offset": page * self.__limit,
                "with_parameters": "True",
            }
            async with self._request_session.get(
                self.__url_inv_objects, params=params, headers=headers
            ) as owp:
                objects = await owp.json()
                objects = parse_obj_as(list[ObjectWithParams], objects)
                all_objects.extend(objects)
                if len(objects) < self.__limit:
                    break
                else:
                    page += 1
        self._inv_objects_cache[object_type_id] = all_objects
        return all_objects

    async def _get_nodes(
        self, objects: list[ObjectWithParams], level: Level
    ) -> list[Node]:
        nodes = []
        for obj in objects:
            node_data = dict()
            for param in obj.params:
                match param.tprm_id:
                    case level.param_type_id:
                        node_data["key"] = param.value
                    case level.additional_params_id:
                        node_data["additional_params"] = param.value
                    case level.latitude_id:
                        node_data["latitude"] = (
                            float(param.value) if param.value else None
                        )
                    case level.longitude_id:
                        node_data["longitude"] = (
                            float(param.value) if param.value else None
                        )
            if len(node_data) == 0:
                continue
            node_data["hierarchy_id"] = level.hierarchy_id
            node_data["level"] = level.level
            node_data["level_id"] = level.id
            node_data["object_type_id"] = level.object_type_id
            node = Node(
                object_ids=obj.id,
                data=node_data,
                parent_ids=obj.p_id,
                is_virtual=level.is_virtual,
            )
            nodes.append(node)
        if level.is_virtual:
            nodes = self._group_nodes(nodes)
        return nodes

    @staticmethod
    def _group_nodes(nodes: list[Node]):
        groups = {}
        for node in nodes:
            p_id = next(iter(node.parent_ids))
            if p_id not in groups:
                groups[p_id] = {}
            node_data = frozenset(node.data.items())
            if node_data not in groups[p_id]:
                groups[p_id][node_data]: Node = node
            else:
                groups[p_id][node_data].object_ids.update(node.object_ids)
        nodes = [vv for v in groups.values() for vv in v.values()]
        return nodes

    @staticmethod
    def _connect_nodes(previous_level_nodes: list[Node], new_nodes: list[Node]):
        if previous_level_nodes is None:
            return
        for prev_node in previous_level_nodes:
            parent_ids = prev_node.object_ids
            for new_node in new_nodes:
                if prev_node.is_virtual:
                    child_ids = new_node.object_ids
                else:
                    child_ids = new_node.parent_ids
                intersection = child_ids & parent_ids
                if (
                    len(intersection) > 0
                    and prev_node.is_virtual
                    and new_node.is_virtual
                ):
                    new_node.object_ids = intersection
                if len(intersection) > 0:
                    if prev_node.is_virtual and new_node.is_virtual:
                        new_node.object_ids = intersection
                    prev_node.add_child(new_node)

    @staticmethod
    def compare(old: set[Node], new: set[Node], parent_id=None):
        old = old if old else set()
        new = new if new else set()

        result = []

        if len(old ^ new) > 0:
            current = {"id": parent_id}
            inserted = new - old
            if len(inserted) > 0:
                for ins in inserted:
                    ins.parent_ids = parent_id
                current["inserted"] = inserted
            deleted = old - new
            if len(deleted) > 0:
                current["deleted"] = deleted
            result.append(current)

        if len(old & new) > 0:
            old_branch = new & old
            new_branch = old & new
            for old_node, new_node in zip(old_branch, new_branch):
                current = HierarchyKeeper.compare(
                    old_node.child, new_node.child, old_node.id
                )
                result.extend(current)
        return result

    async def _changes_to_database(self, changes: list[dict]):
        inserted = set()
        deleted = set()
        for change in changes:
            if "inserted" in change:
                inserted.update(change["inserted"])
            if "deleted" in change:
                deleted.update(change["deleted"])
        old_updated = inserted & deleted
        new_updated = deleted & inserted
        inserted -= new_updated
        deleted -= old_updated

        if len(deleted) > 0:
            delete_ids = [d.id for d in deleted]
            del_query = sqlalchemy.delete(Obj).where(Obj.id.in_(delete_ids))  # noqa
            await self._db_session.execute(del_query)

        for ins in inserted:
            objects = ins.build()
            self._db_session.add_all(objects)

        for old_obj, new_obj in zip(old_updated, new_updated):
            obj: Obj = new_obj.build(get_one=True)
            obj.id = old_obj.id
            await self._db_session.merge(obj)
            comparison = self.compare(old_obj.child, new_obj.child)
            await self._changes_to_database(comparison)

        await self._db_session.flush()
        await self._db_session.commit()

    @staticmethod
    async def _send_changes_to_users(hierarchy_id, changes):
        results = []
        for change in changes:
            result = {"parent_id": str(change["id"])}
            if "inserted" in change:
                result["inserted"] = [
                    node.build(get_one=True).json()
                    for node in change["inserted"]
                ]
            if "deleted" in change:
                result["deleted"] = [
                    node.build(get_one=True).json()
                    for node in change["deleted"]
                ]
            results.append(result)
        if len(results) == 0:
            return

        global changes_list
        changes_list = json.dumps(results)
        notifier = Notifier()
        await notifier.notify(hierarchy_id, changes_list)

    async def update_database(self, hierarchy_id: int = None):
        query_hierarchies = select(Hierarchy)
        if hierarchy_id:
            query_hierarchies = query_hierarchies.where(
                Hierarchy.id == hierarchy_id
            )
        hierarchies = await self._db_session.execute(query_hierarchies)
        hierarchies = hierarchies.scalars().all()
        for hierarchy in hierarchies:
            query_levels = select(Level).where(
                Level.hierarchy_id == hierarchy.id
            )
            levels = await self._db_session.execute(query_levels)
            levels = levels.scalars().all()
            from_db = await self.get_from_db(hierarchy_id=hierarchy.id)
            from_levels = await self.create_from_levels(levels)
            comparison = self.compare(from_db, from_levels)
            await self._changes_to_database(comparison)
            await self._send_changes_to_users(
                hierarchy_id=hierarchy.id, changes=comparison
            )
