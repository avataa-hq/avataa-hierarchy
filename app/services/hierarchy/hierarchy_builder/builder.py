from collections import defaultdict, deque
import json
from sys import stderr
import traceback
from typing import Any, AsyncGenerator, Callable, Deque

from fastapi import HTTPException
import grpc
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from starlette import status

from grpc_config.inventory_utils import (
    get_all_mo_with_special_params_by_tmo_id,
    get_mo_links_tprms,
)
from kafka_config.connection_handler.handler import (
    async_kafka_stopping_to_perform_a_function,
)
from schemas.enum_models import HierarchyStatus
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from services.hierarchy.hierarchy_builder.configs import (
    DEFAULT_KEY_OF_NULL_NODE,
)
from services.hierarchy.hierarchy_builder.utils import (
    create_path_for_children_node_by_parent_node,
    get_node_key_data,
)
from settings import INV_HOST, INVENTORY_GRPC_PORT


class HierarchyBuilderV2:
    def __init__(
        self,
        db_session: AsyncSession,
        hierarchy_id: int,
        default_key_of_null_node: str = DEFAULT_KEY_OF_NULL_NODE,
        create_empty_nodes: bool = True,
    ):
        self.db_session = db_session
        self.hierarchy_id = hierarchy_id
        self._levels = None
        self.__prev_stage_cache = dict()
        self.__current_stage_cache = dict()
        self.default_key_of_null_node = default_key_of_null_node
        self.create_empty_nodes = create_empty_nodes
        self._node_cache_data = defaultdict(list)
        self.inventory_grpc_channel_options: list[tuple[str, Any]] = [
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 15_000),
            ("grpc.http2.max_pings_without_data", 0),
            ("grpc.keepalive_permit_without_calls", 1),
        ]
        service_config_json = json.dumps(
            {
                "methodConfig": [
                    {
                        "name": [{}],
                        "retryPolicy": {
                            "maxAttempts": 5,
                            "initialBackoff": "2s",
                            "maxBackoff": "15s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE"],
                        },
                    }
                ]
            }
        )
        self.inventory_grpc_channel_options.append(
            ("grpc.service_config", service_config_json)
        )

    SET_OF_EMPTY_KEYS = {None, ""}

    @property
    async def levels(self):
        """Returns tuple of hierarchies levels."""
        if self._levels is not None:
            return self._levels
        stm = (
            select(Level)
            .where(Level.hierarchy_id == self.hierarchy_id)
            .order_by(Level.level)
        )
        res = await self.db_session.execute(stm)
        res = tuple(res.scalars().all())
        self._levels = res
        return self._levels

    async def _stage1_clear_hierarchy(self):
        """Deletes all nodes of current hierarchy"""
        # get obj count by levels of current hierarchy
        levels_stmt = select(Level.id).where(
            Level.hierarchy_id == self.hierarchy_id
        )
        levels = await self.db_session.execute(levels_stmt)
        levels = levels.scalars().all()

        if levels:
            stmt = delete(NodeData).where(NodeData.level_id.in_(levels))
            await self.db_session.execute(stmt)

        stmt = delete(Obj).where(Obj.hierarchy_id == self.hierarchy_id)
        await self.db_session.execute(stmt)
        await self.db_session.commit()

    async def __get_func_to_find_parent_node_from_cache(
        self,
        level: Level,
    ):
        def __get_parent_node_if_parent_level_is_not_virtual(
            mo, parent_level_cache
        ):
            return parent_level_cache.get(mo.get("p_id"), None)

        def __get_parent_node_if_parent_level_is_virtual(
            mo, parent_level_cache
        ):
            return parent_level_cache.get(mo.get("id"), None)

        def __get_parent_node_if_parent_level_is_virtual_but_not_same(
            mo, parent_level_cache
        ):
            return parent_level_cache.get(mo.get("p_id"), None)

        def __get_parent_node_if_parent_level_is_none(*args, **kwargs):
            return None

        if level.parent_id is None:
            return __get_parent_node_if_parent_level_is_none

        stmt = select(Level).where(Level.id == level.parent_id)
        resp = await self.db_session.execute(stmt)
        parent_level: Level = resp.scalar_one()

        if parent_level.is_virtual:
            if parent_level.object_type_id == level.object_type_id:
                return __get_parent_node_if_parent_level_is_virtual
            else:
                return __get_parent_node_if_parent_level_is_virtual_but_not_same
        else:
            return __get_parent_node_if_parent_level_is_not_virtual

    @staticmethod
    def __get_param_data_from_item_in_grpc_response(
        item_data: dict, attr_or_param_name: str
    ):
        item_params = item_data.get("params")
        if item_params:
            return item_params.get(attr_or_param_name)
        return None

    @staticmethod
    def __get_attr_data_from_item_in_grpc_response(
        item_data: dict, attr_or_param_name: str
    ):
        return item_data.get(attr_or_param_name)

    def __get_func_to_get_key_or_additional_data_from_item(
        self, leve_param_type_id_or_attr_name: str | None
    ):
        if leve_param_type_id_or_attr_name is None:
            return lambda *args, **kwargs: None

        return self.__get_attr_data_from_item_in_grpc_response

    def __add_node_data_to_cache(self, mo_data: dict, level: Level):
        mo_id = mo_data.get("id")

        if level.key_attrs:
            level_key_attrs = level.key_attrs
        else:
            level_key_attrs = [level.param_type_id]
        if level.attr_as_parent:
            level_key_attrs.append(str(level.attr_as_parent))

        node_data = dict(
            level_id=level.id,
            mo_id=mo_id,
            mo_name=mo_data.get("name"),
            mo_latitude=mo_data.get("latitude"),
            mo_longitude=mo_data.get("longitude"),
            mo_status=mo_data.get("status"),
            mo_tmo_id=mo_data.get("tmo_id"),
            mo_p_id=mo_data.get("p_id"),
            mo_active=mo_data.get("active"),
            unfolded_key={k: mo_data.get(k) for k in level_key_attrs},
        )

        self._node_cache_data[mo_id].append(node_data)

    async def __add_node_data_from_cache_into_session_and_flush_and_clear_node_data_cache(
        self, current_level_obj_cache: dict
    ):
        for mo_id, data_for_nodes in self._node_cache_data.items():
            node_from_cache = current_level_obj_cache.get(mo_id)
            if node_from_cache:
                node_uuid = node_from_cache.id
                if node_uuid:
                    for node_data in data_for_nodes:
                        node_data["node_id"] = str(node_uuid)

                        new = NodeData(**node_data)

                        self.db_session.add(new)
        await self.db_session.flush()
        self._node_cache_data = defaultdict(list)

    async def _create_real_nodes(
        self,
        level: Level,
        res_async_generator: AsyncGenerator,
        find_parent_function: Callable,
        level_key_attrs: list[str],
        parent_level_cache: dict,
        link_to_cache_of_current_level: dict,
    ):
        item_counter_to_flush = 0

        f_get_addit_data = (
            self.__get_func_to_get_key_or_additional_data_from_item(
                leve_param_type_id_or_attr_name=str(level.additional_params_id)
            )
        )

        # Get mo_links tprms
        mo_links_tprms = await get_mo_links_tprms(tmo_id=level.object_type_id)
        mo_links_attrs = list(
            {
                int(attr) for attr in level_key_attrs if attr.isdigit()
            }.intersection(mo_links_tprms)
        )

        async for chunk in res_async_generator:
            for item in chunk:
                is_active = item.get("active", False)

                key_data = get_node_key_data(
                    ordered_key_attrs=level_key_attrs,
                    mo_data_with_params=item,
                    mo_links_attrs=mo_links_attrs,
                )

                parent_node = find_parent_function(
                    mo=item, parent_level_cache=parent_level_cache
                )

                additional_p_val = f_get_addit_data(
                    item_data=item,
                    attr_or_param_name=str(level.additional_params_id),
                )

                path = None
                if parent_node:
                    path = create_path_for_children_node_by_parent_node(
                        parent_node
                    )

                new_node = Obj(
                    key=key_data.key,
                    object_id=item.get("id"),
                    object_type_id=level.object_type_id,
                    additional_params=additional_p_val,
                    hierarchy_id=level.hierarchy_id,
                    level=level.level,
                    latitude=item.get("latitude", None),
                    longitude=item.get("longitude", None),
                    child_count=0,
                    parent_id=parent_node.id
                    if parent_node is not None
                    else None,
                    level_id=level.id,
                    path=path,
                    active=is_active,
                    key_is_empty=key_data.key_is_empty,
                )
                self.db_session.add(new_node)

                # add node_data
                self.__add_node_data_to_cache(mo_data=item, level=level)

                item_counter_to_flush += 1
                if parent_node is not None:
                    if is_active:
                        parent_node.child_count += 1
                        self.db_session.add(parent_node)
                link_to_cache_of_current_level[item.get("id")] = new_node

                if item_counter_to_flush >= 25000:
                    item_counter_to_flush = 0
                    await self.db_session.flush()

                    # add node data into session and flush one more time
                    await self.__add_node_data_from_cache_into_session_and_flush_and_clear_node_data_cache(
                        current_level_obj_cache=link_to_cache_of_current_level
                    )
        if item_counter_to_flush:
            await self.db_session.flush()

            # add node data into session and flush one more time
            await self.__add_node_data_from_cache_into_session_and_flush_and_clear_node_data_cache(
                current_level_obj_cache=link_to_cache_of_current_level
            )

    async def _create_simple_virtual_nodes(
        self,
        level: Level,
        res_async_generator: AsyncGenerator,
        find_parent_function: Callable,
        level_key_attrs: list[str],
        parent_level_cache: dict,
        link_to_cache_of_current_level: dict,
    ):
        current_virtual_level_cache = dict()
        item_counter_to_flush = 0

        # Get mo_links tprms
        mo_links_tprms = await get_mo_links_tprms(tmo_id=level.object_type_id)
        mo_links_attrs = list(
            {int(attr) for attr in level_key_attrs}.intersection(mo_links_tprms)
        )

        async for chunk in res_async_generator:
            for item in chunk:
                is_active = item.get("active", False)
                key_data = get_node_key_data(
                    ordered_key_attrs=level_key_attrs,
                    mo_data_with_params=item,
                    mo_links_attrs=mo_links_attrs,
                )
                if key_data.key and item.get(level_key_attrs[0], None):
                    item[level_key_attrs[0]] = key_data.key

                parent_node = find_parent_function(
                    mo=item, parent_level_cache=parent_level_cache
                )

                current_level_key = (
                    key_data.key,
                    key_data.key_is_empty,
                    parent_node.id if parent_node is not None else parent_node,
                    is_active,
                )

                virtual_node_exists = current_virtual_level_cache.get(
                    current_level_key, None
                )

                # add node_data
                self.__add_node_data_to_cache(mo_data=item, level=level)

                path = None
                if parent_node:
                    path = create_path_for_children_node_by_parent_node(
                        parent_node
                    )

                if virtual_node_exists is None:
                    new_node = Obj(
                        key=key_data.key,
                        object_id=None,
                        object_type_id=level.object_type_id,
                        additional_params=None,
                        hierarchy_id=level.hierarchy_id,
                        level=level.level,
                        latitude=None,
                        longitude=None,
                        child_count=0,
                        parent_id=parent_node.id
                        if parent_node is not None
                        else None,
                        level_id=level.id,
                        path=path,
                        active=is_active,
                        key_is_emty=key_data.key_is_empty,
                    )
                    self.db_session.add(new_node)

                    item_counter_to_flush += 1
                    if parent_node is not None:
                        if is_active:
                            parent_node.child_count += 1
                            self.db_session.add(parent_node)
                    current_virtual_level_cache[current_level_key] = new_node
                    link_to_cache_of_current_level[item.get("id")] = new_node
                else:
                    self.db_session.add(virtual_node_exists)
                    link_to_cache_of_current_level[item.get("id")] = (
                        virtual_node_exists
                    )

                if item_counter_to_flush >= 25000:
                    item_counter_to_flush = 0
                    await self.db_session.flush()

                    # add node data into session and flush one more time
                    await self.__add_node_data_from_cache_into_session_and_flush_and_clear_node_data_cache(
                        current_level_obj_cache=link_to_cache_of_current_level
                    )
        if item_counter_to_flush:
            await self.db_session.flush()

            # add node data into session and flush one more time
            await self.__add_node_data_from_cache_into_session_and_flush_and_clear_node_data_cache(
                current_level_obj_cache=link_to_cache_of_current_level
            )

    async def _create_hierarchical_virtual_nodes(
        self,
        level: Level,
        res_async_generator: AsyncGenerator,
        find_parent_function: Callable,
        level_key_attrs: list[str],
        parent_level_cache: dict,
        link_to_cache_of_current_level: dict,
    ):
        # Get mo_links tprms
        mo_links_tprms = await get_mo_links_tprms(tmo_id=level.object_type_id)
        mo_links_attrs = list(
            {int(attr) for attr in level_key_attrs}.intersection(mo_links_tprms)
        )

        async def _create_flat_hierarchy_from_tprm_id(
            _chunk: list[dict], tprm_id: int
        ) -> dict[int | None, list[dict]]:
            _hierarchy: dict[int | None, list[dict]] = defaultdict(list)
            str_tprm_id = str(tprm_id)
            for _item in _chunk:
                parent_id = _item.get(str_tprm_id, None)
                if parent_id == 0:
                    parent_id = None
                _hierarchy[parent_id].append(_item)
            return _hierarchy

        def __collect_mo_ids_to_circles(
            _unpassed_mo_ids: set[str],
            _flat_hierarchy: dict[str | None, list[dict]],
        ) -> list[list[str]]:
            _circles = []
            # collect circles
            _passed_circle_mo_ids = set()
            for _mo_id in _unpassed_mo_ids:
                _circle = []
                _circle_queue = deque([_mo_id])
                while _circle_queue:
                    _circle_mo_id = _circle_queue.popleft()
                    if _circle_mo_id in _passed_circle_mo_ids:
                        continue
                    _passed_circle_mo_ids.add(_circle_mo_id)
                    _children_mo_ids = [
                        i["id"] for i in _flat_hierarchy.get(_circle_mo_id, [])
                    ]
                    _circle_queue.extend(_children_mo_ids)
                if _circle:
                    _circles.append(_circle)
            return _circles

        def __find_best_head_of_circles(
            _circles: list[list[str]], _all_items_by_id: dict[str, dict]
        ) -> list[dict]:
            _best_head_of_circles: list[dict] = []
            for _circle in _circles:
                best_head_of_circle: dict | None = None
                sorted(_circle, key=lambda x: int(x))
                for _item_id in _circle:
                    _item = _all_items_by_id[_item_id]
                    best_head_of_circle = _item
                    break
                if not best_head_of_circle:
                    print(
                        "Best head of circle not found for circle {}".format(
                            _circle
                        ),
                        file=stderr,
                    )
                    continue
                _best_head_of_circles.append(best_head_of_circle)
            return _best_head_of_circles

        async def __create_recursive(
            _queue: Deque,
            _top_level_ids: set[str],
            _current_virtual_level_cache: dict,
            _passed_mo_ids: set[str],
            _flat_hierarchy: dict[int | None, list[dict]],
        ):
            f_get_addit_data = (
                self.__get_func_to_get_key_or_additional_data_from_item(
                    leve_param_type_id_or_attr_name=str(
                        level.additional_params_id
                    )
                )
            )

            while _queue:
                items = _queue.popleft()
                level_result = []
                for item in items:  # type: dict
                    if item["id"] in _passed_mo_ids:
                        print(
                            f"Item with id {item['id']} passed before",
                            file=stderr,
                        )
                        continue
                    is_active = item.get("active", False)
                    key_data = get_node_key_data(
                        ordered_key_attrs=level_key_attrs,
                        mo_data_with_params=item,
                        mo_links_attrs=mo_links_attrs,
                    )
                    additional_p_val = f_get_addit_data(
                        item_data=item,
                        attr_or_param_name=str(level.additional_params_id),
                    )
                    if item["id"] in _top_level_ids:
                        parent_node = find_parent_function(
                            mo=item, parent_level_cache=parent_level_cache
                        )
                    else:
                        parent_node = _current_virtual_level_cache.get(
                            item.get(str(level.attr_as_parent), None), None
                        )

                    path = None
                    if parent_node:
                        path = create_path_for_children_node_by_parent_node(
                            parent_node
                        )
                        if is_active:
                            parent_node.child_count += 1
                            self.db_session.add(parent_node)

                    new_node = Obj(
                        key=key_data.key,
                        object_id=item.get("id"),
                        object_type_id=level.object_type_id,
                        additional_params=additional_p_val,
                        hierarchy_id=level.hierarchy_id,
                        level=level.level,
                        latitude=None,
                        longitude=None,
                        child_count=0,
                        parent_id=parent_node.id
                        if parent_node is not None
                        else None,
                        level_id=level.id,
                        path=path,
                        active=is_active,
                        key_is_emty=key_data.key_is_empty,
                    )
                    link_to_cache_of_current_level[item.get("id")] = new_node
                    _current_virtual_level_cache[item.get("id")] = new_node
                    self.__add_node_data_to_cache(mo_data=item, level=level)
                    _passed_mo_ids.add(item["id"])
                    level_result.append(new_node)

                    children_items = _flat_hierarchy.get(item["id"], [])
                    if children_items:
                        _queue.append(children_items)

                if level_result:
                    self.db_session.add_all(level_result)
                    await self.db_session.flush()

                    await self.__add_node_data_from_cache_into_session_and_flush_and_clear_node_data_cache(
                        current_level_obj_cache=link_to_cache_of_current_level
                    )

        current_virtual_level_cache = dict()

        all_items_by_id = {
            item["id"]: item
            async for chunk in res_async_generator
            for item in chunk
        }
        flat_hierarchy = await _create_flat_hierarchy_from_tprm_id(
            list(all_items_by_id.values()), tprm_id=level.attr_as_parent
        )

        passed_mo_ids: set[str] = set()

        queue = flat_hierarchy.get(None, [])
        top_level_ids = {i["id"] for i in queue}
        if queue:
            queue = [queue]
        queue = deque(queue)
        await __create_recursive(
            _queue=queue,
            _top_level_ids=top_level_ids,
            _passed_mo_ids=passed_mo_ids,
            _current_virtual_level_cache=current_virtual_level_cache,
            _flat_hierarchy=flat_hierarchy,
        )

        # circular references by parents
        all_p_ids = set(all_items_by_id.keys())
        unpassed_mo_ids = all_p_ids.difference(passed_mo_ids)
        if unpassed_mo_ids:
            circles = __collect_mo_ids_to_circles(
                _unpassed_mo_ids=unpassed_mo_ids, _flat_hierarchy=flat_hierarchy
            )
            head_of_circles = __find_best_head_of_circles(
                _circles=circles, _all_items_by_id=all_items_by_id
            )
            if head_of_circles:
                top_level_ids.update([i["id"] for i in head_of_circles])
                queue = deque([head_of_circles])
                await __create_recursive(
                    _queue=queue,
                    _top_level_ids=top_level_ids,
                    _passed_mo_ids=passed_mo_ids,
                    _current_virtual_level_cache=current_virtual_level_cache,
                    _flat_hierarchy=flat_hierarchy,
                )

    async def _create_virtual_nodes(
        self,
        level: Level,
        res_async_generator: AsyncGenerator,
        find_parent_function: Callable,
        level_key_attrs: list[str],
        parent_level_cache: dict,
        link_to_cache_of_current_level: dict,
    ):
        if not level.attr_as_parent:
            await self._create_simple_virtual_nodes(
                level=level,
                level_key_attrs=level_key_attrs,
                parent_level_cache=parent_level_cache,
                find_parent_function=find_parent_function,
                link_to_cache_of_current_level=link_to_cache_of_current_level,
                res_async_generator=res_async_generator,
            )
        else:
            await self._create_hierarchical_virtual_nodes(
                level=level,
                level_key_attrs=level_key_attrs,
                parent_level_cache=parent_level_cache,
                find_parent_function=find_parent_function,
                link_to_cache_of_current_level=link_to_cache_of_current_level,
                res_async_generator=res_async_generator,
            )

    async def _create_nodes_by_level_data(
        self, level: Level, res_async_generator: AsyncGenerator
    ):
        parent_level_cache = self.__prev_stage_cache.get(
            level.parent_id, dict()
        )

        self.__current_stage_cache[level.id] = dict(
            object_type_id=level.object_type_id
        )
        link_to_cache_of_current_level = self.__current_stage_cache[level.id]

        find_parent_function = (
            await self.__get_func_to_find_parent_node_from_cache(level)
        )

        if level.key_attrs:
            level_key_attrs = level.key_attrs
        else:
            level_key_attrs = [level.param_type_id]

        if level.is_virtual:
            await self._create_virtual_nodes(
                level=level,
                level_key_attrs=level_key_attrs,
                parent_level_cache=parent_level_cache,
                find_parent_function=find_parent_function,
                link_to_cache_of_current_level=link_to_cache_of_current_level,
                res_async_generator=res_async_generator,
            )
        else:
            await self._create_real_nodes(
                level=level,
                level_key_attrs=level_key_attrs,
                parent_level_cache=parent_level_cache,
                find_parent_function=find_parent_function,
                link_to_cache_of_current_level=link_to_cache_of_current_level,
                res_async_generator=res_async_generator,
            )

        await self.db_session.commit()

        # add notes into session and commit also

    async def build_hierarchy(self):
        """Builds hierarchy. Deletes all old nodes and creates new."""
        await self._stage1_clear_hierarchy()
        level_stage = None
        levels = await self.levels
        async with grpc.aio.insecure_channel(
            target=f"{INV_HOST}:{INVENTORY_GRPC_PORT}",
            options=self.inventory_grpc_channel_options,
        ) as channel:
            try:
                for level in levels:
                    if level.level != level_stage:
                        self.__prev_stage_cache, self.__current_stage_cache = (
                            self.__current_stage_cache,
                            dict(),
                        )
                        level_stage = level.level

                    tprms_ids = list()
                    if level.key_attrs:
                        for attr in level.key_attrs:
                            if attr.isdigit():
                                tprms_ids.append(int(attr))
                    else:
                        tprms_ids.append(level.param_type_id)

                    if level.additional_params_id:
                        tprms_ids.append(level.additional_params_id)

                    if level.attr_as_parent:
                        tprms_ids.append(level.attr_as_parent)

                    if not tprms_ids:
                        # to not return tprms data
                        tprms_ids = [0]

                    res_async_generator = (
                        get_all_mo_with_special_params_by_tmo_id(
                            channel=channel,
                            tmo_id=level.object_type_id,
                            tprm_ids=tprms_ids,
                        )
                    )
                    await self._create_nodes_by_level_data(
                        level, res_async_generator
                    )
            except Exception as e:
                print(traceback.format_exc(), flush=True, file=stderr)
                raise e
        self.__prev_stage_cache, self.__current_stage_cache = dict(), dict()


@async_kafka_stopping_to_perform_a_function
async def refresh_hierarchy_with_error_catch(
    session: AsyncSession, hierarchy: Hierarchy
):
    """Refresh hierarchy with catching errors"""
    try:
        keeper = HierarchyBuilderV2(
            db_session=session, hierarchy_id=hierarchy.id
        )
        await keeper.build_hierarchy()
    except Exception as ex:
        print(str(ex))
        await session.rollback()
        hierarchy.status = HierarchyStatus.ERROR.value
        session.add(hierarchy)
        await session.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex)
        )
