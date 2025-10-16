from typing import AsyncGenerator

from sqlalchemy import delete, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from grpc_config.inventory_utils import get_mo_with_params_for_tmo_id_by_grpc
from schemas.hier_schemas import Level, Obj

DEFAULT_KEY_OF_NULL_NODE = "Null"


class HierarchyBuilder:
    def __init__(
        self,
        db_session: AsyncSession,
        hierarchy_id: int,
        default_key_of_null_node: str = DEFAULT_KEY_OF_NULL_NODE,
    ):
        self.db_session = db_session
        self.hierarchy_id = hierarchy_id
        self._levels = None
        self.__prev_stage_cache = dict()
        self.__current_stage_cache = dict()
        self.default_key_of_null_node = default_key_of_null_node

    @property
    async def levels(self):
        """Returns hierarchy tuple of hierarchy levels."""
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

    async def clear_hierarchy(self):
        """Deletes all nodes of current hierarchy"""
        # get obj count by levels of current hierarchy
        stmt = (
            select(Level.id)
            .where(Level.hierarchy_id == self.hierarchy_id)
            .order_by(desc(Level.level))
        )
        h_levels_ids = await self.db_session.execute(stmt)
        h_levels_ids = h_levels_ids.scalars().all()

        for level_id in h_levels_ids:
            stmt = delete(Obj).where(Obj.level_id == level_id)
            await self.db_session.execute(stmt)
            await self.db_session.commit()

    async def build_hierarchy(self):
        """Builds hierarchy. Deletes all old nodes and creates new."""
        print("Clear all hierarchy data")
        await self.clear_hierarchy()
        print("Get levels")
        level_stage = None
        levels = await self.levels
        print(f"{levels=}")
        for level in levels:
            print(level)

            if level.level != level_stage:
                self.__prev_stage_cache, self.__current_stage_cache = (
                    self.__current_stage_cache,
                    dict(),
                )
                level_stage = level.level

            res_async_generator = await self.get_data_for_level(level)
            await self.create_nodes_by_level_data(level, res_async_generator)

        self.__prev_stage_cache, self.__current_stage_cache = dict(), dict()

    async def rebuild_hierarchy_branch(self, root_node: Obj):
        """Rebuilds hierarchy branch starts from real root_node"""
        if Obj.object_id is None:
            raise TypeError("root_node should be real node")

        child_levels_stmt = select(Level).where(
            Level.parent_id == root_node.level_id
        )
        child_levels = await self.db_session.execute(child_levels_stmt)
        child_levels = tuple(child_levels.scalars().all())

        if len(child_levels) > 0:
            stm_delete = delete(Obj).where(Obj.parent_id == root_node.id)
            await self.db_session.execute(stm_delete)

            if root_node.child_count != 0:
                root_node.child_count = 0
                self.db_session.add(root_node)
                await self.db_session.commit()

            level_stage = child_levels[0].level
            self.__prev_stage_cache = {
                root_node.level_id: {root_node.id: root_node}
            }

            for level in child_levels:
                if level.level != level_stage:
                    self.__prev_stage_cache, self.__current_stage_cache = (
                        self.__current_stage_cache,
                        dict(),
                    )
                    level_stage = level.level

                res_async_generator = (
                    await self.get_data_for_level_with_specific_parent_mo(
                        level=level, mo_parent_id=root_node.object_id
                    )
                )
                await self.create_nodes_by_level_data(
                    level, res_async_generator
                )

            self.__prev_stage_cache, self.__current_stage_cache = dict(), dict()

    @staticmethod
    def __get_func_to_find_parent_node_from_cache(
        parent_object_type_id=None, current_level_object_type_id=None
    ):
        def __get_parent_node_if_parent_level_is_not_virtual(
            mo, parent_level_cache
        ):
            return parent_level_cache.get(mo.p_id, None)

        def __get_parent_node_if_parent_level_is_virtual(
            mo, parent_level_cache
        ):
            return parent_level_cache.get(mo.mo_id, None)

        def __get_parent_node_if_parent_level_is_none(*args, **kwargs):
            return None

        if parent_object_type_id is None:
            return __get_parent_node_if_parent_level_is_none
        elif parent_object_type_id == current_level_object_type_id:
            return __get_parent_node_if_parent_level_is_virtual
        else:
            return __get_parent_node_if_parent_level_is_not_virtual

    async def create_nodes_by_level_data(
        self, level, res_async_generator: AsyncGenerator
    ):
        parent_level_cache = self.__prev_stage_cache.get(
            level.parent_id, dict()
        )

        self.__current_stage_cache[level.id] = dict(
            object_type_id=level.object_type_id
        )
        link_to_cache_of_curent_level = self.__current_stage_cache[level.id]

        parent_object_type_id = parent_level_cache.get("object_type_id", None)

        find_parent_function = self.__get_func_to_find_parent_node_from_cache(
            parent_object_type_id, level.object_type_id
        )

        item_counter_to_flush = 0
        items_for_level = 0
        if level.is_virtual:
            current_virtual_level_cache = dict()

            async for item in res_async_generator:
                items_for_level += 1

                tprm_values = dict(item.tprm_values)

                obj_key = tprm_values.get(
                    level.param_type_id, self.default_key_of_null_node
                )

                if obj_key == "":
                    obj_key = self.default_key_of_null_node

                parent_node = find_parent_function(
                    mo=item, parent_level_cache=parent_level_cache
                )

                current_level_key = (
                    obj_key,
                    parent_node.id if parent_node is not None else parent_node,
                )

                virtual_node_exists = current_virtual_level_cache.get(
                    current_level_key, None
                )

                if virtual_node_exists is None:
                    new_node = Obj(
                        key=obj_key,
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
                    )
                    self.db_session.add(new_node)
                    item_counter_to_flush += 1
                    if parent_node is not None:
                        parent_node.child_count += 1
                        self.db_session.add(parent_node)
                    current_virtual_level_cache[current_level_key] = new_node
                    link_to_cache_of_curent_level[item.mo_id] = new_node
                else:
                    self.db_session.add(virtual_node_exists)
                    link_to_cache_of_curent_level[item.mo_id] = (
                        virtual_node_exists
                    )

                if item_counter_to_flush >= 50000:
                    item_counter_to_flush = 0
                    await self.db_session.flush()

        else:
            async for item in res_async_generator:
                items_for_level += 1
                parent_node = find_parent_function(
                    mo=item, parent_level_cache=parent_level_cache
                )

                if level.parent_id is not None and parent_node is None:
                    continue

                tprm_values = dict(item.tprm_values)

                new_node = Obj(
                    key=tprm_values.get(
                        level.param_type_id, DEFAULT_KEY_OF_NULL_NODE
                    ),
                    object_id=item.mo_id,
                    object_type_id=level.object_type_id,
                    additional_params=tprm_values.get(
                        level.additional_params_id, None
                    ),
                    hierarchy_id=level.hierarchy_id,
                    level=level.level,
                    latitude=self.safe_float(
                        tprm_values.get(level.latitude_id, None)
                    ),
                    longitude=self.safe_float(
                        tprm_values.get(level.longitude_id, None)
                    ),
                    child_count=0,
                    parent_id=parent_node.id
                    if parent_node is not None
                    else None,
                    level_id=level.id,
                )
                self.db_session.add(new_node)
                item_counter_to_flush += 1
                if parent_node is not None:
                    parent_node.child_count += 1
                    self.db_session.add(parent_node)
                link_to_cache_of_curent_level[item.mo_id] = new_node

                if item_counter_to_flush >= 50000:
                    item_counter_to_flush = 0
                    await self.db_session.flush()

        await self.db_session.commit()
        print(f"Received {items_for_level} items")

    @staticmethod
    async def get_data_for_level(level: Level):
        """Returns all async_generator (grpc stream) of Inventory MO objects with params that needed to create nodes on
        particular level by gRPC connection"""
        request_data = dict()
        request_data["tmo_id"] = level.object_type_id
        if level.is_virtual:
            request_data["tprm_ids"] = [level.param_type_id]
        else:
            level_attrs = [
                "param_type_id",
                "additional_params_id",
                "latitude_id",
                "longitude_id",
            ]
            tprm_ids = []
            for level_attr in level_attrs:
                param_id = getattr(level, level_attr, None)
                if param_id is not None:
                    tprm_ids.append(param_id)

            request_data["tprm_ids"] = tprm_ids

        res_async_generator = get_mo_with_params_for_tmo_id_by_grpc(
            **request_data
        )

        return res_async_generator

    @staticmethod
    async def get_data_for_level_with_specific_parent_mo(
        level: Level, mo_parent_id: int
    ):
        """Returns all async_generator (grpc stream) of Inventory MO objects where MO.p_id == mo_parent_id with params
        that needed to create nodes on particular level by gRPC connection"""
        request_data = dict()

        request_data["mo_p_id"] = mo_parent_id
        request_data["tmo_id"] = level.object_type_id
        if level.is_virtual:
            request_data["tprm_ids"] = [level.param_type_id]
        else:
            level_attrs = [
                "param_type_id",
                "additional_params_id",
                "latitude_id",
                "longitude_id",
            ]
            tprm_ids = []
            for level_attr in level_attrs:
                param_id = getattr(level, level_attr, None)
                if param_id is not None:
                    tprm_ids.append(param_id)

            request_data["tprm_ids"] = tprm_ids

        res_async_generator = get_mo_with_params_for_tmo_id_by_grpc(
            **request_data
        )

        return res_async_generator

    @staticmethod
    def safe_float(value: str | None) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
