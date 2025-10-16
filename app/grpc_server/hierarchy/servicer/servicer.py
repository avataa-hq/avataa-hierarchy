import grpc
from sqlalchemy import select

from database import async_session_maker_with_admin_perm
from grpc_server.hierarchy.hierarchy_data_pb2 import (
    EmptyRequest,
    GetAllHierarchiesResponse,
    GetLevelsByHierarchyIdResponse,
    GetNodeDatasByLevelIdResponse,
    GetObjsByLevelIdResponse,
    HierarchyIdRequest,
    HierarchyPermissionSchema,
    HierarchySchema,
    LevelIdRequest,
    PermissionStreamResponse,
)
from grpc_server.hierarchy.hierarchy_data_pb2_grpc import HierarchyDataServicer
from grpc_server.hierarchy.servicer.utils import (
    convert_hierarchy_to_hierarchy_proto_schema,
    convert_level_to_level_proto_schema,
    convert_node_data_to_node_data_proto_schema,
    convert_obj_to_obj_proto_schema,
)
from schemas.hier_schemas import Hierarchy, Level, NodeData, Obj
from services.security.data.permissions.hierarchy import HierarchyPermission


class HierarchyDataGRPCManager(HierarchyDataServicer):
    async def GetAllHierarchies(
        self, request: EmptyRequest, context: grpc.aio.ServicerContext
    ) -> GetAllHierarchiesResponse:
        limit_per_step = 10000

        async with async_session_maker_with_admin_perm() as session:
            stmt = select(Hierarchy)
            result_generator = await session.stream_scalars(stmt)
            async for partition in result_generator.yield_per(
                limit_per_step
            ).partitions(limit_per_step):
                msg = GetAllHierarchiesResponse(
                    items=[
                        convert_hierarchy_to_hierarchy_proto_schema(item)
                        for item in partition
                    ]
                )
                yield msg

    async def GetLevelsByHierarchyId(
        self, request: HierarchyIdRequest, context: grpc.aio.ServicerContext
    ) -> GetLevelsByHierarchyIdResponse:
        limit_per_step = 10000

        h_id = request.hierarchy_id
        if not h_id:
            return

        async with async_session_maker_with_admin_perm() as session:
            stmt = select(Level).where(Level.hierarchy_id == h_id)
            result_generator = await session.stream_scalars(stmt)

            async for partition in result_generator.yield_per(
                limit_per_step
            ).partitions(limit_per_step):
                msg = GetLevelsByHierarchyIdResponse(
                    items=[
                        convert_level_to_level_proto_schema(item)
                        for item in partition
                    ]
                )
                yield msg

    async def GetObjsByLevelId(
        self, request: LevelIdRequest, context: grpc.aio.ServicerContext
    ) -> GetObjsByLevelIdResponse:
        limit_per_step = 10000
        l_id = request.level_id
        if not l_id:
            return

        async with async_session_maker_with_admin_perm() as session:
            stmt = select(Obj).where(Obj.level_id == l_id)
            result_generator = await session.stream_scalars(stmt)
            async for partition in result_generator.yield_per(
                limit_per_step
            ).partitions(limit_per_step):
                msg = GetObjsByLevelIdResponse(
                    items=[
                        convert_obj_to_obj_proto_schema(item)
                        for item in partition
                    ]
                )
                yield msg

    async def GetNodeDatasByLevelId(
        self, request: LevelIdRequest, context: grpc.aio.ServicerContext
    ) -> GetNodeDatasByLevelIdResponse:
        limit_per_step = 10000

        l_id = request.level_id
        if not l_id:
            return

        async with async_session_maker_with_admin_perm() as session:
            stmt = select(NodeData).where(NodeData.level_id == l_id)
            result_generator = await session.stream_scalars(stmt)
            async for partition in result_generator.yield_per(
                limit_per_step
            ).partitions(limit_per_step):
                msg = GetNodeDatasByLevelIdResponse(
                    items=[
                        convert_node_data_to_node_data_proto_schema(item)
                        for item in partition
                    ]
                )
                yield msg

    async def GetHierarchyPermissionByHierarchyId(
        self, request: HierarchyIdRequest, context: grpc.aio.ServicerContext
    ) -> PermissionStreamResponse:
        limit_per_step = 10000
        h_id = request.hierarchy_id
        if not h_id:
            return

        async with async_session_maker_with_admin_perm() as session:
            stmt = select(HierarchyPermission).where(
                HierarchyPermission.parent_id == h_id
            )
            result_generator = await session.stream_scalars(stmt)
            async for partition in result_generator.yield_per(
                limit_per_step
            ).partitions(limit_per_step):
                msg = PermissionStreamResponse(
                    items=[
                        HierarchyPermissionSchema(**item.to_dict())
                        for item in partition
                    ]
                )
                yield msg

    async def GetHierarchyById(
        self, request: HierarchyIdRequest, context: grpc.aio.ServicerContext
    ) -> HierarchySchema:
        h_id = request.hierarchy_id
        if not h_id:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return context

        async with async_session_maker_with_admin_perm() as session:
            stmt = select(Hierarchy).where(Hierarchy.id == h_id)
            hierarchy = await session.execute(stmt)
            hierarchy = hierarchy.scalars().first()
            if hierarchy:
                return convert_hierarchy_to_hierarchy_proto_schema(hierarchy)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return context
