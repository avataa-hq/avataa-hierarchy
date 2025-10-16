from concurrent.futures import ThreadPoolExecutor
import traceback
import uuid
from uuid import UUID

from fastapi.datastructures import Headers
from fastapi.requests import Request
import grpc

from database import database
from routers.hierarchy_object.router import (
    get_children_mo_ids_of_particular_nodes,
)
from routers.hierarchy_object.router import (
    get_count_children_with_lifecycle_and_max_severity_by_node_ids as node_severity,
)
from routers.hierarchy_object_router import (
    get_child_nodes_of_parent_id_with_filter_condition,
)
from routers.hierarchy_router import (
    get_children_mo_ids_of_particular_hierarchy,
)
from routers.hierarchy_router import (
    get_count_children_with_lifecycle_and_max_severity_by_hierarchy_ids as hier_severity,
)
from settings import SERVER_GRPC_PORT

from .hierarchy.hierarchy_data_pb2_grpc import (
    add_HierarchyDataServicer_to_server,
)
from .hierarchy.servicer.servicer import HierarchyDataGRPCManager
from .protobuf import severity_pb2_grpc
from .protobuf.severity_pb2 import (
    ListHierarchyId,
    ListNodeId,
    ListOfHierarchiesMoIds,
    ListOfNodesMoIds,
    ListSeverityHierarchyIdResponse,
    ListSeverityNodeIdResponse,
    MoIdsByHierarchy,
    MoIdsByNode,
    RequestNodesWithCondition,
    ResponseNodesWithCondition,
    ResponseNodesWithConditionItem,
    SeverityHierarchyIdResponse,
    SeverityNodeIdResponse,
)
from .protobuf.severity_pb2_grpc import SeverityServicer

session_iterator = database.get_session


class Severity(SeverityServicer):
    async def GetSeverityByHierarchyId(
        self, request: ListHierarchyId, context: grpc.aio.ServicerContext
    ) -> ListSeverityHierarchyIdResponse:
        hierarchy_ids = list(request.hierarchy_id)
        response = dict()
        async for session in session_iterator():
            response = await hier_severity(
                hierarchy_ids=hierarchy_ids, session=session
            )
        result = []
        for key, value in response.items():
            item = SeverityHierarchyIdResponse(
                hierarchy_id=key,
                count=value.get("count", 0),
                severity=value.get("severity", 0),
            )
            result.append(item)
        return ListSeverityHierarchyIdResponse(items=result)

    async def GetSeverityByNodeId(
        self, request: SeverityNodeIdResponse, context: grpc.aio.ServicerContext
    ) -> ListSeverityNodeIdResponse:
        node_ids = list(request.node_id)
        node_ids = [UUID(i) for i in node_ids]
        response = dict()
        async for session in session_iterator():
            response = await node_severity(node_ids=node_ids, session=session)
        result = []
        for key, value in response.items():
            item = SeverityNodeIdResponse(
                node_id=str(key),
                count=value.get("count", 0),
                severity=value.get("severity", 0),
            )
            result.append(item)
        return ListSeverityNodeIdResponse(items=result)

    async def GetMoIdsOfHierarchy(
        self, request: ListHierarchyId, context: grpc.aio.ServicerContext
    ) -> ListOfHierarchiesMoIds:
        result = ListOfHierarchiesMoIds()
        if not request.hierarchy_id:
            return result
        async for session in session_iterator():
            data = await get_children_mo_ids_of_particular_hierarchy(
                list(request.hierarchy_id), session
            )
            list_of_items = [
                MoIdsByHierarchy(hierarchy_id=k, mo_ids=v)
                for k, v in data.items()
            ]
            result = ListOfHierarchiesMoIds(items=list_of_items)
        return result

    async def GetMoIdsOfHierarchyBranch(
        self, request: ListNodeId, context: grpc.aio.ServicerContext
    ) -> ListOfNodesMoIds:
        result = ListOfNodesMoIds()
        if not request.node_id:
            return result

        async for session in session_iterator():
            node_ids = [uuid.UUID(x) for x in request.node_id]
            data = await get_children_mo_ids_of_particular_nodes(
                node_ids, session
            )
            list_of_items = [
                MoIdsByNode(node_id=str(k), mo_ids=v) for k, v in data.items()
            ]
            result = ListOfNodesMoIds(items=list_of_items)
        return result

    async def GetChildNodesOfParentIdWithFilterCondition(
        self,
        request: RequestNodesWithCondition,
        context: grpc.aio.ServicerContext,
    ) -> ResponseNodesWithCondition:
        def build_request(
            method: str = "POST",
            server: str = "127.0.0.1",
            path: str = "/",
            headers: dict = None,
            body: str = None,
        ) -> Request:
            if headers is None:
                headers = {}
            request_ = Request(
                {
                    "type": "http",
                    "path": path,
                    "headers": Headers(headers).raw,
                    "http_version": "1.1",
                    "method": method,
                    "scheme": "https",
                    "client": ("127.0.0.1", 8000),
                    "server": (server, 443),
                    "query_string": path[path.index("?") + 1 :]
                    if "?" in path
                    else None,
                }
            )
            if body:

                async def request_body():
                    return body

                request_.body = request_body
            return request_

        request_mock = build_request(path=request.request_query)
        try:
            async for session in database.get_session():
                response = (
                    await get_child_nodes_of_parent_id_with_filter_condition(
                        hierarchy_id=request.hierarchy_id,
                        request=request_mock,
                        parent_id=request.parent_id,
                        tmo_id=request.tmo_id,
                        session=session,
                    )
                )
                results_list = []
                for r in response:
                    r.pop("_sa_instance_state", None)
                    r["id"] = str(r["id"])
                    result = ResponseNodesWithConditionItem(**r)
                    results_list.append(result)
                return ResponseNodesWithCondition(items=results_list)
        except Exception as e:
            print(traceback.format_exc())
            raise e


async def serve() -> None:
    server = grpc.aio.server(ThreadPoolExecutor(max_workers=10))
    severity_pb2_grpc.add_SeverityServicer_to_server(Severity(), server)
    add_HierarchyDataServicer_to_server(HierarchyDataGRPCManager(), server)
    listen_addr = f"[::]:{SERVER_GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    await server.start()
    print("Starting")
    await server.wait_for_termination()
