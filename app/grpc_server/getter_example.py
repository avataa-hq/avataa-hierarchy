import asyncio

from google.protobuf import json_format
import grpc
from protobuf.severity_pb2 import (
    ListHierarchyId,
    ListNodeId,
    RequestNodesWithCondition,
)
from protobuf.severity_pb2_grpc import SeverityStub

from grpc_server.hierarchy.hierarchy_data_pb2 import (
    EmptyRequest,
    HierarchyIdRequest,
)
from grpc_server.hierarchy.hierarchy_data_pb2_grpc import HierarchyDataStub


async def get_severity_by_id():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        query = ListHierarchyId(hierarchy_id=[4104])
        stub = SeverityStub(channel)
        response = await stub.GetSeverityByHierarchyId(query)

        print(response)


async def get_severity_by_node_id():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        query = ListNodeId(node_id=["dc34cafd-7d4f-4547-b1fe-753199c7e33f"])
        stub = SeverityStub(channel)
        response = await stub.GetSeverityByNodeId(query)

        print(response)


async def get_mo_ids_of_hierarchy():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        msg = ListHierarchyId(hierarchy_id=[4105])
        stub = SeverityStub(channel)
        response = await stub.GetMoIdsOfHierarchy(msg)
        print(response)


async def get_mo_ids_of_hierarchy_branch():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        msg = ListNodeId(node_id=["67b5c03e-69b5-4662-86be-1fc848af2b28"])
        stub = SeverityStub(channel)
        response = await stub.GetMoIdsOfHierarchyBranch(msg)
        print(response)


async def get_child_nodes_of_parent_id_with_filter_condition():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        msg = RequestNodesWithCondition(
            hierarchy_id=4105,
            request_query="tprm_id123405|equals=77.5&tmo_id=40469",
            parent_id="ROOT",
        )
        stub = SeverityStub(channel)
        response = await stub.GetChildNodesOfParentIdWithFilterCondition(msg)
        print(response)


async def get_all_hierarchies():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        msg = EmptyRequest()
        stub = HierarchyDataStub(channel)
        response = stub.GetAllHierarchies(msg)
        print(response)
        async for part in response:
            print(part)


async def get_all_hierarchy_levels():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        msg = HierarchyIdRequest(hierarchy_id=25)
        stub = HierarchyDataStub(channel)
        response = stub.GetLevelsByHierarchyId(msg)
        async for part in response:
            data = json_format.MessageToDict(
                part,
                including_default_value_fields=False,
                preserving_proto_field_name=True,
            )
            print(data)


async def get_hierarchy_by_id():
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        msg = HierarchyIdRequest(hierarchy_id=25)
        stub = HierarchyDataStub(channel)
        response = await stub.GetHierarchyById(msg)
        print(response)
        print(type(response))
        d = json_format.MessageToDict(
            response,
            including_default_value_fields=False,
            preserving_proto_field_name=True,
        )
        print(d)


if __name__ == "__main__":
    # asyncio.run(get_severity_by_id())
    # asyncio.run(get_mo_ids_of_hierarchy())
    # asyncio.run(get_mo_ids_of_hierarchy_branch())
    asyncio.run(get_all_hierarchy_levels())
    # asyncio.run(get_child_nodes_of_parent_id_with_filter_condition())
