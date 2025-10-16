import pickle
from typing import AsyncGenerator, Iterable, List

from google.protobuf import json_format
import grpc
from grpc.aio import AioRpcError, Channel
from starlette.datastructures import QueryParams

from grpc_config.protobuf import mo_info_pb2, mo_info_pb2_grpc
from grpc_config.protobuf.mo_info_pb2_grpc import InformerStub
from settings import INVENTORY_GRPC_URL


async def get_mo_tprm_values_by_grpc(mo_id: int, tprm_ids: set[int]) -> dict:
    async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        request = mo_info_pb2.InfoRequest(mo_id=mo_id, tprm_ids=tprm_ids)
        response = await stub.GetParamsValuesForMO(request)
        response_as_dict = json_format.MessageToDict(
            response,
            always_print_fields_with_no_presence=True,
            preserving_proto_field_name=True,
        )
        result = dict()
        for k, v in response_as_dict["mo_info"].items():
            result[int(k)] = [x["value"] for x in v["mo_tprm_value"]]

        result = {k: str(v[0]) if len(v) == 1 else v for k, v in result.items()}

        return result


async def get_mo_info_by_grpc(mo_id: int) -> dict:
    async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.IntValue(value=mo_id)
        response = await stub.GetTMOidForMo(msg)

        message_as_dict = json_format.MessageToDict(
            response,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
        )

        return message_as_dict


async def get_mo_with_params_for_tmo_id_by_grpc(
    tmo_id: int, tprm_ids: list[int] = None, p_id: int = None
) -> AsyncGenerator:
    async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)

        request_data = dict()
        request_data["object_type_id"] = tmo_id
        if tprm_ids is not None and len(tprm_ids) > 0:
            request_data["tprm_ids"] = tprm_ids

        if p_id is not None:
            request_data["p_id"] = p_id

        msg = mo_info_pb2.RequestForObjInfoByTMO(**request_data)
        response = stub.GetObjWithParams(msg)
        async for msg in response:
            yield msg


async def check_if_tmo_has_lifecycle(tmo_ids: List[int]):
    if tmo_ids:
        async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
            stub = mo_info_pb2_grpc.InformerStub(channel)
            msg = mo_info_pb2.RequestTMOlifecycleByTMOidList(tmo_ids=tmo_ids)
            response = await stub.GetTMOlifecycle(msg)
            return response.tmo_ids_with_lifecycle
    return []


async def get_max_severity_for_mo_ids_with_particular_tmo(
    tmo_id: int, mo_ids: List[int]
):
    """Returns max severity for mo_ids with particular tmo"""
    async with grpc.aio.insecure_channel(
        INVENTORY_GRPC_URL,
        options=[
            ("grpc.max_send_message_length", 104857600),
            ("grpc.max_receive_message_length", 104857600),
            ("grpc.max_metadata_size", 104857600),
            ("grpc.absolute_max_metadata_size", 104857600),
        ],
    ) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)

        result = [0]
        msg = mo_info_pb2.RequestSeverityMoId(tmo_id=tmo_id, mo_ids=mo_ids)
        response = await stub.GetMOSeverityMaxValue(msg)
        result.append(response.max_severity)
        return max(result)


async def get_mo_matched_condition(
    object_type_id: int = None,
    query_params: QueryParams = None,
    order_by: dict = None,
    decoded_jwt: dict = None,
    mo_ids: List[int] = None,
    p_ids: List[int] = None,
    only_ids: bool = False,
    tprm_ids: List[int] = None,
    mo_attrs: List[str] = None,
):
    async with grpc.aio.insecure_channel(
        INVENTORY_GRPC_URL,
        options=[
            ("grpc.max_send_message_length", 104857600),
            ("grpc.max_receive_message_length", 104857600),
            ("grpc.max_metadata_size", 104857600),
            ("grpc.absolute_max_metadata_size", 104857600),
        ],
    ) as channel:
        request_dict = {}
        if object_type_id:
            request_dict["object_type_id"] = object_type_id

        if query_params:
            request_dict["query_params"] = pickle.dumps(query_params).hex()

        if order_by:
            request_dict["order_by"] = pickle.dumps(order_by).hex()

        if decoded_jwt:
            request_dict["decoded_jwt"] = pickle.dumps(decoded_jwt).hex()

        if mo_ids:
            request_dict["mo_ids"] = mo_ids

        if p_ids:
            request_dict["p_ids"] = p_ids

        if only_ids:
            request_dict["only_ids"] = only_ids

        if tprm_ids:
            request_dict["tprm_ids"] = tprm_ids

        if mo_attrs:
            request_dict["mo_attrs"] = mo_attrs

        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestForFilteredObjSpecial(**request_dict)
        grpc_response = stub.GetFilteredObjSpecial(msg)

        response = {"mo_ids": [], "pickle_mo_dataset": []}
        async for grpc_chunk in grpc_response:
            response["mo_ids"].extend(grpc_chunk.mo_ids)
            response["pickle_mo_dataset"].extend(grpc_chunk.pickle_mo_dataset)

        if only_ids:
            return response["mo_ids"]
        else:
            return [
                pickle.loads(bytes.fromhex(item))
                for item in response["pickle_mo_dataset"]
            ]


async def get_children_mo_id_grouped_by_parent_node_id(
    request: mo_info_pb2.RequestListLevels,
):
    async with grpc.aio.insecure_channel(
        INVENTORY_GRPC_URL,
        options=[
            ("grpc.max_send_message_length", 104857600),
            ("grpc.max_receive_message_length", 104857600),
            ("grpc.max_metadata_size", 104857600),
            ("grpc.absolute_max_metadata_size", 104857600),
        ],
    ) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = request
        response = await stub.GetHierarchyLevelChildren(msg)

        return {
            item.node_id: list(item.children_mo_ids) for item in response.items
        }


async def get_tprms_data_by_tprms_ids(tprm_ids: Iterable[int]):
    """getter for GetTPRMData"""
    result = []
    async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.RequestTPRMData(tprm_ids=tprm_ids)
        resp = await stub.GetTPRMData(msg)
        result = [pickle.loads(bytes.fromhex(item)) for item in resp.tprms_data]

    return result


async def get_tmo_data_by_tmo_ids(tmo_ids: List[int]):
    """getter for GetTMOInfoByTMOId"""
    res = dict()
    async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = mo_info_pb2_grpc.InformerStub(channel)
        msg = mo_info_pb2.TMOInfoRequest(tmo_id=tmo_ids)
        resp = await stub.GetTMOInfoByTMOId(msg)
        if resp.tmo_info:
            res = pickle.loads(bytes.fromhex(resp.tmo_info))
    return res


async def get_mo_data_by_mo_ids(channel: Channel, mo_ids: Iterable[int]):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetMODataByIdsRequest(mo_ids=mo_ids)
    grpc_response = stub.GetMODataByIds(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_mo_prm_data_by_prm_ids(channel: Channel, prm_ids: Iterable[int]):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetPRMsByPRMIdsRequest(prm_ids=prm_ids)
    grpc_response = stub.GetPRMsByPRMIds(msg)
    async for grpc_chunk in grpc_response:
        yield grpc_chunk


async def get_all_mo_with_params_by_tmo_id(
    channel: Channel, tmo_id: int
) -> AsyncGenerator:
    """Returns AsyncGenerator with list of Mo attrs and params"""
    stub = mo_info_pb2_grpc.InformerStub(channel)
    msg = mo_info_pb2.GetAllMOWithParamsByTMOIdRequest(tmo_id=tmo_id)
    grpc_response = stub.GetAllMOWithParamsByTMOId(msg)
    async for grpc_chunk in grpc_response:
        res = [
            pickle.loads(bytes.fromhex(item))
            for item in grpc_chunk.mos_with_params
        ]
        yield res


async def get_all_mo_with_special_params_by_tmo_id(
    channel: Channel, tmo_id: int, tprm_ids: List[int] = None
):
    stub = mo_info_pb2_grpc.InformerStub(channel)
    request_data = {"tmo_id": tmo_id}
    if tprm_ids:
        request_data["tprm_ids"] = tprm_ids
    msg = mo_info_pb2.MOWithSpecialParametersRequest(**request_data)
    grpc_response = stub.GetAllMOByTMOIdWithSpecialParameters(msg)
    try:
        async for grpc_chunk in grpc_response:
            res = [
                pickle.loads(bytes.fromhex(item))
                for item in grpc_chunk.mos_with_params
            ]
            yield res
    except AioRpcError as e:
        print(f"gRPC error on request: {request_data}")
        print(f"Status: {e.code()}, Details: {e.details()}")
        raise


async def get_mo_links_tprms(tmo_id: int) -> list[int]:
    async with grpc.aio.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = InformerStub(channel)
        request = mo_info_pb2.RequestGetAllTPRMSByTMOId(tmo_id=tmo_id)
        response = stub.GetAllTPRMSByTMOId(request)

        async for res in response:
            tprms_data = [
                pickle.loads(bytes.fromhex(tprm_data))
                for tprm_data in res.tprms_data
            ]
            mo_links = [
                item["id"]
                for item in tprms_data
                if item.get("val_type") in ["mo_link"]
            ]

        return mo_links


def get_mo_links_values(mo_links: list[int]) -> dict[int, str]:
    with grpc.insecure_channel(INVENTORY_GRPC_URL) as channel:
        stub = InformerStub(channel)
        request = mo_info_pb2.RequestGetMOsNamesByIds(mo_ids=mo_links)
        response = stub.GetMOsNamesByIds(request)

        return response.mo_names


# if __name__ == '__main__':
#     logging.basicConfig()
#     asyncio.run(run())
