from collections import defaultdict
import json
import re

import grpc.aio
from starlette.datastructures import QueryParams

from grpc_config.search.proto import mo_finder_pb2
from grpc_config.search.proto.mo_finder_pb2_grpc import MOFinderStub
from grpc_config.search.settings import SEARCH_GRPC_URL
from models import FilterColumn


class SearchClient:
    regex_pattern = r"tprm_id(\d+)\|([^=]+)"

    @classmethod
    async def get_mo_ids_by_filters(
        cls,
        tmo_id: int,
        query_params: QueryParams = None,
        column_filters: list[FilterColumn] = None,
        mo_ids: list[int] = None,
        p_ids: list[int] = None,
        only_ids: bool = False,
        tprm_ids: list[int] = None,
    ):
        filters = []
        if query_params:
            filters.extend(await cls._convert_filters_to_dicts(query_params))

        if column_filters:
            filters.extend([cf.dict(by_alias=True) for cf in column_filters])
        filters = json.dumps(filters)

        async with grpc.aio.insecure_channel(f"{SEARCH_GRPC_URL}") as channel:
            stub = MOFinderStub(channel=channel)
            request = mo_finder_pb2.RequestGetMOsByFilters(
                tmo_id=tmo_id,
                filters=filters,
                mo_ids=mo_ids,
                p_ids=p_ids,
                only_ids=only_ids,
                tprm_ids=tprm_ids,
            )
            response = stub.GetMOsByFilters(request)
            objects = []
            async for resp in response:
                part = list(resp.mos)
                for obj in part:
                    obj = json.loads(obj)
                    if only_ids:
                        obj = obj["id"]

                    objects.append(obj)

            return objects

    @classmethod
    async def _convert_filters_to_dicts(
        cls, query_filters: QueryParams
    ) -> list[dict]:
        regex_equal = re.compile(r"tprm_id(\d+)\|([^=]+)")
        raw_filters = defaultdict(list)

        for key, value in query_filters.multi_items():
            match = regex_equal.fullmatch(key)
            if match:
                match_values = regex_equal.findall(key)[0]
                tprm_id = int(match_values[0])

                raw_filters[tprm_id].append(
                    {
                        "operator": match_values[1],
                        "value": value,
                    }
                )

        filters = []
        for param, column_filters in raw_filters.items():
            col_filter = {
                "columnName": param,
                "rule": "and",
                "filters": column_filters,
            }
            filters.append(col_filter)

        return filters
