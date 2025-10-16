import time
import uuid

from elasticsearch.helpers import async_bulk, bulk

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from elastic.settings import (
    HIERARCHY_CHILDREN_INDEX_MAPPING,
    HIERARCHY_OBJ_INDEX_MAPPING,
    HIERARCHY_OBJ_INDEX_SETTINGS,
)
from elastic.utils import init_elastic_client


class ElasticClient:
    def __init__(self, hierarchy_id: str):
        self.CHUNK_SIZE = 2000000
        self.client = init_elastic_client()
        self.hierarchy_id = hierarchy_id
        self.index = f"hierarchy_obj_{hierarchy_id}_index"
        self.children_index = f"hierarchy_children_{hierarchy_id}_index"
        print(self.index)

    async def get_nodes_by_parents(
        self,
        parents: list[uuid.UUID] | None,
        include_default: bool = True,
        order_by: list[str] = None,
    ):
        start = time.time()
        print("get_nodes_by_parent")
        if parents is None:
            query = {"bool": {"must_not": [{"exists": {"field": "parent_id"}}]}}
        else:
            if not isinstance(parents, list):
                parents = [parents]
            if len(parents) == 0:
                print("empty")
                print(f"empty time: {time.time() - start}")
                return []
            query = {
                "bool": {
                    "should": [{"terms": {"parent_id": parents}}],
                    "minimum_should_match": 1,
                }
            }

        if not include_default:
            if query["bool"].get("must_not", None) is None:
                query["bool"]["must_not"] = [
                    {"match": {"key": DEFAULT_KEY_OF_NULL_NODE}}
                ]
            else:
                query["bool"]["must_not"].append(
                    {"match": {"key": DEFAULT_KEY_OF_NULL_NODE}}
                )

        if order_by:
            order_by = [{field: {"order": "asc"} for field in order_by}]

        # search_after: None | list = None
        # pit = await self.client.open_point_in_time(index=self.index, keep_alive='1m')
        # pit_clause = {'id': pit['id'], 'keep_alive': '1m'}
        # print(f'creating point in time: {time.time() - st1}')
        # while True:
        #     st = time.time()
        #     res = await self.client.search(query=query, size=self.CHUNK_SIZE, sort=order_by,
        #                                    search_after=search_after, pit=pit_clause)
        #     print(f'elastic request took: {time.time() - st}')
        #     st = time.time()
        #     res = res['hits']['hits']
        #     result.extend([r['_source'] for r in res])
        #
        #     if len(res) == 0:
        #         break
        #
        #     search_after = res[-1]['sort']
        #     print(f'got {len(res)} results')
        #     if search_after[0] is None:
        #         break
        #     print(f'post processing took: {time.time() - st}')
        # await self.client.close_point_in_time(id=pit['id'])

        st = time.time()
        result = await self.client.search(
            index=self.index, query=query, size=self.CHUNK_SIZE, sort=order_by
        )
        print(f"elastic request took: {time.time() - st}")
        st = time.time()
        result = result["hits"]["hits"]
        result = [res["_source"] for res in result]
        print(f"post processing took: {time.time() - st}")

        print(f"got total: {len(result)}")
        return result

    async def __yield_documents(self, generator):
        async for chunk in generator.scalars().partitions(self.CHUNK_SIZE):
            for obj in chunk:
                doc = {"_index": self.index, "_source": obj.dict()}
                yield doc

    async def bulk_insert(self, objects_generator):
        await async_bulk(self.client, self.__yield_documents(objects_generator))

    async def delete_index(self):
        if await self.client.indices.exists(index=self.index):
            await self.client.indices.delete(index=self.index)

    async def create_index(self):
        if not await self.client.indices.exists(index=self.index):
            await self.client.indices.create(
                index=self.index,
                mappings=HIERARCHY_OBJ_INDEX_MAPPING,
                settings=HIERARCHY_OBJ_INDEX_SETTINGS,
            )

    async def delete_children_index(self):
        if await self.client.indices.exists(index=self.children_index):
            await self.client.indices.delete(index=self.children_index)

    def create_children_index(self):
        if not self.client.indices.exists(index=self.children_index):
            self.client.indices.create(
                index=self.children_index,
                mappings=HIERARCHY_CHILDREN_INDEX_MAPPING,
                settings=HIERARCHY_OBJ_INDEX_SETTINGS,
            )

    def bulk_insert_children(self, objects):
        def generate_objects(objs):
            for parent_id, obj in objs.items():
                if parent_id is None:
                    print(parent_id)
                    print(obj)
                    time.sleep(20)
                doc = {
                    "_index": self.children_index,
                    "_source": {"parent_id": parent_id, "children": obj},
                }
                yield doc

        bulk(self.client, generate_objects(objects))

    def get_nodes_children(self, parents):
        if parents is None:
            query = {"bool": {"must_not": {"exists": {"field": {"parent_id"}}}}}
        else:
            if not isinstance(parents, list):
                parents = [parents]
            if len(parents) == 0:
                return []
            query = {
                "bool": {
                    "must": {
                        "terms": {
                            "parent_id": [str(parent) for parent in parents]
                        }
                    }
                }
            }
        result = self.client.search(
            index=self.children_index, query=query, size=self.CHUNK_SIZE
        )
        return {
            res["_source"]["parent_id"]: res["_source"]["children"]
            for res in result["hits"]["hits"]
        }
