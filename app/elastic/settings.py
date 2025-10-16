import os

ES_PROTOCOL = os.getenv("ES_PROTOCOL", "https")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER", "search_user")
ES_PASS = os.getenv("ES_PASS", None)
ES_URL = f"{ES_PROTOCOL}://{ES_HOST}"
if ES_PORT:
    ES_URL += f":{ES_PORT}"

# MAPPINGS
HIERARCHY_OBJ_INDEX_MAPPING = {
    "properties": {
        "id": {"type": "keyword"},
        "hierarchy_id": {"type": "long"},
        "parent_id": {"type": "keyword"},
        "key": {"type": "keyword"},
        "object_id": {"type": "long"},
        "additional_params": {"type": "keyword"},
        "level": {"type": "long"},
        "latitude": {"type": "double"},
        "longitude": {"type": "double"},
        "child_count": {"type": "long"},
        "object_type_id": {"type": "long"},
        "level_id": {"type": "long"},
    }
}

HIERARCHY_OBJ_INDEX_SETTINGS = {
    "index.number_of_shards": 2,
    "index.number_of_replicas": 1,
    "index.max_terms_count": 2147483646,
    "index.max_result_window": 2000000,
    "index.mapping.total_fields.limit": 10000,
    "index.store.preload": ["nvd", "dvd"],
}

HIERARCHY_CHILDREN_INDEX_MAPPING = {
    "properties": {
        "parent_id": {"type": "keyword"},
    }
}
