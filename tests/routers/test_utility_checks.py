import re

import settings

INVENTORY_DB = {"TMO": [1, 2, 3, 4, 5, 6], "TPRM": [7, 8, 9, 10, 11]}
LEVEL_DEFAULT_DATA = {
    "level": 0,
    "name": "Test name",
    "description": "Test description",
    "object_type_id": 1,
    "is_virtual": True,
    "param_type_id": 7,
    "additional_params_id": 8,
    "latitude_id": 9,
    "longitude_id": 10,
    "author": "Test Author",
    "hierarchy_id": 1,
    "key_attrs": [7],
}


# Mock response for mocked async client
class MockResponse:
    def __init__(self, json, status):
        self._json = json
        self._status = status
        self.url = None

    async def json(self):
        return self._json

    @property
    def status(self):
        return self._status

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self


# Mock async client
# Uses for mock request data from inventory
class MockTestClient:
    def get(self, *args, **kwargs):
        url = args[0] if args[0] else kwargs.get("url", "")

        if url:
            tmo_inventory_url = re.compile(rf"{settings.INV_TMO_DETAIL}.*")
            tprm_inventory_url = re.compile(rf"{settings.INV_TPRM_DETAIL}.*")

            if tmo_inventory_url.match(url) is not None:
                item_id = re.search(r"[0-9]*$", url)
                item_id = item_id[0]
                if int(item_id) in INVENTORY_DB["TMO"]:
                    return MockResponse(json={}, status=200)
                else:
                    return MockResponse(json={}, status=404)

            elif tprm_inventory_url.match(url) is not None:
                item_id = re.search(r"[0-9]*$", url)
                item_id = item_id[0]

                if int(item_id) in INVENTORY_DB["TPRM"]:
                    return MockResponse(json={"id": item_id}, status=200)
                else:
                    return MockResponse(json={}, status=404)
            else:
                return MockResponse(json={}, status=404)
        else:
            return MockResponse(json={}, status=404)

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self
