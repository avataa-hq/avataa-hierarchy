# import aiohttp
# import settings
#
#
# async def get_request_session() -> aiohttp.ClientSession():
#     async with aiohttp.ClientSession(
#         settings.INVENTORY_URL, raise_for_status=True
#     ) as session:
#         yield session
