import os

SEARCH_HOST = os.environ.get("SEARCH_HOST", "search")
SEARCH_GRPC_PORT = os.environ.get("SEARCH_GRPC_PORT", "50051")
SEARCH_GRPC_URL = f"{SEARCH_HOST}:{SEARCH_GRPC_PORT}"
