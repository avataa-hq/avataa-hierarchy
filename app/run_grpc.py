import asyncio

from grpc_server.main_grpc_server import serve

if __name__ == '__main__':
    asyncio.run(serve())
