import subprocess

from fastapi import Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.event import listen
from sqlalchemy.orm import Session
import uvicorn

from database import database
from init_app import create_app
from kafka_config.config import KAFKA_TURN_ON
from kafka_producer.session_listener.listener import (
    receive_after_commit,
    receive_after_flush,
)
from routers import hierarchy_object_router, hierarchy_router, level_router
from routers.hierarchy_object.router import router as hierarchy_object_routers
from services.kafka.process_manager.impl import (
    KafkaConsumerProcessManager,
    init_all_kafka_consumer_processes_with_admin_session,
)
from services.security.data import listener  # noqa
from services.security.routers.hierarchy import (
    router as security_permissions_router,
)
from services.security.security_factory import security
import settings

prefix = "/api/hierarchy"
app_title = "Hierarchy"
app_version = "1"
v1_options = {
    "root_path": f"{prefix}/v{app_version}",
    "title": app_title,
    "version": app_version,
}


if settings.DEBUG:
    app = create_app(
        root_path=prefix,
        openapi_url=f"{prefix}/openapi.json",
        docs_url=f"{prefix}/docs",
    )
    v1_app = create_app(**v1_options)

else:
    app = create_app(
        root_path=prefix,
        dependencies=[Depends(security)],
        openapi_url=f"{prefix}/openapi.json",
        docs_url=f"{prefix}/docs",
    )
    v1_app = create_app(dependencies=[Depends(security)], **v1_options)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

v1_app.include_router(hierarchy_router.router)
v1_app.include_router(level_router.router)
v1_app.include_router(hierarchy_object_router.router)
v1_app.include_router(hierarchy_object_routers)

v1_app.include_router(security_permissions_router)

app.mount("/v1", v1_app)


@app.on_event("startup")
async def on_startup():
    await database.init()

    if KAFKA_TURN_ON:
        # Register common listeners for kafka
        listen(Session, "after_flush", receive_after_flush)
        listen(Session, "after_commit", receive_after_commit)
        await init_all_kafka_consumer_processes_with_admin_session()


@app.on_event("shutdown")
def shutdown_event():
    # terminate all active children processes
    p_m = KafkaConsumerProcessManager()
    p_m.stop_all_processes()


if __name__ == "__main__":
    subprocess.run(["python", "grpc_server/grpc_server.py"])
    uvicorn.run(app="main:app", port=8000, reload=True)
