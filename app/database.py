"""
The class for connecting to the database.
Use init_database to create a database connection once at the start.
Use get_session every time you send a new query to the database.
"""

import asyncio
from typing import AsyncIterator

from fastapi import Depends, HTTPException, status
from fastapi.requests import Request
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)

from schemas.main_base_connector import Base
from services.security.data.utils import add_security_data
from services.security.security_data_models import UserData
from services.security.security_factory import security
from services.security.utils import get_admin_user_model
import settings


class Database:
    def __init__(self, database_url):
        self.url = database_url
        self.engine = None
        self.async_session = None
        self.async_session_factory = None
        self.get_session = self.init_get_session_function()

    async def init(self):
        try:
            self.engine = create_async_engine(
                self.url,
                echo=False,
                future=True,
                pool_pre_ping=True,
                connect_args={
                    "server_settings": {
                        "application_name": "Hierarchy MS",
                        "search_path": settings.DB_SCHEMA,
                    },
                },
            )
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            self.async_session_factory = async_sessionmaker(
                self.engine,
                expire_on_commit=False,
                autoflush=False,
                autocommit=False,
            )
            self.async_session = async_scoped_session(
                self.async_session_factory, scopefunc=asyncio.current_task
            )
        except Exception as e:
            print(e)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection error.",
            )

    def init_get_session_function(self):
        result = self.__get_session_with_enable_perm_check
        return result

    async def __get_session_with_disable_perm_check(
        self, request: Request
    ) -> AsyncIterator[AsyncSession]:
        async with self.async_session() as session:
            add_security_data(
                session=session,
                request=request,
                user_data=get_admin_user_model(),
            )
            yield session

    async def __get_session_with_enable_perm_check(
        self, request: Request, user_data: UserData = Depends(security)
    ):
        async with self.async_session() as session:
            add_security_data(
                session=session, request=request, user_data=user_data
            )
            yield session


database = Database(database_url=settings.DATABASE_URL)

async_engine = create_async_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    echo=False,
    connect_args={
        "server_settings": {
            "application_name": "Hierarchy MS Kafka",
            "search_path": settings.DB_SCHEMA,
        },
    },
)
async_session_maker_with_admin_perm = async_sessionmaker(
    async_engine,
    expire_on_commit=False,
)
