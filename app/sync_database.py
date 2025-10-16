from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from settings import SYNC_DB_URL

engine = create_engine(SYNC_DB_URL)
session_maker = sessionmaker(engine)


def get_session():
    with session_maker() as session:
        yield session
