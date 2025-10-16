from sqlmodel import Session, create_engine

from settings import DATABASE_URL

engine = create_engine(DATABASE_URL)


def get_session():
    with Session(engine) as session:
        yield session
