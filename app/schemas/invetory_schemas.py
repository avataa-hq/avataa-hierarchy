from pydantic.typing import Any
from sqlmodel import SQLModel


class ParamInventory(SQLModel):
    id: int
    tprm_id: int
    mo_id: int
    value: Any


class ObjectInventory(SQLModel):
    id: int
    p_id: int | None
    tmo_id: int
    name: str


class ObjectWithParams(ObjectInventory):
    params: list[ParamInventory]
