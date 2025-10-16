from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class Operator(Enum):
    CONTAINS = "contains"
    NOT_CONTAINS = "notContains"
    EQUALS = "equals"
    NOT_EQUALS = "notEquals"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"
    IS_EMPTY = "isEmpty"
    IS_NOT_EMPTY = "isNotEmpty"
    IS_ANY_OF = "isAnyOf"
    IS_NOT_ANY_OF = "isNotAnyOf"
    MORE = "more"
    MORE_OR_EQ = "moreOrEq"
    LESS = "less"
    LESS_OR_EQ = "lessOrEq"


class FilterItem(BaseModel):
    operator: Operator
    value: Any

    def dict(self, *args, **kwargs):
        result = super().dict(*args, **kwargs)
        result["operator"] = result["operator"].value
        return result


class FilterColumn(BaseModel):
    column: str = Field(min_length=1, alias="columnName")
    rule: Literal["and", "or"] = Field(default="and")
    filters: list[FilterItem]
