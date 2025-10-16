from enum import Enum


class ObjEventStatus(str, Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
