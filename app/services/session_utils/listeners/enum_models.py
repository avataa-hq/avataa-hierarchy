from enum import Enum


class SessionDataKeys(Enum):
    NEW = "created_instances"
    DELETED = "deleted_instances"
    DIRTY = "updated_instances"
