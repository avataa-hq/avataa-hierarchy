from collections import namedtuple

from schemas.hier_schemas import Hierarchy
from services.security.data.permissions.hierarchy import HierarchyPermission

Permission = namedtuple("Permission", ["main", "security", "column"])


db_permissions = {
    Hierarchy.__tablename__: Permission(
        main=Hierarchy, security=HierarchyPermission, column="id"
    )
}

db_admins = {"realm_access.__admin"}
