from sqlalchemy.orm import declarative_base

from schemas.hier_schemas import SQLModel
from services.security.data.permissions.hierarchy import HierarchyPermission

Base = declarative_base(metadata=SQLModel.metadata)


list_of_tables = [HierarchyPermission]

# register tables in base metadata
for table in list_of_tables:
    table.__table__.to_metadata(Base.metadata)
