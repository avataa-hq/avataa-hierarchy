from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from services.security.data.permissions.permission_template import (
    PermissionTemplate,
)


class HierarchyPermission(PermissionTemplate):
    __tablename__ = "hierarchy_permission"
    root_permission_id: int | None = Column(
        Integer,
        ForeignKey(
            f"{__tablename__}.id", onupdate="CASCADE", ondelete="CASCADE"
        ),
        nullable=True,
        index=True,
    )
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("hierarchy.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    parent = relationship(
        "HierarchyPermission",
        backref="child",
        remote_side="HierarchyPermission.id",
        uselist=False,
    )

    def to_proto(self):
        res = dict()
        simple_attrs = [
            "id",
            "root_permission_id",
            "permission",
            "permission_name",
            "create",
            "read",
            "update",
            "delete",
            "admin",
            "parent_id",
        ]

        for attr_name in simple_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = atr_val
        return res
