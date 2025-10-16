from sqlalchemy import Column, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()


class PermissionTemplate(Base):
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True)
    root_permission_id: int | None = Column(Integer, nullable=True)
    permission: Mapped[str] = mapped_column(nullable=False)
    permission_name: Mapped[str] = mapped_column(nullable=False)
    create: Mapped[bool] = mapped_column(default=False, nullable=False)
    read: Mapped[bool] = mapped_column(default=False, nullable=False)
    update: Mapped[bool] = mapped_column(default=False, nullable=False)
    delete: Mapped[bool] = mapped_column(default=False, nullable=False)
    admin: Mapped[bool] = mapped_column(default=False, nullable=False)
    parent_id: int = Column(Integer, primary_key=True)

    __table_args__ = (UniqueConstraint("parent_id", "permission"),)

    def update_from_dict(self, item: dict):
        for key, value in item.items():
            if not hasattr(self, key):
                continue
            setattr(self, key, value)

    def to_dict(self, only_actions: bool = False):
        res = self.__dict__
        if "_sa_instance_state" in res:
            res.pop("_sa_instance_state")
            relationships = self.__mapper__.relationships.keys()
            if relationships:
                for relationship in relationships:
                    if relationship in res:
                        res.pop(relationship)
        if only_actions:
            return {
                "create": res["create"],
                "read": res["read"],
                "update": res["update"],
                "delete": res["delete"],
                "admin": res["admin"],
            }
        return res
