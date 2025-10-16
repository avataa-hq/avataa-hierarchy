import datetime
import uuid as uuid_pkg

from google.protobuf import struct_pb2, timestamp_pb2
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    false,
    true,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import validates
from sqlalchemy_utils import UUIDType
from sqlmodel import Field, SQLModel

from schemas.enum_models import SET_OF_AVAILABLE_STATUSES, HierarchyStatus


def default_uuid():
    # making sure uuid str does not start with a leading 0
    val = uuid_pkg.uuid4()
    while val.hex[0] == "0":
        val = uuid_pkg.uuid4()
    return val


class LevelCreate(SQLModel):
    """
    It is a data-only, pydantic model that will be used to create new instances.
    """

    level: int
    name: str
    description: str | None = None
    object_type_id: int
    is_virtual: bool
    additional_params_id: int | None = None
    latitude_id: int | None = None
    longitude_id: int | None = None
    author: str
    parent_id: int | None = None
    key_attrs: list[str]
    show_without_children: bool = True
    attr_as_parent: int | None = None


class LevelBase(LevelCreate):
    """
    The underlying data model.
    Used as a pydantic model for data validation.
    """

    param_type_id: int | None
    created: datetime.datetime = Field(
        default_factory=datetime.datetime.now, nullable=False
    )
    change_author: str | None
    modified: datetime.datetime | None


class Level(LevelBase, table=True):
    """
    It's a table model, so it's a pydantic and SQLAlchemy model.
    It represents a database table.
    """

    id: int | None = Field(default=None, primary_key=True, nullable=False)
    hierarchy_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("hierarchy.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    parent_id: int | None = Field(
        default=None,
        sa_column=Column(
            Integer,
            ForeignKey("level.id", ondelete="CASCADE"),
            index=True,
            nullable=True,
        ),
    )

    show_without_children: bool = Field(
        sa_column=Column(
            Boolean, server_default=true(), default=True, nullable=False
        )
    )
    key_attrs: list[str] = Field(
        sa_column=Column(
            ARRAY(String), server_default="{}", default=list(), nullable=False
        )
    )
    attr_as_parent: int | None = Field(
        sa_column=Column(Integer, nullable=True),
    )

    __table_args__ = (
        UniqueConstraint(
            "hierarchy_id", "name", name="_level_hierarchy_id_name"
        ),
        CheckConstraint(
            "level.id != level.parent_id", name="level_id_level_parent_id"
        ),
    )

    def to_proto(self):
        res = dict()
        simple_attrs = [
            "id",
            "name",
            "description",
            "level",
            "hierarchy_id",
            "parent_id",
            "object_type_id",
            "is_virtual",
            "param_type_id",
            "additional_params_id",
            "latitude_id",
            "longitude_id",
            "author",
            "change_author",
            "show_without_children",
            "key_attrs",
            "attr_as_parent",
        ]
        timestamp_attrs = ["created", "modified"]

        for attr_name in simple_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = atr_val

        for attr_name in timestamp_attrs:
            value = getattr(self, attr_name)
            if value:
                proto_timestamp = timestamp_pb2.Timestamp()
                proto_timestamp.FromDatetime(value)
                res[attr_name] = proto_timestamp
        return res


class HierarchyCreate(SQLModel):
    """
    It is a data-only, pydantic model that will be used to create new instances.
    """

    name: str
    description: str | None
    author: str
    create_empty_nodes: bool | None


class HierarchyBase(HierarchyCreate):
    """
    The underlying data model.
    Used as a pydantic model for data validation.
    """

    created: datetime.datetime = Field(
        default_factory=datetime.datetime.now, nullable=False
    )
    change_author: str | None
    modified: datetime.datetime | None
    status: str = Field(default=HierarchyStatus.NEW.value, nullable=False)

    @validates("status")
    def validate_status(self, key, status):
        if status not in SET_OF_AVAILABLE_STATUSES:
            raise ValueError(
                f"status must be one of {SET_OF_AVAILABLE_STATUSES}"
            )
        return status


class Hierarchy(HierarchyBase, table=True):
    """
    It's a table model, so it's a pydantic and SQLAlchemy model.
    It represents a database table.
    """

    __tablename__ = "hierarchy"

    id: int | None = Field(default=None, primary_key=True, nullable=False)
    create_empty_nodes: bool | None = Field(default=True, nullable=False)

    status: str = Field(
        sa_column=Column(
            String,
            server_default=HierarchyStatus.NEW.value,
            default=HierarchyStatus.NEW.value,
            nullable=False,
        )
    )

    __table_args__ = (UniqueConstraint("name"),)

    def to_proto(self):
        res = dict()
        simple_attrs = [
            "id",
            "name",
            "description",
            "author",
            "change_author",
            "status",
            "create_empty_nodes",
        ]
        timestamp_attrs = ["created", "modified"]

        for attr_name in simple_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = atr_val

        for attr_name in timestamp_attrs:
            value = getattr(self, attr_name)
            if value:
                proto_timestamp = timestamp_pb2.Timestamp()
                proto_timestamp.FromDatetime(value)
                res[attr_name] = proto_timestamp
        return res


class ObjBase(SQLModel):
    """
    The underlying data model.
    Used as a pydantic model for data validation.
    """

    key: str
    object_id: int | None
    object_type_id: int
    additional_params: str | None
    hierarchy_id: int
    level: int
    parent_id: str | None
    latitude: float | None
    longitude: float | None
    child_count: int = Field(default=0)
    active: bool = Field(default=True)
    key_is_empty: bool = Field(default=False)


class Obj(ObjBase, table=True):
    """
    It's a table model, so it's a pydantic and SQLAlchemy model.
    It represents a database table.
    """

    id: uuid_pkg.UUID | None = Field(
        default_factory=default_uuid,
        sa_column=Column(UUIDType(), primary_key=True, nullable=False),
    )
    hierarchy_id: int = Field(
        sa_column=Column(
            Integer, ForeignKey("hierarchy.id", ondelete="CASCADE"), index=True
        )
    )

    path: str | None = Field(default=None, nullable=True)

    # level_id int or None because we already have objects but without information about based level id
    level_id: int | None = Field(
        sa_column=Column(
            Integer, ForeignKey("level.id", ondelete="CASCADE"), index=True
        )
    )
    parent_id: uuid_pkg.UUID | None = Field(
        default=None,
        sa_column=Column(
            UUIDType(),
            ForeignKey("obj.id", ondelete="CASCADE"),
            index=True,
            nullable=True,
        ),
    )
    active: bool = Field(
        sa_column=Column(
            Boolean, server_default=true(), default=True, nullable=False
        )
    )
    key_is_empty: bool = Field(
        sa_column=Column(
            Boolean, server_default=false(), default=False, nullable=False
        )
    )

    def to_proto(self):
        res = dict()
        uuid_attrs = ["id", "parent_id"]
        simple_attrs = [
            "key",
            "object_id",
            "object_type_id",
            "additional_params",
            "hierarchy_id",
            "level",
            "level_id",
            "latitude",
            "longitude",
            "child_count",
            "active",
            "key_is_empty",
            "path",
        ]

        for attr_name in simple_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = atr_val
        for attr_name in uuid_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = str(atr_val)
        return res


class ObjCreate(Obj):
    """
    It is a data-only, pydantic model that will be used to create new instances.
    """

    pass


class ObjResponseNew(SQLModel):
    key: str
    object_id: int | None
    object_type_id: int
    additional_params: str | None
    hierarchy_id: int
    level: int
    parent_id: str | None
    latitude: float | None
    longitude: float | None
    child_count: int = Field(default=0)
    active: bool = Field(default=True)
    key_is_empty: bool = Field(default=False)

    id: uuid_pkg.UUID | None = None
    path: str | None = None
    level_id: int | None = None

    child: list["ObjResponseNew"] | None = None


class HierarchyRebuildOrder(SQLModel, table=True):
    """
    The database table is used to contain information about the ids of the hierarchies in the update queue and
    their stages.
    """

    id: int | None = Field(default=None, primary_key=True, nullable=False)
    hierarchy_id: int
    on_rebuild: bool = Field(default=False)


class NodeData(SQLModel, table=True):
    __tablename__ = "node_data"

    id: int = Field(
        sa_column=Column(BigInteger, primary_key=True, nullable=False)
    )
    level_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("level.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    node_id: uuid_pkg.UUID = Field(
        sa_column=Column(
            UUIDType(),
            ForeignKey("obj.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    mo_id: int = Field(sa_column=Column(BigInteger, nullable=False))
    mo_name: str = Field(sa_column=Column(String, nullable=False))
    mo_latitude: float | None = Field(sa_column=Column(Float, nullable=True))
    mo_longitude: float | None = Field(sa_column=Column(Float, nullable=True))
    mo_status: str = Field(sa_column=Column(String, nullable=True))
    mo_tmo_id: int = Field(sa_column=Column(Integer, nullable=False))
    mo_p_id: int = Field(sa_column=Column(BigInteger, nullable=True))
    mo_active: bool = Field(
        sa_column=Column(
            Boolean, server_default=true(), default=True, nullable=False
        )
    )
    unfolded_key: dict = Field(
        sa_column=Column(
            JSONB, server_default="{}", default=dict(), nullable=False
        )
    )

    def to_proto(self):
        res = dict()
        simple_attrs = [
            "id",
            "level_id",
            "mo_id",
            "mo_name",
            "mo_latitude",
            "mo_longitude",
            "mo_status",
            "mo_tmo_id",
            "mo_p_id",
            "mo_active",
        ]
        struct_attrs = ["unfolded_key"]
        uuid_attrs = ["node_id"]

        for attr_name in simple_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = atr_val

        for attr_name in struct_attrs:
            value = getattr(self, attr_name)
            if value:
                data = struct_pb2.Struct()
                correcting_dict = {str(k): v for k, v in value.items()}
                data.update(correcting_dict)
                res[attr_name] = data

        for attr_name in uuid_attrs:
            atr_val = getattr(self, attr_name)
            if atr_val is not None:
                res[attr_name] = str(atr_val)
        return res
