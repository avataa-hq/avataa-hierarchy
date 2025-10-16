from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HierarchyMessageSchema(_message.Message):
    __slots__ = ["author", "change_author", "create_empty_nodes", "created", "description", "id", "modified", "name", "status"]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    CHANGE_AUTHOR_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    CREATE_EMPTY_NODES_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    author: str
    change_author: str
    create_empty_nodes: bool
    created: _timestamp_pb2.Timestamp
    description: str
    id: int
    modified: _timestamp_pb2.Timestamp
    name: str
    status: str
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., author: _Optional[str] = ..., change_author: _Optional[str] = ..., status: _Optional[str] = ..., create_empty_nodes: bool = ..., created: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modified: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class HierarchyPermissionMessageSchema(_message.Message):
    __slots__ = ["admin", "create", "delete", "id", "parent_id", "permission", "permission_name", "read", "root_permission_id", "update"]
    ADMIN_FIELD_NUMBER: _ClassVar[int]
    CREATE_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_NAME_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    ROOT_PERMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    admin: bool
    create: bool
    delete: bool
    id: int
    parent_id: int
    permission: str
    permission_name: str
    read: bool
    root_permission_id: int
    update: bool
    def __init__(self, id: _Optional[int] = ..., root_permission_id: _Optional[int] = ..., permission: _Optional[str] = ..., permission_name: _Optional[str] = ..., create: bool = ..., read: bool = ..., update: bool = ..., delete: bool = ..., admin: bool = ..., parent_id: _Optional[int] = ...) -> None: ...

class LevelMessageSchema(_message.Message):
    __slots__ = ["additional_params_id", "attr_as_parent", "author", "change_author", "created", "description", "hierarchy_id", "id", "is_virtual", "key_attrs", "latitude_id", "level", "longitude_id", "modified", "name", "object_type_id", "param_type_id", "parent_id", "show_without_children"]
    ADDITIONAL_PARAMS_ID_FIELD_NUMBER: _ClassVar[int]
    ATTR_AS_PARENT_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    CHANGE_AUTHOR_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    IS_VIRTUAL_FIELD_NUMBER: _ClassVar[int]
    KEY_ATTRS_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_ID_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    PARAM_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    SHOW_WITHOUT_CHILDREN_FIELD_NUMBER: _ClassVar[int]
    additional_params_id: int
    attr_as_parent: int
    author: str
    change_author: str
    created: _timestamp_pb2.Timestamp
    description: str
    hierarchy_id: int
    id: int
    is_virtual: bool
    key_attrs: _containers.RepeatedScalarFieldContainer[str]
    latitude_id: int
    level: int
    longitude_id: int
    modified: _timestamp_pb2.Timestamp
    name: str
    object_type_id: int
    param_type_id: int
    parent_id: int
    show_without_children: bool
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., level: _Optional[int] = ..., hierarchy_id: _Optional[int] = ..., parent_id: _Optional[int] = ..., object_type_id: _Optional[int] = ..., is_virtual: bool = ..., param_type_id: _Optional[int] = ..., additional_params_id: _Optional[int] = ..., latitude_id: _Optional[int] = ..., longitude_id: _Optional[int] = ..., author: _Optional[str] = ..., change_author: _Optional[str] = ..., created: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., modified: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., show_without_children: bool = ..., key_attrs: _Optional[_Iterable[str]] = ..., attr_as_parent: _Optional[int] = ...) -> None: ...

class ListHierarchy(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[HierarchyMessageSchema]
    def __init__(self, objects: _Optional[_Iterable[_Union[HierarchyMessageSchema, _Mapping]]] = ...) -> None: ...

class ListHierarchyPermission(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[HierarchyPermissionMessageSchema]
    def __init__(self, objects: _Optional[_Iterable[_Union[HierarchyPermissionMessageSchema, _Mapping]]] = ...) -> None: ...

class ListLevel(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[LevelMessageSchema]
    def __init__(self, objects: _Optional[_Iterable[_Union[LevelMessageSchema, _Mapping]]] = ...) -> None: ...

class ListNode(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[NodeMessageSchema]
    def __init__(self, objects: _Optional[_Iterable[_Union[NodeMessageSchema, _Mapping]]] = ...) -> None: ...

class ListNodeData(_message.Message):
    __slots__ = ["objects"]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    objects: _containers.RepeatedCompositeFieldContainer[NodeDataMessageSchema]
    def __init__(self, objects: _Optional[_Iterable[_Union[NodeDataMessageSchema, _Mapping]]] = ...) -> None: ...

class NodeDataMessageSchema(_message.Message):
    __slots__ = ["id", "level_id", "mo_active", "mo_id", "mo_latitude", "mo_longitude", "mo_name", "mo_p_id", "mo_status", "mo_tmo_id", "node_id", "unfolded_key"]
    ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ID_FIELD_NUMBER: _ClassVar[int]
    MO_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    MO_ID_FIELD_NUMBER: _ClassVar[int]
    MO_LATITUDE_FIELD_NUMBER: _ClassVar[int]
    MO_LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    MO_NAME_FIELD_NUMBER: _ClassVar[int]
    MO_P_ID_FIELD_NUMBER: _ClassVar[int]
    MO_STATUS_FIELD_NUMBER: _ClassVar[int]
    MO_TMO_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    UNFOLDED_KEY_FIELD_NUMBER: _ClassVar[int]
    id: int
    level_id: int
    mo_active: bool
    mo_id: int
    mo_latitude: float
    mo_longitude: float
    mo_name: str
    mo_p_id: int
    mo_status: str
    mo_tmo_id: int
    node_id: str
    unfolded_key: _struct_pb2.Struct
    def __init__(self, id: _Optional[int] = ..., level_id: _Optional[int] = ..., node_id: _Optional[str] = ..., mo_id: _Optional[int] = ..., mo_name: _Optional[str] = ..., mo_latitude: _Optional[float] = ..., mo_longitude: _Optional[float] = ..., mo_status: _Optional[str] = ..., mo_tmo_id: _Optional[int] = ..., mo_p_id: _Optional[int] = ..., mo_active: bool = ..., unfolded_key: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class NodeMessageSchema(_message.Message):
    __slots__ = ["active", "additional_params", "child_count", "hierarchy_id", "id", "key", "key_is_empty", "latitude", "level", "level_id", "longitude", "object_id", "object_type_id", "parent_id", "path"]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_PARAMS_FIELD_NUMBER: _ClassVar[int]
    CHILD_COUNT_FIELD_NUMBER: _ClassVar[int]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    KEY_IS_EMPTY_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ID_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    active: bool
    additional_params: str
    child_count: int
    hierarchy_id: int
    id: str
    key: str
    key_is_empty: bool
    latitude: float
    level: int
    level_id: int
    longitude: float
    object_id: int
    object_type_id: int
    parent_id: str
    path: str
    def __init__(self, id: _Optional[str] = ..., key: _Optional[str] = ..., object_id: _Optional[int] = ..., object_type_id: _Optional[int] = ..., additional_params: _Optional[str] = ..., hierarchy_id: _Optional[int] = ..., level: _Optional[int] = ..., level_id: _Optional[int] = ..., parent_id: _Optional[str] = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., child_count: _Optional[int] = ..., active: bool = ..., key_is_empty: bool = ..., path: _Optional[str] = ...) -> None: ...
