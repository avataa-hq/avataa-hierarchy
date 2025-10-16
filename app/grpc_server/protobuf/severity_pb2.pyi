from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListHierarchyId(_message.Message):
    __slots__ = ["hierarchy_id"]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    hierarchy_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, hierarchy_id: _Optional[_Iterable[int]] = ...) -> None: ...

class SeverityHierarchyIdResponse(_message.Message):
    __slots__ = ["hierarchy_id", "count", "severity"]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    hierarchy_id: int
    count: int
    severity: float
    def __init__(self, hierarchy_id: _Optional[int] = ..., count: _Optional[int] = ..., severity: _Optional[float] = ...) -> None: ...

class ListSeverityHierarchyIdResponse(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[SeverityHierarchyIdResponse]
    def __init__(self, items: _Optional[_Iterable[_Union[SeverityHierarchyIdResponse, _Mapping]]] = ...) -> None: ...

class ListNodeId(_message.Message):
    __slots__ = ["node_id"]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    node_id: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, node_id: _Optional[_Iterable[str]] = ...) -> None: ...

class SeverityNodeIdResponse(_message.Message):
    __slots__ = ["node_id", "count", "severity"]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    count: int
    severity: float
    def __init__(self, node_id: _Optional[str] = ..., count: _Optional[int] = ..., severity: _Optional[float] = ...) -> None: ...

class ListSeverityNodeIdResponse(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[SeverityNodeIdResponse]
    def __init__(self, items: _Optional[_Iterable[_Union[SeverityNodeIdResponse, _Mapping]]] = ...) -> None: ...

class MoIdsByNode(_message.Message):
    __slots__ = ["node_id", "mo_ids"]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, node_id: _Optional[str] = ..., mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class MoIdsByHierarchy(_message.Message):
    __slots__ = ["hierarchy_id", "mo_ids"]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    hierarchy_id: int
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, hierarchy_id: _Optional[int] = ..., mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ListOfHierarchiesMoIds(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[MoIdsByHierarchy]
    def __init__(self, items: _Optional[_Iterable[_Union[MoIdsByHierarchy, _Mapping]]] = ...) -> None: ...

class ListOfNodesMoIds(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[MoIdsByNode]
    def __init__(self, items: _Optional[_Iterable[_Union[MoIdsByNode, _Mapping]]] = ...) -> None: ...

class RequestNodesWithCondition(_message.Message):
    __slots__ = ["hierarchy_id", "request_query", "parent_id", "tmo_id"]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_QUERY_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    hierarchy_id: int
    request_query: str
    parent_id: str
    tmo_id: int
    def __init__(self, hierarchy_id: _Optional[int] = ..., request_query: _Optional[str] = ..., parent_id: _Optional[str] = ..., tmo_id: _Optional[int] = ...) -> None: ...

class ResponseNodesWithCondition(_message.Message):
    __slots__ = ["items"]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ResponseNodesWithConditionItem]
    def __init__(self, items: _Optional[_Iterable[_Union[ResponseNodesWithConditionItem, _Mapping]]] = ...) -> None: ...

class ResponseNodesWithConditionItem(_message.Message):
    __slots__ = ["id", "parent_id", "object_id", "additional_params", "latitude", "child_count", "key", "hierarchy_id", "level_id", "object_type_id", "level", "longitude", "children_mo_ids"]
    ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_PARAMS_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    CHILD_COUNT_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    HIERARCHY_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_MO_IDS_FIELD_NUMBER: _ClassVar[int]
    id: str
    parent_id: str
    object_id: int
    additional_params: str
    latitude: float
    child_count: int
    key: str
    hierarchy_id: int
    level_id: int
    object_type_id: int
    level: int
    longitude: float
    children_mo_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, id: _Optional[str] = ..., parent_id: _Optional[str] = ..., object_id: _Optional[int] = ..., additional_params: _Optional[str] = ..., latitude: _Optional[float] = ..., child_count: _Optional[int] = ..., key: _Optional[str] = ..., hierarchy_id: _Optional[int] = ..., level_id: _Optional[int] = ..., object_type_id: _Optional[int] = ..., level: _Optional[int] = ..., longitude: _Optional[float] = ..., children_mo_ids: _Optional[_Iterable[int]] = ...) -> None: ...
