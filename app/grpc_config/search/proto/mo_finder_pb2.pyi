from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RequestGetMOsByFilters(_message.Message):
    __slots__ = ("tmo_id", "filters", "mo_ids", "p_ids", "tprm_ids", "only_ids")
    TMO_ID_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    MO_IDS_FIELD_NUMBER: _ClassVar[int]
    P_IDS_FIELD_NUMBER: _ClassVar[int]
    TPRM_IDS_FIELD_NUMBER: _ClassVar[int]
    ONLY_IDS_FIELD_NUMBER: _ClassVar[int]
    tmo_id: int
    filters: str
    mo_ids: _containers.RepeatedScalarFieldContainer[int]
    p_ids: _containers.RepeatedScalarFieldContainer[int]
    tprm_ids: _containers.RepeatedScalarFieldContainer[int]
    only_ids: bool
    def __init__(self, tmo_id: _Optional[int] = ..., filters: _Optional[str] = ..., mo_ids: _Optional[_Iterable[int]] = ..., p_ids: _Optional[_Iterable[int]] = ..., tprm_ids: _Optional[_Iterable[int]] = ..., only_ids: bool = ...) -> None: ...

class ResponseGetMOsByFilters(_message.Message):
    __slots__ = ("mos",)
    MOS_FIELD_NUMBER: _ClassVar[int]
    mos: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, mos: _Optional[_Iterable[str]] = ...) -> None: ...
