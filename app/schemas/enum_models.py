from enum import Enum


class HierarchyStatus(Enum):
    NEW = "New"  # not filled yet
    IN_PROCESS = "In Process"  # being refreshed
    COMPLETE = "Complete"  # refresh done
    ERROR = "Error"  # refresh failed


SET_OF_AVAILABLE_STATUSES = {i.value for i in HierarchyStatus}


class AvailableMOAttrsForLevelKeyAttrs(Enum):
    NAME = "name"
    LABEL = "label"
    STATUS = "status"


SET_OF_AVAILABLE_MO_ATTRS_FOR_LEVEL_KEY_ATTRS = {
    i.value for i in AvailableMOAttrsForLevelKeyAttrs
}


class InventoryClassNames(str, Enum):
    TMO = "TMO"
    MO = "MO"
    TPRM = "TPRM"
    PRM = "PRM"
