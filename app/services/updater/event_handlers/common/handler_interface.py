from abc import ABC, abstractmethod

from sqlalchemy.ext.asyncio import AsyncSession


class HierarchyChangeInterface(ABC):
    def __init__(self, msg: dict, session: AsyncSession, hierarchy_id: int):
        self.msg = msg
        self.session = session
        self.hierarchy_id = hierarchy_id

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "make_changes")
            and callable(subclass.make_changes)
            or NotImplemented
        )

    @abstractmethod
    async def make_changes(self):
        """Makes changes for special hierarchy"""
        pass
