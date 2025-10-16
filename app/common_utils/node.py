from typing import Iterable
import uuid as uuid_pkg

from schemas.hier_schemas import Obj


class Node:
    def __init__(
        self,
        object_ids,
        data: dict,
        parent_ids=None,
        child: set | None = None,
        node_id=None,
        is_virtual=False,
    ):
        self.id = node_id if node_id else uuid_pkg.uuid4()
        self.object_ids = object_ids
        self.data: dict = data
        self.parent_ids: set = parent_ids
        self.child: set[Node] | None = self.__check(child) if child else None
        self.is_virtual = is_virtual

    @property
    def object_ids(self):
        return self._object_ids

    @object_ids.setter
    def object_ids(self, new_value):
        if new_value is None:
            self._object_ids = None
        elif isinstance(new_value, Iterable):
            self._object_ids = set(new_value)
        else:
            self._object_ids = {new_value}

    @property
    def parent_ids(self):
        return self._parent_ids

    @parent_ids.setter
    def parent_ids(self, new_value):
        if new_value is None:
            self._parent_ids = None
        elif isinstance(new_value, Iterable):
            self._parent_ids = set(new_value)
        else:
            self._parent_ids = {new_value}

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, new_value):
        if new_value is None:
            self._child = None
        elif isinstance(new_value, Iterable):
            self._child = set(new_value)
        else:
            self._child = {new_value}

    @staticmethod
    def __check(node_object):
        if not isinstance(node_object, Node):
            raise TypeError(
                f'Type mismatch. Expected data type "{__class__.__name__}", '
                f'received data type "{type(node_object).__name__}"'
            )
        return node_object

    def __repr__(self):
        return (
            f"Node[id={self.id},"
            f"object_ids={self.object_ids}, "
            f"data={self.data}, "
            f"parent_ids={self.parent_ids}, "
            f"is_virtual={self.is_virtual}, "
            f"child={self._child}]"
        )

    def __hash__(self):
        if self.is_virtual:
            return hash(
                (
                    __name__,
                    self.data["hierarchy_id"],
                    self.data["level"],
                    self.data["key"],
                )
            )
        else:
            return hash(
                (
                    __name__,
                    self.data["hierarchy_id"],
                    self.data["level"],
                    frozenset(self.object_ids),
                )
            )

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False
        result = [self.data == other.data, self.is_virtual == other.is_virtual]
        if not self.is_virtual:
            result.append(self.object_ids == other.object_ids)
        return all(result)

    def add_child(self, child):
        self.__check(child)
        if not self.child:
            self.child = set()
        self.child.add(child)

    def build(self, parent_id=None, get_one=False) -> list[Obj]:
        object_id = None if self.is_virtual else next(iter(self._object_ids))
        parent_id = parent_id
        if parent_id is None:
            parent_id = (
                next(iter(self._parent_ids)) if self._parent_ids else None
            )
        child_count = len(self._child or {})
        result = [
            Obj(
                id=self.id,
                object_id=object_id,
                parent_id=parent_id,
                child_count=child_count,
                **self.data,
            )
        ]
        if get_one:
            return result[0]
        if self._child:
            child_nodes = []
            for c in self._child:
                child_nodes.extend(c.build(parent_id=self.id))
            result.extend(child_nodes)
        return result
