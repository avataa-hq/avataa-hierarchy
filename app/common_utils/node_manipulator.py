"""Node manipulator class"""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from schemas.hier_schemas import Obj


class NodeManipulator:
    def __init__(self, node: Obj, session: AsyncSession):
        self.node = node
        self.session = session

    async def get_short_chain_of_parent_nodes(self):
        """Looks for a path to the nearest parent with a different tmo
        and returns ordered list of parent nodes for this node."""

        async def recursive_find_parents(
            parent_node_id: UUID, session: AsyncSession
        ):
            stm = select(Obj).where(Obj.id == parent_node_id)
            parent = await session.execute(stm)
            parent = parent.scalars().first()

            if parent.object_id is not None:
                return [parent]
            else:
                if parent.parent_id is not None:
                    return [parent] + await recursive_find_parents(
                        parent.parent_id, session
                    )
                else:
                    return [parent]

        if self.node.parent_id is None:
            return []
        else:
            return await recursive_find_parents(
                self.node.parent_id, self.session
            )

    async def get_full_chain_of_parent_nodes(self):
        """Returns full breadcrumbs for current node"""

        async def recursive_find_parent(node: Obj, session: AsyncSession):
            if node is None:
                return []
            if node.parent_id is None:
                return [node]
            stmt = select(Obj).where(Obj.id == node.parent_id)
            parent_node = await session.execute(stmt)
            parent_node = parent_node.scalars().first()
            return await recursive_find_parent(parent_node, session) + [node]

        return await recursive_find_parent(self.node, self.session)

    async def delete_node(self):
        """Deletes current node and change child count for parent node.
        And if parent object is virtual and have child_count
        equal to 0 - delete this parent object and make same check for parent of deleted parent objects..."""

        parents = await self.get_short_chain_of_parent_nodes()

        await self.session.delete(self.node)

        for parent in parents:
            parent.child_count = parent.child_count - 1

            if parent.object_id is None and parent.child_count == 0:
                await self.session.delete(parent)
                continue
            else:
                self.session.add(parent)
                break

        await self.session.commit()
