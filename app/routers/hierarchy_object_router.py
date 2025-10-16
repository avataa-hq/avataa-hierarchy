import http
import time
import uuid

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    WebSocket,
    status,
)
from fastapi.requests import Request
from fastapi.websockets import WebSocketDisconnect
from google.protobuf import json_format
import grpc
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from common_utils.hierarchy_builder import DEFAULT_KEY_OF_NULL_NODE
from common_utils.hierarchy_filter import HierarchyFilter
from common_utils.notifier import Notifier
from database import database
from grpc_config.protobuf import mo_info_pb2_grpc
from grpc_config.protobuf.mo_info_pb2 import RequestTMOlifecycleByTMOidList
from models import FilterColumn
from routers.hierarchy_object.utills.utils import (
    get_count_of_children_nodes_with_not_default_key,
    get_node_or_raise_error,
)
from routers.utility_checks import (
    check_hierarchy_exist,
    create_tree_from_parent_node,
)
from routers.utils import (
    get_total_results,
    update_nodes_key_if_mo_link_or_prm_link,
    update_nodes_key_if_mo_link_or_prm_link_as_dict,
)
from schemas.enum_models import HierarchyStatus
from schemas.hier_schemas import Hierarchy, Level, Obj, ObjResponseNew
from services.hierarchy.hierarchy_builder.builder import (
    HierarchyBuilderV2,
    refresh_hierarchy_with_error_catch,
)
from settings import INV_HOST, INVENTORY_GRPC_PORT

router = APIRouter()


@router.post(
    "/refresh",
    status_code=http.HTTPStatus.OK,
    tags=["Create/change hierarchy methods"],
)
async def refresh(
    hierarchy_id: int, session: AsyncSession = Depends(database.get_session)
):
    print("refresh endpoint")
    hierarchy = await check_hierarchy_exist(hierarchy_id, session)
    hierarchy.status = HierarchyStatus.IN_PROCESS.value
    session.add(hierarchy)
    print(hierarchy)
    await session.commit()
    print("status commited")

    # elastic = ElasticClient(hierarchy_id=hierarchy_id)
    # await elastic.delete_index()
    # await elastic.create_index()
    # print('Elastic end')
    print("Hierarchy refresh begins", flush=True)
    # try:
    #     keeper = HierarchyBuilderV2(db_session=session, hierarchy_id=hierarchy_id)
    #     await keeper.build_hierarchy()
    # except Exception as ex:
    #     await session.rollback()
    #     hierarchy.status = HierarchyStatus.ERROR.value
    #     session.add(hierarchy)
    #     await session.commit()
    #     raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(ex))
    await refresh_hierarchy_with_error_catch(
        session=session, hierarchy=hierarchy
    )

    hierarchy.status = HierarchyStatus.COMPLETE.value
    session.add(hierarchy)
    await session.commit()
    print("Hierarchy refresh finished")
    # rebuild_order_item = HierarchyRebuildOrder(hierarchy_id=hierarchy_id)
    # session.add(rebuild_order_item)
    # await session.commit()
    return {"details": "Completed"}


@router.post(
    "/refresh_all_hierarchies",
    status_code=http.HTTPStatus.OK,
    tags=["Create/change hierarchy methods"],
)
async def refresh_all_hierarchies(
    session: AsyncSession = Depends(database.get_session),
):
    stmt = select(Hierarchy)
    all_hierarchies = await session.execute(stmt)
    all_hierarchies = all_hierarchies.scalars().all()

    for hierarchy in all_hierarchies:
        hierarchy.status = HierarchyStatus.IN_PROCESS.value
        session.add(hierarchy)
        await session.commit()

        print("Hierarchy refresh begins")
        try:
            keeper = HierarchyBuilderV2(
                db_session=session, hierarchy_id=hierarchy.id
            )
            await keeper.build_hierarchy()
        except Exception as ex:
            await session.rollback()
            hierarchy.status = HierarchyStatus.ERROR.value
            session.add(hierarchy)
            await session.commit()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(ex),
            )

        hierarchy.status = HierarchyStatus.COMPLETE.value
        session.add(hierarchy)
        await session.commit()
        print("Hierarchy refresh finished")
    # rebuild_order_item = HierarchyRebuildOrder(hierarchy_id=hierarchy_id)
    # session.add(rebuild_order_item)
    # await session.commit()

    return {"details": "Completed"}


@router.get(
    "/hierarchy/{hierarchy_id}/parent/{parent_id}",
    response_model=list[Obj],
    tags=["Main"],
)
async def get_hierarchy_objects(
    hierarchy_id: int,
    parent_id: str,
    with_lifecycle: bool = False,
    session: AsyncSession = Depends(database.get_session),
):
    hierarchy_exist = await check_hierarchy_exist(hierarchy_id, session)

    if parent_id.upper() in ("ROOT", "NONE", "NULL"):
        parent_id = None
    else:
        try:
            parent_id = uuid.UUID(parent_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="badly formed hexadecimal UUID string",
            )
    if with_lifecycle:
        stmt = (
            select(Level.object_type_id)
            .where(Level.hierarchy_id == hierarchy_id)
            .distinct()
        )  # noqa
        uniq_tmo_for_hierarchy = await session.execute(stmt)
        response = uniq_tmo_for_hierarchy.scalars().all()
        if response:
            async with grpc.aio.insecure_channel(
                f"{INV_HOST}:{INVENTORY_GRPC_PORT}"
            ) as channel:
                stub = mo_info_pb2_grpc.InformerStub(channel)
                info_request = RequestTMOlifecycleByTMOidList(tmo_ids=response)
                tmo_with_lifecycles = await stub.GetTMOlifecycle(info_request)
                tmo_with_lifecycles_as_dict = json_format.MessageToDict(
                    tmo_with_lifecycles,
                    including_default_value_fields=True,
                    preserving_proto_field_name=True,
                )
                if tmo_with_lifecycles_as_dict["tmo_ids_with_lifecycle"]:
                    if hierarchy_exist.create_empty_nodes:
                        stmt = select(Obj).where(
                            and_(
                                Obj.hierarchy_id == hierarchy_id,
                                Obj.parent_id == parent_id,
                                Obj.object_type_id.in_(
                                    tmo_with_lifecycles_as_dict[
                                        "tmo_ids_with_lifecycle"
                                    ]
                                ),
                            )
                        )
                    else:
                        stmt = select(Obj).where(
                            and_(
                                Obj.hierarchy_id == hierarchy_id,
                                Obj.parent_id == parent_id,
                                Obj.object_type_id.in_(
                                    tmo_with_lifecycles_as_dict[
                                        "tmo_ids_with_lifecycle"
                                    ]
                                ),
                                Obj.key != DEFAULT_KEY_OF_NULL_NODE,
                            )
                        )

                    response = await session.execute(stmt)
                    response = response.scalars().all()
                    if len(response) == 0:
                        raise HTTPException(
                            status_code=http.HTTPStatus.NOT_FOUND,
                            detail="Not found",
                        )
                    return response
                else:
                    return tmo_with_lifecycles_as_dict["tmo_ids_with_lifecycle"]

    else:
        if hierarchy_exist.create_empty_nodes:
            query = select(Obj).where(
                and_(
                    Obj.hierarchy_id == hierarchy_id, Obj.parent_id == parent_id
                )
            )
        else:
            query = select(Obj).where(
                and_(
                    Obj.hierarchy_id == hierarchy_id,
                    Obj.parent_id == parent_id,
                    Obj.key != DEFAULT_KEY_OF_NULL_NODE,
                )
            )

        response = await session.execute(query)
        response = response.scalars().all()
        if len(response) == 0:
            raise HTTPException(
                status_code=http.HTTPStatus.NOT_FOUND, detail="Not found"
            )

        if not hierarchy_exist.create_empty_nodes:
            # recount children
            object_ids = [item.id for item in response]
            children_count = (
                await get_count_of_children_nodes_with_not_default_key(
                    p_ids=object_ids, session=session
                )
            )

            for item in response:
                item.child_count = children_count.get(item.id, 0)

        response = await update_nodes_key_if_mo_link_or_prm_link(
            response, session
        )

    # find levels with show_without_children is false
    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy_id,
        Level.show_without_children == False,  # noqa: E712
    )
    h_lev_show_without_children_false = await session.execute(stmt)
    h_lev_show_without_children_false = (
        h_lev_show_without_children_false.scalars().all()
    )

    if h_lev_show_without_children_false:
        level_ids = {level.id for level in h_lev_show_without_children_false}

        response = [
            item
            for item in response
            if item.level_id not in level_ids or item.child_count > 0
        ]

    return response


@router.post("/hierarchy/{hierarchy_id}/parent/{parent_id}/with_conditions")
async def get_child_nodes_of_parent_id_with_filter_condition(
    hierarchy_id: int,
    request: Request,
    parent_id: str,
    column_filters: list[FilterColumn] | None = Body(default=None),
    tmo_id: int = Query(default=None),
    session: AsyncSession = Depends(database.get_session),
):
    """Returns children nodes for parent_node with list of their real children mo_ids. \n
    Can work with filter conditions and without it.
    To obtain results for particular node without filter conditions just set hierarchy_id and parent_id. \n
    To obtain results for particular node with filter conditions set hierarchy_id, parent_id, tmo_id - to which
    will be applied the filter condition and query_params with filter conditions.

    Conditions must be sent in query_params.\n
    query_params example:\n
    tprm_id**8**|**contains**=**value**
    where:
    - tprm_id**number** : number = id of tprm
    - **contains** : filter flag
    - **value** : search value\n
    **for more information go to inventory swagger - tag: Filter helper.**
    """
    start = time.time()
    hierarchy_filter_data = {
        "hierarchy_id": hierarchy_id,
        "filter_conditions": request.query_params,
        "column_filters": column_filters,
        "tmo_id": tmo_id,
        "session": session,
    }

    hierarchy_exist = await check_hierarchy_exist(hierarchy_id, session)

    # check parent_id
    if parent_id.upper() in ("ROOT", "NONE", "NULL"):
        parent_id = None
    else:
        try:
            parent_id = uuid.UUID(parent_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="parent_id must be instance of UUID or be one of values: root, none, null",
            )

    if isinstance(parent_id, uuid.UUID):
        node_from_db = await get_node_or_raise_error(
            node_id=parent_id, session=session
        )
        hierarchy_filter_data["parent_node"] = node_from_db

        if node_from_db.hierarchy_id != hierarchy_id:
            raise HTTPException(
                status_code=422,
                detail=f"Parent node hierarchy id does not match hierarchy_id "
                f"({node_from_db.hierarchy_id} != {hierarchy_id})",
            )
    else:
        hierarchy_filter_data["parent_node"] = None

    # check tmo_id
    # IF tmo id than return filtered results
    if tmo_id:
        stmt = select(Level).where(
            Level.object_type_id == tmo_id, Level.hierarchy_id == hierarchy_id
        )
        levels_with_tmo = await session.execute(stmt)
        levels_with_tmo = levels_with_tmo.scalars().all()

        if not levels_with_tmo:
            raise HTTPException(
                status_code=422,
                detail=f"Levels with tmo =={tmo_id} does not exist in this hierarchy",
            )

        hierarchy_filter = HierarchyFilter(
            collect_data_cache=True, **hierarchy_filter_data
        )

        nodes = await hierarchy_filter.get_first_depth_children_nodes()
        node_real_children = await hierarchy_filter.get_dict_of_ids_of_first_depth_children_nodes_and_based_mo_ids()
        nodes = [node.dict() for node in nodes]
        for node in nodes:
            node["id"] = str(node["id"])
        print(len(nodes))
        keys = list(node_real_children.keys())
        for key in keys:
            node_real_children[str(key)] = list(node_real_children.pop(key))

    else:
        print("ELSE")
        # else return results without filter
        # st = time.time()
        # stmt = select(Obj).where(Obj.parent_id == parent_id, Obj.hierarchy_id == hierarchy_id).order_by(Obj.key)
        # nodes = await session.execute(stmt)
        # nodes = nodes.scalars().all()
        # print(f'select objects took: {time.time() - st}')
        # st = time.time()
        # stmt = select(Level).where(Level.hierarchy_id == hierarchy_exist.id)
        # h_levels = await session.execute(stmt)
        # h_levels = h_levels.scalars().all()
        # print(f'select levels took: {time.time() - st}')
        # levels_param_type_ids_dict = {level.id: level.param_type_id for level in h_levels}
        # tprms_data = await get_tprms_data_by_tprms_ids(tprm_ids=levels_param_type_ids_dict.values())
        # tprms_val_types = {tprm['id']: tprm['val_type'] for tprm in tprms_data}
        #
        # level_ids_with_mo_link = {level_id for level_id, level_param_type_id in levels_param_type_ids_dict.items()
        #                           if tprms_val_types.get(level_param_type_id, None) in {'mo_link', 'two-way link'}}
        #
        # node_real_children = {}
        #
        # st = time.time()
        # for node in nodes:
        #     if node.level_id in level_ids_with_mo_link and node.key.isdigit():
        #         node_real_children[node.id] = [int(node.key)]
        #     elif node.object_id:
        #         node_real_children[node.id] = [node.object_id]
        #     else:
        #         node_real_children[node.id] = list()
        # print(f'iterating by nodes ({len(nodes)}) took: {time.time() - st}')
        # node_id_top_parent_id_cache = {node.id: node.id for node in nodes}
        #
        # st = time.time()
        # first_step = divides_list_on_parts_by_step_limit([node.id for node in nodes], 30000)
        # parent_ids_order = first_step
        # print(f'splitting on parts took: {time.time() - st}')
        # st = time.time()
        # for parent_node_ids in parent_ids_order:
        #     st1 = time.time()
        #     if hierarchy_exist.create_empty_nodes:
        #         stmt = select(Obj).where(Obj.parent_id.in_(parent_node_ids))
        #     else:
        #         stmt = select(Obj).where(Obj.parent_id.in_(parent_node_ids), Obj.key != DEFAULT_KEY_OF_NULL_NODE)
        #
        #     inner_nodes = await session.execute(stmt)
        #     inner_nodes = inner_nodes.scalars().all()
        #     print(f'---selecting objects took: {time.time() - st1}')
        #
        #     if inner_nodes:
        #         st1 = time.time()
        #         for node in inner_nodes:
        #             node_parent = node_id_top_parent_id_cache.get(node.parent_id, False)
        #             if node_parent:
        #                 node_id_top_parent_id_cache[node.id] = node_parent
        #
        #                 if node.level_id in level_ids_with_mo_link and node.key.isdigit():
        #                     node_real_children[node_parent].append(int(node.key))
        #                 elif node.object_id:
        #                     node_real_children[node_parent].append(node.object_id)
        #                 else:
        #                     continue
        #         print(f'---iterating inner nodes took: {time.time() - st1}')
        #         next_step = [item.id for item in inner_nodes if item.child_count > 0]
        #         if len(next_step) > 30000:
        #             parent_ids_order.extend(divides_list_on_parts_by_step_limit(next_step, 30000))
        #         else:
        #             parent_ids_order.append(next_step)
        # print(f'iterating by parts took: {time.time() - st}')
        nodes, node_real_children = await get_total_results(
            session, parent_id, hierarchy_exist
        )
        # nodes, node_real_children = await get_nodes_and_real_children(session, parent_id, hierarchy_exist)
    # nodes = await update_nodes_key_if_mo_link_or_prm_link(nodes, session)

    nodes = await update_nodes_key_if_mo_link_or_prm_link_as_dict(
        nodes, session
    )
    res = list()

    # for node in nodes:
    #     node_children = node_real_children.get(str(node.id), None)
    #
    #     if node_children:
    #         node.__dict__['children_mo_ids'] = node_children
    #     else:
    #         node.__dict__['children_mo_ids'] = []
    #     res.append(node.__dict__)

    # find levels with show_without_children is false
    stmt = select(Level).where(
        Level.hierarchy_id == hierarchy_id,
        Level.show_without_children == False,  # noqa: E712
    )
    h_lev_show_without_children_false = await session.execute(stmt)
    h_lev_show_without_children_false = (
        h_lev_show_without_children_false.scalars().all()
    )

    level_ids_do_not_show_without_child = {
        level.id for level in h_lev_show_without_children_false
    }

    for node in nodes:
        if (
            node["level_id"] in level_ids_do_not_show_without_child
            and node["child_count"] == 0
        ):
            continue
        node_children = node_real_children.get(str(node["id"]), None)

        if node_children:
            node["children_mo_ids"] = node_children
        else:
            node["children_mo_ids"] = []
        res.append(node)

    print(f"response time long: {time.time() - start}")
    return res


@router.get(
    "/hierarchy_child/{parent_id}",
    response_model=list[ObjResponseNew],
    tags=["Main"],
)
async def get_hierarchy_child(
    parent_id: uuid.UUID, session: AsyncSession = Depends(database.get_session)
):
    stmt = select(Obj).where(Obj.id == parent_id)
    parent_obj = await session.execute(stmt)
    parent_obj = parent_obj.scalars().first()

    hierarchy_exist = await check_hierarchy_exist(
        parent_obj.hierarchy_id, session
    )
    result = list()

    if parent_obj is not None:
        result = await create_tree_from_parent_node(
            parent_obj, session, hierarchy_exist.create_empty_nodes
        )
    return result


@router.websocket("/ws/{hierarchy_id}")
async def websocket_endpoint(hierarchy_id: int, websocket: WebSocket):
    await websocket.accept()
    notifier = Notifier()
    notifier.add_user(hierarchy_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_json(data)
    except WebSocketDisconnect:
        notifier.remove_user(hierarchy_id, websocket)
