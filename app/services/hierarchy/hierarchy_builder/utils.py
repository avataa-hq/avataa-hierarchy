from typing import Any

from grpc_config.inventory_utils import get_mo_links_values
from schemas.hier_schemas import Obj
from services.hierarchy.hierarchy_builder.configs import (
    DEFAULT_KEY_OF_NULL_NODE,
)
from services.hierarchy.hierarchy_builder.dto_models import KeyData


def create_path_for_children_node_by_parent_node(parent_node: Obj = None):
    """Returns path for children of parent_node"""
    path = None
    if parent_node:
        parent_path = parent_node.path if parent_node.path else ""
        path = f"{parent_path}{parent_node.id}/"

    return path


def create_node_key(
    ordered_key_attrs: list[str], mo_data_with_params: dict[str, Any]
):
    """Returns node key by common pattern"""
    result = []
    at_least_on_value_is_not_none = False
    for key in ordered_key_attrs:
        value = mo_data_with_params.get(key)
        result.append(str(value))
        if value is not None:
            at_least_on_value_is_not_none = True

    if at_least_on_value_is_not_none is False or len(result) == 0:
        return DEFAULT_KEY_OF_NULL_NODE

    return "-".join(result)


def get_node_key_data(
    ordered_key_attrs: list[str],
    mo_data_with_params: dict[str, Any],
    mo_links_attrs: list[int] | None = None,
) -> KeyData:
    """Returns node key by common pattern"""
    if mo_links_attrs is None:
        mo_links_attrs = []

    mo_links = [
        int(mo_data_with_params[attr])
        for attr in ordered_key_attrs
        if attr.isdigit()
        and int(attr) in mo_links_attrs
        and attr in mo_data_with_params
    ]
    mo_links = get_mo_links_values(mo_links)

    result = []
    at_least_on_value_is_not_none = False

    for key in ordered_key_attrs:
        value = mo_data_with_params.get(key)
        if (
            key.isdigit()
            and int(key) in mo_links_attrs
            and key in mo_data_with_params
        ):
            value = mo_links.get(int(value))

        result.append(str(value))
        if value is not None:
            at_least_on_value_is_not_none = True

    if at_least_on_value_is_not_none is False or len(result) == 0:
        return KeyData(key=DEFAULT_KEY_OF_NULL_NODE, key_is_empty=True)

    return KeyData(key="-".join(result), key_is_empty=False)
