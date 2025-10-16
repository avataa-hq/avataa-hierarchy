"""TESTS for kafka adapter"""

import pytest

from kafka_config.protobuf_consumer import adapter_function
from services.obj_events.status import ObjEventStatus

PRM_DELETE_MSG = {"objects": [{"mo_id": 1, "tprm_id": 1}]}
PRM_UPDATE_MSG = {"objects": [{"mo_id": 1, "tprm_id": 1}]}
TMO_DELETE_MSG = {"objects": [{"id": 1, "tprm_id": 1}]}
TPRM_DELETE_MSG = {"objects": [{"id": 1, "tprm_id": 1}]}
MO_DELETE_MSG = {"objects": [{"id": 1, "name": "Test", "tmo_id": 25}]}


@pytest.mark.asyncio
async def test_adapter_function_on_delete_prm_successful(mocker):
    """TEST On receiving PRM:deleted msg - adapter_function call on_delete_prm"""

    spy = mocker.patch("kafka_config.protobuf_consumer.on_delete_prm")

    await adapter_function("PRM", ObjEventStatus.DELETED.value, PRM_DELETE_MSG)

    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_adapter_function_on_update_prm_successful(mocker):
    """TEST On receiving PRM:updated msg - adapter_function call on_update_prm"""

    spy = mocker.patch("kafka_config.protobuf_consumer.on_update_prm")

    await adapter_function("PRM", ObjEventStatus.UPDATED.value, PRM_UPDATE_MSG)

    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_adapter_function_on_delete_tmo_successful(mocker):
    """TEST On receiving TMO:deleted msg - adapter_function call on_delete_tmo"""

    spy = mocker.patch("kafka_config.protobuf_consumer.on_delete_tmo")

    await adapter_function("TMO", ObjEventStatus.DELETED.value, TMO_DELETE_MSG)

    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_adapter_function_on_delete_tprm_successful(mocker):
    """TEST On receiving TPRM:deleted msg - adapter_function call on_delete_tprm"""

    spy = mocker.patch("kafka_config.protobuf_consumer.on_delete_tprm")

    await adapter_function(
        "TPRM", ObjEventStatus.DELETED.value, TPRM_DELETE_MSG
    )

    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_adapter_function_on_delete_mo_successful(mocker):
    """TEST On receiving MO:deleted msg - adapter_function call on_delete_mo"""

    spy = mocker.patch("kafka_config.protobuf_consumer.on_delete_mo")

    await adapter_function("MO", ObjEventStatus.DELETED.value, MO_DELETE_MSG)

    assert spy.call_count == 1
