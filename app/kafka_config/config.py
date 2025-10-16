import os

from .protobuf import inventory_instances_pb2

KAFKA_TURN_ON = str(os.environ.get("KAFKA_TURN_ON", True)).upper() in (
    "TRUE",
    "Y",
    "YES",
    "1",
)
KAFKA_URL = os.environ.get("KAFKA_URL", "kafka:9092")
KAFKA_CONSUMER_GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_ID", "Hierarchy")
KAFKA_CONSUMER_OFFSET = os.environ.get("KAFKA_CONSUMER_OFFSET", "latest")
KAFKA_SUBSCRIBE_TOPICS = os.environ.get(
    "KAFKA_SUBSCRIBE_TOPICS", "inventory.changes"
)
KAFKA_SUBSCRIBE_TOPICS = KAFKA_SUBSCRIBE_TOPICS.split(",")
KAFKA_KEYCLOAK_SCOPES = os.environ.get("KAFKA_KEYCLOAK_SCOPES", "profile")
KAFKA_KEYCLOAK_CLIENT_ID = os.environ.get("KAFKA_KEYCLOAK_CLIENT_ID", "kafka")
KAFKA_KEYCLOAK_SECRET = os.environ.get("KAFKA_KEYCLOAK_CLIENT_SECRET", None)

KEYCLOAK_HOST = os.environ.get("KEYCLOAK_HOST", "keycloak")
KEYCLOAK_PORT = os.environ.get("KEYCLOAK_PORT", "8080")
KEYCLOAK_PROTOCOL = os.environ.get("KEYCLOAK_PROTOCOL", "http")
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "avataa")
url = f"{KEYCLOAK_PROTOCOL}://{KEYCLOAK_HOST}"
if KEYCLOAK_PORT:
    url = f"{url}:{KEYCLOAK_PORT}"
KAFKA_KEYCLOAK_TOKEN_URL = (
    f"{url}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token"
)

KAFKA_SECURED = str(os.environ.get("KAFKA_SECURED", False)).upper() in (
    "TRUE",
    "Y",
    "YES",
    "1",
)

KAFKA_CONSUMER_CONNECT_CONFIG = {
    "bootstrap.servers": KAFKA_URL,
    "group.id": KAFKA_CONSUMER_GROUP_ID,
    "auto.offset.reset": KAFKA_CONSUMER_OFFSET,
    "enable.auto.commit": False,
}
if KAFKA_SECURED:
    KAFKA_CONSUMER_CONNECT_CONFIG.update(
        {
            "security.protocol": "sasl_plaintext",
            "sasl.mechanisms": "OAUTHBEARER",
        }
    )


KAFKA_PROTOBUF_DESERIALIZERS = {
    "MO": inventory_instances_pb2.ListMO,
    "TMO": inventory_instances_pb2.ListTMO,
    "TPRM": inventory_instances_pb2.ListTPRM,
    "PRM": inventory_instances_pb2.ListPRM,
}
