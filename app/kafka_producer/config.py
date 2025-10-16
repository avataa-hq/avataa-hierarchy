import os

from kafka_config.config import KAFKA_SECURED, KAFKA_URL

# if KAFKA_PRODUCER_TURN_ON all below is mandatory
KAFKA_PRODUCER_TURN_ON = str(
    os.environ.get("KAFKA_PRODUCER_TURN_ON", True)
).upper() in ("TRUE", "Y", "YES", "1")
# KAFKA_KEYCLOAK_CLIENT_ID = os.environ.get("KAFKA_KEYCLOAK_CLIENT_ID", "kafka")
# KAFKA_KEYCLOAK_CLIENT_SECRET = os.environ.get("KAFKA_KEYCLOAK_CLIENT_SECRET")
# KAFKA_KEYCLOAK_TOKEN_URL = os.environ.get("KAFKA_KEYCLOAK_TOKEN_URL")
KAFKA_KEYCLOAK_SCOPES = os.environ.get("KAFKA_KEYCLOAK_SCOPES", "profile")
# KAFKA_SECURED = str(os.environ.get("KAFKA_SECURED", False)).upper() in (
#     "TRUE",
#     "Y",
#     "YES",
#     "1",
# )

# KAFKA_URL = os.environ.get("KAFKA_URL", "kafka")
KAFKA_PRODUCER_TOPIC = os.environ.get(
    "KAFKA_PRODUCER_TOPIC", "hierarchy.changes"
)
KAFKA_PRODUCER_MSG_MAX_MSG_LEN = int(
    os.environ.get("KAFKA_PRODUCER_MSG_MAX_MSG_LEN", 1000)
)


KAFKA_PRODUCER_CONNECT_CONFIG = {"bootstrap.servers": KAFKA_URL}

if KAFKA_SECURED:
    SECURED_SETTINGS = {
        "security.protocol": "sasl_plaintext",
        "sasl.mechanisms": "OAUTHBEARER",
    }
    KAFKA_PRODUCER_CONNECT_CONFIG.update(SECURED_SETTINGS)
