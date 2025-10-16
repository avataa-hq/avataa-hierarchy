import functools
import os
import time
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings
import requests

# GENERAL
TITLE = "Hierarchy"
PREFIX = f"/api/{TITLE.replace(' ', '_').lower()}"

DB_USER = os.environ.get("DB_USER", "hierarchy_admin")
DB_PASS = os.environ.get("DB_PASS", None)
DB_HOST = os.environ.get("DB_HOST", "pgbouncer")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "hierarchy")
DB_TYPE = os.environ.get("DB_TYPE", "postgresql+asyncpg")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")
DATABASE_URL = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# SYNC_DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


INV_HOST = os.environ.get("INV_HOST", "inventory")
# INV_PORT = os.environ.get("INV_PORT", "8000")
INVENTORY_GRPC_PORT = os.environ.get("INVENTORY_GRPC_PORT", "50051")
INVENTORY_GRPC_URL = f"{INV_HOST}:{INVENTORY_GRPC_PORT}"
# INVENTORY_URL = f"http://{INV_HOST}:{INV_PORT}"
INV_TMO_DETAIL = "/api/inventory/v1/object_type/"
INV_TPRM_DETAIL = "/api/inventory/v1/param_type/"

# KEYCLOAK
# KEYCLOAK_PROTOCOL = os.environ.get('KEYCLOAK_PROTOCOL', 'https')
# KEYCLOAK_HOST = os.environ.get('KEYCLOAK_HOST', 'keycloak')
# KEYCLOAK_PORT = os.environ.get('KEYCLOAK_PORT', '8080')
# KEYCLOAK_REALM = os.environ.get('KEYCLOAK_REALM', 'master')
# KEYCLOAK_CLIENT_ID = os.environ.get('KC_HIERARCHY_CLIENT_ID')
# KEYCLOAK_CLIENT_SECRET = os.environ.get('KC_HIERARCHY_CLIENT_SECRET')
# KEYCLOAK_URL = f'{KEYCLOAK_PROTOCOL}://{KEYCLOAK_HOST}:{KEYCLOAK_PORT}'


# KEYCLOAK_EXTERNAL_HOST = os.environ.get('KEYCLOAK_EXTERNAL_HOST', 'http://127.0.0.1')
#
# # KEYCLOAK_EXTERNAL_URL uses for redirect on login form
# KEYCLOAK_EXTERNAL_URL = f'{KEYCLOAK_EXTERNAL_HOST}:{KEYCLOAK_PORT}'
# KEYCLOAK_TOKEN_URL = f'{KEYCLOAK_EXTERNAL_URL}/realms/' \
#                      f'{KEYCLOAK_REALM}/protocol/openid-connect/token'
# KEYCLOAK_AUTHORIZATION_URL = f'{KEYCLOAK_EXTERNAL_URL}/realms/' \
#                              f'{KEYCLOAK_REALM}/protocol/openid-connect/auth'
# KEYCLOAK_INTROSPECT_URL = f'{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}' \
#                           f'/protocol/openid-connect/token/introspect'
# KEYCLOAK_PUBLIC_KEY_URL = f'{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}'

# KEYCLOAK_REDIRECT_PROTOCOL = os.environ.get('KEYCLOAK_REDIRECT_PROTOCOL', None)
# KEYCLOAK_REDIRECT_HOST = os.environ.get('KEYCLOAK_REDIRECT_HOST', None)
# KEYCLOAK_REDIRECT_PORT = os.environ.get('KEYCLOAK_REDIRECT_PORT', None)
# if KEYCLOAK_REDIRECT_PROTOCOL is None:
#     KEYCLOAK_REDIRECT_PROTOCOL = KEYCLOAK_PROTOCOL
# if KEYCLOAK_REDIRECT_HOST is None:
#     KEYCLOAK_REDIRECT_HOST = KEYCLOAK_HOST
# if KEYCLOAK_REDIRECT_PORT is None:
#     KEYCLOAK_REDIRECT_PORT = KEYCLOAK_PORT

# KEYCLOAK_URL = f'{KEYCLOAK_PROTOCOL}://{KEYCLOAK_HOST}:{KEYCLOAK_PORT}'
# KEYCLOAK_PUBLIC_KEY_URL = f'{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}'
# KEYCLOAK_REDIRECT_URL = f'{KEYCLOAK_REDIRECT_PROTOCOL}://{KEYCLOAK_REDIRECT_HOST}:{KEYCLOAK_REDIRECT_PORT}'
# KEYCLOAK_TOKEN_URL = f'{KEYCLOAK_REDIRECT_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/token'
# KEYCLOAK_AUTHORIZATION_URL = f'{KEYCLOAK_REDIRECT_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/auth'

DEBUG = os.environ.get("DEBUG", "False").upper() in ("TRUE", "Y", "YES", "1")

# security by data permissions
TURN_ON_DATA_PERMISSIONS_CHECK = os.environ.get(
    "TURN_ON_DATA_PERMISSIONS_CHECK", "False"
).upper() in ("TRUE", "Y", "YES", "1")

POSTGRES_ITEMS_LIMIT_IN_QUERY = 32_000
LIMIT_OF_POSTGRES_RESULTS_PER_STEP = 50_000
GRPC_MESSAGE_MAX_SIZE = 4_000_000

# DOCUMENTATION
DOCS_ENABLED = os.environ.get("DOCS_ENABLED", "True").upper() in (
    "TRUE",
    "Y",
    "YES",
    "1",
)
DOCS_CUSTOM_ENABLED = os.environ.get(
    "DOCS_CUSTOM_ENABLED", "False"
).upper() in ("TRUE", "Y", "YES", "1")
DOCS_SWAGGER_JS_URL = os.environ.get("DOCS_SWAGGER_JS_URL", None)
DOCS_SWAGGER_CSS_URL = os.environ.get("DOCS_SWAGGER_CSS_URL", None)
DOCS_REDOC_JS_URL = os.environ.get("DOCS_REDOC_JS_URL", None)
SERVER_GRPC_PORT = os.environ.get("SERVER_GRPC_PORT", "50051")

# INVENTORY CHANGES TOPIC CONFIGS


class KafkaKeycloakConfigs(BaseSettings):
    scopes: str = Field(default="profile", alias="kafka_keycloak_scopes")
    client_id: str = Field(
        default="kafka", min_length=1, alias="kafka_keycloak_client_id"
    )
    client_secret: SecretStr | None = Field(
        None, alias="kafka_keycloak_client_secret"
    )
    protocol: Literal["http", "https"] = Field(
        default="http", alias="keycloak_protocol"
    )
    host: str = Field(default="keycloak", min_length=1, alias="keycloak_host")
    port: int | None = Field(
        default=8080, ge=1, le=65_535, alias="keycloak_port"
    )
    realm: str = Field(default="avataa", min_length=1, alias="keycloak_realm")

    @property
    def url(self) -> str:
        url = f"{self.protocol}://{self.host}"
        if self.port:
            url = f"{url}:{self.port}"
        return url

    @property
    def token_url(self) -> str:
        return f"{self.url}/realms/{self.realm}/protocol/openid-connect/token"


class KafkaConfigs(BaseSettings):
    turn_on: bool = Field(True, alias="kafka_turn_on")
    bootstrap_servers: str = Field(
        "kafka:9092", alias="kafka_url", min_length=1
    )
    group_id_starts_with: str = Field(
        "Hierarchy",
        serialization_alias="kafka_consumer_group_id",
        min_length=1,
    )
    # 'latest'
    offset: str = Field("latest", alias="kafka_consumer_offset")
    topics_as_str: str = Field(
        "inventory.changes", alias="kafka_subscribe_topics"
    )
    secured: bool = Field(False, alias="kafka_secured")

    @property
    def topics(self):
        return self.topics_as_str.split(",")

    @staticmethod
    def _get_token_for_kafka_producer(
        conf, keycloak_config: KafkaKeycloakConfigs
    ) -> tuple[str, float]:
        """Get token from Keycloak for MS Inventory kafka producer and returns it with expires time"""
        payload = {
            "grant_type": "client_credentials",
            "scope": keycloak_config.scopes,
        }

        attempt = 5
        while attempt > 0:
            try:
                resp = requests.post(
                    keycloak_config.token_url,
                    auth=(
                        keycloak_config.client_id,
                        keycloak_config.client_secret.get_secret_value()
                        if keycloak_config.client_secret
                        else None,
                    ),
                    data=payload,
                    timeout=5,
                )
            except ConnectionError:
                time.sleep(1)
                attempt -= 1
            else:
                if resp.status_code == 200:
                    break
                else:
                    time.sleep(1)
                    attempt -= 1
                    continue
        else:
            raise PermissionError("Token verification service unavailable")

        token = resp.json()

        return token["access_token"], time.time() + float(token["expires_in"])

    def get_common_settings(self) -> dict:
        res = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"{self.group_id_starts_with}",
            "auto.offset.reset": self.offset,
            "enable.auto.commit": False,
        }
        if self.secured:
            res.update(
                {
                    "security.protocol": "sasl_plaintext",
                    "sasl.mechanisms": "OAUTHBEARER",
                }
            )
        return res

    def get_connection_settings_for_special_hierarchy(
        self, hierarchy_id: int
    ) -> dict:
        res = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"{self.group_id_starts_with}_{hierarchy_id}",
            "auto.offset.reset": self.offset,
            "enable.auto.commit": False,
        }
        if self.secured:
            res.update(
                {
                    "security.protocol": "sasl_plaintext",
                    "sasl.mechanisms": "OAUTHBEARER",
                    "oauth_cb": functools.partial(
                        self._get_token_for_kafka_producer,
                        keycloak_config=KafkaKeycloakConfigs(),
                    ),
                }
            )

        return res
