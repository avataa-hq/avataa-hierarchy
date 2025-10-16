import functools
import os
import time
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings
import requests


class TestsConfig(BaseSettings):
    host: str = Field("localhost", min_length=1, alias="TESTS_DB_HOST")
    name: str = Field("test_hierarchy", min_length=1, alias="TESTS_DB_NAME")
    db_pass: str = Field("password", min_length=1, alias="TESTS_DB_PASS")
    port: int = Field(5432, alias="TESTS_DB_PORT")
    db_type: str = Field(
        "postgresql+asyncpg", min_length=1, alias="TESTS_DB_TYPE"
    )
    user: str = Field("test_user", min_length=1, alias="TESTS_DB_USER")

    docker_db_host: str = Field(
        "localhost", min_length=1, alias="TESTS_DOCKER_DB_HOST"
    )
    run_container_postgres_local: bool = Field(
        True, alias="TESTS_RUN_CONTAINER_POSTGRES_LOCAL"
    )

    @property
    def test_database_url(self) -> str:
        _url = f"{self.db_type}://{self.user}:{self.db_pass}@{self.host}:{self.port}/{self.name}"
        return _url


INV_HOST = os.environ.get("INV_HOST", "inventory")
INVENTORY_GRPC_PORT = os.environ.get("INVENTORY_GRPC_PORT", "50051")
INVENTORY_GRPC_URL = f"{INV_HOST}:{INVENTORY_GRPC_PORT}"
LIMIT_OF_POSTGRES_RESULTS_PER_STEP = 50_000
POSTGRES_ITEMS_LIMIT_IN_QUERY = 32_000

DB_USER = os.environ.get("DB_USER", "hierarchy_admin")
DB_PASS = os.environ.get("DB_PASS", None)
DB_HOST = os.environ.get("DB_HOST", "pgbouncer")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "hierarchy")
DB_TYPE = os.environ.get("DB_TYPE", "postgresql+asyncpg")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")
DATABASE_URL = f"{DB_TYPE}://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DOCS_CUSTOM_ENABLED = os.environ.get(
    "DOCS_CUSTOM_ENABLED", "False"
).upper() in ("TRUE", "Y", "YES", "1")
DOCS_SWAGGER_JS_URL = os.environ.get("DOCS_SWAGGER_JS_URL", None)
DOCS_SWAGGER_CSS_URL = os.environ.get("DOCS_SWAGGER_CSS_URL", None)
DOCS_REDOC_JS_URL = os.environ.get("DOCS_REDOC_JS_URL", None)
DOCS_ENABLED = os.environ.get("DOCS_ENABLED", "True").upper() in (
    "TRUE",
    "Y",
    "YES",
    "1",
)
DEBUG = os.environ.get("DEBUG", "False").upper() in ("TRUE", "Y", "YES", "1")


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
