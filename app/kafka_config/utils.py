from enum import Enum
import functools
import time

from fastapi import HTTPException
import requests
from requests.exceptions import ConnectionError

from kafka_config import config


class ObjClassNames(Enum):
    MO = "MO"
    TMO = "TMO"
    TPRM = "TPRM"
    PRM = "PRM"


def consumer_config(conf):
    if conf.get("sasl.mechanisms", "") == "OAUTHBEARER":
        conf["oauth_cb"] = functools.partial(_get_token_for_kafka_producer)

    return conf


def _get_token_for_kafka_producer(conf):
    """Get token from Keycloak for MS Inventory kafka producer and returns it with
    expires time"""
    payload = {
        "grant_type": "client_credentials",
        "scope": str(config.KAFKA_KEYCLOAK_SCOPES),
    }

    attempt = 5
    while attempt > 0:
        try:
            resp = requests.post(
                config.KAFKA_KEYCLOAK_TOKEN_URL,
                auth=(
                    config.KAFKA_KEYCLOAK_CLIENT_ID,
                    config.KAFKA_KEYCLOAK_SECRET,
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
        raise HTTPException(
            status_code=503, detail="Token verification service unavailable"
        )

    token = resp.json()

    return token["access_token"], time.time() + float(token["expires_in"])
