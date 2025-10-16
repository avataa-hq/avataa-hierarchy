from elasticsearch import AsyncElasticsearch

from elastic.settings import ES_PASS, ES_PROTOCOL, ES_URL, ES_USER


def init_elastic_client():
    if ES_PROTOCOL == "https":
        print("Creating certified client...")
        client = AsyncElasticsearch(
            ES_URL,
            ca_certs="./elastic/ca.crt",
            basic_auth=(ES_USER, ES_PASS),
            timeout=10000,
        )
    else:
        print("Creating client...")
        client = AsyncElasticsearch(ES_URL, timeout=10000)

    return client
