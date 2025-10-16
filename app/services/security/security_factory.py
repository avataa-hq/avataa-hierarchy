from services.security import security_config
from services.security.implementation.disabled import DisabledSecurity
from services.security.implementation.keycloak import Keycloak, KeycloakInfo
from services.security.implementation.mixed import OpaJwtParsed, OpaJwtRaw
from services.security.implementation.utils.user_info_cache import UserInfoCache
from services.security.security_interface import SecurityInterface


class SecurityFactory:
    def get(self, security_type: str) -> SecurityInterface:
        match security_type.upper():
            case "KEYCLOAK":
                return self._get_keycloak()
            case "OPA-JWT-RAW":
                return self._get_opa_jwt_raw()
            case "OPA-JWT-PARSED":
                return self._get_opa_jwt_parsed()
            case "KEYCLOAK-INFO":
                return self._get_keycloak_info()
            case _:
                return self._get_disabled()

    @staticmethod
    def _get_disabled() -> SecurityInterface:
        return DisabledSecurity()

    def _get_keycloak(self) -> SecurityInterface:
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {
            "profile": "Read claims that represent basic profile information"
        }

        return Keycloak(
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_opa_jwt_raw(self) -> SecurityInterface:
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {
            "profile": "Read claims that represent basic profile information"
        }
        return OpaJwtRaw(
            opa_url=security_config.OPA_URL,
            policy_path=security_config.OPA_POLICY_PATH,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_opa_jwt_parsed(self) -> SecurityInterface:
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {
            "profile": "Read claims that represent basic profile information"
        }
        return OpaJwtParsed(
            opa_url=security_config.OPA_URL,
            policy_path=security_config.OPA_POLICY_PATH,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
        )

    def _get_keycloak_info(self) -> SecurityInterface:
        keycloak_public_url = security_config.KEYCLOAK_PUBLIC_KEY_URL
        token_url = security_config.KEYCLOAK_TOKEN_URL
        authorization_url = security_config.KEYCLOAK_AUTHORIZATION_URL
        refresh_url = authorization_url
        scopes = {
            "profile": "Read claims that represent basic profile information"
        }
        cache = UserInfoCache()
        cache_user_info_url = security_config.SECURITY_MIDDLEWARE_URL
        return KeycloakInfo(
            cache=cache,
            keycloak_public_url=keycloak_public_url,
            token_url=token_url,
            authorization_url=authorization_url,
            refresh_url=refresh_url,
            scopes=scopes,
            cache_user_info_url=cache_user_info_url,
        )


security = SecurityFactory().get(security_config.SECURITY_TYPE)
