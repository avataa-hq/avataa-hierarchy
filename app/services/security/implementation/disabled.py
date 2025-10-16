from starlette.requests import Request  # noqa

from services.security.security_config import ADMIN_ROLE
from services.security.security_interface import SecurityInterface
from services.security.security_data_models import UserData, ClientRoles


default_user = UserData(
    id=None,
    audience=None,
    name="Anonymous",
    preferred_name="Anonymous",
    realm_access=ClientRoles(name="realm_access", roles=[ADMIN_ROLE]),
    resource_access=None,
    groups=None,
)


class DisabledSecurity(SecurityInterface):
    async def __call__(self, request: Request) -> UserData:
        return default_user
