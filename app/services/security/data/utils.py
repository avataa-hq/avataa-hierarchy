from fastapi.requests import Request
from sqlalchemy import false
from sqlalchemy.orm import Session

from services.security.security_data_models import UserData

role_prefix = "__"


def get_user_permissions(jwt: UserData) -> list[str]:
    permissions = []
    if jwt.realm_access:
        permissions.extend(
            [
                f"{jwt.realm_access.name}.{r}"
                for r in jwt.realm_access.roles
                if r.startswith(role_prefix)
            ]
        )
    if jwt.resource_access:
        for resource_access in jwt.resource_access:
            permissions.extend(
                [
                    f"{resource_access.name}.{r}"
                    for r in resource_access.roles
                    if r.startswith(role_prefix)
                ]
            )
    return permissions


PREFIX = "/security"

ACTIONS = {
    "POST": "create",
    "GET": "read",
    "PATCH": "update",
    "PUT": "update",
    "DELETE": "delete",
}


def _get_action(request: Request):
    actions = ACTIONS.get(request.method, false())
    if request.url.path.startswith(PREFIX):
        actions = "admin"
    return actions


def add_security_data(session: Session, request: Request, user_data: UserData):
    session.info["jwt"] = user_data
    session.info["action"] = _get_action(request)
    return
