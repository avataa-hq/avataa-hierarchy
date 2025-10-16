from dataclasses import dataclass


@dataclass
class ClientRoles:
    name: str
    roles: list[str]


@dataclass
class UserData:
    id: str | None
    audience: list[str] | str | None
    name: str
    preferred_name: str
    realm_access: ClientRoles | None
    resource_access: list[ClientRoles] | None
    groups: list[str] | None

    @classmethod
    def from_jwt(cls, jwt: dict):
        realm_access = jwt.get("realm_access", None)
        if realm_access is not None:
            realm_access = ClientRoles(
                name="realm_access", roles=realm_access.get("roles", [])
            )
        resource_access = jwt.get("resource_access", None)
        if resource_access is not None:
            resource_access = [
                ClientRoles(name=k, roles=v.get("roles", []))
                for k, v in resource_access.items()
            ]

        return cls(
            id=jwt.get("sub", None),
            audience=jwt.get("aud", None),
            name=" ".join(
                [
                    jwt.get("given_name", "").strip(),
                    jwt.get("family_name", "").strip(),
                ]
            ).strip(),
            preferred_name=jwt.get("preferred_username", ""),
            realm_access=realm_access,
            resource_access=resource_access,
            groups=jwt.get("groups", None),
        )
