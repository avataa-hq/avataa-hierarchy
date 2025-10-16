from services.security.security_data_models import ClientRoles, UserData


def get_admin_user_model():
    return UserData(
        id=None,
        audience=None,
        name="Anonymous",
        preferred_name="Anonymous",
        realm_access=ClientRoles(name="realm_access", roles=["__admin"]),
        resource_access=None,
        groups=None,
    )
