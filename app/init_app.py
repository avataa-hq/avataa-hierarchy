from fastapi import FastAPI
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from starlette.requests import Request
from starlette.responses import HTMLResponse

from settings import (
    DOCS_CUSTOM_ENABLED,
    DOCS_ENABLED,
    DOCS_REDOC_JS_URL,
    DOCS_SWAGGER_CSS_URL,
    DOCS_SWAGGER_JS_URL,
)


def register_static_docs_routes(app: FastAPI):
    root_path = app.root_path.rstrip("/")
    openapi_url = root_path + app.openapi_url
    oauth2_redirect_url = app.swagger_ui_oauth2_redirect_url
    if oauth2_redirect_url:
        oauth2_redirect_url = root_path + oauth2_redirect_url

    async def custom_swagger_ui_html(req: Request) -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=oauth2_redirect_url,
            swagger_js_url=DOCS_SWAGGER_JS_URL,
            swagger_css_url=DOCS_SWAGGER_CSS_URL,
        )

    docs_url = app.docs_url or "/docs"
    if DOCS_SWAGGER_JS_URL and DOCS_SWAGGER_CSS_URL:
        app.add_route(docs_url, custom_swagger_ui_html, include_in_schema=False)
    # else:
    #     warnings.warn(
    #         f'Endpoint "{docs_url}" disabled. Environment variables "REDOC_JS_URL" or "SWAGGER_CSS_URL" are not set')

    async def swagger_ui_redirect(req: Request) -> HTMLResponse:
        return get_swagger_ui_oauth2_redirect_html()

    swagger_ui_oauth2_redirect_url = (
        app.swagger_ui_oauth2_redirect_url or "/docs/oauth2-redirect"
    )
    app.add_route(
        swagger_ui_oauth2_redirect_url,
        swagger_ui_redirect,
        include_in_schema=False,
    )

    async def redoc_html(req: Request) -> HTMLResponse:
        return get_redoc_html(
            openapi_url=openapi_url,
            title=app.title + " - ReDoc",
            redoc_js_url=DOCS_REDOC_JS_URL,
        )

    redoc_url = app.redoc_url or "/redoc"
    if DOCS_REDOC_JS_URL:
        app.add_route(redoc_url, redoc_html, include_in_schema=False)
    # else:
    #     warnings.warn(f'Endpoint "{redoc_url}" disabled. Environment variable "REDOC_JS_URL" is not set')


def create_app(documentation_enabled: bool = DOCS_ENABLED, **kwargs) -> FastAPI:
    options = kwargs
    if not documentation_enabled:
        options["openapi_url"] = None
    elif DOCS_CUSTOM_ENABLED:
        if DOCS_SWAGGER_JS_URL and DOCS_SWAGGER_CSS_URL:
            options["docs_url"] = None
        if DOCS_REDOC_JS_URL:
            options["redoc_url"] = None
    app = FastAPI(**options)
    if documentation_enabled and DOCS_CUSTOM_ENABLED:
        register_static_docs_routes(app)
    return app
