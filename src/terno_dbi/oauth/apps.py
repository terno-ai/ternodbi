from django.apps import AppConfig


class TernoDbiOAuthConfig(AppConfig):
    """Registered as an app so its templates are discoverable.

    It has no models — the only reason it is in `INSTALLED_APPS` is the
    app-directories template loader, which is how
    `templates/oauth2_provider/authorize.html` overrides DOT's consent screen.
    That override only works if this app is listed *before* `oauth2_provider`.
    """

    name = "terno_dbi.oauth"
    label = "terno_dbi_oauth"
    verbose_name = "TernoDBI OAuth"
