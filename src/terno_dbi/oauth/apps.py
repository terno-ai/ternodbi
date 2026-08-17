from django.apps import AppConfig


class TernoDbiOAuthConfig(AppConfig):
    """Django app used to provide the OAuth consent template.

    This app has no models. It is included in `INSTALLED_APPS` so Django can find
    `templates/oauth2_provider/authorize.html`, which overrides DOT's default
    consent screen.

    It must be listed before `oauth2_provider` so the override takes precedence.
    """

    name = "terno_dbi.oauth"
    label = "terno_dbi_oauth"
    verbose_name = "TernoDBI OAuth"
