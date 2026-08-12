import os
from pathlib import Path
import terno_dbi

DEFAULT_DBI_HOME = Path.home() / '.ternodbi'

TERNO_DBI_PATH = Path(terno_dbi.__file__).resolve().parent
BASE_DIR = TERNO_DBI_PATH.parent.parent

SECRET_KEY = os.environ.get('DBI_SECRET_KEY', 'django-insecure-change-me-in-production')
DEBUG = os.environ.get('DBI_DEBUG', 'True').lower() == 'true'
ALLOWED_HOSTS = os.environ.get('DBI_ALLOWED_HOSTS', '*').split(',')

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'terno_dbi.core.apps.TernoDBIConfig',
    'reversion',
]

# OAuth is optional for the standalone server: it is only needed to exercise the
# connector flow locally. Without django-oauth-toolkit the server still runs and
# serves /mcp with a hand-issued service token.
#
# ORDER MATTERS: terno_dbi.oauth must precede oauth2_provider, or DOT's default
# consent template wins over ours, silently.
try:
    import oauth2_provider  # noqa: F401

    INSTALLED_APPS += ['terno_dbi.oauth', 'oauth2_provider']
    OAUTH_ENABLED = True
except ImportError:
    OAUTH_ENABLED = False

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'terno_dbi.middleware.ServiceTokenMiddleware',
]

ROOT_URLCONF = 'terno_dbi.server.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            TERNO_DBI_PATH / 'core' / 'frontend' / 'templates',
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'terno_dbi.server.wsgi.application'

DJANGO_PROJECT_PATH = os.environ.get('DJANGO_PROJECT_PATH', '')

if os.environ.get('DATABASE_ENGINE') == 'MYSQL':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': os.environ.get('MYSQL_DB', 'ternodbi'),
            'USER': os.environ.get('MYSQL_USER'),
            'PASSWORD': os.environ.get('MYSQL_PASS'),
            'HOST': os.environ.get('MYSQL_HOST', 'localhost'),
            'PORT': os.environ.get('MYSQL_PORT', '3306'),
            'CONN_MAX_AGE': 1800,
            'OPTIONS': {
                'charset': 'utf8mb4',
                'init_command': "SET NAMES 'utf8mb4'"
            },
        }
    }
elif os.environ.get('DATABASE_ENGINE') == 'POSTGRESQL':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.environ.get('POSTGRES_DB', 'ternodbi'),
            'USER': os.environ.get('POSTGRES_USER'),
            'PASSWORD': os.environ.get('POSTGRES_PASS'),
            'HOST': os.environ.get('POSTGRES_HOST', 'localhost'),
            'PORT': os.environ.get('POSTGRES_PORT', '5432'),
            'CONN_MAX_AGE': 1800,
        }
    }
elif DJANGO_PROJECT_PATH:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': Path(DJANGO_PROJECT_PATH) / 'db.sqlite3',
            'OPTIONS': {
                'timeout': 20,
            }
        }
    }
else:
    # Ensure the default terno_dbi home directory exists
    DEFAULT_DBI_HOME.mkdir(parents=True, exist_ok=True)
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': DEFAULT_DBI_HOME / 'db.sqlite3',
            'OPTIONS': {
                'timeout': 20,
            }
        }
    }


AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True
STATIC_URL = '/static/'
STATICFILES_DIRS = [
    TERNO_DBI_PATH / 'core' / 'frontend' / 'static',
]
STATIC_ROOT = DEFAULT_DBI_HOME / 'staticfiles'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {name} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'terno_dbi': {
            'handlers': ['console'],
            'level': os.environ.get('TERNODBI_LOG_LEVEL', 'INFO'),
            'propagate': True,
        },
    },
}


# ---------------------------------------------------------------------------
# Local connector settings
# ---------------------------------------------------------------------------
_LOCAL_PORT = os.environ.get('TERNO_LOCAL_PORT', '8376')
TERNO_MCP_BASE_URL = os.environ.get('TERNO_MCP_BASE_URL', f'http://127.0.0.1:{_LOCAL_PORT}')
PROVISIONER_URL = os.environ.get('PROVISIONER_URL', f'http://127.0.0.1:{_LOCAL_PORT}')

# Self-serve org creation on first connect. The standalone server has no
# terno-ai models, so it uses the simpler local provisioner.
TERNO_ORG_PROVISIONER = os.environ.get(
    'TERNO_ORG_PROVISIONER', 'terno_dbi.server.provisioning.provision_local_org'
)

if OAUTH_ENABLED:
    from terno_dbi.oauth.scopes import DEFAULT_SCOPES as _DEFAULT_SCOPES
    from terno_dbi.oauth.scopes import SCOPE_DESCRIPTIONS as _SCOPES

    OAUTH2_PROVIDER = {
        'SCOPES': dict(_SCOPES),
        'DEFAULT_SCOPES': sorted(_DEFAULT_SCOPES),
        'PKCE_REQUIRED': True,
        'ACCESS_TOKEN_GENERATOR': 'terno_dbi.oauth.minting.generate_oauth_access_token',
        'OAUTH2_VALIDATOR_CLASS': 'terno_dbi.oauth.validator.TernoOAuth2Validator',
        'ACCESS_TOKEN_EXPIRE_SECONDS': 60 * 60 * 8,
        'ROTATE_REFRESH_TOKEN': True,
        # Loopback http is how a local client receives its callback.
        'ALLOWED_REDIRECT_URI_SCHEMES': ['http', 'https'],
    }
