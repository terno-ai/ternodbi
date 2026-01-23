import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

import terno_dbi

TERNO_DBI_PATH = Path(terno_dbi.__file__).resolve().parent

SECRET_KEY = os.environ.get('DBI_SECRET_KEY', 'django-insecure-change-me-in-production')

DEBUG = os.environ.get('DBI_DEBUG', 'True').lower() == 'true'

ALLOWED_HOSTS = os.environ.get('DBI_ALLOWED_HOSTS', 'localhost,127.0.0.1').split(',')


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

ROOT_URLCONF = 'dbi_server.urls'

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

WSGI_APPLICATION = 'dbi_server.wsgi.application'


# Database Configuration
# ======================
# Options:
# 1. DATABASE_ENGINE=MYSQL (production MySQL)
# 2. DATABASE_ENGINE=POSTGRESQL (production PostgreSQL)
# 3. DJANGO_PROJECT_PATH=/path/to/project (SQLite only - share with another Django project)
# 4. Default: standalone SQLite
#
# To share database with your existing Django project, use options 1, 2, or 3:
#   - For SQLite: set DJANGO_PROJECT_PATH to your Django project directory
#   - For MySQL/PostgreSQL: use the same MYSQL_* or POSTGRES_* credentials as your Django project
#   - Note: SQLite DB path is constructed relative to the PROJECT_PATH if provided, otherwise BASE_DIR
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
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db.sqlite3',
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
STATIC_ROOT = BASE_DIR / 'staticfiles'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
