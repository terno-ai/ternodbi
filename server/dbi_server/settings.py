import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

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
    'terno_dbi.core.apps.DbiLayerConfig',
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
        'DIRS': [],
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


TERNO_PROJECT_PATH = os.environ.get('TERNO_PROJECT_PATH', '')
USER_SQLITE_PATH = os.environ.get('USER_SQLITE_PATH', '')

DB_PATH = None

if TERNO_PROJECT_PATH:
    terno_db = Path(TERNO_PROJECT_PATH) / 'db.sqlite3'
    if terno_db.exists():
        DB_PATH = terno_db

if DB_PATH is None:
    terno_ai_path = Path(__file__).resolve().parent.parent.parent.parent / 'terno-ai' / 'terno' / 'db.sqlite3'
    if terno_ai_path.exists():
        DB_PATH = terno_ai_path

if DB_PATH is None:
    DB_PATH = BASE_DIR / 'db.sqlite3'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': DB_PATH,
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
STATIC_URL = 'static/'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
