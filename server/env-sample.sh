#!/bin/bash
# TernoDBI Environment Configuration
# ===================================
# Copy this file to env.sh and customize for your setup:
#   cp env-sample.sh env.sh


# REQUIRED in production: Generate a unique secret key
# You can generate one with: python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
export DBI_SECRET_KEY="django-insecure-change-me-in-production"
export DBI_DEBUG="True"
export DBI_ALLOWED_HOSTS="localhost,127.0.0.1"

# Database Configuration

# OPTION 1: MySQL
#==========================================
# Set DATABASE_ENGINE=MYSQL and configure MySQL credentials.
# To share database with your django project, use the same credentials as your django project.
# export DATABASE_ENGINE="MYSQL"
# export MYSQL_DB="your_db_name"  # Use Django project's DB name to share
# export MYSQL_USER="your_mysql_user"
# export MYSQL_PASS="your_mysql_password"
# export MYSQL_HOST="localhost"
# export MYSQL_PORT="3306"

# OPTION 2: PostgreSQL
#==========================================
# Set DATABASE_ENGINE=POSTGRESQL and configure PostgreSQL credentials.
# To share database with your django project, use the same credentials as your django project.
# export DATABASE_ENGINE="POSTGRESQL"
# export POSTGRES_DB="your_db_name"  # Use Django project's DB name to share
# export POSTGRES_USER="your_postgres_user"
# export POSTGRES_PASS="your_postgres_password"
# export POSTGRES_HOST="localhost"
# export POSTGRES_PORT="5432"

# OPTION 3: SQLite - Share with existing Django project
#==========================================
# Point to your Django project directory. TernoDBI will use that project's db.sqlite3.
# This is useful for local development when you want TernoDBI to share data with TernoAI.
# export DJANGO_PROJECT_PATH="/path/to/terno-ai/terno"


# OPTION 4: SQLite - Standalone (default)
#==========================================
# Leave DATABASE_ENGINE and DJANGO_PROJECT_PATH empty.
# TernoDBI will create and use its own db.sqlite3 in the server directory.
export DATABASE_ENGINE=""
export DJANGO_PROJECT_PATH=""


# Logging Configuration
#==========================================
# Control the verbosity of TernoDBI logs.
# Available levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

# For development - show all logs including debug info:
# export TERNODBI_LOG_LEVEL="DEBUG"

# For production - show only warnings and errors (recommended):
# export TERNODBI_LOG_LEVEL="WARNING"

# Default: INFO (shows normal operations without debug details)
export TERNODBI_LOG_LEVEL="INFO"
