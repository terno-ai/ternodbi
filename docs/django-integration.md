# Integrating TernoDBI with a Custom Django Project

If your organization already has a mature Django infrastructure, you do not need to run TernoDBI as a standalone isolated service. Because TernoDBI is built fundamentally on Django, you can install and mount its core apps directly into your existing Django project. 

This allows your custom web interface, task queues (Celery), and internal services to interact with TernoDBI's Models, SQL optimization engines, and MCP layers seamlessly.

---

## Step 1: Install the Package

Ensure your Django environment's Python version is `3.10+` and you are running Django `4.2+`.

Install TernoDBI via pip or your preferred package manager (uv, poetry):

```bash
pip install terno-dbi
```

---

## Step 2: Update `settings.py`

You need to register TernoDBI's core app and its dependencies in your `INSTALLED_APPS`. 

Open your project's `settings.py` and modify it:

```python
INSTALLED_APPS = [
    # ... your existing apps ...

    # TernoDBI Core functionality
    'terno_dbi.core.apps.TernoDBIConfig',
    
    # Required dependencies for TernoDBI
    'reversion',  # Used for versioning TernoDBI's metadata changes
]
```

### Middleware Integration

To enable API tokens (required for the Query API), add TernoDBI's token middleware to your `MIDDLEWARE` array. Position it *after* Django's built-in `AuthenticationMiddleware`.

```python
MIDDLEWARE = [
    # ... existing middleware ...
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    
    # TernoDBI Bearer Token Auth
    'terno_dbi.middleware.ServiceTokenMiddleware',
]
```

---

## Step 3: Mount the URLs

You must mount TernoDBI's API endpoints so that your external AI Agents (or MCP desktop clients) can route traffic into the plugin.

Open your root `urls.py`:

```python
from django.urls import path, include

urlpatterns = [
    # ... your existing urls ...

    # Mount TernoDBI's REST API endpoints
    path('api/terno/', include('terno_dbi.core.urls')),
]
```

*Note: In this example, the endpoints will be accessible at `/api/terno/query/...`. You can change the prefix to whatever fits your architecture.*

---

## Step 4: Apply Database Migrations

TernoDBI stores its own configuration state (Datasources, Tokens, Column Metadata, Security Roles). It needs to create these tables inside **your existing Django database** (e.g., your primary Postgres or MySQL instance).

Run the standard migration command:

```bash
python manage.py migrate
```

*TernoDBI will safely create tables prefixed with `core_`, such as `core_datasource`, `core_tablecolumn`, and `core_servicetoken`.*

---

## Step 5: Advanced Usage (Programmatic Access)

Once integrated, you are no longer limited to the REST API. You can import TernoDBI directly into your Django Views, Admin actions, or Celery tasks to heavily customize how data is ingested or how LLMs connect.

### Example: Programmatically fetching schema context for an LLM
```python
from terno_dbi.core.models import DataSource
from terno_dbi.services.schema_utils import get_datasource_tables_info

def my_custom_agent_view(request):
    # Retrieve the configured Snowflake connection
    datasource = DataSource.objects.get(name="Production Snowflake")
    
    # Retrieve context-optimized schema (applies Role filters and Column privacy)
    schema_context = get_datasource_tables_info(datasource.id)
    
    # Inject directly into your Langchain or LlamaIndex prompt!
    prompt = f"Given this schema:\n{schema_context}\nWrite a SQL query for..."
    
    # ...
```

### Next Steps
Once integrated, you can start your Django server normally (`python manage.py runserver`). Go to your Django Admin panel (`/admin/`), and you will see the new **TernoDBI** models ready to be configured!
