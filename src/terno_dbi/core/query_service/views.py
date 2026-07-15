import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from terno_dbi.core.models import PromptExample
from terno_dbi.vector_store.utils import find_similar_examples, sync_prompt_example
from terno_dbi.llm.base import LLMFactory
from terno_dbi.services import memory as memory_service
from terno_dbi.core import models
from terno_dbi.services.query import (
    execute_native_sql,
    execute_paginated_query,
    export_native_sql_result,
    execute_streaming_query,
)
from terno_dbi.services.shield import prepare_mdb, generate_native_sql
from terno_dbi.services.access import get_admin_config_object
from terno_dbi.services.resolver import resolve_datasource
from terno_dbi.decorators import require_service_auth
from django.contrib.auth.models import Group
from django.contrib.auth.models import User

logger = logging.getLogger(__name__)


def _resolve_roles(request, role_ids=None):
    """Resolve Django Group roles from explicit IDs or fall back to token-inherited groups."""
    if role_ids:
        if isinstance(role_ids, str):
            role_ids = [int(r) for r in role_ids.split(',') if r.strip()]
        return Group.objects.filter(id__in=role_ids)
    if hasattr(request, 'service_token') and request.service_token.groups.exists():
        return request.service_token.groups.all()
    return Group.objects.none()


@require_service_auth()
@require_http_methods(["GET"])
def list_datasources(request):
    datasources = request.allowed_datasources
    logger.debug("List datasources requested: count=%d", len(datasources))

    data = []
    for ds in datasources:
        data.append({
            'id': ds.id,
            'name': ds.display_name,
            'type': ds.type,
            'description': ds.description,
            'is_erp': ds.is_erp,
            'dialect_name': ds.dialect_name,
            'dialect_version': ds.dialect_version,
        })

    return JsonResponse({
        "status": "success",
        "datasources": data,
        "count": len(data)
    })



@require_service_auth()
@require_http_methods(["GET"])
def list_tables(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.debug("List tables requested: datasource='%s'", ds.display_name)

    role_ids_str = request.GET.get('roles', '')
    roles = _resolve_roles(request, role_ids_str if role_ids_str else None)

    tables, columns = get_admin_config_object(ds, roles)

    tables_list = []
    for table in tables:
        tables_list.append({
            'id': table.id,
            'name': table.public_name,
            'description': table.description or "",
            'estimated_row_count': table.estimated_row_count
        })

    table_data = []
    for table in tables:
        table_columns = columns.filter(table_id=table.id)
        column_data = list(table_columns.values('public_name', 'data_type'))
        table_data.append({
            'table_name': table.public_name,
            'table_description': table.description,
            'column_data': column_data
        })

    return JsonResponse({
        'status': 'success',
        'tables': tables_list,
        'table_data': table_data,

    })


@require_service_auth()
@require_http_methods(["GET"])
def list_table_columns(request, datasource_identifier, table_identifier):
    ds = request.resolved_datasource

    try:
        table = models.Table.objects.get(id=int(table_identifier), data_source=ds)
    except (ValueError, models.Table.DoesNotExist):
        try:
            table = models.Table.objects.get(public_name=table_identifier, data_source=ds)
        except models.Table.DoesNotExist:
            return JsonResponse({
                "status": "error",
                "error": f"Table '{table_identifier}' not found"
            }, status=404)

    # Enforce column visibility
    roles = _resolve_roles(request)
    _, allowed_columns = get_admin_config_object(ds, roles)
    columns = allowed_columns.filter(table=table)

    return JsonResponse({
        "status": "success",
        "table_id": table.id,
        "table_name": table.public_name,
        "columns": [
            {
                'id': c.id,
                'name': c.public_name,
                'data_type': c.data_type,
                'description': c.description or "",
                'primary_key': c.primary_key,
            }
            for c in columns
        ]
    })



@require_service_auth()
@require_http_methods(["GET"])
def list_foreign_keys(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.debug("List foreign keys requested: datasource='%s'", ds.display_name)

    fks = models.ForeignKey.objects.filter(
        constrained_table__data_source=ds
    ).select_related('constrained_table', 'referred_table')

    fk_data = []
    for fk in fks:
        fk_data.append({
            "id": fk.id,
            "constrained_table": fk.constrained_table.public_name,
            "constrained_column": fk.constrained_columns.public_name if fk.constrained_columns else None,
            "referred_table": fk.referred_table.public_name,
            "referred_column": fk.referred_columns.public_name if fk.referred_columns else None,
        })

    return JsonResponse({
        "status": "success",
        "datasource_id": datasource_identifier,
        "foreign_keys": fk_data
    })


@require_service_auth()
@require_http_methods(["GET"])
def get_sample_data(request, table_id):
    table = request.resolved_table
    ds = table.data_source
    logger.info("Sample data requested: table='%s', datasource='%s'", table.public_name, ds.display_name)

    try:
        rows = int(request.GET.get("rows", 10))
        rows = min(rows, 100)
    except ValueError:
        rows = 10

    sql = f"SELECT * FROM {table.name} LIMIT {rows}"

    try:
        result = execute_native_sql(ds, sql, page=1, per_page=rows)

        if result.get("status") == "error":
            return JsonResponse({
                "status": "error",
                "error": result.get("error", "Query failed")
            }, status=500)

        table_data = result.get("table_data", {})
        return JsonResponse({
            "status": "success",
            "table_id": table_id,
            "columns": table_data.get("columns", []),
            "data": table_data.get("data", [])
        })
    except Exception as e:
        logger.exception(f"Sample data error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)



@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def execute_query(request, datasource_identifier=None):
    try:
        body = json.loads(request.body)

        if datasource_identifier and hasattr(request, 'resolved_datasource'):
            ds = request.resolved_datasource
        else:
            ds_identifier = body.get("datasource") or body.get("datasourceId")
            if not ds_identifier:
                return JsonResponse({
                    "status": "error",
                    "error": "Datasource name or ID required"
                }, status=400)

            try:
                ds = resolve_datasource(ds_identifier)

                if not request.allowed_datasources.filter(id=ds.id).exists():
                    return JsonResponse({
                        "status": "error",
                        "error": "Access denied to datasource"
                    }, status=403)
            except Exception as e:
                return JsonResponse({
                    "status": "error",
                    "error": str(e)
                }, status=404)

        sql = body.get("sql")
        if not sql:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'sql' in request body"
            }, status=400)

        max_rows = body.get("max_rows")

        role_ids = body.get("roles", [])
        roles = _resolve_roles(request, role_ids if role_ids else None)

        mDb = prepare_mdb(ds, roles)
        logger.info("Execute query: datasource='%s'", ds.display_name)
        transform_result = generate_native_sql(mDb, sql, ds.dialect_name)

        if transform_result.get('status') == 'error':
            return JsonResponse({
                "status": "error",
                "error": transform_result.get('error', 'SQL transformation failed')
            }, status=400)

        native_sql = transform_result.get('native_sql', sql)
        logger.debug("Resolved Native SQL: %s", native_sql)

        result = execute_paginated_query(
            datasource=ds,
            native_sql=native_sql,
            max_rows=max_rows,
        )
        return JsonResponse(result)

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Query execution error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def stream_query(request, datasource_identifier=None):
    """Stream query results using SQLAlchemy server-side cursors.
    """
    try:
        body = json.loads(request.body)

        if datasource_identifier and hasattr(request, 'resolved_datasource'):
            ds = request.resolved_datasource
        else:
            ds_identifier = body.get("datasource") or body.get("datasourceId")
            if not ds_identifier:
                return JsonResponse({
                    "status": "error",
                    "error": "Datasource name or ID required"
                }, status=400)

            try:
                ds = resolve_datasource(ds_identifier)

                if not request.allowed_datasources.filter(id=ds.id).exists():
                    return JsonResponse({
                        "status": "error",
                        "error": "Access denied to datasource"
                    }, status=403)
            except Exception as e:
                return JsonResponse({
                    "status": "error",
                    "error": str(e)
                }, status=404)

        sql = body.get("sql")
        if not sql:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'sql' in request body"
            }, status=400)

        role_ids = body.get("roles", [])
        roles = _resolve_roles(request, role_ids if role_ids else None)

        max_rows = body.get("max_rows")
        if max_rows is not None and isinstance(max_rows, int):
            sql = f"SELECT * FROM ({sql}) AS __query_wrapper LIMIT {max_rows}"

        mDb = prepare_mdb(ds, roles)
        logger.info("Stream query: datasource='%s'", ds.display_name)
        transform_result = generate_native_sql(mDb, sql, ds.dialect_name)

        if transform_result.get('status') == 'error':
            return JsonResponse({
                "status": "error",
                "error": transform_result.get('error', 'SQL transformation failed')
            }, status=400)

        native_sql = transform_result.get('native_sql', sql)
        logger.debug("Stream SQL: %s", native_sql)

        from django.http import StreamingHttpResponse
        response = StreamingHttpResponse(
            execute_streaming_query(ds, native_sql),
            content_type='application/x-ndjson'
        )
        return response

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Stream query error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def export_query(request, datasource_identifier=None):
    try:
        body = json.loads(request.body)

        if datasource_identifier and hasattr(request, 'resolved_datasource'):
            ds = request.resolved_datasource
        else:
            ds_identifier = body.get("datasource") or body.get("datasourceId")
            if not ds_identifier:
                return JsonResponse({
                    "status": "error",
                    "error": "Datasource name or ID required"
                }, status=400)

            try:
                ds = resolve_datasource(ds_identifier)
                if not request.allowed_datasources.filter(id=ds.id).exists():
                    return JsonResponse({
                        "status": "error",
                        "error": "Access denied to datasource"
                    }, status=403)
            except Exception as e:
                return JsonResponse({
                    "status": "error",
                    "error": str(e)
                }, status=404)

        sql = body.get("sql")
        if not sql:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'sql' in request body"
            }, status=400)
        role_ids = body.get("roles", [])
        roles = _resolve_roles(request, role_ids if role_ids else None)

        mDb = prepare_mdb(ds, roles)
        transform_result = generate_native_sql(mDb, sql, ds.dialect_name)

        if transform_result.get('status') == 'error':
            return JsonResponse({
                "status": "error",
                "error": transform_result.get('error', 'SQL transformation failed')
            }, status=400)

        native_sql = transform_result.get('native_sql', sql)
        logger.info("Export query: datasource='%s'", ds.display_name)
        return export_native_sql_result(ds, native_sql)

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Export error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)

@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def get_similar_examples_for_agent(request):
    try:
        body = json.loads(request.body)

        org_id = body.get("org_id")
        if not org_id and getattr(request, 'token_organisation', None):
            org_id = request.token_organisation.id

        query = body.get("query", "")
        threshold = body.get("threshold", 0.85)
        limit = body.get("limit", 3)

        if not org_id:
            return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

        try:
            org = models.CoreOrganisation.objects.get(id=org_id)
        except models.CoreOrganisation.DoesNotExist:
            return JsonResponse({"status": "error", "error": "Organisation not found"}, status=404)

        llm = LLMFactory.create_llm(org)
        embedding = llm.generate_vector(query)

        similar = find_similar_examples(
            embedding=embedding,
            org_id=org_id,
            user_id=body.get("user_id"),
            threshold=threshold,
            limit=limit
        )

        return JsonResponse({
            "status": "success",
            "examples": similar
        })
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "error": "Invalid JSON"}, status=400)
    except Exception as e:
        logger.exception("Error finding similar examples")
        return JsonResponse({"status": "error", "error": str(e)}, status=500)


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def add_prompt_example(request):
    try:
        body = json.loads(request.body)

        org_id = body.get("org_id")
        if not org_id and getattr(request, 'token_organisation', None):
            org_id = request.token_organisation.id

        key = body.get("key", "")
        value = body.get("value", "")
        user_id = body.get("user_id")
        is_shared = body.get("is_shared", False)

        if not org_id:
            return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

        try:
            org = models.CoreOrganisation.objects.get(id=org_id)
        except models.CoreOrganisation.DoesNotExist:
            return JsonResponse({"status": "error", "error": "Organisation not found"}, status=404)

        create_kwargs = {
            "organisation": org,
            "key": key,
            "value": value,
            "is_shared": is_shared,
        }
        if user_id:
            try:
                create_kwargs["created_by"] = User.objects.get(id=user_id)
            except User.DoesNotExist:
                pass

        example = PromptExample.objects.create(**create_kwargs)

        sync_prompt_example(example)

        return JsonResponse({
            "status": "success",
            "example": {
                "id": example.id,
                "key": example.key,
                "value": example.value
            }
        })
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "error": "Invalid JSON"}, status=400)
    except Exception as e:
        logger.exception("Error adding prompt example")
        return JsonResponse({"status": "error", "error": str(e)}, status=500)


def _memory_org_id(request):
    """Org for a memory op — strictly the token's own organisation."""
    org = getattr(request, "token_organisation", None)
    return org.id if org else None


def _memory_user_id(request):
    """Acting user for a memory op — strictly the token's own bound user."""
    token = getattr(request, "service_token", None)
    return token.created_by_id if token else None


def _memory_store(body):
    """Normalise the requested store; default to private 'user'."""
    store = (body.get("store") or "user").strip().lower()
    return store if store in ("user", "org") else "user"


def _memory_error_response(exc):
    """Map a memory service exception to a JSON error + status code."""
    from terno_dbi.services.memory import (
        MemoryNotFound, MemoryConflict, MemoryNotUnique, MemoryNoMatch,
        MemoryPermission,
    )
    if isinstance(exc, MemoryNotFound):
        return JsonResponse({"status": "error", "error": str(exc)}, status=404)
    if isinstance(exc, MemoryConflict):
        return JsonResponse({"status": "error", "error": str(exc)}, status=409)
    if isinstance(exc, MemoryPermission):
        return JsonResponse({"status": "error", "error": str(exc)}, status=403)
    if isinstance(exc, (MemoryNotUnique, MemoryNoMatch)):
        return JsonResponse({"status": "error", "error": str(exc)}, status=400)
    logger.exception("Memory operation failed")
    return JsonResponse({"status": "error", "error": str(exc)}, status=500)


def _check_write_perms(request, store, user_id):
    """Return an error JsonResponse if the caller may not write this store, else None.

    org store is gated by the same 'admin:write' scope convention every other
    admin-only view in this codebase uses (ServiceToken.has_scope) — not a
    hand-rolled token_type check — so it honours both scope grants and the
    token_type fallback consistently.
    """
    if store == "org":
        token = getattr(request, "service_token", None)
        if not (token and token.has_scope("admin:write")):
            return JsonResponse(
                {"status": "error",
                 "error": "Writing org-shared memory requires a token with "
                          "'admin:write' scope. Use store='user' for personal memory."},
                status=403,
            )
    else:  # user store
        if not user_id:
            return JsonResponse(
                {"status": "error",
                 "error": "This token is not bound to a user, so it cannot write "
                          "user-store (private) memory. Re-issue it with "
                          "`--user <username>`, or use store='org' with an "
                          "admin:write-scoped token."},
                status=400,
            )
    return None


@require_service_auth()
@require_http_methods(["GET"])
def list_memories(request):
    """Return the memory index (name/description/type/scope, no bodies)."""
    org_id = _memory_org_id(request)
    if not org_id:
        return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

    ds_id = request.GET.get("datasource_id")
    ds_id = int(ds_id) if ds_id else None
    user_id = _memory_user_id(request)

    memories = memory_service.list_memories(
        organisation_id=org_id, user_id=user_id, data_source_id=ds_id,
    )
    resp = {"status": "success", "memories": memories, "count": len(memories)}
    if request.GET.get("render") in ("1", "true", "yes"):
        resp["index"] = memory_service.render_index(memories)
    return JsonResponse(resp)


@require_service_auth()
@require_http_methods(["GET"])
def get_memory(request, name):
    """Return the full content (+ content_hash) of one memory by name."""
    org_id = _memory_org_id(request)
    if not org_id:
        return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

    ds_id = request.GET.get("datasource_id")
    ds_id = int(ds_id) if ds_id else None
    user_id = _memory_user_id(request)

    try:
        mem = memory_service.read_memory(
            organisation_id=org_id, user_id=user_id, name=name, data_source_id=ds_id,
        )
    except Exception as e:
        return _memory_error_response(e)

    return JsonResponse({"status": "success", "memory": memory_service.serialize(mem)})


@require_service_auth()
@require_http_methods(["GET"])
def grep_memory(request):
    """Regex-search memory bodies; returns matching index rows (no bodies)."""
    org_id = _memory_org_id(request)
    if not org_id:
        return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

    pattern = request.GET.get("pattern")
    if not pattern:
        return JsonResponse({"status": "error", "error": "pattern is required"}, status=400)

    ds_id = request.GET.get("datasource_id")
    ds_id = int(ds_id) if ds_id else None
    user_id = _memory_user_id(request)

    try:
        matches = memory_service.grep_memory(
            organisation_id=org_id, user_id=user_id, pattern=pattern, data_source_id=ds_id,
        )
    except Exception as e:
        return _memory_error_response(e)
    return JsonResponse({"status": "success", "matches": matches, "count": len(matches)})


#
# @require_service_auth()
# @require_http_methods(["GET"])
# def get_datasource_context(request, datasource_identifier):
#     """Bundle schema metadata AND the memory index for one datasource.
#
#     The "complete package" call: an agent gets the tables/columns it can see
#     plus the memory index (global + this datasource) in one response. Full
#     memory bodies are fetched lazily via get_memory.
#     """
#     ds = request.resolved_datasource
#     org_id = _memory_org_id(request)
#     user_id = _memory_user_id(request)
#
#     roles = _resolve_roles(request)
#     tables, columns = get_admin_config_object(ds, roles)
#
#     table_data = []
#     for table in tables:
#         table_columns = columns.filter(table_id=table.id)
#         table_data.append({
#             "table_name": table.public_name,
#             "table_description": table.description or "",
#             "estimated_row_count": table.estimated_row_count,
#             "columns": list(table_columns.values("public_name", "data_type", "description")),
#         })
#
#     memories = []
#     if org_id:
#         memories = memory_service.list_memories(
#             organisation_id=org_id, user_id=user_id, data_source_id=ds.id,
#         )
#
#     return JsonResponse({
#         "status": "success",
#         "datasource": {"id": ds.id, "name": ds.display_name, "description": ds.description},
#         "schema": table_data,
#         "memory_index": memories,
#         "memory_note": (
#             "memory_index shows one line per fact. Call get_memory(name=...) for "
#             "the full content of any entry that looks relevant before relying on it."
#         ),
#     })


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def save_memory(request):
    """Create or fully replace a memory (upsert by scope+name).

    Replacing an existing memory requires expected_hash (read-before-write).
    """
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "error": "Invalid JSON"}, status=400)

    org_id = _memory_org_id(request)
    if not org_id:
        return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

    name = (body.get("name") or "").strip()
    if not name:
        return JsonResponse({"status": "error", "error": "name is required"}, status=400)

    store = _memory_store(body)
    user_id = _memory_user_id(request)
    perm_err = _check_write_perms(request, store, user_id)
    if perm_err:
        return perm_err

    try:
        mem, action = memory_service.write_memory(
            organisation_id=org_id,
            name=name,
            description=body.get("description", ""),
            memory_type=body.get("memory_type", "project"),
            content=body.get("content", ""),
            store=store,
            created_by_id=user_id,
            data_source_id=body.get("datasource_id"),
            expected_hash=body.get("expected_hash"),
        )
    except Exception as e:
        return _memory_error_response(e)

    return JsonResponse({
        "status": "success",
        "action": action,
        "memory": {"name": mem.name, "store": mem.store,
                   "datasource_id": mem.data_source_id,
                   "content_hash": mem.content_hash},
    })


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def edit_memory(request, name):
    """Exact string replacement inside an existing memory (read-before-write)."""
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"status": "error", "error": "Invalid JSON"}, status=400)

    org_id = _memory_org_id(request)
    if not org_id:
        return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

    old_string = body.get("old_string")
    new_string = body.get("new_string")
    if not old_string:
        return JsonResponse({"status": "error", "error": "old_string is required"}, status=400)
    if new_string is None:
        return JsonResponse({"status": "error", "error": "new_string is required"}, status=400)

    store = _memory_store(body)
    user_id = _memory_user_id(request)
    perm_err = _check_write_perms(request, store, user_id)
    if perm_err:
        return perm_err

    try:
        mem = memory_service.edit_memory(
            organisation_id=org_id,
            name=name,
            old_string=old_string,
            new_string=new_string,
            store=store,
            created_by_id=user_id,
            expected_hash=body.get("expected_hash"),
            replace_all=bool(body.get("replace_all", False)),
            data_source_id=body.get("datasource_id"),
        )
    except Exception as e:
        return _memory_error_response(e)

    return JsonResponse({
        "status": "success",
        "memory": {"name": mem.name, "content_hash": mem.content_hash},
    })


@csrf_exempt
@require_service_auth()
@require_http_methods(["DELETE", "POST"])
def delete_memory(request, name):
    """Delete a memory by name within the acting scope."""
    body = {}
    if request.body:
        try:
            body = json.loads(request.body)
        except json.JSONDecodeError:
            body = {}

    org_id = _memory_org_id(request)
    if not org_id:
        return JsonResponse({"status": "error", "error": "org_id is required"}, status=400)

    store = _memory_store(body)
    user_id = _memory_user_id(request)
    perm_err = _check_write_perms(request, store, user_id)
    if perm_err:
        return perm_err

    removed = memory_service.delete_memory(
        organisation_id=org_id,
        name=name,
        store=store,
        created_by_id=user_id,
        data_source_id=body.get("datasource_id"),
    )
    if not removed:
        return JsonResponse({"status": "error", "error": f"No memory named '{name}'."}, status=404)
    return JsonResponse({"status": "success", "removed": removed})
