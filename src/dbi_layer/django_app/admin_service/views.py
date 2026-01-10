"""
Admin Service Views.

Endpoints for managing tokens and schema metadata (rename, hide, update).
Requires an admin token.
"""

import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

from dbi_layer.django_app import models
from dbi_layer.django_app.auth import require_admin_token, check_datasource_access, require_auth

logger = logging.getLogger(__name__)


# =============================================================================
# Token Management
# =============================================================================

@csrf_exempt
@require_admin_token
@require_http_methods(["POST"])
def create_token(request):
    """
    Create a new service token.
    
    Request body:
        {
            "name": "My Query Token",
            "type": "query",  # or "admin"
            "expires_days": 30,  # optional
            "datasource_ids": [1, 2]  # optional, empty = global
        }
    """
    try:
        body = json.loads(request.body)
        name = body.get("name")
        token_type = body.get("type", "query")
        expires_days = body.get("expires_days")
        datasource_ids = body.get("datasource_ids", [])
        
        if not name:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'name' in request body"
            }, status=400)
        
        if token_type not in ['admin', 'query']:
            return JsonResponse({
                "status": "error",
                "error": "Invalid token type. Must be 'admin' or 'query'"
            }, status=400)
        
        # Generate token
        raw_key = models.ServiceToken.generate_key()
        key_hash = models.ServiceToken.hash_key(raw_key)
        key_prefix = raw_key[:12]
        
        # Calculate expiry
        expires_at = None
        if expires_days:
            from datetime import timedelta
            expires_at = timezone.now() + timedelta(days=int(expires_days))
        
        # Create token
        token = models.ServiceToken.objects.create(
            key_hash=key_hash,
            key_prefix=key_prefix,
            name=name,
            token_type=token_type,
            expires_at=expires_at,
            created_by=getattr(request, 'user', None) if hasattr(request, 'user') and request.user.is_authenticated else None,
        )
        
        # Add datasource scope
        if datasource_ids:
            datasources = models.DataSource.objects.filter(id__in=datasource_ids)
            token.datasources.set(datasources)
        
        return JsonResponse({
            "status": "success",
            "token": {
                "key": raw_key,  # Only returned once!
                "id": token.id,
                "name": token.name,
                "type": token.token_type,
                "prefix": token.key_prefix,
                "expires_at": token.expires_at.isoformat() if token.expires_at else None,
                "scope": "global" if not datasource_ids else f"{len(datasource_ids)} datasource(s)"
            },
            "warning": "Save this token now! It cannot be retrieved later."
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Token creation error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@require_admin_token
@require_http_methods(["GET"])
def list_tokens(request):
    """List all service tokens (without keys)."""
    tokens = models.ServiceToken.objects.all()
    
    data = []
    for token in tokens:
        data.append({
            "id": token.id,
            "name": token.name,
            "type": token.token_type,
            "prefix": token.key_prefix,
            "is_active": token.is_active,
            "created_at": token.created_at.isoformat(),
            "expires_at": token.expires_at.isoformat() if token.expires_at else None,
            "last_used": token.last_used.isoformat() if token.last_used else None,
            "scope": "global" if not token.datasources.exists() else f"{token.datasources.count()} datasource(s)"
        })
    
    return JsonResponse({
        "status": "success",
        "count": len(data),
        "tokens": data
    })


@csrf_exempt
@require_admin_token
@require_http_methods(["POST"])
def revoke_token(request, token_id):
    """Revoke a service token."""
    try:
        token = models.ServiceToken.objects.get(id=token_id)
        token.is_active = False
        token.save(update_fields=['is_active'])
        
        return JsonResponse({
            "status": "success",
            "message": f"Token '{token.name}' has been revoked"
        })
    except models.ServiceToken.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Token {token_id} not found"
        }, status=404)


@csrf_exempt
@require_admin_token
@require_http_methods(["DELETE"])
def delete_token(request, token_id):
    """Permanently delete a service token."""
    try:
        token = models.ServiceToken.objects.get(id=token_id)
        name = token.name
        token.delete()
        
        return JsonResponse({
            "status": "success",
            "message": f"Token '{name}' has been deleted"
        })
    except models.ServiceToken.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Token {token_id} not found"
        }, status=404)


# =============================================================================
# Table Management
# =============================================================================

@csrf_exempt
@require_admin_token
@require_http_methods(["PATCH"])
def update_table(request, table_id):
    """
    Update table metadata.
    
    Request body:
        {
            "public_name": "New Display Name",
            "description": "Updated description",
            "hidden": true  # Hide from query API
        }
    """
    try:
        table = models.Table.objects.get(id=table_id)
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, table.data_source)
    if not allowed:
        return error_response
    
    try:
        body = json.loads(request.body)
        
        updated = []
        if "public_name" in body:
            table.public_name = body["public_name"]
            updated.append("public_name")
        if "description" in body:
            table.description = body["description"]
            updated.append("description")
        
        if updated:
            table.save(update_fields=updated)
        
        return JsonResponse({
            "status": "success",
            "message": f"Table updated: {', '.join(updated)}",
            "table": {
                "id": table.id,
                "name": table.name,
                "public_name": table.public_name,
                "description": table.description,
            }
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


# =============================================================================
# Column Management
# =============================================================================

@csrf_exempt
@require_admin_token
@require_http_methods(["PATCH"])
def update_column(request, column_id):
    """
    Update column metadata.
    
    Request body:
        {
            "public_name": "New Display Name"
        }
    """
    try:
        column = models.TableColumn.objects.select_related('table__data_source').get(id=column_id)
    except models.TableColumn.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Column {column_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, column.table.data_source)
    if not allowed:
        return error_response
    
    try:
        body = json.loads(request.body)
        
        updated = []
        if "public_name" in body:
            column.public_name = body["public_name"]
            updated.append("public_name")
        
        if updated:
            column.save(update_fields=updated)
        
        return JsonResponse({
            "status": "success",
            "message": f"Column updated: {', '.join(updated)}",
            "column": {
                "id": column.id,
                "name": column.name,
                "public_name": column.public_name,
                "data_type": column.data_type,
            }
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


# =============================================================================
# DataSource Management
# =============================================================================



@csrf_exempt
@require_auth()
@require_http_methods(["POST"])
def add_datasource(request):
    """
    Create a new datasource (replaces create_datasource).
    
    Supports:
    - Session auth (TernoAI) with org checks
    - Token auth (Admin only)
    - BigQuery JSON parsing
    - Post-creation signal trigger (for metadata generation)
    """
    from dbi_layer.services.validation import validate_datasource_input
    from dbi_layer.django_app import conf
    
    # 1. Auth Check
    auth_type = getattr(request, 'auth_type', None)
    org_id = getattr(request, 'org_id', None)
    user_id = request.user.id if request.user.is_authenticated else None
    
    if auth_type == 'session':
        # Verify org membership
        if not org_id:
             return JsonResponse({'status': 'error', 'error': 'Organisation ID required'}, status=400)
             
        if not conf.check_org_membership(request.user, org_id):
            return JsonResponse({'status': 'error', 'error': 'You do not belong to this organisation.'}, status=403)
            
    elif auth_type == 'token':
        # Ensure it's an admin token
        if request.service_token.token_type != 'admin':
             return JsonResponse({'status': 'error', 'error': 'Admin token required'}, status=403)
    else:
        return JsonResponse({'status': 'error', 'error': 'Invalid authentication'}, status=401)

    try:
        body = json.loads(request.body)
        
        # Support both TernoAI and TernoDBI field names
        name = body.get("display_name") or body.get("name")
        db_type = body.get("type")
        connection_string = body.get("connection_str") or body.get("connection_string")
        connection_json = body.get("connection_json")
        description = body.get("description", "")
        
        if not name or not db_type or not connection_string:
            return JsonResponse({
                "status": "error",
                "error": "Missing required fields: display_name, type, connection_str"
            }, status=400)
            
        # BigQuery special handling: Parse JSON if string
        if db_type == 'BigQuery' or db_type == 'bigquery':
             if not connection_json:
                 return JsonResponse({'status': 'error', 'error': 'connection_json is required for BigQuery'}, status=400)
             if isinstance(connection_json, str):
                 try:
                     connection_json = json.loads(connection_json)
                 except json.JSONDecodeError:
                     return JsonResponse({'status': 'error', 'error': 'Invalid connection_json format'}, status=400)
        
        # Validate connection
        error = validate_datasource_input(db_type, connection_string, connection_json)
        if error:
            return JsonResponse({
                "status": "error",
                "error": f"Connection validation failed: {error}"
            }, status=400)
        
        # Create datasource
        ds = models.DataSource.objects.create(
            display_name=name,
            type=db_type,
            connection_str=connection_string,
            connection_json=connection_json,
            description=description,
            dialect_name=db_type,
            enabled=True,
        )
        
        # Create Link (OrganisationDataSource)
        if org_id:
            OrgDS = conf.get_organisation_datasource_model()
            if OrgDS:
                OrgDS.objects.create(
                    organisation_id=org_id,
                    datasource=ds
                )
        
        # TRIGGER BACKGROUND TASKS (Hybrid Signal/Callback)
        conf.trigger_post_datasource_creation(
            datasource_id=ds.id,
            org_id=org_id,
            user_id=user_id
        )
        
        return JsonResponse({
            "status": "success",
            "datasource_id": ds.id,  # TernoAI expects this key
            "datasource": {
                "id": ds.id,
                "name": ds.display_name,
                "type": ds.type,
                "enabled": ds.enabled,
                "org_linked": org_id is not None,
            }
        }, status=201)
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Datasource creation error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)




@csrf_exempt
@require_admin_token
@require_http_methods(["PATCH"])
def update_datasource(request, datasource_id):
    """
    Update a datasource.
    
    Request body:
        {
            "name": "New Name",
            "description": "Updated description",
            "enabled": false
        }
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, ds)
    if not allowed:
        return error_response
    
    try:
        body = json.loads(request.body)
        
        updated = []
        if "name" in body:
            ds.display_name = body["name"]
            updated.append("display_name")
        if "description" in body:
            ds.description = body["description"]
            updated.append("description")
        if "enabled" in body:
            ds.enabled = body["enabled"]
            updated.append("enabled")
        
        if updated:
            ds.save(update_fields=updated)
        
        return JsonResponse({
            "status": "success",
            "message": f"Datasource updated: {', '.join(updated)}",
            "datasource": {
                "id": ds.id,
                "name": ds.display_name,
                "type": ds.type,
                "description": ds.description,
                "enabled": ds.enabled,
            }
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


@csrf_exempt
@require_admin_token
@require_http_methods(["DELETE"])
def delete_datasource(request, datasource_id):
    """Delete a datasource and all its metadata."""
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, ds)
    if not allowed:
        return error_response
    
    name = ds.display_name
    ds.delete()
    
    return JsonResponse({
        "status": "success",
        "message": f"Datasource '{name}' and all its metadata have been deleted"
    })


# =============================================================================
# Datasource Suggestions Management
# =============================================================================

@require_admin_token
@require_http_methods(["GET"])
def list_suggestions(request, datasource_id):
    """List all suggestions for a datasource."""
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, ds)
    if not allowed:
        return error_response
    
    suggestions = models.DatasourceSuggestions.objects.filter(data_source=ds)
    data = [{"id": s.id, "suggestion": s.suggestion} for s in suggestions]
    
    return JsonResponse({
        "status": "success",
        "datasource_id": datasource_id,
        "count": len(data),
        "suggestions": data
    })


@csrf_exempt
@require_admin_token
@require_http_methods(["POST"])
def add_suggestion(request, datasource_id):
    """
    Add a suggestion to a datasource.
    
    Request body:
        {
            "suggestion": "Show me top 10 customers"
        }
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, ds)
    if not allowed:
        return error_response
    
    try:
        body = json.loads(request.body)
        suggestion_text = body.get("suggestion")
        
        if not suggestion_text:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'suggestion' in request body"
            }, status=400)
        
        suggestion = models.DatasourceSuggestions.objects.create(
            data_source=ds,
            suggestion=suggestion_text
        )
        
        return JsonResponse({
            "status": "success",
            "suggestion": {
                "id": suggestion.id,
                "suggestion": suggestion.suggestion,
                "datasource_id": ds.id
            }
        }, status=201)
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


@csrf_exempt
@require_admin_token
@require_http_methods(["DELETE"])
def delete_suggestion(request, suggestion_id):
    """Delete a suggestion."""
    try:
        suggestion = models.DatasourceSuggestions.objects.select_related('data_source').get(id=suggestion_id)
    except models.DatasourceSuggestions.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Suggestion {suggestion_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, suggestion.data_source)
    if not allowed:
        return error_response
    
    suggestion.delete()
    
    return JsonResponse({
        "status": "success",
        "message": "Suggestion deleted"
    })
