from django.shortcuts import render
from django.http import JsonResponse
from .auth import require_session
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt

from dbi_layer.django_app import models as dbi_models
from dbi_layer.django_app import conf
from dbi_layer.services import access as dbi_access

from .actions import LIST_TABLES, LIST_DATASOURCES, EXECUTE_SQL, ActionError, GET_DB_SCHEMA, GET_TABLES


@require_POST
@csrf_exempt
@require_session
def list_tables(request):
    payload = request.json
    ds_id = payload.get("datasource_id")
    search_queries = payload.get("search_queries")

    if not (ds_id):
        return JsonResponse(
            {"status_type": "error",
             "message": "datasource_id are required"},
            status=400,
        )

    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )

    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)

        if not conf.check_org_membership(request.user, org_id):
            raise ActionError("You do not belong to this organisation", 403)

        OrgDS = conf.get_organisation_datasource_model()
        org_ds = OrgDS.objects.get(
            organisation=organisation, datasource_id=ds_id
        )
        datasource = org_ds.datasource

        action = LIST_TABLES(
            datasource_id=datasource.id,
        )
        roles = request.user.groups.all()
        allowed_tables, allowed_columns = dbi_access.get_admin_config_object(datasource, roles)
        allowed_tables_list = [tab.name for tab in allowed_tables]
        result = action.run(allowed_tables=allowed_tables_list, search_queries=search_queries, org_ds=org_ds) # org_ds passed for LLM config

        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )

    except (OrgDS.DoesNotExist, Organisation.DoesNotExist):
        return JsonResponse(
            {"status_type": "error",
             "message": "Datasource not found or unauthorized"},
            status=403,
        )
    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=e.status
        )

@require_POST
@csrf_exempt
@require_session
def list_table_columns(request):
    payload = request.json
    ds_id = payload.get("datasource_id")
    table_name = payload.get("table_name")

    if not (ds_id):
        return JsonResponse(
            {"status_type": "error",
             "message": "datasource_id is required"},
            status=400,
        )
    if not (table_name):
        return JsonResponse(
            {"status_type": "error",
             "message": "table_name is required"},
            status=400,
        )
    
    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )
    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)

        if not conf.check_org_membership(request.user, org_id):
             raise ActionError("You do not belong to this organisation", 403)
        
        OrgDS = conf.get_organisation_datasource_model()
        org_ds = OrgDS.objects.get(
            organisation=organisation, datasource_id=ds_id
        )
        datasource = org_ds.datasource
        roles = request.user.groups.all()
        allowed_tables, allowed_columns = dbi_access.get_admin_config_object(datasource, roles)
        filtered_tables = allowed_tables.filter(public_name = table_name)
        if len(filtered_tables) == 0:
            raise ActionError(f"You do not have access to the table {table_name}", 403)
        table_obj = filtered_tables[0]
        filtered_columns = allowed_columns.filter(table=table_obj)
        #result = list(filtered_columns.values_list('public_name', 'data_type', 'description'))
        columns = list(filtered_columns.values_list('public_name', 'data_type', 'description'))
        result = [
            (public_name, data_type, description if description is not None else "")
            for (public_name, data_type, description) in columns]
        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )
    
    except (OrgDS.DoesNotExist, Organisation.DoesNotExist):
        return JsonResponse(
            {"status_type": "error",
             "message": "Datasource not found or unauthorized"},
            status=403,
        )
    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=e.status
        )


@require_POST
@csrf_exempt
@require_session
def execute_sql(request):
    print("Execute requst recieved")
    payload = request.json
    ds_id = payload.get("datasource_id")
    sql = payload.get("query")

    if not (ds_id and sql):
        return JsonResponse(
            {"status_type": "error",
             "message": "datasource_id and sql query are required"},
            status=400,
        )

    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )

    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)
        
        if not conf.check_org_membership(request.user, org_id):
             raise ActionError("You do not belong to this organisation", 403)

        # datasource ownership check
        OrgDS = conf.get_organisation_datasource_model()
        OrgDS.objects.get(
            organisation=organisation, datasource_id=ds_id
        )

        action = EXECUTE_SQL(
            datasource_id=ds_id,
            sql_query=sql,
        )
        result = action.run(request.session_obj)
        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )

    except (OrgDS.DoesNotExist, Organisation.DoesNotExist):
        return JsonResponse(
            {"status_type": "error",
             "message": "Datasource not found or unauthorized"},
            status=403,
        )
    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=200
        )


@require_POST
@csrf_exempt
@require_session
def list_datasources(request):
    print("list datasource recieved")
    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )

    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)

        if not conf.check_org_membership(request.user, org_id):
             raise ActionError("You do not belong to this organisation", 403)

        result = LIST_DATASOURCES().run(request.session_obj)
        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )

    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=e.status
        )


@require_POST
@csrf_exempt
@require_session
def get_db_schema(request):
    payload = request.json
    ds_id = payload.get("datasource_id")
    tables = payload.get("tables")

    if not (ds_id and tables):
        return JsonResponse(
            {"status_type": "error",
             "message": "datasource_id and tables are required"},
            status=400,
        )

    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )

    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)
        
        if not conf.check_org_membership(request.user, org_id):
             raise ActionError("You do not belong to this organisation", 403)

        OrgDS = conf.get_organisation_datasource_model()
        org_ds = OrgDS.objects.get(
            organisation=organisation, datasource_id=ds_id
        )
        datasource = org_ds.datasource

        action = GET_DB_SCHEMA(
            datasource_id=datasource.id,
            tables=tables
        )
        result = action.run()
        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )

    except (OrgDS.DoesNotExist, Organisation.DoesNotExist):
        return JsonResponse(
            {"status_type": "error",
             "message": "Datasource not found or unauthorized"},
            status=403,
        )
    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=e.status
        )
    
@require_POST
@csrf_exempt
@require_session
def list_all_tables(request):
    payload = request.json
    ds_id = payload.get("datasource_id")

    if not (ds_id):
        return JsonResponse(
            {"status_type": "error",
             "message": "datasource_id are required"},
            status=400,
        )

    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )

    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)
        
        if not conf.check_org_membership(request.user, org_id):
             raise ActionError("You do not belong to this organisation", 403)

        OrgDS = conf.get_organisation_datasource_model()
        org_ds = OrgDS.objects.get(
            organisation=organisation, datasource_id=ds_id
        )
        datasource = org_ds.datasource

        roles = request.user.groups.all()
        allowed_tables, allowed_columns = dbi_access.get_admin_config_object(datasource, roles)
        result = {tbl.name: (tbl.description or "") for tbl in allowed_tables}

        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )

    except (OrgDS.DoesNotExist, Organisation.DoesNotExist):
        return JsonResponse(
            {"status_type": "error",
             "message": "Datasource not found or unauthorized"},
            status=403,
        )
    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=e.status
        ) 

@require_POST
@csrf_exempt
@require_session
def get_tables(request):
    payload = request.json
    ds_id = payload.get("datasource_id")

    if not (ds_id):
        return JsonResponse(
            {"status_type": "error",
             "message": "datasource_id are required"},
            status=400,
        )

    org_id = request.session_obj.metadata.get("org_id")
    if not org_id:
        return JsonResponse(
            {"status_type": "error",
             "message": "Session metadata does not contain org_id"},
            status=400,
        )

    try:
        Organisation = conf.get_organisation_model()
        organisation = Organisation.objects.get(id=org_id)
        
        if not conf.check_org_membership(request.user, org_id):
             raise ActionError("You do not belong to this organisation", 403)

        OrgDS = conf.get_organisation_datasource_model()
        org_ds = OrgDS.objects.get(
            organisation=organisation, datasource_id=ds_id
        )
        datasource = org_ds.datasource

        roles = request.user.groups.all()
        allowed_tables, allowed_columns = dbi_access.get_admin_config_object(datasource, roles)
        # Result format different from list_all_tables: public_name vs name?
        # Original code used public_name for get_tables, name for list_all_tables.
        result = {tbl.public_name: (tbl.description or "") for tbl in allowed_tables}

        return JsonResponse(
            {"status_type": "success", "message": result}, status=200
        )

    except (OrgDS.DoesNotExist, Organisation.DoesNotExist):
        return JsonResponse(
            {"status_type": "error",
             "message": "Datasource not found or unauthorized"},
            status=403,
        )
    except ActionError as e:
        return JsonResponse(
            {"status_type": "error", "message": e.message}, status=e.status
        )
