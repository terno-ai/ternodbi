import ast
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any, Union, List
import json
import logging

from dbi_layer.django_app import models as dbi_models
from dbi_layer.django_app import conf
from dbi_layer.services import shield, query as query_service

# TernoAI Dependencies
# These are required for the AI/Logic features of the Executor.
# If running outside TernoAI, these must be provided or mocked.
try:
    from suggestions.utils import search_vector_DB_multiple_queries
    from terno.llm.base import LLMFactory
except ImportError:
    search_vector_DB_multiple_queries = None
    LLMFactory = None
    logging.getLogger(__name__).warning("TernoAI dependencies (suggestions, terno.llm) not found in Executor.")

@dataclass
class Action(ABC):
    """
    Base class for all agent‐invokable actions.
    """
    action_type: str = field(repr=False)

    @classmethod
    @abstractmethod
    def get_action_description(cls) -> str:
        """
        Returns a markdown‐formatted description.
        """
        pass

    @abstractmethod
    def run(self, *args, **kwargs) -> Dict:
        """
        Perform the action. Raise ActionError on any user‐ or server‐facing error.
        Returns a JSON‐serializable dict.
        """
        pass


class ActionError(Exception):
    """
    Raised by an Action.run() on any user‐ or server‐facing error.
    Carries an HTTP status code for the view to use.
    """
    def __init__(self, message, status=400):
        super().__init__(message)
        self.message = message
        self.status = status


@dataclass
class LIST_TABLES(Action):
    """
    Action to list tables in a datasource with optional LLM filtering.
    """
    action_type: str = field(default="list_tables", init=False, repr=False)

    datasource_id: int = field(
        metadata={"help": "ID of the datasource for which to list tables"}
    )

    @classmethod
    def get_action_description(cls) -> str:
        return """"""

    def run(self, allowed_tables, search_queries, org_ds) -> Dict:
        print('-------------Get list of tables')
        
        # dbi_models.Table matches terno_models.Table
        # allowed_tables is a list of table names
        
        table_objs = dbi_models.Table.objects.filter(
            data_source=self.datasource_id,
            name__in=allowed_tables
        ).only('name', 'description')

        desc_map = {t.name: t.description or "" for t in table_objs}

        all_descriptions_empty = all(
            not (desc_map.get(tbl, "") or "").strip()
            for tbl in allowed_tables
        )

        # If no search queries or no descriptions, return simple list
        # Original code logic: if all descriptions empty -> return all. 
        # But wait, logic: if search_queries is None/Empty? 
        # The original code only checks all_descriptions_empty.
        
        if all_descriptions_empty or not search_queries:
            # Fallback if no search queries logic mentioned in original?
            # Original: 
            # if all_descriptions_empty: return ...
            # rel_tables = search_vector_DB_multiple_queries(...)
            # If search_queries is empty/None in payload, search_vector_DB_multiple_queries might handle it or crash.
            # safe check:
             return {tbl: (desc_map.get(tbl, "") or "") for tbl in allowed_tables}

        # LLM Logic
        if not search_vector_DB_multiple_queries or not LLMFactory:
            raise ActionError("Smart table search not available (missing dependencies)", 501)

        rel_tables = search_vector_DB_multiple_queries(self.datasource_id, search_queries, allowed_tables=allowed_tables)
        tables = dbi_models.Table.objects.filter(data_source=self.datasource_id, name__in=rel_tables)
        result = {}

        for table in tables:
            columns = dbi_models.TableColumn.objects.filter(table=table)
            column_list = [
                {
                    "column_name": col.name,
                    "column_description": col.description or ""
                }
                for col in columns
            ]
            result[table.name] = {
                "table_description": table.description or "",
                "columns": column_list
            }
            
        llm_prompt = f"""
            You are a database expert.
            You are given a set of tables from a relational database. Each table entry has:
            - table name
            - table description
            - a list of columns (each with name and description)

            Here are the tables you may use:
            {json.dumps(result, indent=2)}

            Here are the user search queries:
            {json.dumps(search_queries, indent=2)}

            Your task is to list only the table names that are definitely or possibly relevant to answering the user’s queries, based on the table and column names/descriptions.

            Be inclusive: if there is any reasonable chance (even 50%) that a table could help answer the queries, include it.

            **Output ONLY a Python list of table names (as strings).**
            Do not output column names, explanations, or anything else—just the list of table names.
            If none of the tables seem relevant, return an empty list.
            """

        llm, is_default_llm, config = LLMFactory.create_llm(org_ds)
        llm_response = llm.get_simple_response(llm_prompt)
        print("LLM response for search query ", llm_response)

        try:
            filtered_tables = ast.literal_eval(llm_response)
            if not isinstance(filtered_tables, list):
                raise ValueError("LLM response is not a list")
        except Exception as e:
            print(f"Error parsing LLM response: {e}")
            filtered_tables = []

        print("LLM-filtered table names:", filtered_tables)
        tables = dbi_models.Table.objects.filter(data_source=self.datasource_id, name__in=filtered_tables)
        return {tbl.name: (tbl.description or "") for tbl in tables}


@dataclass
class EXECUTE_SQL(Action):
    action_type: str = field(default="execute_sql", init=False, repr=False)

    datasource_id: int = field(metadata={"help": "Target datasource"})
    sql_query:     str = field(metadata={"help": "The SELECT statement"})

    @classmethod
    def get_action_description(cls) -> str:
        return """"""

    def run(self, session_obj) -> str | None:
        print("Execute sql run called")
        user = session_obj.user

        print("Preparing mdb")
        datasource = dbi_models.DataSource.objects.get(id=self.datasource_id)
        roles = user.groups.all()
        
        # Use DBI Service
        mdb = shield.prepare_mdb(datasource, roles)
        print("Prepared mdb")
        
        # Translate to dialect-specific SQL
        native_sql_resp = shield.generate_native_sql(
            mdb, self.sql_query, datasource.dialect_name
        )

        print("Created native sql", native_sql_resp)
        if native_sql_resp['status'] == 'error':
            raise ActionError(native_sql_resp['error'], 400)

        native_sql = native_sql_resp['native_sql']
        print("Executing native sql")
        
        # Use DBI Query Service
        exec_resp = query_service.execute_native_sql_return_df(
            datasource, native_sql
        )
        if exec_resp['status'] == 'error':
            raise ActionError(exec_resp['error'], 400)

        parquet_b64 = exec_resp.get('parquet_b64')
        return parquet_b64


@dataclass
class LIST_DATASOURCES(Action):
    action_type: str = field(default="list_datasources", init=False, repr=False)

    @classmethod
    def get_action_description(cls) -> str:
        return """"""

    def run(self, session):
        org_id = session.metadata.get("org_id") if session.metadata else None
        if not org_id:
            raise ActionError("Session metadata does not contain org_id", 400)
            
        Organisation = conf.get_organisation_model()
        if not Organisation:
             raise ActionError("Organisation model not configured", 500)

        try:
            organisation = Organisation.objects.get(id=org_id)
        except Organisation.DoesNotExist:
            raise ActionError("Organisation not found", 404)

        # Get datasources via helpers or direct OrgDS model
        OrgDS = conf.get_organisation_datasource_model()
        if not OrgDS:
             raise ActionError("OrganisationDatasource model not configured", 500)
             
        try:
            org_ds_qs = (
                OrgDS.objects
                .filter(organisation=organisation)
                .select_related("datasource")
            )
        except OrgDS.DoesNotExist:
             # Should not happen on filter loop, but safety
            raise ActionError("Organisation Datasource not found", 404)

        result = []
        for org_ds in org_ds_qs:
            ds = org_ds.datasource
            if ds.enabled:
                result.append({
                    "id": ds.id,
                    "name": ds.display_name,
                    "description": ds.description,
                    "is_erp": ds.is_erp,
                    "dialect_name": ds.dialect_name,
                    "dialect_version": ds.dialect_version
                })
        return result


@dataclass
class GET_DB_SCHEMA(Action):
    """
    Action to get schema of tables in a datasource.
    """
    action_type: str = field(default="get_db_schema", init=False, repr=False)

    datasource_id: int = field(
        metadata={"help": "ID of the datasource for which to list tables"}
    )

    tables: list = field(
        metadata={"help": "List of table names"}
    )

    @classmethod
    def get_action_description(cls) -> str:
        return """"""

    def run(self) -> Dict:
        print('-------------Get tables schema for', self.tables)

        datasource = dbi_models.DataSource.objects.get(id=self.datasource_id)
        tables = (
            dbi_models.Table.objects
            .filter(data_source=datasource, name__in=self.tables)
            .only("public_name", "description")
        )

        return {tbl.public_name: (tbl.description or "") for tbl in tables}

@dataclass
class GET_TABLES(Action):
    """
    Action to list tables in a datasource.
    """
    action_type: str = field(default="list_tables", init=False, repr=False)

    datasource_id: int = field(
        metadata={"help": "ID of the datasource for which to list tables"}
    )

    @classmethod
    def get_action_description(cls) -> str:
        return """"""
