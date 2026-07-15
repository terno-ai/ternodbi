
import os
import requests
import logging
from typing import Dict, List, Optional, Any, Union
import json
import pandas as pd

logger = logging.getLogger(__name__)

DatasourceIdentifier = Union[int, str]


class TernoDBIClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = base_url or os.environ.get("TERNODBI_API_URL") or "http://127.0.0.1:8376"
        self.api_key = api_key or os.environ.get("TERNODBI_API_KEY")

        if self.base_url and self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]

        if not self.base_url:
            logger.warning("No TERNODBI_API_URL provided. Client strictly in offline mode??")
        else:
            logger.debug("TernoDBIClient initialized: base_url=%s", self.base_url)

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _handle_response(self, response: requests.Response) -> Any:
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            try:
                error_data = response.json()
                error_msg = error_data.get("error", str(e))
            except (ValueError, requests.exceptions.JSONDecodeError):
                error_msg = str(e)
            logger.error("API request failed: %s %s -> %s", response.request.method, response.url, error_msg)
            raise Exception(f"API Error: {error_msg}")

    def list_datasources(self) -> List[Dict]:
        url = f"{self.base_url}/api/query/datasources/"
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("datasources", [])

    def create_datasource(self, display_name: str, db_type: str,
                          connection_str: str,
                          connection_json: Optional[Dict] = None,
                          description: str = "") -> Dict:
        logger.info("Creating datasource: %s (type=%s)", display_name, db_type)
        url = f"{self.base_url}/api/admin/datasources/"
        payload = {
            "display_name": display_name,
            "type": db_type,
            "connection_str": connection_str,
            "connection_json": connection_json,
            "description": description
        }
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def delete_datasource(self, datasource: DatasourceIdentifier) -> Dict:
        logger.info("Deleting datasource: %s", datasource)
        url = f"{self.base_url}/api/admin/datasources/{datasource}/delete/"
        response = requests.delete(url, headers=self._get_headers())
        return self._handle_response(response)

    def sync_metadata(self, datasource: DatasourceIdentifier, overwrite: bool = False) -> Dict:
        logger.info("Syncing metadata for datasource: %s (overwrite=%s)", datasource, overwrite)
        url = f"{self.base_url}/api/admin/datasources/{datasource}/sync/"
        payload = {"overwrite": overwrite}
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def validate_connection(self, db_type: str, connection_str: str, connection_json: Optional[Dict] = None) -> Dict:
        url = f"{self.base_url}/api/admin/validate/"
        payload = {
            "type": db_type,
            "connection_str": connection_str,
            "connection_json": connection_json
        }
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def list_tables(self, datasource: DatasourceIdentifier) -> List[Dict]:
        url = f"{self.base_url}/api/query/datasources/{datasource}/tables/"
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("tables", [])

    def list_table_columns(self, datasource: DatasourceIdentifier, table: Union[int, str]) -> List[Dict]:
        url = f"{self.base_url}/api/query/datasources/{datasource}/tables/{table}/columns/"
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("columns", [])

    def update_table(
        self,
        table_id: int,
        public_name: Optional[str] = None,
        description: Optional[str] = None,
        is_hidden: Optional[bool] = None
    ) -> Dict:
        url = f"{self.base_url}/api/admin/tables/{table_id}/"
        payload = {}
        if public_name is not None:
            payload["public_name"] = public_name
        if description is not None:
            payload["description"] = description
        if is_hidden is not None:
            payload["is_hidden"] = is_hidden

        response = requests.patch(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def update_column(
        self,
        column_id: int,
        public_name: Optional[str] = None,
        description: Optional[str] = None,
        is_hidden: Optional[bool] = None
    ) -> Dict:
        url = f"{self.base_url}/api/admin/columns/{column_id}/"
        payload = {}
        if public_name is not None:
            payload["public_name"] = public_name
        if description is not None:
            payload["description"] = description
        if is_hidden is not None:
            payload["is_hidden"] = is_hidden
        response = requests.patch(
            url, json=payload, headers=self._get_headers()
        )
        return self._handle_response(response)

    def get_table_info(self, datasource: DatasourceIdentifier, table_name: str) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource}/tables/{table_name}/info/"
        response = requests.get(url, headers=self._get_headers())
        return self._handle_response(response)

    def execute_query(
        self,
        datasource: DatasourceIdentifier,
        sql: str,
        max_rows: Optional[int] = None,
    ) -> Dict:
        url = f"{self.base_url}/api/query/datasources/{datasource}/query/"

        payload = {
            "sql": sql,
        }

        if max_rows is not None:
            payload["max_rows"] = max_rows

        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def stream_query(
        self,
        datasource: DatasourceIdentifier,
        sql: str,
        max_rows: Optional[int] = None,
    ):

        url = f"{self.base_url}/api/query/datasources/{datasource}/stream/"
        payload = {"sql": sql}
        if max_rows is not None:
            payload["max_rows"] = max_rows

        resp = requests.post(
            url, json=payload, headers=self._get_headers(),
            timeout=600, stream=True
        )
        resp.raise_for_status()

        columns = None
        rows = []
        row_count = 0
        header_received = False
        stopped_early = False

        for line in resp.iter_lines(decode_unicode=True):
            if not line or not line.strip():
                continue

            obj = json.loads(line.strip())

            if "__error__" in obj:
                raise Exception(obj["__error__"])
            elif not header_received:
                columns = obj.get("columns", [])
                header_received = True
            elif "__done__" in obj:
                row_count = obj.get("row_count", len(rows))
            else:
                rows.append(obj)
                if max_rows and len(rows) >= max_rows:
                    stopped_early = True
                    break

        # Close the HTTP connection early if we stopped before consuming all data
        if stopped_early:
            resp.close()
            row_count = len(rows)

        if columns is None:
            columns = list(rows[0].keys()) if rows else []

        df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns or [])
        return df

    def find_similar_examples(self, query: str, org_id: Optional[int] = None, user_id: Optional[int] = None, threshold: float = 0.85, limit: int = 3) -> Dict:
        url = f"{self.base_url}/api/query/similar-examples/"
        payload = {
            "query": query,
            "threshold": threshold,
            "limit": limit
        }
        if org_id is not None:
            payload["org_id"] = org_id
        if user_id is not None:
            payload["user_id"] = user_id

        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def add_examples(self, key: str, value: str, org_id: int, user_id: Optional[int] = None, is_shared: bool = False) -> Dict:
        url = f"{self.base_url}/api/query/add-examples/"
        payload = {
            "key": key,
            "value": value,
            "org_id": org_id,
            "is_shared": is_shared,
        }
        if user_id is not None:
            payload["user_id"] = user_id

        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def get_sample_data(self, table_id: int, rows: int = 10) -> Dict:
        url = f"{self.base_url}/api/query/tables/{table_id}/sample/"
        response = requests.get(
            url,
            params={"rows": rows},
            headers=self._get_headers()
        )
        data = self._handle_response(response)
        return data

    # --- Memory -------------------------------------------------------------

    # def get_datasource_context(self, datasource: DatasourceIdentifier) -> Dict:
    #     """Schema metadata + memory index for a datasource, in one call."""
    #     url = f"{self.base_url}/api/query/datasources/{datasource}/context/"
    #     response = requests.get(url, headers=self._get_headers())
    #     return self._handle_response(response)

    def list_memories(self, datasource_id: Optional[int] = None, render: bool = False) -> Dict:
        url = f"{self.base_url}/api/query/memory/"
        params = {}
        if datasource_id is not None:
            params["datasource_id"] = datasource_id
        if render:
            params["render"] = "1"
        response = requests.get(url, params=params or None, headers=self._get_headers())
        return self._handle_response(response)

    def get_memory(self, name: str, datasource_id: Optional[int] = None) -> Dict:
        url = f"{self.base_url}/api/query/memory/{name}/"
        params = {"datasource_id": datasource_id} if datasource_id is not None else None
        response = requests.get(url, params=params, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("memory", {})

    def grep_memory(self, pattern: str, datasource_id: Optional[int] = None) -> List[Dict]:
        url = f"{self.base_url}/api/query/memory/grep/"
        params = {"pattern": pattern}
        if datasource_id is not None:
            params["datasource_id"] = datasource_id
        response = requests.get(url, params=params, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("matches", [])

    def save_memory(
        self,
        name: str,
        description: str,
        content: str,
        memory_type: str = "project",
        store: str = "user",
        datasource_id: Optional[int] = None,
        expected_hash: Optional[str] = None,
    ) -> Dict:
        url = f"{self.base_url}/api/query/memory/save/"
        payload = {
            "name": name,
            "description": description,
            "content": content,
            "memory_type": memory_type,
            "store": store,
        }
        if datasource_id is not None:
            payload["datasource_id"] = datasource_id
        if expected_hash is not None:
            payload["expected_hash"] = expected_hash
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def edit_memory(
        self,
        name: str,
        old_string: str,
        new_string: str,
        expected_hash: str,
        store: str = "user",
        replace_all: bool = False,
        datasource_id: Optional[int] = None,
    ) -> Dict:
        url = f"{self.base_url}/api/query/memory/{name}/edit/"
        payload = {
            "old_string": old_string,
            "new_string": new_string,
            "expected_hash": expected_hash,
            "store": store,
            "replace_all": replace_all,
        }
        if datasource_id is not None:
            payload["datasource_id"] = datasource_id
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def delete_memory(
        self,
        name: str,
        store: str = "user",
        datasource_id: Optional[int] = None,
    ) -> Dict:
        url = f"{self.base_url}/api/query/memory/{name}/delete/"
        payload = {"store": store}
        if datasource_id is not None:
            payload["datasource_id"] = datasource_id
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)
