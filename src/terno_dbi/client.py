
import os
import requests
import logging
from typing import Dict, List, Optional, Any, Union

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
        description: Optional[str] = None
    ) -> Dict:
        url = f"{self.base_url}/api/admin/tables/{table_id}/"
        payload = {}
        if public_name:
            payload["public_name"] = public_name
        if description:
            payload["description"] = description

        response = requests.patch(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def update_column(
        self,
        column_id: int,
        public_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict:
        url = f"{self.base_url}/api/admin/columns/{column_id}/"
        payload = {}
        if public_name:
            payload["public_name"] = public_name
        if description:
            payload["description"] = description
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
        pagination_mode: str = "offset",
        page: int = 1,
        per_page: int = 50,
        cursor: Optional[str] = None,
        direction: str = "forward",
        order_by: Optional[List[Dict[str, str]]] = None,
        limit: Optional[int] = None,
        include_count: bool = False
    ) -> Dict:
        url = f"{self.base_url}/api/query/datasources/{datasource}/query/"

        if limit is not None:
            per_page = limit

        payload = {
            "sql": sql,
            "pagination_mode": pagination_mode,
            "page": page,
            "per_page": min(per_page, 500),
            "include_count": include_count,
        }

        if cursor:
            payload["cursor"] = cursor
        if direction != "forward":
            payload["direction"] = direction
        if order_by:
            payload["order_by"] = order_by

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
