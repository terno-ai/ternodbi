
import os
import requests
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class TernoDBIClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = base_url or os.environ.get("TERNODBI_API_URL") or "http://127.0.0.1:8000"
        self.api_key = api_key or os.environ.get("TERNODBI_API_KEY")

        if self.base_url and self.base_url.endswith("/"):
            self.base_url = self.base_url[:-1]

        if not self.base_url:
            logger.warning("No TERNODBI_API_URL provided. Client strictly in offline mode??")

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
            except:
                error_msg = str(e)
            raise Exception(f"API Error: {error_msg}")

    def list_datasources(self) -> List[Dict]:
        url = f"{self.base_url}/api/query/datasources/"
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("datasources", [])

    def create_datasource(self, display_name: str, db_type: str, connection_str: str, 
                         connection_json: Optional[Dict] = None, description: str = "") -> Dict:
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

    def delete_datasource(self, datasource_id: int) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource_id}/delete/"
        response = requests.delete(url, headers=self._get_headers())
        return self._handle_response(response)

    def sync_metadata(self, datasource_id: int, overwrite: bool = False) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource_id}/sync/"
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

    def list_tables(self, datasource_id: int) -> List[Dict]:
        url = f"{self.base_url}/api/query/datasources/{datasource_id}/tables/"
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("tables", [])

    def list_columns(self, table_id: int) -> List[Dict]:
        url = f"{self.base_url}/api/query/tables/{table_id}/columns/"
        response = requests.get(url, headers=self._get_headers())
        data = self._handle_response(response)
        return data.get("columns", [])

    def get_schema(self, datasource_id: int) -> Dict:
        url = f"{self.base_url}/api/query/datasources/{datasource_id}/schema/"
        response = requests.get(url, headers=self._get_headers())
        return self._handle_response(response)
    
    def update_table(self, table_id: int, public_name: Optional[str] = None, description: Optional[str] = None) -> Dict:
        url = f"{self.base_url}/api/admin/tables/{table_id}/"
        payload = {}
        if public_name: payload["public_name"] = public_name
        if description: payload["description"] = description

        response = requests.patch(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def update_column(self, column_id: int, public_name: Optional[str] = None, description: Optional[str] = None) -> Dict:
        url = f"{self.base_url}/api/admin/columns/{column_id}/"
        payload = {}
        if public_name: payload["public_name"] = public_name
        if description: payload["description"] = description
        response = requests.patch(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def get_table_info(self, datasource_id: int, table_name: str) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource_id}/tables/{table_name}/info/"
        response = requests.get(url, headers=self._get_headers())
        return self._handle_response(response)

    def get_all_tables_info(self, datasource_id: int, table_names: Optional[List[str]] = None) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource_id}/tables/info/"
        payload = {}
        if table_names: payload["table_names"] = table_names
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def list_suggestions(self, datasource_id: int) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource_id}/suggestions/"
        response = requests.get(url, headers=self._get_headers())
        return self._handle_response(response)

    def add_suggestion(self, datasource_id: int, suggestion: str) -> Dict:
        url = f"{self.base_url}/api/admin/datasources/{datasource_id}/suggestions/add/"
        payload = {"suggestion": suggestion}
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def delete_suggestion(self, suggestion_id: int) -> Dict:
        url = f"{self.base_url}/api/admin/suggestions/{suggestion_id}/"
        response = requests.delete(url, headers=self._get_headers())
        return self._handle_response(response)

    def execute_query(
        self, 
        datasource_id: int, 
        sql: str, 
        pagination_mode: str = "offset",
        page: int = 1,
        per_page: int = 50,
        cursor: Optional[str] = None,
        direction: str = "forward",
        order_by: Optional[List[Dict[str, str]]] = None,
        # Legacy parameter (deprecated)
        limit: Optional[int] = None
    ) -> Dict:
        """
        Execute SQL query with pagination support.
        
        Args:
            datasource_id: ID of the datasource to query
            sql: SQL query to execute
            pagination_mode: "offset" (default) or "cursor"
            page: Page number for offset mode (1-indexed)
            per_page: Rows per page (max: 500)
            cursor: Cursor string from previous response (for cursor mode)
            direction: "forward" or "backward" for cursor mode
            order_by: List of {"column": "name", "direction": "DESC"} dicts
            limit: DEPRECATED - use per_page instead
        
        Returns:
            Dict with status, table_data including:
            - columns, data, page, per_page
            - has_next, has_prev
            - next_cursor, prev_cursor (for cursor mode)
        """
        url = f"{self.base_url}/api/query/datasources/{datasource_id}/query/"
        
        # Handle legacy limit parameter
        if limit is not None:
            per_page = limit
        
        payload = {
            "sql": sql,
            "pagination_mode": pagination_mode,
            "page": page,
            "per_page": min(per_page, 500)
        }
        
        if cursor:
            payload["cursor"] = cursor
        if direction != "forward":
            payload["direction"] = direction
        if order_by:
            payload["order_by"] = order_by
        
        response = requests.post(url, json=payload, headers=self._get_headers())
        return self._handle_response(response)

    def iter_query(
        self, 
        datasource_id: int, 
        sql: str, 
        per_page: int = 100,
        order_by: Optional[List[Dict[str, str]]] = None
    ):
        """
        Iterate through all results using cursor pagination.
        
        Memory-efficient for very large datasets. Uses cursor pagination
        internally for O(1) performance at any depth.
        
        Args:
            datasource_id: ID of the datasource to query
            sql: SQL query to execute
            per_page: Batch size (rows per request)
            order_by: List of {"column": "name", "direction": "DESC"} dicts
        
        Yields:
            List[Dict]: Batch of rows as dictionaries
        
        Example:
            for batch in client.iter_query(1, "SELECT * FROM large_table"):
                for row in batch:
                    process(row)
        """
        cursor = None
        while True:
            result = self.execute_query(
                datasource_id, 
                sql, 
                pagination_mode="cursor",
                per_page=per_page,
                cursor=cursor,
                order_by=order_by
            )
            
            if result.get("status") == "error":
                raise Exception(result.get("error", "Query failed"))
            
            table_data = result.get("table_data", {})
            data = table_data.get("data", [])
            
            if data:
                yield data
            
            if not table_data.get("has_next"):
                break
            
            cursor = table_data.get("next_cursor")
            if not cursor:
                break

    def get_sample_data(self, table_id: int, rows: int = 10) -> Dict:
        url = f"{self.base_url}/api/query/tables/{table_id}/sample/"
        response = requests.get(url, params={"rows": rows}, headers=self._get_headers())
        data = self._handle_response(response)
        return data

