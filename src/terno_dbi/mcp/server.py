import argparse
import asyncio
import json
import sys
import os
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from terno_dbi.connectors import ConnectorFactory, BaseConnector

_connector: BaseConnector = None
_config: Dict[str, Any] = {}


def initialize(db_type: str, db_url: str, credentials: Dict = None, **kwargs):
    global _connector, _config
    _config = kwargs
    _connector = ConnectorFactory.create_connector(db_type, db_url, credentials)
    dialect_name, dialect_version = _connector.get_dialect_info()
    print(f"Connected to database: {dialect_name} {dialect_version}", file=sys.stderr)


server = Server("dbi-layer")


@server.list_tools()
async def list_tools() -> List[Tool]:
    return [
        Tool(
            name="list_tables",
            description="List all tables in the connected database",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Optional schema name to filter tables"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_table_info",
            description="Get detailed information about a table including columns and types",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="execute_query",
            description="Execute a SQL SELECT query on the database",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return (default: 100)"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_sample_data",
            description="Get sample rows from a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    },
                    "rows": {
                        "type": "integer",
                        "description": "Number of sample rows (default: 10)"
                    }
                },
                "required": ["table_name"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    global _connector

    if _connector is None:
        return [TextContent(type="text", text="Error: Database not initialized")]

    try:
        result = None

        if name == "list_tables":
            mdb = _connector.get_metadata()
            tables = list(mdb.tables.keys())
            result = {"tables": tables, "count": len(tables)}

        elif name == "get_table_info":
            table_name = arguments["table_name"]
            mdb = _connector.get_metadata()

            if table_name not in mdb.tables:
                result = {"error": f"Table '{table_name}' not found"}
            else:
                table = mdb.tables[table_name]
                columns = []
                for col_name, col in table.columns.items():
                    columns.append({
                        "name": col_name,
                        "type": str(col.type),
                    })
                result = {
                    "table": table_name,
                    "columns": columns,
                    "column_count": len(columns)
                }

        elif name == "execute_query":
            sql = arguments["sql"]
            limit = arguments.get("limit", 100)

            sql_lower = sql.lower().strip()
            if "limit" not in sql_lower:
                sql = f"{sql} LIMIT {limit}"

            import sqlalchemy
            with _connector.get_connection() as conn:
                result_proxy = conn.execute(sqlalchemy.text(sql))
                rows = result_proxy.fetchall()
                columns = list(result_proxy.keys())

                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))

                result = {
                    "columns": columns,
                    "data": data,
                    "row_count": len(data)
                }

        elif name == "get_sample_data":
            table_name = arguments["table_name"]
            rows = arguments.get("rows", 10)

            import sqlalchemy
            sql = f"SELECT * FROM {table_name} LIMIT {rows}"

            with _connector.get_connection() as conn:
                result_proxy = conn.execute(sqlalchemy.text(sql))
                fetch_rows = result_proxy.fetchall()
                columns = list(result_proxy.keys())

                data = []
                for row in fetch_rows:
                    data.append(dict(zip(columns, row)))

                result = {
                    "table": table_name,
                    "columns": columns,
                    "data": data,
                    "row_count": len(data)
                }

        else:
            result = {"error": f"Unknown tool: {name}"}

        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def run_server(db_type: str, db_url: str, credentials: Dict = None):
    print("Starting DBI Layer MCP Server", file=sys.stderr)
    initialize(db_type, db_url, credentials)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    parser = argparse.ArgumentParser(description="DBI Layer MCP Server")
    parser.add_argument(
        default=os.environ.get("DBI_DB_TYPE", "sqlite"),
        help="Database type (postgres, mysql, sqlite, bigquery, snowflake)"
    )
    parser.add_argument(
        default=os.environ.get("DBI_DATABASE_URL", "sqlite:///test.db"),
        help="Database connection URL"
    )
    parser.add_argument(
        help="Path to JSON credentials file (for BigQuery)"
    )

    args = parser.parse_args()

    credentials = None
    if args.credentials_file:
        with open(args.credentials_file) as f:
            credentials = json.load(f)

    asyncio.run(run_server(args.db_type, args.db_url, credentials))


if __name__ == "__main__":
    main()
