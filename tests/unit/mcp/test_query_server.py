"""
Unit tests for the MCP Query Server.
"""
import pytest
import json
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
from mcp.types import Tool
from terno_dbi.mcp.query_server import list_tools, call_tool, run_server, server

class TestQueryServer(unittest.IsolatedAsyncioTestCase):
    
    async def test_list_tools(self):
        """Should list all available query tools."""
        tools = await list_tools()
        tool_names = [t.name for t in tools]
        
        expected_tools = [
            "list_datasources",
            "list_tables",
            "list_table_columns",
            "execute_query",
            "get_sample_data",
            "list_memories",
            "get_memory",
            "grep_memory"
        ]
        
        for name in expected_tools:
            assert name in tool_names


    async def test_call_tool_dispatch(self):
        """Should dispatch query tools to client."""
        with patch('terno_dbi.mcp.query_server.client') as mock_client:
            # 1. list_datasources
            mock_client.list_datasources.return_value = [{"id": 1}]
            result = await call_tool("list_datasources", {})
            mock_client.list_datasources.assert_called()
            data = json.loads(result[0].text)
            assert data["datasources"][0]["id"] == 1
            assert data["count"] == 1

            # 2. execute_query
            mock_client.execute_query.return_value = {"rows": []}
            await call_tool("execute_query", {
                "datasource": "ds1", 
                "sql": "SELECT 1"
            })
            mock_client.execute_query.assert_called_with(
                "ds1", "SELECT 1", 
                max_rows=None
            )

            # 3. list_tables
            mock_client.list_tables.return_value = ["t1"]
            await call_tool("list_tables", {"datasource": "ds1"})
            mock_client.list_tables.assert_called_with("ds1")

            # 4. list_table_columns
            mock_client.list_table_columns.return_value = ["c1"]
            await call_tool("list_table_columns", {"datasource": "ds1", "table": "t1"})
            mock_client.list_table_columns.assert_called_with("ds1", "t1")

            # 5. get_sample_data
            mock_client.get_sample_data.return_value = []
            await call_tool("get_sample_data", {"table_id": 5, "rows": 10})
            mock_client.get_sample_data.assert_called_with(5, 10)

            # 6. list_memories
            mock_client.list_memories.return_value = [{"name": "test"}]
            await call_tool("list_memories", {"datasource_id": 1})
            mock_client.list_memories.assert_called_with(datasource_id=1)

            # 7. get_memory
            mock_client.get_memory.return_value = {"name": "test"}
            await call_tool("get_memory", {"name": "test", "datasource_id": 1})
            mock_client.get_memory.assert_called_with("test", datasource_id=1)

            # 8. grep_memory
            mock_client.grep_memory.return_value = [{"name": "test"}]
            await call_tool("grep_memory", {"pattern": "search", "datasource_id": 1})
            mock_client.grep_memory.assert_called_with("search", datasource_id=1)


    async def test_call_tool_max_rows(self):
        """Should pass max_rows argument correctly."""
        with patch('terno_dbi.mcp.query_server.client') as mock_client:
            await call_tool("execute_query", {
                "datasource": "ds1", 
                "sql": "SELECT 1",
                "max_rows": 100
            })
            
            mock_client.execute_query.assert_called_with(
                "ds1", "SELECT 1",
                max_rows=100
            )


    async def test_call_tool_error(self):
        """Should return error JSON on exception."""
        with patch('terno_dbi.mcp.query_server.client') as mock_client:
            mock_client.list_tables.side_effect = Exception("DB Down")
            
            result = await call_tool("list_tables", {"datasource": "ds1"})
            data = json.loads(result[0].text)
            assert data["error"] == "DB Down"


    async def test_run_server(self):
        """Should start the query server."""
        with patch('terno_dbi.mcp.query_server.stdio_server') as mock_stdio:
            mock_context = AsyncMock()
            mock_stdio.return_value = mock_context
            mock_context.__aenter__.return_value = (MagicMock(), MagicMock())
            
            with patch.object(server, 'run', new_callable=AsyncMock) as mock_run:
                await run_server()
                mock_run.assert_called_once()
