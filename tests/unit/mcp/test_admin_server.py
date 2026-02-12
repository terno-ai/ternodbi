"""
Unit tests for the MCP Admin Server.
"""
import pytest
import json
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
from mcp.types import Tool
from terno_dbi.mcp.admin_server import list_tools, call_tool, run_server, server

class TestAdminServer(unittest.IsolatedAsyncioTestCase):
    
    async def test_list_tools(self):
        """Should list all available admin tools."""
        tools = await list_tools()
        tool_names = [t.name for t in tools]
        
        expected_tools = [
            "rename_table",
            "update_table_description",
            "rename_column",
            "validate_connection",
            "add_datasource",
            "delete_datasource",
            "get_table_info",
            "update_column_description",
            "sync_metadata"
        ]
        
        for name in expected_tools:
            assert name in tool_names
            
        # Verify schema structure for one tool
        rename_tool = next(t for t in tools if t.name == "rename_table")
        assert "table_id" in rename_tool.inputSchema["properties"]
        assert "public_name" in rename_tool.inputSchema["properties"]


    async def test_call_tool_dispatch(self):
        """Should dispatch tool calls to appropriate client methods."""
        with patch('terno_dbi.mcp.admin_server.client') as mock_client:
            # 1. rename_table
            mock_client.update_table.return_value = {"success": True}
            result = await call_tool("rename_table", {"table_id": 1, "public_name": "NewName"})
            mock_client.update_table.assert_called_with(1, public_name="NewName")
            assert "success" in json.loads(result[0].text)

            # 2. add_datasource
            mock_client.create_datasource.return_value = {"id": 10}
            await call_tool("add_datasource", {
                "display_name": "DS", 
                "type": "postgres", 
                "connection_str": "conn"
            })
            mock_client.create_datasource.assert_called_with(
                "DS", "postgres", "conn", None, ""
            )

            # 3. sync_metadata
            mock_client.sync_metadata.return_value = {"synced": True}
            await call_tool("sync_metadata", {"datasource_id": 5, "overwrite": True})
            mock_client.sync_metadata.assert_called_with(5, overwrite=True)

            # 4. update_table_description
            mock_client.update_table.reset_mock()
            mock_client.update_table.return_value = {"ok": True}
            await call_tool("update_table_description", {"table_id": 1, "description": "Desc"})
            mock_client.update_table.assert_called_with(1, description="Desc")

            # 5. rename_column
            mock_client.update_column.return_value = {"ok": True}
            await call_tool("rename_column", {"column_id": 2, "public_name": "Col"})
            mock_client.update_column.assert_called_with(2, public_name="Col")

            # 6. validate_connection
            mock_client.validate_connection.return_value = {"valid": True}
            await call_tool("validate_connection", {"type": "mysql", "connection_str": "uri"})
            mock_client.validate_connection.assert_called()

            # 7. delete_datasource
            mock_client.delete_datasource.return_value = {"deleted": True}
            await call_tool("delete_datasource", {"datasource_id": 9})
            mock_client.delete_datasource.assert_called_with(9)

            # 8. get_table_info
            mock_client.get_table_info.return_value = {}
            await call_tool("get_table_info", {"datasource_id": 1, "table_name": "t"})
            mock_client.get_table_info.assert_called()

            # 9. update_column_description
            mock_client.update_column.reset_mock()
            await call_tool("update_column_description", {"column_id": 3, "description": "D"})
            mock_client.update_column.assert_called_with(3, description="D")


    async def test_call_tool_error(self):
        """Should handle client errors gracefully."""
        with patch('terno_dbi.mcp.admin_server.client') as mock_client:
            mock_client.update_table.side_effect = Exception("API Error")
            
            result = await call_tool("rename_table", {"table_id": 1, "public_name": "X"})
            data = json.loads(result[0].text)
            
            assert "error" in data
            assert data["error"] == "API Error"


    async def test_call_tool_unknown(self):
        """Should return error for unknown tool."""
        result = await call_tool("unknown_tool", {})
        data = json.loads(result[0].text)
        assert "error" in data
        assert "Unknown tool" in data["error"]


    async def test_run_server(self):
        """Should start the MCP server."""
        with patch('terno_dbi.mcp.admin_server.stdio_server') as mock_stdio:
            # Mock the async context manager
            mock_context = AsyncMock()
            mock_stdio.return_value = mock_context
            mock_context.__aenter__.return_value = (MagicMock(), MagicMock())
            
            with patch.object(server, 'run', new_callable=AsyncMock) as mock_run:
                await run_server()
                mock_run.assert_called_once()
