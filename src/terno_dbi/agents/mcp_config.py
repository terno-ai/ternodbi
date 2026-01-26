import os
import sys
from typing import List
from mcp.client.stdio import StdioServerParameters


def get_default_server_params() -> List[StdioServerParameters]:
    """
    Get the default MCP server configuration for TernoDBI.
    configures connection to the local 'ternodbi-query' and 'ternodbi-admin' servers.
    """

    # use the current python executable to ensure we use the same environment
    python_executable = sys.executable

    query_env = os.environ.copy()
    if os.environ.get("TERNODBI_QUERY_KEY"):
        query_env["TERNODBI_API_KEY"] = os.environ["TERNODBI_QUERY_KEY"]

    query_server_params = StdioServerParameters(
        command=python_executable,
        args=["-m", "terno_dbi.mcp.query_server"],
        env=query_env
    )

    admin_env = os.environ.copy()
    if os.environ.get("TERNODBI_ADMIN_KEY"):
        admin_env["TERNODBI_API_KEY"] = os.environ["TERNODBI_ADMIN_KEY"]

    admin_server_params = StdioServerParameters(
        command=python_executable,
        args=["-m", "terno_dbi.mcp.admin_server"],
        env=admin_env
    )

    return [query_server_params, admin_server_params]
