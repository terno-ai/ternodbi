import logging
import os
import sys
from typing import List
from mcp.client.stdio import StdioServerParameters

logger = logging.getLogger(__name__)


def get_default_server_params() -> List[StdioServerParameters]:
    """
    Get the default MCP server configuration for TernoDBI.
    configures connection to the local 'ternodbi-query' and 'ternodbi-admin' servers.
    """

    # use the current python executable to ensure we use the same environment
    python_executable = sys.executable
    logger.debug("Using Python executable: %s", python_executable)

    query_env = os.environ.copy()
    if os.environ.get("TERNODBI_QUERY_KEY"):
        query_env["TERNODBI_API_KEY"] = os.environ["TERNODBI_QUERY_KEY"]
        logger.debug("Query server API key configured from TERNODBI_QUERY_KEY")
    else:
        logger.warning("TERNODBI_QUERY_KEY not set - query server may require authentication")

    query_server_params = StdioServerParameters(
        command=python_executable,
        args=["-m", "terno_dbi.mcp.query_server"],
        env=query_env
    )

    admin_env = os.environ.copy()
    if os.environ.get("TERNODBI_ADMIN_KEY"):
        admin_env["TERNODBI_API_KEY"] = os.environ["TERNODBI_ADMIN_KEY"]
        logger.debug("Admin server API key configured from TERNODBI_ADMIN_KEY")
    else:
        logger.warning("TERNODBI_ADMIN_KEY not set - admin server may require authentication")

    admin_server_params = StdioServerParameters(
        command=python_executable,
        args=["-m", "terno_dbi.mcp.admin_server"],
        env=admin_env
    )

    logger.info("MCP server parameters configured: query + admin")
    return [query_server_params, admin_server_params]
