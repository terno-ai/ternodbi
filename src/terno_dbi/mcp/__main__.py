import argparse
import logging
import sys

logger = logging.getLogger(__name__)

_DESCRIPTION = """\
Run a Terno MCP server.

  query   read-only tools (list tables, run queries, read memory)
  admin   write tools (schema metadata, memory, organisation prompt)
  serve   both registries on one server — what the hosted connector runs

stdio is the default and is unchanged: credentials come from TERNODBI_API_KEY
and never leave the machine.
"""

_HTTP_EPILOG = """\
examples:
  dbi-mcp query                          stdio, read-only (default)
  dbi-mcp admin                          stdio, write access
  dbi-mcp serve                          stdio, both registries
  dbi-mcp serve --transport http         HTTP on 127.0.0.1:8377/mcp

Over HTTP every request must carry `Authorization: Bearer <token>`; there is no
environment-credential fallback, because that would run a stranger's request as
whoever started the process.
"""


def _run_http(server, host: str, port: int) -> None:
    try:
        import uvicorn
    except ImportError:
        print(
            "The HTTP transport needs uvicorn:  pip install 'terno-dbi[http]'",
            file=sys.stderr,
        )
        sys.exit(1)

    from terno_dbi.mcp.http_app import build_asgi_app

    app = build_asgi_app(server, base_url=f"http://{host}:{port}")
    print(f"Terno MCP server on http://{host}:{port}/mcp", file=sys.stderr)
    uvicorn.run(app, host=host, port=port)


def main():
    parser = argparse.ArgumentParser(
        prog="dbi-mcp",
        description=_DESCRIPTION,
        epilog=_HTTP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("server", choices=["query", "admin", "serve"])
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="stdio (default) or http",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--port", type=int, default=8377, help="HTTP bind port")
    args = parser.parse_args()

    logger.info("Starting MCP server: type='%s' transport='%s'", args.server, args.transport)

    if args.transport == "http":
        if args.server == "query":
            from terno_dbi.mcp.query_server import server
        elif args.server == "admin":
            from terno_dbi.mcp.admin_server import server
        else:
            from terno_dbi.mcp.merged_server import server
        _run_http(server, args.host, args.port)
        return

    if args.server == "query":
        from terno_dbi.mcp.query_server import main as run
    elif args.server == "admin":
        from terno_dbi.mcp.admin_server import main as run
    else:
        from terno_dbi.mcp.merged_server import main as run
    run()


if __name__ == "__main__":
    main()
