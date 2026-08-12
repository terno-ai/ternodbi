"""OAuth 2.1 support for the hosted MCP connector.

Split from `terno_dbi.core` so the parts that need no Django request cycle —
the scope registry, the discovery documents, DCR validation — stay importable
and testable on their own.
"""
