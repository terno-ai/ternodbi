"""Signals the connector layer emits for host applications to act on.

ternodbi records events; the host (e.g. terno-ai) owns side effects such as
sending email, so ternodbi needs no mail configuration of its own.
"""

from django.dispatch import Signal

# Sent after a ConnectorRequest is committed. kwargs: `request` (the row).
connector_requested = Signal()
