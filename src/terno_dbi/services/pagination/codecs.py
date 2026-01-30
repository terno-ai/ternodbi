import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, List, Optional
from .types import OrderColumn

logger = logging.getLogger(__name__)

DEFAULT_CURSOR_TTL = 3600


class CursorCodec:
    """Encode/decode cursors with HMAC signing and TTL for security."""

    def __init__(self, secret_key: str, ttl_seconds: Optional[int] = None):
        self.secret_key = secret_key
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else DEFAULT_CURSOR_TTL

    def encode(self, values: Dict[str, Any], order_by: List[OrderColumn]) -> str:
        """Encode cursor with signature and expiration timestamp."""
        payload = {
            "v": 1,
            "values": values,
            "order": [(o.column, o.direction) for o in order_by],
        }

        # Add expiration timestamp if TTL is set
        if self.ttl_seconds > 0:
            payload["exp"] = int(time.time()) + self.ttl_seconds

        json_bytes = json.dumps(payload, default=str).encode()
        signature = hmac.new(
            self.secret_key.encode(), json_bytes, hashlib.sha256
        ).hexdigest()[:16]

        encoded = base64.urlsafe_b64encode(json_bytes).decode()
        logger.debug("Cursor encoded successfully")
        return f"{encoded}.{signature}"

    def decode(self, cursor: str) -> Dict[str, Any]:
        """Decode and verify cursor signature and expiration."""
        try:
            # Split cursor into payload and signature
            parts = cursor.rsplit(".", 1)
            if len(parts) != 2:
                logger.warning("Invalid cursor format: expected 'payload.signature'")
                raise ValueError("Invalid cursor format")

            encoded, signature = parts
            json_bytes = base64.urlsafe_b64decode(encoded)

            expected = hmac.new(
                self.secret_key.encode(), json_bytes, hashlib.sha256
            ).hexdigest()[:16]

            if not hmac.compare_digest(signature, expected):
                logger.warning("Cursor signature verification failed")
                raise ValueError("Invalid cursor signature")

            payload = json.loads(json_bytes)

            exp = payload.get("exp")
            if exp is not None and exp < time.time():
                logger.warning("Cursor has expired")
                raise ValueError("Cursor expired")

            logger.debug("Cursor decoded successfully")
            return payload
        except ValueError:
            raise
        except Exception as e:  # pragma: no cover
            logger.error("Cursor decode error: %s", str(e))
            raise ValueError(f"Invalid cursor: {e}")
