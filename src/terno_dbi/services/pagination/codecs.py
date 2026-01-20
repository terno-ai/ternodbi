import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, List, Optional
from .types import OrderColumn

# Default cursor TTL: 1 hour (3600 seconds)
DEFAULT_CURSOR_TTL = 3600


class CursorCodec:
    """Encode/decode cursors with HMAC signing and TTL for security."""
    
    def __init__(self, secret_key: str, ttl_seconds: Optional[int] = None):
        """
        Initialize CursorCodec.
        
        Args:
            secret_key: Secret key for HMAC signing
            ttl_seconds: Cursor TTL in seconds (None = no expiration)
        """
        self.secret_key = secret_key
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else DEFAULT_CURSOR_TTL
    
    def encode(self, values: Dict[str, Any], order_by: List[OrderColumn]) -> str:
        """Encode cursor with signature and expiration timestamp."""
        payload = {
            "v": 1,  # Version for future compatibility
            "values": values,
            "order": [(o.column, o.direction) for o in order_by],
        }
        
        # Add expiration timestamp if TTL is set
        if self.ttl_seconds > 0:
            payload["exp"] = int(time.time()) + self.ttl_seconds
        
        json_bytes = json.dumps(payload, default=str).encode()
        signature = hmac.new(
            self.secret_key.encode(), json_bytes, hashlib.sha256
        ).hexdigest()[:16]  # Truncate for brevity
        
        encoded = base64.urlsafe_b64encode(json_bytes).decode()
        return f"{encoded}.{signature}"
    
    def decode(self, cursor: str) -> Dict[str, Any]:
        """Decode and verify cursor signature and expiration."""
        try:
            # Split cursor into payload and signature
            parts = cursor.rsplit(".", 1)
            if len(parts) != 2:
                raise ValueError("Invalid cursor format")
            
            encoded, signature = parts
            json_bytes = base64.urlsafe_b64decode(encoded)
            
            # Verify signature
            expected = hmac.new(
                self.secret_key.encode(), json_bytes, hashlib.sha256
            ).hexdigest()[:16]
            
            if not hmac.compare_digest(signature, expected):
                raise ValueError("Invalid cursor signature")
            
            payload = json.loads(json_bytes)
            
            # Check expiration
            exp = payload.get("exp")
            if exp is not None and exp < time.time():
                raise ValueError("Cursor expired")
            
            return payload
        except ValueError:
            raise  # Re-raise ValueError as-is
        except Exception as e:
            raise ValueError(f"Invalid cursor: {e}")

