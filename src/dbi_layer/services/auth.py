
import secrets
import hashlib
from typing import Tuple, Optional
from django.utils import timezone
from dbi_layer.django_app.models import ServiceToken

def generate_service_token(name: str, token_type: str = ServiceToken.TokenType.QUERY, created_by=None) -> Tuple[ServiceToken, str]:
    """
    Generate a new ServiceToken.
    
    Returns:
        (token_obj, plain_text_key)
        
    CRITICAL: plain_text_key is shown ONLY ONCE. It is not stored in DB.
    """
    # 1. Generate 32 bytes of entropy (256 bits)
    random_part = secrets.token_urlsafe(32)
    
    # 2. Format: dbi_{type}_{random}
    # e.g. dbi_admin_... or dbi_query_...
    prefix_type = token_type.lower()
    prefix = f"dbi_{prefix_type}_"
    full_key = f"{prefix}{random_part}"
    
    # 3. Hash for storage
    key_hash = hashlib.sha256(full_key.encode()).hexdigest()
    
    # 4. Create DB Object
    token = ServiceToken.objects.create(
        name=name,
        token_type=token_type,
        key_prefix=prefix, # Store prefix for hints
        key_hash=key_hash,
        created_by=created_by,
        is_active=True
    )
    
    return token, full_key

def verify_token(plain_text_key: str) -> Optional[ServiceToken]:
    """
    Verify a token string against the database.
    Returns the ServiceToken object if valid, None otherwise.
    """
    if not plain_text_key or not plain_text_key.startswith("dbi_"):
        return None
        
    # 1. Hash incoming key
    incoming_hash = hashlib.sha256(plain_text_key.encode()).hexdigest()
    
    # 2. Database Lookup (Exact Match on Hash)
    try:
        # Filter for active tokens only
        token = ServiceToken.objects.get(key_hash=incoming_hash, is_active=True)
        
        # 3. Check Expiry
        if token.expires_at and token.expires_at < timezone.now():
            return None
            
        return token
    except ServiceToken.DoesNotExist:
        return None

def update_token_usage(token: ServiceToken):
    """Update last_used timestamp (best effort/async friendly)."""
    token.last_used = timezone.now()
    token.save(update_fields=['last_used'])
