
import secrets
import hashlib
from typing import Tuple, Optional, List
from django.utils import timezone
from terno_dbi.core.models import ServiceToken, DataSource


def generate_service_token(
    name: str, 
    token_type: str = ServiceToken.TokenType.QUERY, 
    created_by=None,
    expires_at=None,
    datasource_ids: Optional[List[int]] = None
) -> Tuple[ServiceToken, str]:
    random_part = secrets.token_urlsafe(32)
    prefix_type = token_type.lower()
    prefix = f"dbi_{prefix_type}_"
    full_key = f"{prefix}{random_part}"

    key_hash = hashlib.sha256(full_key.encode()).hexdigest()

    token = ServiceToken.objects.create(
        name=name,
        token_type=token_type,
        key_prefix=prefix,
        key_hash=key_hash,
        created_by=created_by,
        is_active=True,
        expires_at=expires_at
    )

    if datasource_ids:
        datasources = DataSource.objects.filter(id__in=datasource_ids)
        token.datasources.set(datasources)

    return token, full_key


def verify_token(plain_text_key: str) -> Optional[ServiceToken]:
    if not plain_text_key or not plain_text_key.startswith("dbi_"):
        return None

    incoming_hash = hashlib.sha256(plain_text_key.encode()).hexdigest()

    try:
        token = ServiceToken.objects.get(key_hash=incoming_hash, is_active=True)

        if token.expires_at and token.expires_at < timezone.now():
            return None
        return token
    except ServiceToken.DoesNotExist:
        return None


def update_token_usage(token: ServiceToken):
    token.last_used = timezone.now()
    token.save(update_fields=['last_used'])
