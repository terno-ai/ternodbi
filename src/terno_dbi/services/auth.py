import logging
import secrets
import hashlib
from typing import Tuple, Optional, List
from django.utils import timezone
from terno_dbi.core.models import ServiceToken, DataSource

logger = logging.getLogger(__name__)


def generate_service_token(
    name: str,
    token_type: str = ServiceToken.TokenType.QUERY, 
    created_by=None,
    expires_at=None,
    datasource_ids: Optional[List[int]] = None,
    organisation=None,
    scopes: Optional[List[str]] = None
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
        organisation=organisation,
        is_active=True,
        expires_at=expires_at,
        scopes=scopes or []
    )

    if datasource_ids:
        datasources = DataSource.objects.filter(id__in=datasource_ids)
        token.datasources.set(datasources)
        logger.info(
            "Service token created: name='%s', type='%s', datasources=%s",
            name, token_type, datasource_ids
        )
    else:
        logger.info("Service token created: name='%s', type='%s'", name, token_type)

    return token, full_key


def verify_token(plain_text_key: str) -> Optional[ServiceToken]:
    if not plain_text_key or not plain_text_key.startswith("dbi_"):
        logger.debug("Token verification failed: invalid format")
        return None

    incoming_hash = hashlib.sha256(plain_text_key.encode()).hexdigest()

    try:
        token = ServiceToken.objects.get(key_hash=incoming_hash, is_active=True)

        if token.expires_at and token.expires_at < timezone.now():
            logger.warning("Token verification failed: token '%s' has expired", token.name)
            return None
        
        logger.debug("Token verified successfully: name='%s'", token.name)
        return token
    except ServiceToken.DoesNotExist:
        logger.warning("Token verification failed: token not found or inactive")
        return None


def update_token_usage(token: ServiceToken):
    token.last_used = timezone.now()
    token.save(update_fields=['last_used'])
    logger.debug("Token usage updated: name='%s'", token.name)
