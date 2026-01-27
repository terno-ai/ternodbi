"""
This module provides a unified way to resolve datasources using either
their numeric ID or display name, enabling the hybrid lookup pattern.
"""

from typing import Union
from django.http import Http404

from terno_dbi.core.models import DataSource


def resolve_datasource(identifier: Union[int, str], enabled_only: bool = True) -> DataSource:
    """
    Resolve a datasource by ID (int) or display_name (str).

    Args:
        identifier: Either an integer ID or string display_name.
        enabled_only: If True, only return enabled datasources.

    Returns:
        DataSource object.

    Raises:
        Http404: If datasource not found.

    Examples:
        >>> resolve_datasource(1)  # By ID
        >>> resolve_datasource("my_postgres")  # By name
        >>> resolve_datasource("123")  # String that looks like ID - tries ID first
    """
    # Build base queryset
    qs = DataSource.objects.all()
    if enabled_only:
        qs = qs.filter(enabled=True)

    try:
        ds_id = int(identifier)
        try:
            return qs.get(id=ds_id)
        except DataSource.DoesNotExist:
            raise Http404(f"DataSource with ID {ds_id} not found")
    except (ValueError, TypeError):
        pass

    try:
        return qs.get(display_name=identifier)
    except DataSource.DoesNotExist:
        raise Http404(f"DataSource '{identifier}' not found")
    except DataSource.MultipleObjectsReturned:
        raise Http404(f"Multiple datasources found with name '{identifier}'. Please use ID instead.")


def get_datasource_id(identifier: Union[int, str], enabled_only: bool = True) -> int:
    ds = resolve_datasource(identifier, enabled_only=enabled_only)
    return ds.id
