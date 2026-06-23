from unittest.mock import Mock, patch

import pytest
import requests

from terno_dbi.client import TernoDBIClient



@patch("terno_dbi.client.requests.post")
def test_regenerate_db_guide(mock_post):

    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "status": "success",
        "guide_id": 12,
    }

    mock_post.return_value = response

    client = TernoDBIClient(
        base_url="http://testserver"
    )

    result = client.regenerate_db_guide(4)

    mock_post.assert_called_once_with(
        "http://testserver/api/admin/datasources/4/guide/regenerate/",
        headers=client._get_headers(),
    )

    assert result["status"] == "success"
    assert result["guide_id"] == 12




@patch("terno_dbi.client.requests.get")
def test_get_db_guide(mock_get):

    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "status": "success",
        "guide": "# Database Guide"
    }

    mock_get.return_value = response

    client = TernoDBIClient(
        base_url="http://testserver"
    )

    result = client.get_db_guide(4)

    mock_get.assert_called_once_with(
        "http://testserver/api/query/datasources/4/guide/",
        headers=client._get_headers(),
    )

    assert result["status"] == "success"
    assert result["guide"] == "# Database Guide"




@patch("terno_dbi.client.requests.get")
def test_get_db_guide_api_error(mock_get):

    response = Mock()

    response.raise_for_status.side_effect = (
        requests.exceptions.HTTPError("404")
    )

    response.json.return_value = {
        "error": "Guide not found"
    }

    mock_get.return_value = response

    client = TernoDBIClient(
        base_url="http://testserver"
    )

    with pytest.raises(Exception) as exc:
        client.get_db_guide(999)

    assert "Guide not found" in str(exc.value)