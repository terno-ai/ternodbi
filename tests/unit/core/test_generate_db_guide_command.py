import pytest
from io import StringIO

from unittest.mock import patch, Mock
from django.core.management import call_command


@pytest.mark.django_db
@patch(
    "terno_dbi.core.management.commands.generate_db_guide.generate_db_guide"
)
def test_command_calls_generate_db_guide(mock_generate):

    mock_generate.return_value = Mock(id=123)

    datasource_id = 4

    call_command(
        "generate_db_guide",
        str(datasource_id)
    )

    mock_generate.assert_called_once_with(
        datasource_id
    )


@pytest.mark.django_db
@patch(
    "terno_dbi.core.management.commands.generate_db_guide.generate_db_guide"
)
def test_command_prints_success_message(mock_generate):

    mock_generate.return_value = Mock(id=55)

    out = StringIO()

    datasource_id = 4

    call_command(
        "generate_db_guide",
        str(datasource_id),
        stdout=out
    )

    assert "Generated guide 55" in out.getvalue()


@pytest.mark.django_db
@patch(
    "terno_dbi.core.management.commands.generate_db_guide.generate_db_guide"
)
def test_command_returns_correct_guide_id(mock_generate):

    mock_generate.return_value = Mock(id=999)

    out = StringIO()

    call_command(
        "generate_db_guide",
        "4",
        stdout=out
    )

    assert "Generated guide 999" in out.getvalue()