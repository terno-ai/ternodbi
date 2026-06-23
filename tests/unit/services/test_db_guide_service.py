import pytest
from unittest.mock import patch
from terno_dbi.core.models import (
    DataSource,
    Table,
    TableColumn,
    ForeignKey,
    DBGuide,
)
from terno_dbi.services.db_guide_service import (
    collect_datasource_metadata,
    build_compact_generation_context,
    build_guide_prompt,
    save_guide,
    get_db_guide,
    generate_db_guide,
    get_key_columns,
)


class DummyColumn:
    def __init__(
        self,
        name,
        primary_key=False,
        description=None,
        data_type="string",
        public_name=None,
    ):
        self.name = name
        self.primary_key = primary_key
        self.description = description
        self.data_type = data_type
        self.public_name = public_name


def test_get_key_columns():
    cols = [
        DummyColumn("id", primary_key=True),
        DummyColumn("customer_id"),
        DummyColumn("amount"),
        DummyColumn("notes", description="Some notes"),
        DummyColumn("random_text"),
    ]

    result = get_key_columns(cols)

    names = [c["name"] for c in result]

    assert "id" in names
    assert "customer_id" in names
    assert "notes" in names
    assert "amount" in names
    assert "random_text" not in names




@pytest.mark.django_db
def test_collect_datasource_metadata():

    ds = DataSource.objects.create(
        display_name="Test DS",
        type="sqlite"
    )

    table = Table.objects.create(
        data_source=ds,
        name="users"
    )

    TableColumn.objects.create(
        table=table,
        name="id"
    )

    metadata = collect_datasource_metadata(ds.id)

    assert metadata["datasource"].id == ds.id
    assert metadata["tables"].count() == 1
    assert metadata["columns"].count() == 1



@pytest.mark.django_db
def test_build_compact_generation_context():

    ds = DataSource.objects.create(
        display_name="Test DS",
        type="sqlite"
    )

    table = Table.objects.create(
        data_source=ds,
        name="users"
    )

    TableColumn.objects.create(
        table=table,
        name="id"
    )

    metadata = collect_datasource_metadata(ds.id)

    context = build_compact_generation_context(
        metadata
    )

    assert context["table_count"] == 1
    assert context["column_count"] == 1

    assert len(context["important_tables"]) == 1

    assert (
        context["important_tables"][0]["physical_name"]
        == "users"
    )



def test_build_prompt():

    context = {
    "datasource": {
        "id": 1,
        "name": "Test",
        "type": "sqlite",
        "description": "",
        "dialect_name": "sqlite",
        "dialect_version": "3",
    },
    "table_count": 1,
    "column_count": 2,
    "important_tables": [],
    "business_rules": [],
}

    prompt = build_guide_prompt(context)

    assert "Datasource Purpose" in prompt
    assert "Metadata Summary" in prompt
    assert "Key Dimensions" in prompt
    assert "Key Tables" in prompt
    assert "Analyst Notes" in prompt

@pytest.mark.django_db
def test_context_contains_key_columns():

    ds = DataSource.objects.create(
        display_name="Test DS",
        type="sqlite",
    )

    table = Table.objects.create(
        data_source=ds,
        name="customers",
    )

    TableColumn.objects.create(
        table=table,
        name="customer_id",
        primary_key=True,
    )

    metadata = collect_datasource_metadata(ds.id)

    context = build_compact_generation_context(
        metadata
    )

    important_table = context["important_tables"][0]

    assert important_table["physical_name"] == "customers"

    assert len(
        important_table["key_columns"]
    ) == 1

    assert (
        important_table["key_columns"][0]["name"]
        == "customer_id"
    )


@pytest.mark.django_db
@patch("terno_dbi.services.db_guide_service.get_backend_llm")
def test_generate_db_guide_fallback(
    mock_llm
):

    mock_llm.side_effect = Exception(
        "LLM unavailable"
    )

    ds = DataSource.objects.create(
        display_name="Test",
        type="sqlite",
    )

    guide = generate_db_guide(ds.id)

    assert guide.generated_by == "fallback"

    assert "Guide generation failed" in guide.content




@pytest.mark.django_db
def test_save_guide():

    ds = DataSource.objects.create(
        display_name="Test",
        type="sqlite"
    )

    guide = save_guide(
        ds,
        "# Test Guide",
        "pytest"
    )

    assert guide.content == "# Test Guide"
    assert guide.generated_by == "pytest"



@pytest.mark.django_db
def test_get_db_guide():

    ds = DataSource.objects.create(
        display_name="Test",
        type="sqlite"
    )

    save_guide(
        ds,
        "# Guide",
        "pytest"
    )

    guide = get_db_guide(ds.id)

    assert guide is not None
    assert guide.content == "# Guide"




from unittest.mock import patch


@pytest.mark.django_db
@patch(
    "terno_dbi.services.db_guide_service.get_backend_llm"
)
def test_generate_db_guide(mock_llm):

    fake_llm = mock_llm.return_value

    fake_llm.model_name = "fake-model"

    fake_llm.get_simple_response.return_value = (
        "# Generated Guide"
    )

    ds = DataSource.objects.create(
        display_name="Test",
        type="sqlite"
    )

    guide = generate_db_guide(ds.id)

    assert guide.content == "# Generated Guide"
    assert guide.generated_by == "fake-model"



