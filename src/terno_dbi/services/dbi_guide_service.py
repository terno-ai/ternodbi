import json

from django.utils import timezone
# from terno_dbi.llm.base import LLMFactory
# from terno.models import LLMConfiguration as TernoLLMConfiguration

import logging

logger = logging.getLogger(__name__)


from terno_dbi.core.models import (
    DataSource,
    Table,
    TableColumn,
    ForeignKey,
    PromptExample,
    DBIGuide,
)




def get_backend_llm():
    from terno.models import LLMConfiguration
    from terno.llm.openai import OpenAILLM
    config = (
        LLMConfiguration.objects
        .filter(enabled=True)
        .first()
    )

    if not config:
        raise Exception(
            "No enabled backend LLM configuration found"
        )

    return OpenAILLM(
        api_key=config.api_key,
        model_name=config.model_name,
        temperature=config.temperature,
        max_tokens=config.max_tokens,
        top_p=config.top_p,
    )

# ------------------------------------------------------------
# Metadata Collection
# ------------------------------------------------------------

def collect_datasource_metadata(datasource_id):
    """
    Collect all metadata needed to generate a DBI Guide.
    """

    datasource = DataSource.objects.get(id=datasource_id)

    tables = Table.objects.filter(
        data_source=datasource,
        is_hidden=False
    )

    columns = TableColumn.objects.filter(
        table__data_source=datasource,
        is_hidden=False
    )

    foreign_keys = ForeignKey.objects.filter(
        constrained_table__data_source=datasource
    )

    prompt_examples = PromptExample.objects.filter(
        organisation=datasource.organisation
    )

    return {
        "datasource": datasource,
        "tables": tables,
        "columns": columns,
        "foreign_keys": foreign_keys,
        "prompt_examples": prompt_examples,
    }


# ------------------------------------------------------------
# Key Column Extraction
# WE ARE NOT USING THIS SECTION
# ------------------------------------------------------------

KEY_COLUMN_PATTERNS = (
    "_id",
    "_key",
    "customer",
    "product",
    "sku",
    "brand",
    "region",
    "country",
    "state",
    "city",
    "vendor",
    "supplier",
    "store",
    "employee",
    "date",
    "created",
    "updated",
    "amount",
    "revenue",
    "cost",
    "price",
    "quantity",
)


def get_key_columns(columns):
    """
    Return only columns that help explain the table.
    """

    important = []

    for column in columns:

        name = (column.name or "").lower()

        if (
            column.primary_key
            or column.description
            or any(
                pattern in name
                for pattern in KEY_COLUMN_PATTERNS
            )
        ):
            important.append(
                {
                    "name": column.name,
                    "public_name": column.public_name,
                    "data_type": column.data_type,
                    "description": column.description,
                    "primary_key": column.primary_key,
                }
            )
    return important


# ------------------------------------------------------------
# Relationship Inference
# ------------------------------------------------------------

def infer_relationships(tables_qs, columns_qs):

    relationships = []

    primary_keys = {}

    #
    # Collect PKs
    #
    for column in columns_qs:

        if column.primary_key:

            primary_keys[
                column.name.lower()
            ] = column.table

    #
    # Match PK names in other tables
    #
    for column in columns_qs:

        name = (column.name or "").lower()

        if name not in primary_keys:
            continue

        parent_table = primary_keys[name]

        if column.table == parent_table:
            continue

        relationships.append(
            {
                "from_table":
                    column.table.name,

                "from_column":
                    column.name,

                "to_table":
                    parent_table.name,

                "to_column":
                    name,

                "confidence":
                    "medium",
            }
        )

    return relationships


# ------------------------------------------------------------
# Business Rules
# ------------------------------------------------------------

def extract_business_rules(prompt_examples_qs):
    """
    Convert prompt examples into business rules.
    """

    rules = []

    for example in prompt_examples_qs:

        rules.append(
            {
                "key": example.key,
                "rule": example.value,
            }
        )

    return rules


# ------------------------------------------------------------
# Compact Context Builder
# ------------------------------------------------------------

def build_compact_generation_context(metadata):
    """
    Compact context for DBI Guide generation.

    Goal:
    - Keep prompt size reasonable.
    - Focus on business meaning.
    - Avoid sending thousands of columns.
    """

    datasource = metadata["datasource"]
    tables_qs = metadata["tables"]
    columns_qs = metadata["columns"]
    prompt_examples_qs = metadata["prompt_examples"]

    important_tables = []

    for table in tables_qs:

        table_columns = columns_qs.filter(table=table)

        key_columns = get_key_columns(table_columns)

        important_tables.append(
            {
                "name": table.public_name or table.name,
                "physical_name": table.name,
                "description": table.description,
                "notes": table.notes,
                "estimated_row_count": table.estimated_row_count,
                "key_columns": key_columns[:10],
            }
        )
 
    
    #
    # BUSINESS RULES
    #
    business_rules = []

    for example in prompt_examples_qs:

        business_rules.append(
            {
                "key": (example.key or "")[:150],
                "rule": (example.value or "")[:300],
            }
        )

    return {
    "datasource": {
        "name": datasource.display_name,
        "description": datasource.description,
    },

    "table_count": tables_qs.count(),

    "important_tables": [
        {
            "name": table.public_name or table.name,
            "description": (table.description or "")[:150],
            "key_columns": key_columns[:5],
        }
        for table in selected_tables
    ]
}

# ------------------------------------------------------------
# Prompt Builder
# ------------------------------------------------------------

def build_guide_prompt(context):

    return f"""
You are a senior analytics engineer.

Generate a concise DBI Guide for AI agents.

The agent already has access to:
- schema metadata
- table names
- column names
- relationships

DO NOT document every table or column.

DO NOT create a data dictionary.

Focus only on business meaning and analytical guidance.

Datasource:
{json.dumps(context["datasource"], indent=2)}

Metadata Summary:
{json.dumps({
    "table_count": context["table_count"],
    "column_count": context["column_count"],
}, indent=2)}

Important Tables:
{json.dumps(context["important_tables"], indent=2)}

Output ONLY markdown.

# Database Guide

## Datasource Purpose

Describe:
- what business domain this datasource supports
- what business process it represents
- who typically uses it

## Metadata Summary

Provide:
- table count
- column count
- major subject areas represented

## Key Dimensions

Only include the most important business dimensions.

For each dimension:
- business meaning
- primary identifier/key
- why analysts use it

## Key Tables

Only include the most important tables.

For each table provide:
- business purpose
- why it matters
- key columns only

Limit to the top 5-15 most important tables.

## Analyst Notes

Provide:
- business rules
- common pitfalls
- recommended usage patterns
"""

# ------------------------------------------------------------
# Persistence
# ------------------------------------------------------------

def save_guide(datasource, markdown, generated_by="llm"):

    guide, _ = DBIGuide.objects.update_or_create(
        datasource=datasource,
        defaults={
            "content": markdown,
            "generated_by": generated_by,
            "metadata_snapshot_at": timezone.now(),
            "is_stale": False,
        }
    )

    return guide


# ------------------------------------------------------------
# Main Entry Point
# ------------------------------------------------------------

def generate_dbi_guide(datasource_id):

    logger.info(
        "Starting DBI guide generation for datasource=%s",
        datasource_id
    )

    metadata = collect_datasource_metadata(
        datasource_id
    )

    context = build_compact_generation_context(
        metadata
    )

    prompt = build_guide_prompt(
        context
    )

    logger.info(
        "Prompt built. chars=%s tables=%s important_tables=%s rules=%s",
        len(prompt),
        context["table_count"],
        len(context["important_tables"]),
        len(context["business_rules"])
    )

    datasource = metadata["datasource"]

    try:
        llm = get_backend_llm()
        logger.info(
            "Calling LLM model=%s",
            llm.model_name
        )

        markdown = llm.get_simple_response(
            prompt
        )
        logger.info(
            "LLM returned response. markdown_length=%s",
            len(markdown) if markdown else 0
        )

        generated_by = llm.model_name

    except Exception as exc:

        logger.exception(
            "DBI Guide generation failed"
        )

        markdown = f"""
        # Database Guide

        ## Datasource Purpose

        Datasource: {datasource.display_name}

        ## Metadata Summary

        - Tables: {context["table_count"]}
        - Important Tables: {len(context["important_tables"])}

        Guide generation failed:
        {str(exc)}
        """

        generated_by = "fallback"

    if not markdown or len(markdown.strip()) < 1:
        raise ValueError(
            "Guide generation returned empty response"
        )
    
    logger.info(
        "Saving DBI guide for datasource=%s",
        datasource.id
    )

    guide = save_guide(
        datasource=datasource,
        markdown=markdown,
        generated_by=generated_by,
    )

    logger.info(
        "DBI guide saved. guide_id=%s content_length=%s",
        guide.id,
        len(guide.content)
    )

    return guide


def generate_guide_markdown(context):

        return f"""
    # Database Guide

    ## Datasource Purpose

    Datasource: {context["datasource"]["name"]}

    ## Metadata Summary

    - Tables: {context["table_count"]}
    - Important Tables: {len(context["important_tables"])}
    """

def get_dbi_guide(datasource_id):
    """
    Retrieve latest DBI Guide for datasource.
    """

    try:
        return DBIGuide.objects.get(
            datasource_id=datasource_id
        )
    except DBIGuide.DoesNotExist:
        return None



