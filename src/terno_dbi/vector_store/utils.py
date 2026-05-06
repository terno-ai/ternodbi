from pymilvus import MilvusClient, DataType, FieldSchema, CollectionSchema
from terno_dbi.core import conf
from terno_dbi.llm.base import LLMFactory
from terno_dbi.core.models import PromptExample, CoreOrganisation
from django.db import transaction
import json
import re


def get_milvus_client():
    milvus_uri = conf.get("MILVUS_URI")
    if not milvus_uri:
        print("[MILVUS] Using LOCAL Milvus Lite DB")
        return MilvusClient(uri="default_vector_DB.db")
    else:
        print(f"[MILVUS] Using remote: {milvus_uri}")
        return MilvusClient(uri=milvus_uri)


def get_or_create_example_collection():
    COLLECTION_NAME = "query_example"
    milvus_client = get_milvus_client()
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="key", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="value", dtype=DataType.VARCHAR, max_length=4096),
        FieldSchema(name="example_type", dtype=DataType.VARCHAR, max_length=512, is_primary=False, default=None),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536),
        FieldSchema(name="org_id", dtype=DataType.INT64, is_primary=False, default=None),
    ]
    schema = CollectionSchema(fields, description="Prompt Example Vectors")
    existing = milvus_client.list_collections()
    if COLLECTION_NAME not in existing:
        print(f"Creating collection '{COLLECTION_NAME}' ...")
        milvus_client.create_collection(
            collection_name=COLLECTION_NAME,
            schema=schema
        )
    else:
        print(f"Collection '{COLLECTION_NAME}' already exists.")

    indexes = milvus_client.list_indexes(collection_name=COLLECTION_NAME)
    if not indexes:
        index_params = milvus_client.prepare_index_params()
        index_params.add_index(
            field_name="embedding",
            index_type="IVF_FLAT",
            metric_type="COSINE",
            params={"nlist": 1024}
        )
        print(f"Creating index on '{COLLECTION_NAME}.embedding' ...")
        milvus_client.create_index(
            collection_name=COLLECTION_NAME,
            index_params=index_params
        )
    else:
        print(f"Index already exists for '{COLLECTION_NAME}': {indexes}")

    milvus_client.load_collection(collection_name=COLLECTION_NAME)

    collection = milvus_client.describe_collection(collection_name=COLLECTION_NAME)
    return collection


def extract_examples_from_conversation(org, conversation, llm=None):
    if llm is None:
        llm = LLMFactory.create_llm(org)

    prompt = f"""
You are an expert system that extracts reusable prompt examples.

Prompt examples are the key value pairs for information or internal logics. These prompt examples are fetched using the key and added to the user’s question for further use as few-shot examples. These prompt examples are stored in a vector store and are fetched using the key. So, for a recommended prompt example, we need a key-value pair.
we also have example_type field to differentiate different types of examples, for example, "query_sql" for SQL query examples the value contains pure or pseudo python and SQLcode and "question_plan" for planning examples the value contains the logical reasoning and internal logic this may conatin filtering logic, tables and columns names even FK relationships.

You may get learnings from:
- From the user's question, there might be inputs
- From the different approaches tried, to find the answer

Rules:
- Keep key short and natural (user query) as key will be matched with user's query
- Value should be clean SQL only
- Ignore explanations
- For updating existing prompt example provided to assistant just add a new one, we have deduplicate implemented by default

Return JSON list:
[
  {{
    "key": "...",
    "value": "...",
    "example_type": "..." ("query_sql" or "question_plan" according to value content)
  }}
]

From the conversation below, extract HIGH QUALITY examples. You can create as many prompt example as you want.

Conversation:
{conversation}
"""

    response = llm.get_simple_response(prompt)

    try:
        response = re.sub(r"```json|```", "", response).strip()
        return json.loads(response)
    except:
        return []


def compress_examples(organisation, new_example: dict, similar_examples, llm=None):

    if not similar_examples:
        return [new_example]

    if llm is None:
        llm = LLMFactory.create_llm(organisation)

    examples_text = "\n\n".join([
        f"KEY: {e['key']}\nVALUE: {e['value']}"
        for e in similar_examples
    ])

    prompt = f"""
You are expert in optimizing prompt examples. prompt examples are the key value pairs for information or internal logics. These prompt examples are stored and fetched using the key and the corresponding values are added to the user’s question for further use as few-shot examples.
we also have example_type field to differentiate different types of examples, for example, "query_sql" for SQL query examples the value contains pure or pseudo python and SQLcode and "question_plan" for planning examples the value contains the logical reasoning and internal logic this may conatin filtering logic, tables and columns names even FK relationships.

CONTEXT:
- Keys are used for semantic search (embedding match)
- Values are used as execution examples
- Example Type is used for categorization and filtering during retrieval

STRICT RULES:
- Always return at least 1 example
- NEVER return empty list
- Merge nearly identical examples into one with combined information(key and value both). Don't just pick one and drop others.
- Values should be concise and clean
- Keep minimum number of examples
- Preserve all unique information in values
- You may merge examples with different example type and assign correct example type to merged example based on value content.

Existing examples:
{examples_text}

New example:
KEY: {new_example["key"]}
VALUE:{new_example["value"]}
EXAMPLE_TYPE:{new_example["example_type"]}

Return JSON list:
[
  {{
    "key": "...",
    "value": "...",
    "example_type": "..." ("query_sql" or "question_plan" according to value content)
  }}
]

Note: You can create new keys as well if you think it can better represent the cluster of examples. Just make sure to keep the key short and natural as it will be matched with user's query in future.
"""

    try:
        response = llm.get_simple_response(prompt)
        # clean markdown if LLM returns ```json
        response = re.sub(r"```json|```", "", response).strip()
        return json.loads(response)
    except Exception:
        return []


def deduplicate_and_store(id, key, embedding, value, example_type, org_id, llm=None):
    print(f"[DEDUP] Start for ID={id}")

    org = CoreOrganisation.objects.get(id=org_id)
    if llm is None:
        llm = LLMFactory.create_llm(org)

    threshold = 0.85

    # Step 1: find cluster (ALL similar examples)
    similar = find_similar_examples(embedding, org_id, ["query_sql", "question_plan"], threshold, limit=5)

    print(f"[DEDUP] Raw similar: {similar}")

    if not similar:
        print(f"[DEDUP] No similar found → inserting ID={id}")
        insert_example_vector(
            id=id,
            key=key,
            embedding=embedding,
            value=value,
            example_type=example_type,
            org_id=org_id,
        )
        return

    # Step 2: collect all IDs in cluster
    cluster_ids = list(set([
        e["id"] for e in similar if "id" in e
    ] + [id]))

    print(f"[DEDUP] Cluster IDs: {cluster_ids}")

    # Step 3: fetch all cluster records from DB
    cluster_examples = list(
        PromptExample.objects.filter(id__in=cluster_ids)
    )

    print(f"[DEDUP] Cluster size (DB): {len(cluster_examples)}")

    # Step 4: build compression input
    examples_for_compression = [
        {"key": e.key, "value": e.value, "example_type": e.example_type}
        for e in cluster_examples
    ]

    # include current input
    new_example = {"key": key, "value": value, "example_type": example_type}
    cluster_ids = list(set(cluster_ids + [id]))

    # Step 5: compress entire cluster
    new_examples = compress_examples(org, new_example, examples_for_compression, llm=llm)
    print(f"[DEDUP] Compressed examples: {new_examples}")

    # SAFETY CHECK
    if not new_examples:
        print("[DEDUP] Compression failed → aborting delete")
        return

    # Step 6: delete ENTIRE cluster from DB + Milvus
    with transaction.atomic():
        print(f"[DEDUP] Deleting full cluster: {cluster_ids}")
        PromptExample.objects.filter(id__in=cluster_ids).delete()
        delete_from_milvus(cluster_ids)

        # Step 7: insert new canonical examples
        for ex in new_examples:
            print(f"[DEDUP] Inserting new canonical example: {ex}")

            new_obj = PromptExample(
                organisation=org,
                example_type=ex["example_type"],
                key=ex["key"],
                value=ex["value"]
            )

            new_obj._skip_signal = True
            new_obj.save()

            new_embedding = llm.generate_vector(ex["key"])

            insert_example_vector(
                id=new_obj.id,
                key=new_obj.key,
                embedding=new_embedding,
                value=new_obj.value,
                example_type=new_obj.example_type,
                org_id=org_id,
            )


def find_similar_examples(embedding, org_id, example_types, threshold=0.75, limit=3):
    print(f"[MILVUS SEARCH] Running search for org_id={org_id}")
    client = get_milvus_client()
    collection = "query_example"
    type_conditions = " || ".join([f'example_type == "{t}"' for t in example_types])
    expr = f'org_id == {org_id} && ({type_conditions})'

    results = client.search(
        collection_name=collection,
        data=[embedding],
        anns_field="embedding",
        metric_type="COSINE",
        limit=limit,
        filter=expr,
        output_fields=["id", "key", "value", "example_type", "org_id"],
    )
    print(f"[MILVUS SEARCH] Raw results count: {len(results[0])}")

    matches = []
    print(f"Raw search results for examples: {results}")

    for i, hit in enumerate(results[0]):
        print(hit["distance"], hit["entity"]["key"], hit["entity"]["value"])
        if hit["distance"] > threshold:
            matches.append({
                "id": hit["id"],
                "key": hit["entity"]["key"],
                "value": hit["entity"]["value"],
                "example_type": hit["entity"]["example_type"],
                "similarity": hit["distance"]
            })

    matches = sorted(matches, key=lambda x: x["similarity"], reverse=True)
    print(f"Final sorted example matches: {[m['key'][:30] for m in matches]}")

    return matches


def insert_example_vector(id, key, embedding, value, example_type, org_id):
    client = get_milvus_client()
    collection = "query_example"

    # delete old if exists (Milvus doesn't auto-upsert)
    try:
        client.delete(
            collection_name=collection,
            ids=[id]
        )
    except Exception:
        pass

    client.insert(
        collection_name=collection,
        data=[{
            "id": id,
            "key": key,
            "value": value,
            "embedding": embedding,
            "example_type": example_type,
            "org_id": org_id
        }]
    )


def sync_prompt_example(example: PromptExample, llm=None):
    """
    Insert/update in Milvus
    """
    print(f"[SYNC] Triggered for ID={example.id}, KEY={example.key}")
    if llm is None:
        llm = LLMFactory.create_llm(example.organisation)
    embedding = llm.generate_vector(example.key)
    print("[SYNC] Embedding generated, calling dedup...")
    deduplicate_and_store(
        id=example.id,
        key=example.key,
        value=example.value,
        embedding=embedding,
        example_type=example.example_type,
        org_id=example.organisation_id,
        llm=llm
    )


def delete_from_milvus(ids):
    client = get_milvus_client()

    print(f"[MILVUS DELETE] Deleting IDs: {ids}")

    client.delete(
        collection_name="query_example",
        ids=ids
    )
