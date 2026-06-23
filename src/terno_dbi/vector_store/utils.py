from pymilvus import MilvusClient, DataType, FieldSchema, CollectionSchema
from terno_dbi.core import conf
from terno_dbi.core.models import PromptExample, CoreOrganisation
from django.db import transaction
import json
import logging
import re

logger = logging.getLogger(__name__)


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
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536),
        FieldSchema(name="org_id", dtype=DataType.INT64, is_primary=False, default=None),
        FieldSchema(name="user_id", dtype=DataType.INT64, is_primary=False, default=0),
        FieldSchema(name="is_shared", dtype=DataType.BOOL, is_primary=False, default=False),
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


def extract_examples_from_conversation(org, conversation, llm):

    # Filter to only user messages — we only extract from user-provided information
    user_messages = [msg for msg in conversation if msg.get("role") == "user"]
    if not user_messages:
        return []

    user_text = "\n\n".join([msg["content"] for msg in user_messages])

    prompt = f"""
You are an expert system that extracts reusable domain knowledge from user-provided information.

Your job is to identify valuable DOMAIN KNOWLEDGE, BUSINESS RULES, TERMINOLOGY, and INTERNAL LOGIC that the user has explicitly shared in their messages. These will be stored as question-answer pairs (key-value) and used as few-shot examples to help the AI answer similar future questions.

IMPORTANT RULES:
- ONLY extract information explicitly stated by the user in their messages
- DO NOT extract SQL queries, code, or agent-generated content
- DO NOT extract generic questions that don't contain domain-specific knowledge
- Key should be a generalized question pattern, not the specific question asked. Replace any specific values (dates, names, numbers, metric names) with generic placeholders.
- If the value contains only generic conceptual knowledge (definitions, common formulas) that any LLM already knows without domain context, skip that example entirely and do not include it.
- Focus on: business terminology, internal naming conventions, calculation logic, filtering rules, entity relationships, data interpretation rules
- If the user hasn't provided any meaningful domain knowledge, return an empty list []

Return JSON list:
[
  {{
    "key": "...",
    "value": "..."
  }}
]

User messages:
{user_text}
"""

    response = llm.get_simple_response(prompt)

    try:
        response = re.sub(r"```json|```", "", response).strip()
        return json.loads(response)
    except:
        return []


def compress_examples(organisation, new_example: dict, similar_examples, llm):

    if not similar_examples:
        return [new_example]

    examples_text = "\n\n".join([
        f"KEY: {e['key']}\nVALUE: {e['value']}"
        for e in similar_examples
    ])

    prompt = f"""
You are an expert at consolidating prompt examples. Each example is a question-answer pair (key-value) storing domain knowledge, business rules, or internal logic. Keys are embedded and semantically matched against future user queries; values are injected as few-shot context.

You are given a CLUSTER of semantically similar examples (existing + one new). Consolidate them into the smallest set of non-redundant examples.

MERGING CRITERIA:
- Merge examples that describe the SAME domain context — i.e. the same dataset, entity, or metric — even if they phrase different sub-questions about it. Combine their values into one richer answer and write a single key that covers the combined knowledge.
- Keep examples SEPARATE only when they concern UNRELATED domains/metrics (e.g. a Nielsen sales-value rule vs a fill-rate rule).
- When merging, preserve every unique, non-conflicting detail from each value. If the new example contradicts an old fact, the new one wins.

KEY:
- Write each key as a natural question a user might actually ask (it is embedding-matched against user queries).
VALUE:
- Concise, clean, factual.

Existing examples:
{examples_text}

New example:
KEY: {new_example["key"]}
VALUE: {new_example["value"]}

Return JSON list:
[
  {{
    "key": "...",
    "value": "..."
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


def deduplicate_and_store(id, key, embedding, value, org_id, user_id, is_shared, llm):
    logger.debug("[DEDUP] Start for ID=%s", id)

    org = CoreOrganisation.objects.get(id=org_id)

    threshold = 0.80

    # Step 1: find similar examples owned by same user (not org-shared ones)
    similar = find_similar_examples(
        embedding, org_id, user_id=user_id,
        threshold=threshold, limit=5
    )

    print(f"[DEDUP] Raw similar: {similar}")

    # Exclude self from similar results — an example is always similar to itself
    similar = [s for s in similar if s["id"] != id]

    if not similar:
        print(f"[DEDUP] No other similar found → inserting/updating ID={id}")
        insert_example_vector(
            id=id,
            key=key,
            embedding=embedding,
            value=value,
            org_id=org_id,
            user_id=user_id,
            is_shared=is_shared,
        )
        return None

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
        {"key": e.key, "value": e.value}
        for e in cluster_examples
    ]

    # include current input
    new_example = {"key": key, "value": value}
    cluster_ids = list(set(cluster_ids + [id]))

    # Step 5: compress entire cluster
    new_examples = compress_examples(org, new_example, examples_for_compression, llm=llm)
    print(f"[DEDUP] Compressed examples: {new_examples}")

    # SAFETY CHECK
    if not new_examples:
        print("[DEDUP] Compression failed → aborting delete")
        return None

    # Step 6: delete ENTIRE cluster from DB + Milvus
    # post_delete signal handles Milvus deletion automatically.
    created_examples = []
    with transaction.atomic():
        logger.debug("[DEDUP] Deleting full cluster: %s", cluster_ids)
        PromptExample.objects.filter(id__in=cluster_ids).delete()

        # Step 7: insert new canonical examples
        for ex in new_examples:
            logger.debug("[DEDUP] Inserting new canonical example: %s", ex)

            new_obj = PromptExample(
                organisation=org,
                created_by_id=user_id if user_id else None,
                is_shared=is_shared,
                key=ex["key"],
                value=ex["value"]
            )

            new_obj._skip_vector_sync = True
            new_obj.save()

            new_embedding = llm.generate_vector(ex["key"])

            insert_example_vector(
                id=new_obj.id,
                key=new_obj.key,
                embedding=new_embedding,
                value=new_obj.value,
                org_id=org_id,
                user_id=user_id,
                is_shared=is_shared,
            )
            created_examples.append({
                "id": new_obj.id,
                "key": new_obj.key,
                "value": new_obj.value,
            })

    return {"deleted_ids": cluster_ids, "new_examples": created_examples}


def find_similar_examples(embedding, org_id, user_id=None, threshold=0.75, limit=3):
    """
    Layered retrieval:
    - If user_id is provided: fetch user's own + org-level shared memories
    - If user_id is None: fetch all org memories (admin/service context)
    """
    print(f"[MILVUS SEARCH] Running search for org_id={org_id}, user_id={user_id}")
    client = get_milvus_client()
    collection = "query_example"
    
    get_or_create_example_collection()
    client.load_collection(collection)

    if user_id:
        # Layered: user's own memories + org-level shared memories
        expr = f'org_id == {org_id} && (user_id == {user_id} || is_shared == true)'
    else:
        # Service/admin context: all org memories
        expr = f'org_id == {org_id}'

    results = client.search(
        collection_name=collection,
        data=[embedding],
        anns_field="embedding",
        metric_type="COSINE",
        limit=limit,
        filter=expr,
        output_fields=["id", "key", "value", "org_id", "user_id", "is_shared"],
    )
    print(f"[MILVUS SEARCH] Raw results count: {len(results[0])}")

    matches = []
    print(f"Raw search results for examples: {results}")

    for i, hit in enumerate(results[0]):
        cosine_distance = hit["distance"]
        cosine_similarity = 1-cosine_distance
        
        print(cosine_similarity, hit["entity"]["key"], hit["entity"]["value"])
        if cosine_similarity > threshold:
            matches.append({
                "id": hit["id"],
                "key": hit["entity"]["key"],
                "value": hit["entity"]["value"],
                "user_id": hit["entity"].get("user_id", 0),
                "is_shared": hit["entity"].get("is_shared", False),
                "similarity": cosine_similarity
            })

    matches = sorted(matches, key=lambda x: x["similarity"], reverse=True)
    print(f"Final sorted example matches: {[m['key'][:30] for m in matches]}")

    return matches


def insert_example_vector(id, key, embedding, value, org_id, user_id=0, is_shared=False):
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
            "org_id": org_id,
            "user_id": user_id or 0,
            "is_shared": is_shared,
        }]
    )


def sync_prompt_example(example: PromptExample, llm):
    logger.debug("[SYNC] Triggered for ID=%s, KEY=%s", example.id, example.key)
    embedding = llm.generate_vector(example.key)
    print("[SYNC] Embedding generated, calling dedup...")
    return deduplicate_and_store(
        id=example.id,
        key=example.key,
        value=example.value,
        embedding=embedding,
        org_id=example.organisation_id,
        user_id=example.created_by_id or 0,
        is_shared=example.is_shared,
        llm=llm
    )


def delete_from_milvus(ids):
    client = get_milvus_client()

    print(f"[MILVUS DELETE] Deleting IDs: {ids}")

    client.delete(
        collection_name="query_example",
        ids=ids
    )
