import json
import logging
import re

logger = logging.getLogger(__name__)


def generate_table_note_updates(conversation_text, org_prompt, tables_info, llm):
    if not conversation_text:
        return {}

    org_prompt = (org_prompt or "").strip() or "(no organisation prompt provided)"
    tables_info = (tables_info or "").strip() or "(no existing notes available)"

    prompt = f"""You are helping maintain table-level business knowledge for SQL generation.
When a user asks to save a note, based on a corrected query or business clarification for
the conversation :
1. Identify the most appropriate tables where the business rule belongs.
   - Prefer the fact table containing the columns used in the metric calculation.
   - Do not choose dimension tables unless the rule is specifically about dimension attributes.
2. The existing notes for the relevant tables are provided below under EXISTING TABLE NOTES.
   Read them carefully before writing anything.
3. Do NOT add information that is already conveyed (in any wording) by the existing notes
   or the organization prompt or the table description. If the rule is already covered, return {{}}.
4. Otherwise extract only the business rule that changes how the metric should be calculated.
   - Keep the note concise and reusable.
   - Maximum 1-2 sentences.
   - Do not include joins, filters, date logic, example SQL, implementation details, assumptions, query plans, reporting instructions.
   - Try to avoid full sentences. One line mathematical rule would be the best.
5. The note should help future SQL generation choose the correct metric definition.

ORGANISATION PROMPT (overall business context):
{org_prompt}

EXISTING TABLE NOTES (already saved — do NOT repeat anything already covered here, even if reworded):
{tables_info}

CONVERSATION:
{conversation_text}

Return ONLY a JSON object in this exact format:
{{"table": "<table name exactly as referred to in the conversation>", "note": "<short note>"}}
If there is no NEW business rule worth saving, return {{}}.
"""

    try:
        response = llm.get_simple_response(prompt)
    except Exception as e:
        logger.warning("[TABLE-NOTES] LLM call failed: %s", e)
        return {}

    try:
        response = re.sub(r"```json|```", "", response or "").strip()
        if not response:
            return {}
        data = json.loads(response)
        if not isinstance(data, dict):
            return {}
        table = (data.get("table") or "").strip()
        note = (data.get("note") or "").strip()
        if not table or not note:
            return {}
        return {"table": table, "note": note}
    except Exception as e:
        logger.warning("[TABLE-NOTES] Failed to parse LLM response: %s", e)
        return {}
