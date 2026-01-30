import re
import json
import logging
import asyncio
from typing import Dict, Any, List, Optional
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .llm_interface import LLMProvider

logger = logging.getLogger(__name__)


class ChainOfThoughtAgent:
    """
    A Chain of Thought (CoT) agent that acts as an MCP Client.
    It connects to multiple MCP servers to discover and execute tools.
    Implements a Think-Act-Observe loop until reaching a final answer or max steps.
    """

    def __init__(
        self,
        server_params: List[StdioServerParameters],
        llm: LLMProvider,
        max_steps: int = 10,
        verbose: bool = True
    ):
        self.server_params = server_params
        self.llm = llm
        self.max_steps = max_steps
        self.verbose = verbose
        self.messages: List[Dict[str, str]] = []

        self.exit_stack = AsyncExitStack()
        self.sessions: List[ClientSession] = []
        self.tools_registry: Dict[str, ClientSession] = {}
        self.tool_descriptions: List[str] = []

    async def __aenter__(self):
        self.sessions = []
        self.tools_registry = {}
        self.tool_descriptions = []

        for params in self.server_params:
            try:
                stdio_transport = await self.exit_stack.enter_async_context(stdio_client(params))
                session = await self.exit_stack.enter_async_context(ClientSession(stdio_transport[0], stdio_transport[1]))
                await session.initialize()
                self.sessions.append(session)

                result = await session.list_tools()
                for tool in result.tools:
                    self.tools_registry[tool.name] = session

                    schema_str = json.dumps(tool.inputSchema.get("properties", {}), indent=2)
                    self.tool_descriptions.append(f"- `{tool.name}`: {tool.description}\n  Args: {schema_str}")

            except Exception as e:
                logger.error(f"Failed to connect to MCP server {params.command}: {e}")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exit_stack.aclose()

    def _get_system_prompt(self) -> str:
        if not self.tool_descriptions:
            return "You are a helpful assistant. No tools are currently available. Please apologize to the user."

        tools_section = "\n".join(self.tool_descriptions)

        return f"""You are a helpful database assistant. You can answer questions about databases connected via TernoDBI.

You have access to the following tools via MCP (Master Control Protocol):

{tools_section}

To answer a question, you should use a Think-Act-Observe loop.

FORMAT:
Thought: <your reasoning about what to do next>
Action: <the tool to call>
Action Input: <JSON string of arguments for the tool>

... (wait for Observation) ...

Observation: <the result of the tool execution>

... (repeat Thought/Action/Observation as needed) ...

When you have the final answer:
Final Answer: <your final answer to the user's question>

IMPORTANT:
- Action Input must be a valid JSON dictionary. e.g. {{"datasource_id": 1}}
- Do not output the "Observation:" part yourself. I will provide it.
- Always check available datasources first if you don't know the ID.
- Use get_sample_data to preview table contents before writing complex queries.
"""

    def _extract_json_from_string(self, text: str) -> Optional[str]:
        start_idx = text.find('{')
        if start_idx == -1:
            return None

        brace_count = 0
        for i, char in enumerate(text[start_idx:], start=start_idx):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    return text[start_idx:i + 1]
        return None

    async def _execute_tool(self, tool_name: str, tool_args: Dict[str, Any]) -> str:
        session = self.tools_registry.get(tool_name)
        if not session:
            return f"Error: Unknown tool '{tool_name}'. Available tools: {list(self.tools_registry.keys())}"

        try:
            result = await session.call_tool(tool_name, arguments=tool_args)

            output = []
            if result.content:
                for content in result.content:
                    if content.type == "text":
                        output.append(content.text)
                    else:
                        output.append(f"[{content.type} content]")

            return "\n".join(output) if output else "Tool executed successfully (no output)."

        except Exception as e:
            logger.exception(f"Error executing tool {tool_name}")
            return f"Error executing tool {tool_name}: {str(e)}"

    def _log(self, message: str) -> None:
        if self.verbose:
            print(message)

    async def run(self, question: str) -> str:
        if not self.sessions:
            return "Error: Agent is not connected to any MCP servers. Please use 'async with agent:'."

        self.messages = [
            {"role": "system", "content": self._get_system_prompt()},
            {"role": "user", "content": question}
        ]

        for step in range(self.max_steps):
            response = await asyncio.to_thread(self.llm.generate, self.messages)

            self._log(f"\n[Agent Step {step + 1}]\n{response}\n")
            self.messages.append({"role": "assistant", "content": response})

            if "Final Answer:" in response:
                final_answer = response.split("Final Answer:")[-1].strip()
                self._log(f"[Final Answer] {final_answer}")
                return final_answer

            action_match = re.search(r"Action:\s*(.+?)(?:\n|$)", response)
            if not action_match:

                if "Thought:" in response:
                    self.messages.append({
                        "role": "user",
                        "content": "You provided a Thought but no Action. Please specify an Action and Action Input, or provide a Final Answer."
                    })
                    continue
                else:
                    return response

            tool_name = action_match.group(1).strip()

            action_input_start = response.find("Action Input:")
            if action_input_start == -1:
                self.messages.append({
                    "role": "user",
                    "content": "Error: No Action Input found. Please provide Action Input as a JSON object."
                })
                continue

            json_str = self._extract_json_from_string(response[action_input_start:])
            if not json_str:
                self.messages.append({
                    "role": "user",
                    "content": "Error: Could not parse Action Input. Please provide valid JSON."
                })
                continue

            try:
                tool_args = json.loads(json_str)
                self._log(f"[Executing] {tool_name}({tool_args})")

                observation = await self._execute_tool(tool_name, tool_args)
                truncated = observation[:1000] + "..." if len(observation) > 1000 else observation
                self._log(f"[Observation] {truncated}")

                self.messages.append({"role": "user", "content": f"Observation: {observation}"})

            except json.JSONDecodeError as e:
                error_msg = f"Error: Action Input is not valid JSON: {e}"
                self._log(error_msg)
                self.messages.append({"role": "user", "content": error_msg})

        return "Error: Maximum steps reached without a final answer."
