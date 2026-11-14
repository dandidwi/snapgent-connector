#!/usr/bin/env python3
"""Expose the Snapgent bridge's local AI API as an MCP server.

Background: bridge.py already runs an OpenAI- *and* Ollama-compatible HTTP API
(default http://127.0.0.1:11434) whose replies are produced by a FREE AI chat
tab in the user's browser (Gemini / ChatGPT / Claude / DeepSeek / ...). That is
the exact shape of a local model server like Ollama or LM Studio.

This file wraps that endpoint as an MCP stdio server, so any MCP client
(Claude Desktop, Cursor, another snapgent target, ...) can use the browser AI
as if it were a local model - without needing to know about the bridge API.

Zero third-party dependencies: standard library only.
"""

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass


ROOT = Path(__file__).resolve().parent
RUNTIME_PORTS_PATH = ROOT / "snapgent-extension" / "runtime_ports.json"
DEFAULT_API_PORT = 11434
# Browser replies can be slow. The bridge waits up to ~240s for the browser tab,
# so give the HTTP call a little more headroom than that.
DEFAULT_TIMEOUT = 260.0


def api_base() -> str:
    """Resolve the bridge API base URL, honouring the live port from the bridge."""
    port = DEFAULT_API_PORT
    try:
        data = json.loads(RUNTIME_PORTS_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("api_port"), int):
            port = data["api_port"]
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return f"http://127.0.0.1:{port}"


def _http(method: str, path: str, payload: Optional[Dict[str, Any]] = None,
          timeout: float = DEFAULT_TIMEOUT) -> Any:
    url = api_base() + path
    data = None
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "snapgent-browser-ai-mcp/1.0",
    }
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Bridge API returned HTTP {error.code}: {body[:500]}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Cannot reach the Snapgent bridge at {api_base()} - start it with start.bat "
            f"and open an AI chat tab first ({error.reason})."
        ) from error
    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"Bridge API returned invalid JSON: {raw[:200]}") from error


def list_browser_models() -> Dict[str, Any]:
    result = _http("GET", "/v1/models", timeout=15)
    models = []
    for item in result.get("data", []) if isinstance(result, dict) else []:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            models.append(item["id"])
    return {"models": models}


def browser_ai_health() -> Dict[str, Any]:
    result = _http("GET", "/", timeout=10)
    return result if isinstance(result, dict) else {"raw": result}


def ask_browser_ai(prompt: str, model: str = "auto", system: Optional[str] = None,
                   tools: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("prompt cannot be empty.")
    messages: List[Dict[str, Any]] = []
    if isinstance(system, str) and system.strip():
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    payload: Dict[str, Any] = {
        "model": (model or "auto").strip() or "auto",
        "messages": messages,
        "stream": False,
    }
    if isinstance(tools, list) and tools:
        payload["tools"] = tools

    result = _http("POST", "/v1/chat/completions", payload)
    choices = result.get("choices") if isinstance(result, dict) else None
    if not choices:
        return {"text": "", "tool_calls": [], "note": "Bridge returned no choices."}
    message = choices[0].get("message") or {}
    text = message.get("content") or ""
    raw_calls = message.get("tool_calls") or []
    parsed_calls: List[Dict[str, Any]] = []
    for call in raw_calls:
        function = call.get("function") if isinstance(call, dict) else None
        if not isinstance(function, dict):
            continue
        arguments = function.get("arguments")
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                pass
        parsed_calls.append({"name": function.get("name"), "arguments": arguments})
    out: Dict[str, Any] = {"text": text}
    if parsed_calls:
        out["tool_calls"] = parsed_calls
    return out


STR = {"type": "string"}
TOOLS: List[Dict[str, Any]] = [
    {
        "name": "ask_browser_ai",
        "description": (
            "Send one prompt to the free AI chat currently open in the user's browser "
            "(via the local Snapgent bridge) and return its reply text. Works like "
            "calling a local model such as Ollama, but the answer comes from a browser "
            "AI tab. If you pass `tools`, the model may answer with tool_calls instead "
            "of text. NOTE: browser replies can take 30-240s; set the MCP tool timeout "
            "to at least 280000 ms when calling this tool."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {**STR, "description": "The user message to send."},
                "model": {**STR, "description": "Model/provider hint: auto (default), or a provider id such as gemini-web, chatgpt-web, claude-web, deepseek-web."},
                "system": {**STR, "description": "Optional system instructions prepended to the prompt."},
                "tools": {
                    "type": "array",
                    "description": "Optional OpenAI-style tool/function specs. If given, the model may return tool_calls instead of plain text.",
                    "items": {"type": "object"},
                },
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "list_browser_models",
        "description": "List the model/provider ids available through the local browser-AI bridge (GET /v1/models).",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "browser_ai_health",
        "description": "Check whether the local Snapgent bridge API is running and reachable.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
]


def tool_dispatch(name: str, args: Dict[str, Any]) -> Any:
    if name == "ask_browser_ai":
        return ask_browser_ai(
            args["prompt"],
            model=args.get("model", "auto"),
            system=args.get("system"),
            tools=args.get("tools"),
        )
    if name == "list_browser_models":
        return list_browser_models()
    if name == "browser_ai_health":
        return browser_ai_health()
    raise ValueError(f"Unknown tool: {name}")


def send_message(message: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n")
    sys.stdout.flush()


def handle_request(request: Dict[str, Any]) -> None:
    method = request.get("method")
    request_id = request.get("id")
    params = request.get("params") or {}

    if method == "initialize":
        send_message({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "browser-ai-mcp", "version": "1.0.0"},
            },
        })
        return

    if method == "ping":
        send_message({"jsonrpc": "2.0", "id": request_id, "result": {}})
        return

    if method == "tools/list":
        send_message({"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}})
        return

    if method == "tools/call":
        try:
            output = tool_dispatch(params.get("name", ""), params.get("arguments") or {})
            text = output if isinstance(output, str) else json.dumps(output, ensure_ascii=False)
            send_message({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": text}],
                    "structuredContent": {"result": output},
                },
            })
        except Exception as error:
            send_message({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(error)},
            })
        return

    if method == "notifications/initialized":
        return

    send_message({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    })


if __name__ == "__main__":
    for line in sys.stdin:
        stripped = line.strip()
        if not stripped:
            continue
        try:
            request = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(request, dict):
            handle_request(request)
