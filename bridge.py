#!/usr/bin/env python3
"""Cross-platform zero-dependency local WebSocket & OpenAI/Ollama proxy bridge for Snapgent."""

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import signal
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple


if getattr(sys, "frozen", False):
    ROOT = Path(sys.executable).resolve().parent
else:
    ROOT = Path(__file__).resolve().parent

CONFIG_PATH = ROOT / "config.json"
LOG_PATH = ROOT / "logs" / "bridge_debug.log"
RUNTIME_PORTS_PATH = ROOT / "snapgent-extension" / "runtime_ports.json"
HOST = "127.0.0.1"
PORT = int(os.environ.get("ZS_BRIDGE_PORT", "37613"))
API_PORT = int(os.environ.get("ZS_API_PORT", "11434"))
MCP_PROTOCOL_VERSION = "2024-11-05"
BRIDGE_VERSION = "3.9.5-alpha"
MAX_LOG_CHARS = 1000
MAX_WS_PAYLOAD = 50 * 1024 * 1024
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def log(message: str) -> None:
    logging.info(message)
    print(message, flush=True)


def _json_line(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class PureWebSocketConnection:
    """RFC 6455 compliant server-side WebSocket using pure Python stdlib."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self._write_lock = asyncio.Lock()
        self.closed = False

    async def send(self, data: str) -> None:
        if self.closed:
            raise ConnectionResetError("WebSocket is closed")
        payload = data.encode("utf-8")
        await self._send_frame(0x1, payload)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.closed:
            return
        length = len(payload)
        if length < 126:
            header = struct.pack("!BB", 0x80 | opcode, length)
        elif length < 65536:
            header = struct.pack("!BBH", 0x80 | opcode, 126, length)
        else:
            header = struct.pack("!BBQ", 0x80 | opcode, 127, length)
        async with self._write_lock:
            if self.closed:
                return
            self.writer.write(header + payload)
            await self.writer.drain()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        try:
            payload = struct.pack("!H", code) + reason.encode("utf-8")[:123]
            await self._send_frame(0x8, payload)
        except Exception:
            pass
        self.closed = True
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass

    async def _read_frame(self) -> Optional[Tuple[bool, int, bytes]]:
        try:
            header = await self.reader.readexactly(2)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return None
        b1, b2 = header[0], header[1]
        fin = bool(b1 & 0x80)
        opcode = b1 & 0x0F
        masked = bool(b2 & 0x80)
        length = b2 & 0x7F
        try:
            if length == 126:
                ext = await self.reader.readexactly(2)
                length = struct.unpack("!H", ext)[0]
            elif length == 127:
                ext = await self.reader.readexactly(8)
                length = struct.unpack("!Q", ext)[0]
            if length > MAX_WS_PAYLOAD:
                await self.close(1009, "Message too big")
                return None
            mask_key = await self.reader.readexactly(4) if masked else b""
            payload = await self.reader.readexactly(length) if length > 0 else b""
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            return None
        if masked:
            payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
        return fin, opcode, payload

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iter_messages()

    async def _iter_messages(self) -> AsyncIterator[str]:
        fragments: List[bytes] = []
        frag_opcode: Optional[int] = None
        try:
            while not self.closed:
                frame = await self._read_frame()
                if frame is None:
                    break
                fin, opcode, payload = frame
                if opcode == 0x8:
                    break
                if opcode == 0x9:
                    try:
                        await self._send_frame(0xA, payload)
                    except Exception:
                        break
                    continue
                if opcode == 0xA:
                    continue
                if opcode in (0x1, 0x2):
                    if fin:
                        if opcode == 0x1:
                            yield payload.decode("utf-8", errors="replace")
                    else:
                        frag_opcode = opcode
                        fragments = [payload]
                elif opcode == 0x0 and frag_opcode is not None:
                    fragments.append(payload)
                    if fin:
                        full_data = b"".join(fragments)
                        fragments.clear()
                        if frag_opcode == 0x1:
                            yield full_data.decode("utf-8", errors="replace")
                        frag_opcode = None
        finally:
            await self.close()


async def _perform_ws_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> Optional[PureWebSocketConnection]:
    try:
        raw_head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
    except Exception:
        writer.close()
        return None
    head_text = raw_head.decode("utf-8", errors="replace")
    lines = head_text.split("\r\n")
    if not lines or not lines[0].upper().startswith("GET "):
        writer.close()
        return None
    headers: Dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    ws_key = headers.get("sec-websocket-key")
    if not ws_key or headers.get("upgrade", "").lower() != "websocket":
        body = b"Snapgent Pure-Python WebSocket Bridge"
        resp = (
            b"HTTP/1.1 426 Upgrade Required\r\n"
            b"Upgrade: websocket\r\n"
            b"Connection: Upgrade\r\n"
            b"Content-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
            + body
        )
        writer.write(resp)
        await writer.drain()
        writer.close()
        return None
    accept_raw = hashlib.sha1((ws_key + WS_GUID).encode("ascii")).digest()
    accept_key = base64.b64encode(accept_raw).decode("ascii")
    handshake_resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept_key}\r\n\r\n"
    ).encode("ascii")
    writer.write(handshake_resp)
    await writer.drain()
    return PureWebSocketConnection(reader, writer)


class MCPServer:
    def __init__(self, server_id: str, config: Dict[str, Any]) -> None:
        self.server_id = server_id
        self.config = config
        self.process: Optional[asyncio.subprocess.Process] = None
        self.tools: List[Dict[str, Any]] = []
        self._next_id = 1
        self._pending: Dict[int, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._start_lock = asyncio.Lock()
        self.error: Optional[str] = None
        self.ready = False

    @property
    def alive(self) -> bool:
        reader_dead = bool(self._reader_task and self._reader_task.done())
        return bool(self.process and self.process.returncode is None and not reader_dead)

    async def start(self) -> bool:
        async with self._start_lock:
            if self.alive and self.ready:
                return True
            await self.stop()
            command = self.config.get("command")
            args = self.config.get("args", [])
            if not isinstance(command, str) or not command.strip():
                self.error = "MCP server command is missing."
                log(f"[{self.server_id}] {self.error}")
                return False
            if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
                self.error = "MCP server args must be an array of strings."
                log(f"[{self.server_id}] {self.error}")
                return False

            env = os.environ.copy()
            server_env = self.config.get("env", {})
            if not isinstance(server_env, dict):
                self.error = "MCP server env must be an object."
                log(f"[{self.server_id}] {self.error}")
                return False
            env.update({str(key): str(value) for key, value in server_env.items()})
            try:
                self.process = await asyncio.create_subprocess_exec(
                    command,
                    *args,
                    cwd=str(ROOT),
                    env=env,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    limit=64 * 1024 * 1024,
                )
            except (OSError, ValueError) as error:
                self.error = str(error)
                log(f"[{self.server_id}] failed to launch: {self.error}")
                return False

            self.tools = []
            self.error = None
            self._reader_task = asyncio.create_task(self._read_stdout())
            self._stderr_task = asyncio.create_task(self._read_stderr())
            try:
                await self._request(
                    "initialize",
                    {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "snapgent-bridge", "version": BRIDGE_VERSION},
                    },
                    timeout=12,
                )
                await self._notify("notifications/initialized")
                result = await self._request("tools/list", {}, timeout=20)
                raw_tools = result.get("tools", []) if isinstance(result, dict) else []
                if not isinstance(raw_tools, list):
                    raise RuntimeError("MCP tools/list returned an invalid tool list.")
                self.tools = [
                    tool for tool in raw_tools
                    if isinstance(tool, dict) and isinstance(tool.get("name"), str)
                ]
                self.ready = True
                log(f"[{self.server_id}] MCP server up ({len(self.tools)} tools advertised)")
                return True
            except (asyncio.TimeoutError, RuntimeError, OSError, ValueError) as error:
                self.error = str(error)
                log(f"[{self.server_id}] MCP startup failed: {self.error}")
                await self.stop()
                return False

    async def stop(self) -> None:
        process = self.process
        self.process = None
        self.tools = []
        self.ready = False
        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
        tasks = [task for task in (self._reader_task, self._stderr_task) if task]
        self._reader_task = None
        self._stderr_task = None
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RuntimeError(f"{self.server_id} MCP server stopped."))
        self._pending.clear()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if not process or process.returncode is not None:
            return
        if process.stdin:
            try:
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except asyncio.TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

    async def _send(self, payload: Dict[str, Any]) -> None:
        process = self.process
        if not process or process.returncode is not None or not process.stdin:
            raise RuntimeError(f"{self.server_id} MCP server is offline.")
        data = (_json_line(payload) + "\n").encode("utf-8")
        async with self._write_lock:
            process.stdin.write(data)
            await process.stdin.drain()

    async def _request(
        self, method: str, params: Dict[str, Any], timeout: float = 30
    ) -> Any:
        request_id = self._next_id
        self._next_id += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
            response = await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)
        if "error" in response:
            detail = response["error"]
            if isinstance(detail, dict):
                raise RuntimeError(str(detail.get("message") or detail))
            raise RuntimeError(str(detail))
        return response.get("result")

    async def _notify(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        await self._send(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params or {},
            }
        )

    async def call_tool(
        self, tool_name: str, arguments: Dict[str, Any], timeout: float
    ) -> Dict[str, Any]:
        if not self.alive:
            raise RuntimeError(self.error or f"{self.server_id} MCP server is offline.")
        result = await self._request(
            "tools/call",
            {"name": tool_name, "arguments": arguments},
            timeout=timeout,
        )
        return result if isinstance(result, dict) else {"content": [{"text": str(result)}]}

    async def _read_stdout(self) -> None:
        process = self.process
        if not process or not process.stdout:
            return
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                try:
                    message = json.loads(line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    log(f"[{self.server_id}] ignored non-JSON MCP stdout.")
                    continue
                request_id = message.get("id")
                if isinstance(request_id, int):
                    future = self._pending.get(request_id)
                    if future and not future.done():
                        future.set_result(message)
        except asyncio.CancelledError:
            raise
        except (ValueError, asyncio.LimitOverrunError) as error:
            log(f"[{self.server_id}] MCP stdout read failed: {error}")
        finally:
            if self.process is process and process.returncode is None:
                log(f"[{self.server_id}] MCP stdout closed.")
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(RuntimeError(f"{self.server_id} MCP server disconnected."))

    async def _read_stderr(self) -> None:
        process = self.process
        if not process or not process.stderr:
            return
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    log(f"[{self.server_id}] stderr: {text[:MAX_LOG_CHARS]}")
        except asyncio.CancelledError:
            raise


class Bridge:
    def __init__(self) -> None:
        self.servers: Dict[str, MCPServer] = {}
        self.clients = set()
        self._client_lock = asyncio.Lock()
        self._catalog: List[Dict[str, Any]] = []
        self._last_studio: Optional[bool] = None
        self._api_pending: Dict[str, asyncio.Future] = {}
        self._api_next_id = 1
        self._api_lock = asyncio.Lock()
        self.active_providers: List[str] = []
        self.api_requests_handled: int = 0
        self.api_last_client: str = "None yet"

    def load_config(self) -> Dict[str, Dict[str, Any]]:
        if not CONFIG_PATH.exists():
            return {}
        try:
            config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"Could not read {CONFIG_PATH.name}: {error}") from error
        servers = config.get("mcpServers") if isinstance(config, dict) else None
        if not isinstance(servers, dict):
            raise RuntimeError(f"{CONFIG_PATH.name} must contain an object named mcpServers.")
        return {
            server_id: value
            for server_id, value in servers.items()
            if isinstance(server_id, str)
            and isinstance(value, dict)
            and re.fullmatch(r"[A-Za-z0-9._-]{1,64}", server_id)
        }

    async def start_servers(self) -> None:
        config = self.load_config()
        self.servers = {
            server_id: MCPServer(server_id, server_config)
            for server_id, server_config in config.items()
        }
        async def _start_one(server: MCPServer) -> None:
            try:
                await server.start()
            except Exception as error:
                server.error = str(error)
                log(f"[{server.server_id}] unexpected startup error: {error}")
            self._rebuild_catalog()
            await self.broadcast(self._connected_message())

        await asyncio.gather(*(_start_one(server) for server in self.servers.values()))
        self._rebuild_catalog()
        self._log_health()

    async def stop_servers(self) -> None:
        await asyncio.gather(*(server.stop() for server in self.servers.values()))
        self.servers.clear()
        self._catalog = []

    def _rebuild_catalog(self) -> None:
        entries = []
        for server in self.servers.values():
            if server.alive:
                entries.extend((server.server_id, tool) for tool in server.tools)
        name_counts: Dict[str, int] = {}
        for _, tool in entries:
            name_counts[tool["name"]] = name_counts.get(tool["name"], 0) + 1
        self._catalog = []
        for server_id, tool in entries:
            bare_name = tool["name"]
            name = f"{server_id}/{bare_name}" if name_counts[bare_name] > 1 else bare_name
            self._catalog.append(
                {
                    "name": name,
                    "server": server_id,
                    "description": tool.get("description", ""),
                    "inputSchema": tool.get("inputSchema", {"type": "object", "properties": {}}),
                }
            )

    def _server_status(self) -> List[Dict[str, Any]]:
        return [
            {
                "id": server.server_id,
                "alive": server.alive,
                "tools": len(server.tools) if server.alive else 0,
            }
            for server in self.servers.values()
        ]

    def _log_health(self) -> None:
        ids = ", ".join(self.servers) or "none"
        active = ", ".join(server.server_id for server in self.servers.values() if server.alive) or "none"
        log(f"configured {len(self.servers)} MCP server(s): {ids}  (active: {active})")
        total = len(self._catalog)
        log(f"ready {total} tools available ({len(self.servers)} MCP server(s))")

    def _connected_message(self) -> Dict[str, Any]:
        return {
            "type": "connected",
            "bridge_version": BRIDGE_VERSION,
            "mcp_alive": any(server.alive for server in self.servers.values()),
            "tools": self._catalog,
            "servers": self._server_status(),
        }

    async def broadcast(self, message: Dict[str, Any]) -> None:
        async with self._client_lock:
            clients = list(self.clients)
        if not clients:
            return
        await asyncio.gather(
            *(self._send_client(client, message) for client in clients),
            return_exceptions=True,
        )

    async def _send_client(self, client: Any, message: Dict[str, Any]) -> None:
        try:
            await client.send(_json_line(message))
        except Exception:
            pass

    async def handle_stream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        ws = await _perform_ws_handshake(reader, writer)
        if ws is None:
            return
        await self.handle_client(ws)

    async def handle_client(self, websocket: Any, _path: Optional[str] = None) -> None:
        async with self._client_lock:
            self.clients.add(websocket)
        log("extension connected")
        await self._send_client(websocket, self._connected_message())
        try:
            async for raw in websocket:
                try:
                    request = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    await self._send_client(websocket, {"type": "error", "error": "Invalid JSON request."})
                    continue
                if not isinstance(request, dict):
                    await self._send_client(websocket, {"type": "error", "error": "Request must be a JSON object."})
                    continue
                await self._handle_request(websocket, request)
        finally:
            async with self._client_lock:
                self.clients.discard(websocket)
            log("extension disconnected")

    async def _handle_request(self, websocket: Any, request: Dict[str, Any]) -> None:
        request_id = request.get("id")
        kind = request.get("type")
        if kind == "ping":
            await self._send_client(websocket, {"type": "pong", "id": request_id})
        elif kind == "studio_status":
            studio, studio_app, studio_proc = await self._studio_status()
            await self._send_client(
                websocket,
                {
                    "type": "studio_status",
                    "id": request_id,
                    "studio": studio,
                    "studio_app": studio_app,
                    "studio_proc": studio_proc,
                },
            )
        elif kind == "list_tools":
            await self._send_client(
                websocket,
                {
                    **self._connected_message(),
                    "type": "tools",
                    "id": request_id,
                },
            )
        elif kind == "call_tool":
            await self._call_tool(websocket, request)
        elif kind == "restart_mcp":
            try:
                await self.stop_servers()
                await self.start_servers()
                response = {
                    "type": "mcp_status",
                    "id": request_id,
                    "ok": True,
                    "alive": any(server.alive for server in self.servers.values()),
                    "tools": self._catalog,
                    "servers": self._server_status(),
                }
            except (OSError, RuntimeError) as error:
                response = {"type": "mcp_status", "id": request_id, "ok": False, "alive": False, "error": str(error)}
            await self._send_client(websocket, response)
            await self.broadcast(self._connected_message())
        elif kind == "add_server":
            await self._change_server(websocket, request, remove=False)
        elif kind == "remove_server":
            await self._change_server(websocket, request, remove=True)
        elif kind == "providers_update":
            providers = request.get("providers")
            if isinstance(providers, list):
                self.active_providers = [str(p) for p in providers if p]
        elif kind == "api_completion_result":
            req_id = str(request.get("id", ""))
            fut = self._api_pending.get(req_id)
            if fut and not fut.done():
                if request.get("ok"):
                    fut.set_result(str(request.get("text", "")))
                else:
                    fut.set_exception(RuntimeError(str(request.get("error") or "Browser completion failed")))
        else:
            await self._send_client(
                websocket,
                {"type": "error", "id": request_id, "error": f"Unknown bridge request: {kind}"},
            )

    async def request_browser_completion(self, prompt: str, model: str = "auto", timeout: float = 240.0) -> str:
        async with self._api_lock:
            async with self._client_lock:
                clients = list(self.clients)
            if not clients:
                raise RuntimeError("No browser extension connected to snapgent bridge. Open an AI chat tab in Chrome first.")
            req_id = f"api_{self._api_next_id}"
            self._api_next_id += 1
            loop = asyncio.get_running_loop()
            fut: asyncio.Future = loop.create_future()
            self._api_pending[req_id] = fut
            try:
                await self._send_client(
                    clients[0],
                    {
                        "type": "api_completion",
                        "id": req_id,
                        "prompt": prompt,
                        "model": model,
                        "timeout": int(timeout * 1000),
                    },
                )
                return await asyncio.wait_for(fut, timeout=timeout)
            finally:
                self._api_pending.pop(req_id, None)

    async def _call_tool(self, websocket: Any, request: Dict[str, Any]) -> None:
        request_id = request.get("id")
        name = request.get("name")
        arguments = request.get("arguments") or {}
        timeout = request.get("timeout", 120000)
        if not isinstance(name, str) or not isinstance(arguments, dict):
            await self._send_client(
                websocket,
                {"type": "tool_result", "id": request_id, "ok": False, "error": "Invalid tool call."},
            )
            return
        try:
            timeout_seconds = max(1.0, min(float(timeout) / 1000, 300.0))
        except (TypeError, ValueError):
            timeout_seconds = 120.0
        server, tool_name = self._resolve_tool(name)
        allowed = request.get("server")
        if isinstance(allowed, str) and allowed:
            if server is None and "/" not in name:
                allowed_server = self.servers.get(allowed)
                if allowed_server and allowed_server.alive and any(t["name"] == name for t in allowed_server.tools):
                    server, tool_name = allowed_server, name
            if server is not None and server.server_id != allowed:
                await self._send_client(
                    websocket,
                    {
                        "type": "tool_result",
                        "id": request_id,
                        "ok": False,
                        "kind": "unknown_tool",
                        "error": f"MCP tool not available for the selected server: {name}",
                    },
                )
                return
        if not server:
            await self._send_client(
                websocket,
                {
                    "type": "tool_result",
                    "id": request_id,
                    "ok": False,
                    "kind": "unknown_tool",
                    "error": f"Unknown or unavailable MCP tool: {name}",
                },
            )
            return
        try:
            result = await server.call_tool(tool_name, arguments, timeout_seconds)
            text_parts = []
            images = []
            for item in result.get("content", []):
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "text" and isinstance(item.get("text"), str):
                    text_parts.append(item["text"])
                elif item.get("type") == "image" and isinstance(item.get("data"), str):
                    images.append(
                        {
                            "data": item["data"],
                            "mimeType": item.get("mimeType", "image/png"),
                        }
                    )
            structured = result.get("structuredContent")
            if not text_parts and structured is not None:
                text_parts.append(json.dumps(structured, ensure_ascii=False))
            text = "\n".join(text_parts)
            if result.get("isError"):
                await self._send_client(
                    websocket,
                    {"type": "tool_result", "id": request_id, "ok": False, "error": text or "MCP tool failed."},
                )
            else:
                await self._send_client(
                    websocket,
                    {"type": "tool_result", "id": request_id, "ok": True, "text": text, "images": images},
                )
            log(f"<- {name} ({text[:120].replace(chr(10), ' ')})")
        except (asyncio.TimeoutError, RuntimeError, OSError, ValueError) as error:
            message = str(error)
            await self._send_client(
                websocket,
                {
                    "type": "tool_result",
                    "id": request_id,
                    "ok": False,
                    "kind": "timeout" if isinstance(error, asyncio.TimeoutError) else "tool_error",
                    "error": message,
                },
            )
            log(f"<- {name} ERROR: {message[:MAX_LOG_CHARS]}")

    def _resolve_tool(self, name: str) -> Tuple[Optional[MCPServer], str]:
        if "/" in name:
            server_id, bare_name = name.split("/", 1)
            server = self.servers.get(server_id)
            if server and server.alive and any(tool["name"] == bare_name for tool in server.tools):
                return server, bare_name
        matches = [
            (server, tool)
            for server in self.servers.values()
            if server.alive
            for tool in server.tools
            if tool["name"] == name
        ]
        if len(matches) == 1:
            return matches[0][0], name
        return None, name

    async def _studio_status(self) -> Tuple[Optional[bool], Optional[bool], Optional[bool]]:
        studio_server = self.servers.get("roblox")
        studio_proc = _process_running("RobloxStudioBeta.exe" if os.name == "nt" else "RobloxStudio")
        if not studio_server or not studio_server.alive:
            return None, None, studio_proc
        if not any(tool["name"] == "get_studio_state" for tool in studio_server.tools):
            return None, None, studio_proc
        try:
            result = await studio_server.call_tool("get_studio_state", {}, 8)
        except (asyncio.TimeoutError, RuntimeError, OSError, ValueError):
            return False, studio_proc, studio_proc
        text = " ".join(
            item.get("text", "")
            for item in result.get("content", [])
            if isinstance(item, dict) and isinstance(item.get("text"), str)
        )
        if result.get("isError") or re.search(
            r"Unable to find an active Studio instance|previously active Studio has disconnected",
            text,
            re.IGNORECASE,
        ):
            return False, studio_proc, studio_proc
        return True, True, studio_proc

    async def _change_server(
        self, websocket: Any, request: Dict[str, Any], remove: bool
    ) -> None:
        request_id = request.get("id")
        server_id = request.get("server_id")
        if (
            not isinstance(server_id, str)
            or server_id == "roblox"
            or not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", server_id)
        ):
            await self._send_client(
                websocket,
                {"type": "server_changed", "id": request_id, "ok": False, "error": "Invalid or protected MCP server id."},
            )
            return
        try:
            config = json.loads(CONFIG_PATH.read_text(encoding="utf-8")) if CONFIG_PATH.exists() else {}
            servers = config.setdefault("mcpServers", {})
            if not isinstance(servers, dict):
                raise RuntimeError("mcpServers must be a JSON object.")
            if remove:
                if server_id == "vscode":
                    raise RuntimeError("The VS Code MCP server cannot be removed.")
                servers.pop(server_id, None)
            else:
                command = request.get("command")
                args = request.get("args", [])
                env = request.get("env", {})
                if not isinstance(command, str) or not command.strip():
                    raise RuntimeError("MCP server command is required.")
                if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
                    raise RuntimeError("MCP server args must be an array of strings.")
                if not isinstance(env, dict) or not all(isinstance(k, str) for k in env):
                    raise RuntimeError("MCP server env must be an object.")
                servers[server_id] = {"command": command, "args": args, "env": env}
            temp_path = CONFIG_PATH.with_suffix(".json.tmp")
            temp_path.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            temp_path.replace(CONFIG_PATH)
            await self.stop_servers()
            await self.start_servers()
            await self._send_client(
                websocket,
                {"type": "server_changed", "id": request_id, "ok": True, "restarting": False},
            )
            await self.broadcast(self._connected_message())
        except (OSError, json.JSONDecodeError, RuntimeError) as error:
            await self._send_client(
                websocket,
                {"type": "server_changed", "id": request_id, "ok": False, "error": str(error)},
            )


def _process_running(process_name: str) -> Optional[bool]:
    try:
        if os.name == "nt":
            result = subprocess_run(
                ["tasklist", "/FI", f"IMAGENAME eq {process_name}", "/NH"],
                timeout=5,
            )
            return process_name.lower() in result.lower()
        result = subprocess_run(["pgrep", "-f", process_name], timeout=5)
        return bool(result.strip())
    except (OSError, subprocess.TimeoutExpired):
        return None


def subprocess_run(command: List[str], timeout: int) -> str:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    return result.stdout


def _compile_api_prompt(messages: List[Dict[str, Any]], tools: Optional[List[Dict[str, Any]]] = None) -> str:
    parts: List[str] = []
    sys_msgs = [m.get("content", "") for m in messages if m.get("role") == "system" and m.get("content")]
    if sys_msgs:
        parts.append("=== SYSTEM INSTRUCTIONS ===\n" + "\n\n".join(str(s) for s in sys_msgs))
    if tools:
        tool_specs = []
        for t in tools:
            fn = t.get("function") if isinstance(t.get("function"), dict) else t
            if isinstance(fn, dict) and fn.get("name"):
                tool_specs.append({
                    "name": fn.get("name"),
                    "description": fn.get("description", ""),
                    "parameters": fn.get("parameters", {}),
                })
        if tool_specs:
            parts.append(
                "=== AVAILABLE TOOLS ===\n"
                "You have access to the following tools. To call a tool, respond ONLY with a fenced JSON block in this exact format:\n"
                "```json\n"
                "{\"tool_calls\":[{\"name\":\"exact_tool_name\",\"arguments\":{...}}]}\n"
                "```\n"
                "If you do not need to call a tool, reply normally in plain text.\n\n"
                + json.dumps(tool_specs, ensure_ascii=False, indent=2)
            )
    convo: List[str] = []
    for m in messages:
        role = m.get("role", "user")
        if role == "system":
            continue
        content = m.get("content")
        if isinstance(content, list):
            content = "\n".join(str(c.get("text", "")) for c in content if isinstance(c, dict))
        content_str = str(content or "")
        if role == "tool":
            tc_id = m.get("tool_call_id") or m.get("name") or "tool"
            convo.append(f"[TOOL OUTPUT ({tc_id})]:\n{content_str}")
        elif role == "assistant":
            tcs = m.get("tool_calls")
            if tcs:
                convo.append(f"[ASSISTANT TOOL CALL]:\n{json.dumps(tcs, ensure_ascii=False)}")
            elif content_str:
                convo.append(f"[ASSISTANT]:\n{content_str}")
        else:
            convo.append(f"[USER]:\n{content_str}")
    if convo:
        parts.append("=== CONVERSATION ===\n" + "\n\n".join(convo))
    parts.append("=== YOUR RESPONSE ===")
    return "\n\n".join(parts)


def _parse_api_response(raw_text: str) -> Tuple[Optional[str], Optional[List[Dict[str, Any]]]]:
    text = (raw_text or "").strip()
    fenced = re.findall(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
    candidates = fenced if fenced else ([text] if text.startswith("{") and text.endswith("}") else [])
    for cand in candidates:
        try:
            obj = json.loads(cand)
        except Exception:
            continue
        if isinstance(obj, dict) and isinstance(obj.get("tool_calls"), list) and obj["tool_calls"]:
            parsed_calls = []
            for idx, item in enumerate(obj["tool_calls"]):
                if not isinstance(item, dict):
                    continue
                fn_obj = item.get("function") if isinstance(item.get("function"), dict) else item
                name = fn_obj.get("name") or item.get("command")
                args = fn_obj.get("arguments") if "arguments" in fn_obj else item.get("params", {})
                if not isinstance(name, str) or not name:
                    continue
                args_str = args if isinstance(args, str) else json.dumps(args or {}, ensure_ascii=False)
                parsed_calls.append({
                    "id": f"call_{int(time.time()*1000)}_{idx}",
                    "type": "function",
                    "function": {"name": name, "arguments": args_str},
                })
            if parsed_calls:
                return None, parsed_calls
    return text, None


async def _handle_http_client(bridge: Bridge, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        raw_head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=15)
    except Exception:
        writer.close()
        return
    head_text = raw_head.decode("utf-8", errors="replace")
    lines = head_text.split("\r\n")
    req_line = lines[0].split()
    if len(req_line) < 2:
        writer.close()
        return
    method, path = req_line[0].upper(), req_line[1].split("?", 1)[0]
    headers: Dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    content_len = int(headers.get("content-length", "0") or "0")
    body_bytes = b""
    if content_len > 0:
        try:
            body_bytes = await asyncio.wait_for(reader.readexactly(content_len), timeout=30)
        except Exception:
            writer.close()
            return

    async def send_json(status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        status_text = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error", 502: "Bad Gateway"}.get(status, "OK")
        resp_head = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            "Content-Type: application/json; charset=utf-8\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Headers: *\r\n"
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(resp_head + body)
        await writer.drain()
        writer.close()

    if method == "OPTIONS":
        await send_json(200, {"ok": True})
        return

    available_models = bridge.active_providers or ["snapgent-browser", "gemini-web", "chatgpt-web", "claude-web", "deepseek-web"]

    if method == "GET" and path in ("/", "/api/version"):
        await send_json(200, {
            "status": "ok",
            "version": BRIDGE_VERSION,
            "service": "snapgent-local-ai",
            "requests_handled": bridge.api_requests_handled,
            "last_client": bridge.api_last_client,
            "busy": bridge._api_lock.locked(),
        })
        return

    if method == "GET" and path in ("/v1/models", "/models"):
        now_ts = int(time.time())
        await send_json(200, {
            "object": "list",
            "data": [{"id": m, "object": "model", "created": now_ts, "owned_by": "snapgent-browser"} for m in available_models],
        })
        return

    if method == "GET" and path == "/api/tags":
        await send_json(200, {
            "models": [
                {"name": f"{m}:latest", "model": f"{m}:latest", "modified_at": "2026-01-01T00:00:00Z", "size": 0, "digest": "snapgent"}
                for m in available_models
            ]
        })
        return

    if method == "POST" and path in ("/v1/chat/completions", "/chat/completions", "/api/chat", "/api/generate"):
        try:
            payload = json.loads(body_bytes.decode("utf-8") or "{}")
        except Exception:
            await send_json(400, {"error": {"message": "Invalid JSON body"}})
            return
        model = str(payload.get("model") or "auto").replace(":latest", "")
        stream = bool(payload.get("stream", False))
        is_ollama = path.startswith("/api/")
        if path == "/api/generate":
            messages = []
            if payload.get("system"):
                messages.append({"role": "system", "content": payload["system"]})
            messages.append({"role": "user", "content": payload.get("prompt", "")})
        else:
            messages = payload.get("messages") or []
        tools = payload.get("tools")
        ua = headers.get("user-agent", "").split("/")[0].strip()
        bridge.api_last_client = ua or "External Agent"
        compiled_prompt = _compile_api_prompt(messages, tools)
        log(f"[local-api] {path} client={bridge.api_last_client} model={model} stream={stream} msgs={len(messages)} tools={len(tools) if tools else 0}")
        try:
            raw_reply = await bridge.request_browser_completion(compiled_prompt, model=model)
            bridge.api_requests_handled += 1
        except Exception as err:
            log(f"[local-api] error: {err}")
            await send_json(502, {"error": {"message": str(err), "type": "bridge_error"}})
            return

        content, tool_calls = _parse_api_response(raw_reply)
        created = int(time.time())
        chat_id = f"chatcmpl-snapgent-{created}"

        if is_ollama:
            ollama_msg: Dict[str, Any] = {"role": "assistant", "content": content or ""}
            if tool_calls:
                ollama_msg["tool_calls"] = [
                    {"function": {"name": tc["function"]["name"], "arguments": json.loads(tc["function"]["arguments"])}}
                    for tc in tool_calls
                ]
            if stream:
                chunk = json.dumps({
                    "model": model,
                    "created_at": "2026-01-01T00:00:00Z",
                    "message": ollama_msg,
                    "response": content or "",
                    "done": True,
                    "done_reason": "stop",
                }, ensure_ascii=False) + "\n"
                raw_chunk = chunk.encode("utf-8")
                writer.write((
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/x-ndjson\r\n"
                    "Access-Control-Allow-Origin: *\r\n"
                    f"Content-Length: {len(raw_chunk)}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("utf-8") + raw_chunk)
                await writer.drain()
                writer.close()
                return
            await send_json(200, {
                "model": model,
                "created_at": "2026-01-01T00:00:00Z",
                "message": ollama_msg,
                "response": content or "",
                "done": True,
                "done_reason": "stop",
            })
            return

        if stream:
            delta: Dict[str, Any] = {"role": "assistant"}
            if tool_calls:
                delta["tool_calls"] = [{**tc, "index": i} for i, tc in enumerate(tool_calls)]
            else:
                delta["content"] = content or ""
            c1 = f"data: {json.dumps({'id': chat_id, 'object': 'chat.completion.chunk', 'created': created, 'model': model, 'choices': [{'index': 0, 'delta': delta, 'finish_reason': None}]}, ensure_ascii=False)}\n\n"
            c2 = f"data: {json.dumps({'id': chat_id, 'object': 'chat.completion.chunk', 'created': created, 'model': model, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'tool_calls' if tool_calls else 'stop'}]}, ensure_ascii=False)}\n\n"
            c3 = "data: [DONE]\n\n"
            sse_bytes = (c1 + c2 + c3).encode("utf-8")
            writer.write((
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream; charset=utf-8\r\n"
                "Cache-Control: no-cache\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                f"Content-Length: {len(sse_bytes)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("utf-8") + sse_bytes)
            await writer.drain()
            writer.close()
            return

        msg_obj: Dict[str, Any] = {"role": "assistant", "content": content}
        if tool_calls:
            msg_obj["tool_calls"] = tool_calls
        await send_json(200, {
            "id": chat_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": msg_obj,
                "finish_reason": "tool_calls" if tool_calls else "stop",
            }],
            "usage": {"prompt_tokens": len(compiled_prompt) // 4, "completion_tokens": len(raw_reply) // 4, "total_tokens": (len(compiled_prompt) + len(raw_reply)) // 4},
        })
        return

    await send_json(404, {"error": {"message": f"Not found: {path}"}})


async def run() -> None:
    bridge = Bridge()
    active_ws_port = PORT
    server = None
    for offset in range(25):
        candidate = PORT + offset
        try:
            server = await asyncio.start_server(
                bridge.handle_stream,
                HOST,
                candidate,
            )
            active_ws_port = candidate
            break
        except OSError:
            log(f"[port-check] WS port {candidate} busy, trying {candidate + 1}...")
    if server is None:
        raise RuntimeError(f"Could not bind any WS port starting at {PORT}")

    active_api_port = API_PORT
    api_server = None
    for offset in range(25):
        candidate = API_PORT + offset
        try:
            api_server = await asyncio.start_server(
                lambda r, w: _handle_http_client(bridge, r, w),
                HOST,
                candidate,
            )
            active_api_port = candidate
            break
        except OSError:
            log(f"[port-check] API port {candidate} busy, trying {candidate + 1}...")
    if api_server is None:
        raise RuntimeError(f"Could not bind any HTTP API port starting at {API_PORT}")

    try:
        if RUNTIME_PORTS_PATH.parent.is_dir():
            RUNTIME_PORTS_PATH.write_text(
                json.dumps({"bridge_port": active_ws_port, "api_port": active_api_port}, indent=2) + "\n",
                encoding="utf-8",
            )
    except Exception as exc:
        log(f"[warn] failed to write runtime_ports.json: {exc}")

    log(f"snapgent bridge v{BRIDGE_VERSION} (zero-dependency pure Python)")
    log(f"listening on ws://{HOST}:{active_ws_port} - load the extension and open a supported AI chat")
    log(f"Local AI API (OpenAI & Ollama compatible) listening on http://{HOST}:{active_api_port}/v1")
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    async def _startup_then_monitor() -> None:
        try:
            await bridge.start_servers()
        except RuntimeError as error:
            log(f"config error: {error}")
        await _monitor_servers(bridge)

    monitor_task = asyncio.create_task(_startup_then_monitor())
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await stop_event.wait()
    finally:
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)
        server.close()
        await server.wait_closed()
        api_server.close()
        await api_server.wait_closed()
        await bridge.stop_servers()
        log("bridge stopped")


async def _monitor_servers(bridge: Bridge) -> None:
    while True:
        await asyncio.sleep(5)
        changed = False
        for server in bridge.servers.values():
            if not server.alive:
                log(f"[{server.server_id}] MCP server disconnected; restarting.")
                await server.start()
                changed = True
        if changed:
            bridge._rebuild_catalog()
            bridge._log_health()
            await bridge.broadcast(bridge._connected_message())


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (OSError, RuntimeError, ValueError) as error:
        log(f"FATAL: {error}")
        raise SystemExit(1)
