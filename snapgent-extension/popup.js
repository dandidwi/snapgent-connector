// SPDX-License-Identifier: GPL-3.0-or-later
const SUPPORTED_HOSTS = [
  "chat.deepseek.com", "deepseek.com", "chatgpt.com", "chat.openai.com", "claude.ai",
  "gemini.google.com", "www.kimi.ai", "kimi.ai",
  "chat.z.ai", "chat.qwen.ai", "arena.ai", "www.meta.ai", "meta.ai", "ptero.pro", "grok.com", "chat.mistral.ai",
];
const DEFAULT_AI_URL = "https://gemini.google.com/app";

function render(s) {
  const state = document.getElementById("state");
  const tools = document.getElementById("tools");
  const servers = document.getElementById("servers");
  const list = s.servers || [];
  const dot = document.getElementById("dot");

  dot.className = "dot " + (s.connected ? "ok" : "bad");
  state.textContent = s.connected ? "Bridge & Local API Ready" : "Bridge offline";

  const ver = document.getElementById("ver");
  const extVer = chrome.runtime.getManifest().version;
  ver.textContent = !s.connected ? `v${extVer}`
    : s.bridgeVersion ? `v${extVer} - bridge v${s.bridgeVersion}`
    : `v${extVer}`;

  chrome.tabs.query({}, (tabs) => {
    const aiTabs = (tabs || []).filter((t) => t.url && SUPPORTED_HOSTS.some((h) => t.url.includes(h)));
    if (!s.connected) {
      tools.textContent = "Run start.bat to start Local API server";
    } else if (aiTabs.length === 0) {
      tools.textContent = "Open Gemini, ChatGPT, Claude, or DeepSeek tab";
    } else {
      const names = aiTabs.map((t) => {
        try { return new URL(t.url).hostname.replace(/^www\./, ""); } catch { return "AI Tab"; }
      });
      tools.textContent = `${aiTabs.length} AI worker tab(s): ${[...new Set(names)].join(", ")}`;
    }
  });

  servers.textContent = s.connected && list.length
    ? list.map((x) => `${x.alive ? "[ON]" : "[OFF]"} MCP ${x.id} (${x.alive ? x.tools + " tools" : "down"})`).join("\n")
    : "";
}

let activeApiPort = 11434;

async function syncRuntimePorts() {
  try {
    const res = await fetch(chrome.runtime.getURL("runtime_ports.json"), { cache: "no-store" });
    if (res.ok) {
      const data = await res.json();
      if (data && Number.isInteger(data.api_port)) {
        activeApiPort = data.api_port;
        const inp = document.getElementById("api-url");
        if (inp) inp.value = `http://127.0.0.1:${activeApiPort}/v1`;
      }
    }
  } catch {}
}

async function checkApiServer() {
  await syncRuntimePorts();
  const box = document.getElementById("api-activity");
  if (!box) return;
  try {
    const res = await fetch(`http://127.0.0.1:${activeApiPort}/`, { cache: "no-store" });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();
    const reqCount = data.requests_handled || 0;
    const lastAgent = data.last_client || "None yet";
    const busy = data.busy ? "[BUSY] Processing external agent request..." : `[READY] Listening on port ${activeApiPort}`;
    box.textContent = `${busy}\nRequests handled: ${reqCount} | Last client: ${lastAgent}`;
  } catch {
    box.textContent = `[OFFLINE] Port ${activeApiPort} offline (run start.bat)`;
  }
}

function refresh() {
  chrome.runtime.sendMessage({ type: "status" }, (s) => s && render(s));
  checkApiServer();
}

document.getElementById("copy-url").addEventListener("click", () => {
  const inp = document.getElementById("api-url");
  const btn = document.getElementById("copy-url");
  navigator.clipboard.writeText(inp.value).catch(() => {
    inp.select();
    document.execCommand("copy");
  });
  btn.textContent = "Copied!";
  setTimeout(() => { btn.textContent = "Copy"; }, 1400);
});

document.getElementById("reconnect").addEventListener("click", () => {
  chrome.runtime.sendMessage({ type: "reconnect" }, () => setTimeout(refresh, 600));
});
document.getElementById("restart").addEventListener("click", (e) => {
  e.target.textContent = "Restarting...";
  chrome.runtime.sendMessage({ type: "restart_mcp" }, () => {
    e.target.textContent = "Restart";
    setTimeout(refresh, 600);
  });
});
document.getElementById("settings").addEventListener("click", () => {
  chrome.tabs.query({}, (tabs) => {
    const active = tabs.find((t) => t.active && t.url && SUPPORTED_HOSTS.some((h) => t.url.includes(h)));
    const anySupported = active || tabs.find((t) => t.url && SUPPORTED_HOSTS.some((h) => t.url.includes(h)));
    if (anySupported) {
      chrome.tabs.sendMessage(anySupported.id, { type: "zs-open-menu" });
      chrome.tabs.update(anySupported.id, { active: true });
    } else {
      chrome.tabs.create({ url: DEFAULT_AI_URL });
    }
  });
});

chrome.runtime.onMessage.addListener((msg) => {
  if (msg && msg.type === "zs-status") render(msg);
});
refresh();
setInterval(refresh, 2000);
