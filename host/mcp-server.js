#!/usr/bin/env node

// MCP Server for Open Claude in Chrome extension.
// Started by Claude Code via stdio MCP transport.
//
// Operates in one of two modes:
// - PRIMARY: Owns the TCP port, accepts native host + client connections
// - CLIENT: Port already taken by another session, connects as a client
//
// This allows multiple Claude Code sessions to share one browser extension.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import net from "node:net";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { z } from "zod";


const DEFAULT_PORT = 18765;

function getPort() {
  const configPath = path.join(os.homedir(), ".config", "open-claude-in-chrome", "config.json");
  try {
    const config = JSON.parse(fs.readFileSync(configPath, "utf-8"));
    return config.port || DEFAULT_PORT;
  } catch {
    return DEFAULT_PORT;
  }
}

const TCP_PORT = getPort();

// --- Mode detection ---
// Try to bind the port. If it's taken, switch to client mode.
let mode = "primary"; // or "client"

// --- Shared state ---
let nativeHostSocket = null;
const pendingRequests = new Map(); // id -> { resolve, reject, timer, tool, args, resent }
let requestIdCounter = 0;

// Primary mode: track client MCP server connections
const clientSockets = new Map(); // clientId -> socket
let clientIdCounter = 0;
// Map from prefixed request ID -> { clientId, originalId }
const clientRequestMap = new Map();

// Client mode: TCP connection to the primary
let primarySocket = null;
let clientBuffer = Buffer.alloc(0);

// --- sendToExtension: works in both modes ---

function sendToExtension(tool, args) {
  return new Promise((resolve, reject) => {
    const id = String(++requestIdCounter);
    const timer = setTimeout(() => {
      pendingRequests.delete(id);
      reject(new Error("Tool request timed out after 60s"));
    }, 60000);
    pendingRequests.set(id, { resolve, reject, timer, tool, args, resent: false });

    if (mode === "primary") {
      if (!nativeHostSocket || nativeHostSocket.destroyed) {
        clearTimeout(timer);
        pendingRequests.delete(id);
        reject(new Error("Browser extension is not connected. Make sure a supported Chromium browser is running with the Open Claude in Chrome extension installed and enabled."));
        return;
      }
      const msg = JSON.stringify({ id, type: "tool_request", tool, args }) + "\n";
      nativeHostSocket.write(msg);
    } else {
      // Client mode: send to primary server
      if (!primarySocket || primarySocket.destroyed) {
        clearTimeout(timer);
        pendingRequests.delete(id);
        reject(new Error("Lost connection to primary MCP server."));
        return;
      }
      const msg = JSON.stringify({ id, type: "tool_request", tool, args }) + "\n";
      primarySocket.write(msg);
    }
  });
}

// --- Pidfile management ---

const pidfilePath = path.join(os.tmpdir(), `open-claude-in-chrome-mcp-${TCP_PORT}.pid`);

function writePidfile() {
  try { fs.writeFileSync(pidfilePath, String(process.pid)); } catch {}
}

function cleanupPidfile() {
  try {
    const content = fs.readFileSync(pidfilePath, "utf-8").trim();
    if (content === String(process.pid)) fs.unlinkSync(pidfilePath);
  } catch {}
}

function shutdown() {
  if (mode === "primary") cleanupPidfile();
  if (nativeHostSocket && !nativeHostSocket.destroyed) nativeHostSocket.destroy();
  if (primarySocket && !primarySocket.destroyed) primarySocket.destroy();
  for (const [, sock] of clientSockets) {
    if (!sock.destroyed) sock.destroy();
  }
  for (const [, { reject, timer }] of pendingRequests) {
    clearTimeout(timer);
    reject(new Error("Server shutting down"));
  }
  pendingRequests.clear();
  if (mode === "primary") tcpServer.close();
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("SIGHUP", shutdown);
process.stdin.on("end", shutdown);
process.stdin.resume();

// --- Primary mode: handle incoming TCP connections ---

function handleResponse(msg) {
  // Check if this response is for a client request (prefixed ID)
  if (msg.id && clientRequestMap.has(msg.id)) {
    const { clientId, originalId } = clientRequestMap.get(msg.id);
    clientRequestMap.delete(msg.id);
    const clientSocket = clientSockets.get(clientId);
    if (clientSocket && !clientSocket.destroyed) {
      const fwd = JSON.stringify({ ...msg, id: originalId }) + "\n";
      clientSocket.write(fwd);
    }
    return;
  }

  // Otherwise it's for a local request
  if (msg.id && pendingRequests.has(msg.id)) {
    const { resolve, reject, timer } = pendingRequests.get(msg.id);
    clearTimeout(timer);
    pendingRequests.delete(msg.id);
    if (msg.type === "tool_error") {
      reject(new Error(msg.error || "Tool execution failed"));
    } else {
      resolve(msg.result);
    }
  }
}

function processLine(line) {
  if (!line) return;
  try {
    const msg = JSON.parse(line);
    if (msg.type === "heartbeat") return;
    handleResponse(msg);
  } catch {}
}

const tcpServer = net.createServer((socket) => {
  // Classification: wait briefly for a client_hello. If none arrives, treat as native host.
  // Native hosts (launched by the browser) don't send data immediately on connect.
  // Client MCP servers send client_hello immediately.
  let classified = false;
  let earlyBuffer = Buffer.alloc(0);

  const classifyTimeout = setTimeout(() => {
    if (!classified) {
      classified = true;
      setupNativeHostConnection(socket, earlyBuffer);
    }
  }, 500); // 500ms is plenty for a local client_hello

  socket.on("data", function onEarlyData(chunk) {
    if (classified) return; // Already classified, data handler was replaced
    earlyBuffer = Buffer.concat([earlyBuffer, chunk]);
    const newlineIdx = earlyBuffer.indexOf(10);
    if (newlineIdx === -1) return; // No full line yet, keep buffering

    const firstLine = earlyBuffer.subarray(0, newlineIdx).toString("utf-8").trim();
    try {
      const firstMsg = JSON.parse(firstLine);
      if (firstMsg.type === "client_hello") {
        classified = true;
        clearTimeout(classifyTimeout);
        socket.removeListener("data", onEarlyData);
        setupClientConnection(socket, earlyBuffer.subarray(newlineIdx + 1));
        return;
      }
    } catch {}

    // Got data but it's not a client_hello, this is a native host
    classified = true;
    clearTimeout(classifyTimeout);
    socket.removeListener("data", onEarlyData);
    setupNativeHostConnection(socket, earlyBuffer);
  });
});

function setupNativeHostConnection(socket, initialBuffer) {
  if (nativeHostSocket && !nativeHostSocket.destroyed) {
    // Already have a native host. Reject.
    socket.end(JSON.stringify({ type: "error", error: "Another browser profile is already connected." }) + "\n");
    socket.destroy();
    return;
  }

  nativeHostSocket = socket;
  let buffer = initialBuffer;

  // Process any data already in the buffer
  let idx;
  while ((idx = buffer.indexOf(10)) !== -1) {
    processLine(buffer.subarray(0, idx).toString("utf-8").trim());
    buffer = buffer.subarray(idx + 1);
  }

  socket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    let newlineIdx;
    while ((newlineIdx = buffer.indexOf(10)) !== -1) {
      processLine(buffer.subarray(0, newlineIdx).toString("utf-8").trim());
      buffer = buffer.subarray(newlineIdx + 1);
    }
  });

  socket.on("error", () => { nativeHostSocket = null; });

  socket.on("close", () => {
    if (nativeHostSocket === socket) nativeHostSocket = null;
    if (pendingRequests.size > 0) {
      setTimeout(() => {
        if (nativeHostSocket && !nativeHostSocket.destroyed) {
          for (const [id, entry] of pendingRequests) {
            if (entry.resent) continue;
            entry.resent = true;
            nativeHostSocket.write(JSON.stringify({ id, type: "tool_request", tool: entry.tool, args: entry.args }) + "\n");
          }
        } else {
          for (const [, { reject, timer }] of pendingRequests) {
            clearTimeout(timer);
            reject(new Error("Native host disconnected"));
          }
          pendingRequests.clear();
        }
      }, 5000);
    }
  });
}

function setupClientConnection(socket, initialBuffer) {
  const clientId = String(++clientIdCounter);
  clientSockets.set(clientId, socket);
  process.stderr.write(`Client MCP server connected (client ${clientId})\n`);

  // Send ack
  socket.write(JSON.stringify({ type: "client_ack", clientId }) + "\n");

  let buffer = initialBuffer;

  function processClientData() {
    let idx;
    while ((idx = buffer.indexOf(10)) !== -1) {
      const line = buffer.subarray(0, idx).toString("utf-8").trim();
      buffer = buffer.subarray(idx + 1);
      if (!line) continue;
      try {
        const msg = JSON.parse(line);
        if (msg.type === "tool_request" && msg.id) {
          // Forward to native host with a prefixed ID
          const prefixedId = `c${clientId}_${msg.id}`;
          clientRequestMap.set(prefixedId, { clientId, originalId: msg.id });

          if (!nativeHostSocket || nativeHostSocket.destroyed) {
            // Send error back to client
            socket.write(JSON.stringify({ id: msg.id, type: "tool_error", error: "Browser extension is not connected." }) + "\n");
            clientRequestMap.delete(prefixedId);
          } else {
            nativeHostSocket.write(JSON.stringify({ ...msg, id: prefixedId }) + "\n");
          }
        }
      } catch {}
    }
  }

  // Process initial buffer
  processClientData();

  socket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    processClientData();
  });

  socket.on("error", () => {});
  socket.on("close", () => {
    clientSockets.delete(clientId);
    // Clean up any pending client requests
    for (const [prefixedId, info] of clientRequestMap) {
      if (info.clientId === clientId) clientRequestMap.delete(prefixedId);
    }
    process.stderr.write(`Client MCP server disconnected (client ${clientId})\n`);
  });
}

// --- Client mode: connect to primary ---

function startClientMode() {
  mode = "client";
  process.stderr.write(`Port ${TCP_PORT} in use. Connecting as client to primary MCP server...\n`);

  function connect() {
    primarySocket = net.createConnection(TCP_PORT, "127.0.0.1", () => {
      process.stderr.write(`Connected to primary MCP server on :${TCP_PORT}\n`);
      // Send handshake
      primarySocket.write(JSON.stringify({ type: "client_hello" }) + "\n");
    });

    primarySocket.on("data", (chunk) => {
      clientBuffer = Buffer.concat([clientBuffer, chunk]);
      let idx;
      while ((idx = clientBuffer.indexOf(10)) !== -1) {
        const line = clientBuffer.subarray(0, idx).toString("utf-8").trim();
        clientBuffer = clientBuffer.subarray(idx + 1);
        if (!line) continue;
        try {
          const msg = JSON.parse(line);
          if (msg.type === "client_ack") continue;
          if (msg.type === "error") {
            process.stderr.write(`Primary server error: ${msg.error}\n`);
            continue;
          }
          // Tool response routed back from primary
          if (msg.id && pendingRequests.has(msg.id)) {
            const { resolve, reject, timer } = pendingRequests.get(msg.id);
            clearTimeout(timer);
            pendingRequests.delete(msg.id);
            if (msg.type === "tool_error") {
              reject(new Error(msg.error || "Tool execution failed"));
            } else {
              resolve(msg.result);
            }
          }
        } catch {}
      }
    });

    primarySocket.on("error", (err) => {
      process.stderr.write(`Client connection error: ${err.message}\n`);
    });

    primarySocket.on("close", () => {
      primarySocket = null;
      // Primary died, reject pending requests
      for (const [, { reject, timer }] of pendingRequests) {
        clearTimeout(timer);
        reject(new Error("Primary MCP server disconnected"));
      }
      pendingRequests.clear();
      // Try to reconnect after a delay (primary might restart)
      setTimeout(connect, 2000);
    });
  }

  connect();
}

// --- Startup: try primary, fall back to client ---

async function start() {
  // Clean up stale pidfiles (but don't kill live servers)
  const pidfiles = [
    pidfilePath,
    path.join(os.tmpdir(), `unblocked-chrome-mcp-${TCP_PORT}.pid`),
  ];
  for (const pf of pidfiles) {
    try {
      const oldPid = parseInt(fs.readFileSync(pf, "utf-8").trim(), 10);
      if (oldPid && oldPid !== process.pid) {
        try {
          process.kill(oldPid, 0); // Check if alive
          // It's alive. DON'T kill it. We'll run as client instead.
        } catch {
          // Dead process, clean up pidfile
          try { fs.unlinkSync(pf); } catch {}
        }
      }
    } catch {}
  }

  // Try to bind the port
  return new Promise((resolve) => {
    tcpServer.once("error", (err) => {
      if (err.code === "EADDRINUSE") {
        // Port taken by another live session. Run as client.
        startClientMode();
        resolve();
      } else {
        process.stderr.write(`TCP server error: ${err.message}\n`);
        process.exit(1);
      }
    });

    tcpServer.listen(TCP_PORT, "127.0.0.1", () => {
      mode = "primary";
      writePidfile();
      process.stderr.write(`Primary MCP server listening on :${TCP_PORT}\n`);
      resolve();
    });
  });
}

await start();

// --- Helper to wrap tool results for MCP ---

function textResult(text) {
  return { content: [{ type: "text", text }] };
}

function imageResult(base64, mimeType = "image/png") {
  return { content: [{ type: "image", data: base64, mimeType }] };
}

function mixedResult(parts) {
  return { content: parts };
}

async function callTool(toolName, args) {
  try {
    const result = await sendToExtension(toolName, args);
    if (typeof result === "string") return textResult(result);
    if (result && result.content) return result;
    return textResult(JSON.stringify(result, null, 2));
  } catch (err) {
    return textResult(`Error: ${err.message}`);
  }
}

// --- MCP Server with 13 consolidated tools ---
// NOTE: Intentionally kept at 13 tools (down from 19) to stay under Claude Code's tool-search
// deferral threshold. When Claude Code has too many tools, it activates "Tool Search" mode,
// which generates `tool_reference` content blocks. These blocks are rejected with a 400 error
// by proxies, AWS Bedrock, Vertex AI, and older model versions. Keeping tool count low
// prevents this. (Fixes Issue #10: API Error "Unexpected content chunk type tool_reference")
//
// Consolidations made (19 → 13):
//   tabs_context_mcp + tabs_create_mcp   → tabs_mcp (action: "context"|"create")
//   read_console_messages + read_network_requests → read_debug (type: "console"|"network")
//   shortcuts_list + shortcuts_execute   → shortcuts (action: "list"|"execute")
//   upload_image + upload_local_file     → upload (type: "image"|"file")
//   get_page_text + resize_window        → merged into read_page (format="text", width+height)

const server = new McpServer({
  name: "open-claude-in-chrome",
  version: "1.0.0",
});

// Pre-validation arg coercion
{
  const origSetRequestHandler = server.server.setRequestHandler.bind(server.server);
  server.server.setRequestHandler = function(schema, handler) {
    return origSetRequestHandler(schema, async (request, extra) => {
      const args = request?.params?.arguments;
      if (args) {
        if (typeof args.tabId === "string") args.tabId = Number(args.tabId);
        if (typeof args.coordinate === "string") {
          try { args.coordinate = JSON.parse(args.coordinate); } catch {}
        }
        if (typeof args.start_coordinate === "string") {
          try { args.start_coordinate = JSON.parse(args.start_coordinate); } catch {}
        }
        if (typeof args.region === "string") {
          try { args.region = JSON.parse(args.region); } catch {}
        }
      }
      return handler(request, extra);
    });
  };
}

// 1. tabs_mcp — consolidated tabs_context_mcp + tabs_create_mcp
server.tool(
  "tabs_mcp",
  'Manage MCP tab group. CRITICAL: Call with action="context" at least once before using other browser tools to get valid tab IDs. Each new conversation should create its own tab with action="create" rather than reusing existing tabs.',
  {
    action: z.enum(["context", "create"]).describe(
      'Action to perform: "context" — get the current MCP tab group info and all tab IDs (use createIfEmpty to auto-create if none exists); "create" — create a new empty tab in the MCP tab group.'
    ),
    createIfEmpty: z.boolean().optional().describe(
      'For action="context" only: if no MCP tab group exists, create a new window with a tab group. Has no effect if a group already exists.'
    ),
  },
  async (args) => {
    if (args.action === "create") return callTool("tabs_create_mcp", args);
    // action === "context"
    return callTool("tabs_context_mcp", args);
  }
);

// 2. navigate
server.tool(
  "navigate",
  'Navigate to a URL, or go forward/back in browser history. If you don\'t have a valid tab ID, use tabs_mcp first to get available tabs.',
  {
    url: z.string().describe('The URL to navigate to. Can be provided with or without protocol (defaults to https://). Use "forward" to go forward in history or "back" to go back in history.'),
    tabId: z.number().describe("Tab ID to navigate. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
  },
  async (args) => callTool("navigate", args)
);

// 3. computer
server.tool(
  "computer",
  "Use a mouse and keyboard to interact with a web browser, and take screenshots. If you don't have a valid tab ID, use tabs_mcp first to get available tabs.\n* Whenever you intend to click on an element like an icon, you should consult a screenshot to determine the coordinates of the element before moving the cursor.\n* If you tried clicking on a program or link but it failed to load, even after waiting, try adjusting your click location so that the tip of the cursor visually falls on the element that you want to click.\n* Make sure to click any buttons, links, icons, etc with the cursor tip in the center of the element. Don't click boxes on their edges unless asked.",
  {
    action: z.enum([
      "left_click", "right_click", "double_click", "triple_click",
      "type", "screenshot", "wait", "scroll", "key",
      "left_click_drag", "zoom", "scroll_to", "hover"
    ]).describe('The action to perform:\n* `left_click`: Click the left mouse button at the specified coordinates.\n* `right_click`: Click the right mouse button at the specified coordinates to open context menus.\n* `double_click`: Double-click the left mouse button at the specified coordinates.\n* `triple_click`: Triple-click the left mouse button at the specified coordinates.\n* `type`: Type a string of text.\n* `screenshot`: Take a screenshot of the screen.\n* `wait`: Wait for a specified number of seconds.\n* `scroll`: Scroll up, down, left, or right at the specified coordinates.\n* `key`: Press a specific keyboard key.\n* `left_click_drag`: Drag from start_coordinate to coordinate.\n* `zoom`: Take a screenshot of a specific region for closer inspection.\n* `scroll_to`: Scroll an element into view using its element reference ID from read_page or find tools.\n* `hover`: Move the mouse cursor to the specified coordinates or element without clicking. Useful for revealing tooltips, dropdown menus, or triggering hover states.'),
    tabId: z.number().describe("Tab ID to execute the action on. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
    coordinate: z.array(z.number()).min(2).max(2).optional().describe("(x, y): The x (pixels from the left edge) and y (pixels from the top edge) coordinates. Required for `left_click`, `right_click`, `double_click`, `triple_click`, and `scroll`. For `left_click_drag`, this is the end position."),
    duration: z.number().min(0).max(30).optional().describe("The number of seconds to wait. Required for `wait`. Maximum 30 seconds."),
    modifiers: z.string().optional().describe('Modifier keys for click actions. Supports: "ctrl", "shift", "alt", "cmd" (or "meta"), "win" (or "windows"). Can be combined with "+" (e.g., "ctrl+shift", "cmd+alt"). Optional.'),
    ref: z.string().optional().describe('Element reference ID from read_page or find tools (e.g., "ref_1", "ref_2"). Required for `scroll_to` action. Can be used as alternative to `coordinate` for click actions.'),
    region: z.array(z.number()).min(4).max(4).optional().describe("(x0, y0, x1, y1): The rectangular region to capture for `zoom`. Coordinates define a rectangle from top-left (x0, y0) to bottom-right (x1, y1) in pixels from the viewport origin. Required for `zoom` action. Useful for inspecting small UI elements like icons, buttons, or text."),
    repeat: z.number().min(1).max(100).optional().describe("Number of times to repeat the key sequence. Only applicable for `key` action. Must be a positive integer between 1 and 100. Default is 1. Useful for navigation tasks like pressing arrow keys multiple times."),
    scroll_direction: z.enum(["up", "down", "left", "right"]).optional().describe("The direction to scroll. Required for `scroll`."),
    scroll_amount: z.number().min(1).max(10).optional().describe("The number of scroll wheel ticks. Optional for `scroll`, defaults to 3."),
    start_coordinate: z.array(z.number()).min(2).max(2).optional().describe("(x, y): The starting coordinates for `left_click_drag`."),
    text: z.string().optional().describe('The text to type (for `type` action) or the key(s) to press (for `key` action). For `key` action: Provide space-separated keys (e.g., "Backspace Backspace Delete"). Supports keyboard shortcuts using the platform\'s modifier key (use "cmd" on Mac, "ctrl" on Windows/Linux, e.g., "cmd+a" or "ctrl+a" for select all).'),
  },
  async (args) => callTool("computer", args)
);

// 4. find
server.tool(
  "find",
  'Find elements on the page using natural language. Can search for elements by their purpose (e.g., "search bar", "login button") or by text content (e.g., "organic mango product"). Returns up to 20 matching elements with references that can be used with other tools. If more than 20 matches exist, you\'ll be notified to use a more specific query. If you don\'t have a valid tab ID, use tabs_mcp first to get available tabs.',
  {
    query: z.string().describe('Natural language description of what to find (e.g., "search bar", "add to cart button", "product title containing organic")'),
    tabId: z.number().describe("Tab ID to search in. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
  },
  async (args) => callTool("find", args)
);

// 5. form_input
server.tool(
  "form_input",
  "Set values in form elements using element reference ID from the read_page tool. If you don't have a valid tab ID, use tabs_mcp first to get available tabs.",
  {
    ref: z.string().describe('Element reference ID from the read_page tool (e.g., "ref_1", "ref_2")'),
    value: z.union([z.string(), z.boolean(), z.number()]).describe("The value to set. For checkboxes use boolean, for selects use option value or text, for other inputs use appropriate string/number"),
    tabId: z.number().describe("Tab ID to set form value in. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
  },
  async (args) => callTool("form_input", args)
);

// 7. gif_creator
server.tool(
  "gif_creator",
  "Manage GIF recording and export for browser automation sessions. Control when to start/stop recording browser actions (clicks, scrolls, navigation), then export as an animated GIF with visual overlays (click indicators, action labels, progress bar, watermark). All operations are scoped to the tab's group. When starting recording, take a screenshot immediately after to capture the initial state as the first frame. When stopping recording, take a screenshot immediately before to capture the final state as the last frame. For export, either provide 'coordinate' to drag/drop upload to a page element, or set 'download: true' to download the GIF.",
  {
    action: z.enum(["start_recording", "stop_recording", "export", "clear"]).describe("Action to perform: 'start_recording' (begin capturing), 'stop_recording' (stop capturing but keep frames), 'export' (generate and export GIF), 'clear' (discard frames)"),
    tabId: z.number().describe("Tab ID to identify which tab group this operation applies to"),
    download: z.boolean().optional().describe("Always set this to true for the 'export' action only. This causes the gif to be downloaded in the browser."),
    filename: z.string().optional().describe("Optional filename for exported GIF (default: 'recording-[timestamp].gif'). For 'export' action only."),
    options: z.object({
      showClickIndicators: z.boolean().optional().describe("Show orange circles at click locations (default: true)"),
      showDragPaths: z.boolean().optional().describe("Show red arrows for drag actions (default: true)"),
      showActionLabels: z.boolean().optional().describe("Show black labels describing actions (default: true)"),
      showProgressBar: z.boolean().optional().describe("Show orange progress bar at bottom (default: true)"),
      showWatermark: z.boolean().optional().describe("Show Claude logo watermark (default: true)"),
      quality: z.number().optional().describe("GIF compression quality, 1-30 (lower = better quality, slower encoding). Default: 10"),
    }).optional().describe("Optional GIF enhancement options for 'export' action. Properties: showClickIndicators (bool), showDragPaths (bool), showActionLabels (bool), showProgressBar (bool), showWatermark (bool), quality (number 1-30). All default to true except quality (default: 10)."),
  },
  async (args) => callTool("gif_creator", args)
);

// 8. javascript_tool
server.tool(
  "javascript_tool",
  "Execute JavaScript code in the context of the current page. The code runs in the page's context and can interact with the DOM, window object, and page variables. Returns the result of the last expression or any thrown errors. If you don't have a valid tab ID, use tabs_mcp first to get available tabs.",
  {
    action: z.literal("javascript_exec").describe("Must be set to 'javascript_exec'"),
    text: z.string().describe("The JavaScript code to execute. The code will be evaluated in the page context. The result of the last expression will be returned automatically. Do NOT use 'return' statements - just write the expression you want to evaluate (e.g., 'window.myData.value' not 'return window.myData.value'). You can access and modify the DOM, call page functions, and interact with page variables."),
    tabId: z.number().describe("Tab ID to execute the code in. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
  },
  async (args) => callTool("javascript_tool", args)
);

// 9. read_debug — consolidated read_console_messages + read_network_requests
server.tool(
  "read_debug",
  'Read browser debug information from a tab: console messages (console.log/error/warn) or network requests (XHR, Fetch, etc.). If you don\'t have a valid tab ID, use tabs_mcp first to get available tabs.',
  {
    type: z.enum(["console", "network"]).describe(
      '"console" — read browser console messages (console.log, errors, warnings). "network" — read HTTP network requests made by the page (XHR, Fetch, documents, images, etc.).'
    ),
    tabId: z.number().describe("Tab ID to read from. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
    // Console-specific
    pattern: z.string().optional().describe('For type="console": Regex pattern to filter console messages (e.g., "error|warning"). Always provide a pattern to avoid too many irrelevant messages.'),
    onlyErrors: z.boolean().optional().describe('For type="console": If true, only return error and exception messages.'),
    // Network-specific
    urlPattern: z.string().optional().describe('For type="network": URL substring filter. Only requests whose URL contains this string will be returned (e.g., "/api/" or "example.com").'),
    // Shared
    limit: z.number().optional().describe("Maximum number of entries to return. Defaults to 100."),
    clear: z.boolean().optional().describe("If true, clear the entries after reading to avoid duplicates on subsequent calls. Default is false."),
  },
  async (args) => {
    if (args.type === "network") {
      const { type: _t, ...rest } = args;
      return callTool("read_network_requests", rest);
    }
    const { type: _t, urlPattern: _u, ...rest } = args;
    return callTool("read_console_messages", rest);
  }
);

// 10. read_page — also handles get_page_text (format="text") and resize_window via width/height
server.tool(
  "read_page",
  'Read the current page. By default returns an accessibility tree of all elements. Use format="text" to extract plain text (ideal for articles/blogs). Use width+height to resize the window first. If you don\'t have a valid tab ID, use tabs_mcp first.',
  {
    tabId: z.number().describe("Tab ID to read from. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
    format: z.enum(["tree", "text"]).optional().describe(
      '"tree" (default) — accessibility tree of page elements with refs. "text" — extract raw text content, prioritizing article body. Use "text" for reading articles or blogs.'
    ),
    filter: z.enum(["interactive", "all"]).optional().describe('For format="tree": "interactive" for buttons/links/inputs only, "all" for all elements (default).'),
    depth: z.number().optional().describe('For format="tree": Max tree depth (default: 15). Reduce if output is too large.'),
    ref_id: z.string().optional().describe('For format="tree": Focus on a specific element and its children. Use when output is too large.'),
    max_chars: z.number().optional().describe('For format="tree": Max output characters (default: 50000).'),
    width: z.number().optional().describe("If provided along with height, resize the browser window to these dimensions before reading."),
    height: z.number().optional().describe("If provided along with width, resize the browser window to these dimensions before reading."),
  },
  async (args) => {
    // Handle resize first if requested
    if (args.width && args.height) {
      await callTool("resize_window", { tabId: args.tabId, width: args.width, height: args.height });
    }
    if (args.format === "text") {
      const { format: _f, filter: _fi, depth: _d, ref_id: _r, max_chars: _m, width: _w, height: _h, ...rest } = args;
      return callTool("get_page_text", rest);
    }
    const { format: _f, width: _w, height: _h, ...rest } = args;
    return callTool("read_page", rest);
  }
);

// 12. shortcuts — consolidated shortcuts_list + shortcuts_execute
server.tool(
  "shortcuts",
  'List or execute available shortcuts and workflows. Use action="list" first to see what is available, then action="execute" to run one. If you don\'t have a valid tab ID, use tabs_mcp first to get available tabs.',
  {
    action: z.enum(["list", "execute"]).describe(
      '"list" — list all available shortcuts with their commands and descriptions. "execute" — execute a shortcut or workflow by ID or command name.'
    ),
    tabId: z.number().describe("Tab ID. Must be a tab in the current group. Use tabs_mcp first if you don't have a valid tab ID."),
    shortcutId: z.string().optional().describe('For action="execute": The ID of the shortcut to execute.'),
    command: z.string().optional().describe('For action="execute": The command name of the shortcut (e.g., "debug", "summarize"). Do not include the leading slash.'),
  },
  async (args) => {
    if (args.action === "execute") {
      const { action: _a, ...rest } = args;
      return callTool("shortcuts_execute", rest);
    }
    return callTool("shortcuts_list", args);
  }
);

// 13. upload — consolidated upload_image + upload_local_file
server.tool(
  "upload",
  "Upload a file or screenshot to a file input or drag & drop target in the browser. Use type=\"image\" for previously captured screenshots, type=\"file\" for local files on disk. If you don't have a valid tab ID, use tabs_mcp first to get available tabs.",
  {
    type: z.enum(["image", "file"]).describe(
      '"image" — upload a previously captured screenshot (by imageId from the computer screenshot action) to a file input or drag-and-drop target. "file" — upload a local file from disk (by filePath) to a file input element using CDP, bypassing the native OS file picker dialog. Works for video, image, document files.'
    ),
    tabId: z.number().describe("Tab ID where the target element is located."),
    // image-specific
    imageId: z.string().optional().describe('For type="image": ID of a previously captured screenshot (from computer tool\'s screenshot action) or user-uploaded image.'),
    ref: z.string().optional().describe('Element reference ID from read_page or find tools. For type="image": targets a specific file input. For type="file": not used (use selector instead).'),
    coordinate: z.array(z.number()).optional().describe('Viewport coordinates [x, y] for drag & drop. For type="image": use this for drag & drop to visible targets (e.g., Google Docs). Provide either ref or coordinate, not both.'),
    filename: z.string().optional().describe('For type="image": Optional filename for the uploaded file (default: "image.png").'),
    // file-specific
    filePath: z.string().optional().describe('For type="file": Absolute path to the local file on disk (e.g., "C:/Users/Nodnarb/Desktop/video.mp4").'),
    selector: z.string().optional().describe('For type="file": CSS selector to find the file input element. Defaults to \'input[type="file"]\'.'),
    inputIndex: z.number().optional().describe('For type="file": Index of the matching input element to use (0-based). Defaults to 0.'),
  },
  async (args) => {
    if (args.type === "file") {
      const { type: _t, imageId: _i, ref: _r, coordinate: _c, filename: _f, ...rest } = args;
      return callTool("upload_local_file", rest);
    }
    // type === "image"
    const { type: _t, filePath: _fp, selector: _s, inputIndex: _ii, ...rest } = args;
    return callTool("upload_image", rest);
  }
);

// Also expose switch_browser and update_plan as before
server.tool(
  "switch_browser",
  "Switch which Chrome browser is used for browser automation. Call this when the user wants to connect to a different Chrome browser. Broadcasts a connection request to all Chrome browsers with the extension installed \u2014 the user clicks 'Connect' in the desired browser.",
  {},
  async (args) => callTool("switch_browser", args)
);

server.tool(
  "update_plan",
  "Present a plan to the user for approval before taking actions. The user will see the domains you intend to visit and your approach. Once approved, you can proceed with actions on the approved domains without additional permission prompts.",
  {
    domains: z.array(z.string()).describe("List of domains you will visit (e.g., ['github.com', 'stackoverflow.com']). These domains will be approved for the session when the user accepts the plan."),
    approach: z.array(z.string()).describe("High-level description of what you will do. Focus on outcomes and key actions, not implementation details. Be concise - aim for 3-7 items."),
  },
  async (args) => callTool("update_plan", args)
);

// --- Start MCP server ---

const transport = new StdioServerTransport();
await server.connect(transport);
