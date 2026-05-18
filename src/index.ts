import { tool } from "@opencode-ai/plugin";
import { watchFile, unwatchFile, existsSync, mkdirSync } from "node:fs";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

interface AutoQueueOptions {
  toastDurationMs?: number;
  emptyToastDurationMs?: number;
  maxPreviews?: number;
  previewLength?: number;
  placeholderTemplate?: string;
  defaultMode?: "hold" | "immediate";
  maxRetries?: number;
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
  maxQueueSize?: number;
  drainDelayMs?: number;
  autoRetryOnIdle?: boolean;
  persistQueue?: boolean;
  persistPath?: string;
  persistDebounceMs?: number;
  watchDebounceMs?: number;
}

const INTERNAL_KEY = "__auto_queue_internal";

interface QueuedItem {
  sessionID: string;
  agent?: string;
  model?: { providerID: string; modelID: string };
  system?: string;
  tools?: string[];
  messageID?: string;
  variant?: string;
  parts: any[];
  preview: string;
  status: "queued" | "sending" | "sent" | "failed";
  retries: number;
  lastError?: string;
  enqueuedAt: number;
}

interface PersistedState {
  version: number;
  mode: string;
  queues: Record<string, any[]>;
  pausedSessions: string[];
}

function serializeQueue(queue: QueuedItem[]): any[] {
  return queue.filter((i) => i.status !== "sent").map((item) => ({
    sessionID: item.sessionID,
    agent: item.agent,
    model: item.model,
    system: item.system,
    tools: item.tools,
    messageID: item.messageID,
    variant: item.variant,
    parts: item.parts,
    preview: item.preview,
    status: item.status === "sending" ? "queued" : item.status,
    retries: item.retries,
    lastError: item.lastError,
    enqueuedAt: item.enqueuedAt,
  }));
}

function deserializeQueue(items: any[]): QueuedItem[] {
  return items.map((item) => ({
    sessionID: item.sessionID ?? "",
    agent: item.agent,
    model: item.model,
    system: item.system,
    tools: item.tools,
    messageID: item.messageID,
    variant: item.variant,
    parts: item.parts ?? [],
    preview: item.preview ?? "[restored]",
    status: (item.status === "queued" || item.status === "failed") ? item.status : "queued",
    retries: item.retries ?? 0,
    lastError: item.lastError,
    enqueuedAt: item.enqueuedAt ?? Date.now(),
  }));
}

async function loadState(filePath: string): Promise<PersistedState | null> {
  try {
    const data = await readFile(filePath, "utf-8");
    const parsed = JSON.parse(data);
    if (!parsed || parsed.version !== 1) return null;
    return parsed as PersistedState;
  } catch {
    return null;
  }
}

async function saveState(filePath: string, state: PersistedState): Promise<void> {
  try {
    const dir = dirname(filePath);
    await mkdir(dir, { recursive: true });
    await writeFile(filePath, JSON.stringify(state, null, 2), "utf-8");
  } catch {
    // persistence failure is non-fatal
  }
}

function makeTruncate(previewLength: number) {
  return function truncatePreview(text: string): string {
    const len = previewLength;
    const trimmed = text.replace(/\s+/g, " ").trim();
    if (trimmed.length <= len) return trimmed;
    return `${trimmed.slice(0, len - 3)}...`;
  };
}

function makeExtractPreview(truncatePreview: (text: string) => string) {
  return function extractPreview(parts: any[]): string {
    const text = parts.find((p: any) => p.type === "text");
    if (text && "text" in text) return truncatePreview(text.text);
    if (parts.some((p: any) => p.type === "file")) return "[file]";
    if (parts.some((p: any) => p.type === "agent")) return "[agent]";
    if (parts.some((p: any) => p.type === "subtask")) return "[subtask]";
    return "[message]";
  };
}

function toPromptPart(part: any): any {
  switch (part.type) {
    case "text":
      return { type: "text", text: part.text };
    case "file":
      return { type: "file", url: part.url, mime: part.mime, filename: part.filename, source: part.source };
    case "agent":
      return { type: "agent", name: part.name, source: part.source };
    case "subtask":
      return { type: "subtask", prompt: part.prompt, description: part.description, agent: part.agent };
    default:
      return null;
  }
}

function makePlaceholder(parts: any[], count: number, template: string): any {
  const tmpl = parts.find((p: any) => p.type === "text") ?? parts[0];
  if (!tmpl) return null;
  return {
    id: tmpl.id,
    sessionID: tmpl.sessionID,
    messageID: tmpl.messageID,
    type: "text",
    text: template.replace("{count}", String(count)),
    synthetic: true,
    ignored: true,
  };
}

function isInternalMessage(parts: any[]): boolean {
  return parts.some((p: any) => p.type === "text" && Boolean(p.metadata?.[INTERNAL_KEY]));
}

function markInternalParts(parts: any[]): any[] {
  let hasText = false;
  const marked = parts.map((part: any) => {
    if (part.type !== "text") return part;
    hasText = true;
    const existing = part.metadata ?? {};
    return { ...part, metadata: { ...existing, [INTERNAL_KEY]: true } };
  });
  if (hasText) return marked;
  const markerPart = { type: "text", text: "", synthetic: true, ignored: true, metadata: { [INTERNAL_KEY]: true } };
  return [markerPart, ...marked];
}

function getPendingCount(queue: QueuedItem[]): number {
  return queue.filter((item) => item.status === "queued" || item.status === "sending" || item.status === "failed").length;
}

function isTransientError(error: any): boolean {
  if (!error) return true;
  const msg = error instanceof Error ? error.message : String(error);
  const lower = msg.toLowerCase();
  return (
    lower.includes("network") ||
    lower.includes("fetch") ||
    lower.includes("econnrefused") ||
    lower.includes("econnreset") ||
    lower.includes("etimedout") ||
    lower.includes("socket") ||
    lower.includes("abort") ||
    lower.includes("interrupt") ||
    lower.includes("cancel") ||
    lower.includes("timeout") ||
    lower.includes("429") ||
    lower.includes("rate") ||
    lower.includes("502") ||
    lower.includes("503") ||
    lower.includes("504") ||
    lower.includes("500") ||
    lower.includes("overloaded") ||
    lower.includes("capacity") ||
    lower.includes("temporarily") ||
    lower.includes("unavailable") ||
    lower.includes("retry") ||
    lower.includes("connection") ||
    lower.includes("refused") ||
    lower.includes("reset") ||
    lower.includes("broken pipe")
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoffDelay(attempt: number, base: number, max: number): number {
  const jitter = Math.random() * base * 0.5;
  const delay = Math.min(base * Math.pow(2, attempt) + jitter, max);
  return delay;
}

function makeBuildToastMessage(truncatePreview: (text: string) => string) {
  return function buildToastMessage(queue: QueuedItem[], maxPreviews: number): string {
    const pendingCount = getPendingCount(queue);
    const failedCount = queue.filter((i) => i.status === "failed").length;
    const previewCount = Math.min(queue.length, maxPreviews);
    const previews = queue.slice(0, previewCount).map((item, index) => {
      const text = truncatePreview(item.preview);
      if (item.status === "sent") return ` ${index + 1}. [x] ~~${text}~~`;
      if (item.status === "sending") return ` ${index + 1}. [>] ${text}`;
      if (item.status === "failed") return ` ${index + 1}. [!] ${text} (${item.retries} retries)`;
      return ` ${index + 1}. [ ] ${text}`;
    });
    const current = queue.find((item) => item.status === "sending")?.preview;
    const currentLine = current ? `Current: ${truncatePreview(current)}\n` : "";
    const more = queue.length > previewCount ? `\n +${queue.length - previewCount} more` : "";
    const failedLine = failedCount > 0 ? ` (${failedCount} failed, will retry)` : "";
    const header = `Queue (${pendingCount} pending${failedLine})`;
    const rule = "-".repeat(header.length);
    const body = previews.length ? previews.join("\n") : " (empty)";
    return `${header}\n${rule}\n${currentLine}${body}${more}`;
  };
}

export const AutoQueuePlugin = {
  id: "opencode-auto-queue",
  server: async (ctx: any, options: AutoQueueOptions = {}) => {
    const client = ctx.client;

    const {
      toastDurationMs = 86_400_000,
      emptyToastDurationMs = 4_000,
      maxPreviews = 3,
      previewLength = 28,
      placeholderTemplate = "Queued; {count} pending",
      defaultMode = "hold",
      maxRetries = 5,
      retryBaseDelayMs = 2_000,
      retryMaxDelayMs = 30_000,
      maxQueueSize = 100,
      drainDelayMs = 500,
      autoRetryOnIdle = true,
      persistQueue = true,
      persistPath = "",
      persistDebounceMs = 1_000,
      watchDebounceMs = 300,
    } = options;

    const truncatePreview = makeTruncate(previewLength);
    const extractPreview = makeExtractPreview(truncatePreview);
    const buildToastMessage = makeBuildToastMessage(truncatePreview);

    const resolvedPersistPath = persistPath || `${ctx.directory}/.git/queue.json`;

    let currentMode: string = defaultMode;
    const busyBySession = new Map<string, boolean>();
    const queueBySession = new Map<string, QueuedItem[]>();
    const draining = new Set<string>();
    const pausedBySession = new Set<string>();
    let persistTimer: ReturnType<typeof setTimeout> | null = null;
    let lastWrittenHash: string = "";

    async function persistState() {
      if (!persistQueue) return;
      const queues: Record<string, any[]> = {};
      for (const [sessionID, queue] of queueBySession.entries()) {
        const serializable = serializeQueue(queue);
        if (serializable.length > 0) queues[sessionID] = serializable;
      }
      const state: PersistedState = {
        version: 1,
        mode: currentMode,
        queues,
        pausedSessions: [...pausedBySession],
      };
      const json = JSON.stringify(state, null, 2);
      const hash = Bun.hash(json).toString();
      if (hash === lastWrittenHash) return;
      lastWrittenHash = hash;
      await saveState(resolvedPersistPath, state);
    }

    function schedulePersist() {
      if (!persistQueue) return;
      if (persistTimer) clearTimeout(persistTimer);
      persistTimer = setTimeout(() => {
        persistTimer = null;
        persistState().catch(() => {});
      }, persistDebounceMs);
    }

    async function restoreState() {
      if (!persistQueue) return;
      const state = await loadState(resolvedPersistPath);
      if (!state) return;
      if (state.mode === "hold" || state.mode === "immediate") currentMode = state.mode;
      if (state.pausedSessions) {
        for (const s of state.pausedSessions) pausedBySession.add(s);
      }
      if (state.queues) {
        for (const [sessionID, items] of Object.entries(state.queues)) {
          const queue = deserializeQueue(items);
          if (queue.length > 0) queueBySession.set(sessionID, queue);
        }
      }
    }

    async function reloadFromDisk() {
      const state = await loadState(resolvedPersistPath);
      if (!state) return;
      if (state.mode === "hold" || state.mode === "immediate") {
        currentMode = state.mode;
      }
      pausedBySession.clear();
      if (state.pausedSessions) {
        for (const s of state.pausedSessions) pausedBySession.add(s);
      }
      queueBySession.clear();
      if (state.queues) {
        for (const [sessionID, items] of Object.entries(state.queues)) {
          const queue = deserializeQueue(items);
          if (queue.length > 0) queueBySession.set(sessionID, queue);
        }
      }
      try {
        await showToast("external", true);
      } catch { /* noop */ }
    }

    let watchTimer: ReturnType<typeof setTimeout> | null = null;
    let watcherActive = false;

    function startFileWatcher() {
      if (!persistQueue || watcherActive) return;
      watcherActive = true;
      try {
        watchFile(resolvedPersistPath, { interval: watchDebounceMs }, () => {
          if (watchTimer) clearTimeout(watchTimer);
          watchTimer = setTimeout(() => {
            watchTimer = null;
            reloadFromDisk().catch(() => {});
          }, watchDebounceMs);
        });
      } catch {
        // watchFile may fail if path doesn't exist yet; will work once file is created
      }
    }

    function stopFileWatcher() {
      if (!watcherActive) return;
      watcherActive = false;
      try {
        unwatchFile(resolvedPersistPath);
      } catch { /* noop */ }
    }

    await restoreState();
    startFileWatcher();

    function getQueue(sessionID: string): QueuedItem[] {
      const existing = queueBySession.get(sessionID);
      if (existing) return existing;
      const next: QueuedItem[] = [];
      queueBySession.set(sessionID, next);
      return next;
    }

    async function showToast(sessionID: string, forceEmpty = false) {
      const queue = queueBySession.get(sessionID) ?? [];
      const pending = getPendingCount(queue);
      if (queue.length === 0 && !forceEmpty) return;
      const isEmpty = pending === 0;
      const variant = isEmpty ? "success" : "info";
      const duration = isEmpty ? emptyToastDurationMs : toastDurationMs;
      const message = isEmpty ? "Queue empty. All queued messages sent." : buildToastMessage(queue, maxPreviews);
      try {
        await client.tui.showToast({
          body: { title: "Message Queue", message, variant, duration },
        });
      } catch {
        // TUI may not be active
      }
    }

    async function drain(sessionID: string) {
      if (draining.has(sessionID)) return;
      if (pausedBySession.has(sessionID)) return;
      const queue = queueBySession.get(sessionID) ?? [];
      if (queue.length === 0) return;
      draining.add(sessionID);
      try {
        if (drainDelayMs > 0) await sleep(drainDelayMs);
        let showedEmptyToast = false;
        while (true) {
          if (pausedBySession.has(sessionID)) break;
          const next = queue.find((item) => item.status === "queued" || (item.status === "failed" && autoRetryOnIdle));
          if (!next) {
            const failedOnly = queue.find((item) => item.status === "failed");
            if (failedOnly) break;
            break;
          }
          next.status = "sending";
          schedulePersist();
          try {
            await showToast(sessionID);
          } catch { /* TUI may not be active */ }

          let sent = false;
          let attempts = 0;
          const maxAttempts = maxRetries + 1;

          while (!sent && attempts < maxAttempts) {
            attempts++;
            try {
              await client.session.prompt({
                path: { id: sessionID },
                body: {
                  agent: next.agent,
                  model: next.model,
                  system: next.system,
                  tools: next.tools,
                  parts: markInternalParts(next.parts),
                },
              });
              next.status = "sent";
              next.lastError = undefined;
              sent = true;
            } catch (error: any) {
              const errMsg = error instanceof Error ? error.message : String(error);
              if (isTransientError(error) && attempts < maxAttempts) {
                next.retries = attempts;
                next.lastError = errMsg;
                next.status = "failed";
                const delay = backoffDelay(attempts - 1, retryBaseDelayMs, retryMaxDelayMs);
                try {
                  await client.tui.showToast({
                    body: {
                      title: "Message Queue",
                      message: `Retrying "${truncatePreview(next.preview)}" in ${Math.round(delay / 1000)}s (attempt ${attempts}/${maxRetries})\nError: ${errMsg.slice(0, 80)}`,
                      variant: "warning",
                      duration: delay + 2000,
                    },
                  });
                } catch { /* TUI may not be active */ }
                await sleep(delay);
                next.status = "queued";
              } else {
                next.retries = attempts;
                next.lastError = errMsg;
                next.status = "failed";
                try {
                  await client.tui.showToast({
                    body: {
                      title: "Message Queue",
                      message: `Failed "${truncatePreview(next.preview)}" after ${attempts} attempts: ${errMsg.slice(0, 100)}`,
                      variant: "error",
                      duration: toastDurationMs,
                    },
                  });
                } catch { /* TUI may not be active */ }
                break;
              }
            }
          }

          schedulePersist();
          try {
            await showToast(sessionID);
          } catch { /* TUI may not be active */ }
          if (getPendingCount(queue) === 0) showedEmptyToast = true;
        }

        const remaining = queue.filter((item) => item.status !== "sent");
        queueBySession.set(sessionID, remaining);

        if (!showedEmptyToast) {
          try {
            await showToast(sessionID, remaining.length === 0);
          } catch { /* TUI may not be active */ }
        }
        schedulePersist();
      } finally {
        draining.delete(sessionID);
      }
    }

    function isBusy(sessionID: string): boolean {
      return busyBySession.get(sessionID) ?? false;
    }

    function markBusy(sessionID: string) {
      busyBySession.set(sessionID, true);
    }

    function makeTextItem(sessionID: string, text: string): QueuedItem {
      const truncate = makeTruncate(previewLength);
      const extract = makeExtractPreview(truncate);
      const parts = [{ type: "text", text }];
      return {
        sessionID,
        parts,
        preview: extract(parts),
        status: "queued",
        retries: 0,
        enqueuedAt: Date.now(),
      };
    }

    const queueTool = tool({
      description:
        "Control message queue. Actions: hold, immediate, status, clear, drop, peek, retry, pause, resume, count, config, reorder, insert, append, prepend, delete, set, sort, invert, get. Both modes queue messages while session is busy; hold = queued messages held until manual drain, immediate = queued messages auto-drain on idle. Only switch modes when explicitly requested.",
      args: {
        action: tool.schema
          .enum(["hold", "immediate", "status", "clear", "drop", "peek", "retry", "pause", "resume", "count", "config", "reorder", "insert", "append", "prepend", "delete", "set", "sort", "invert", "get"])
          .optional()
          .describe("Action to perform"),
        index: tool.schema
          .number()
          .optional()
          .describe("1-based index for drop/delete/get/set"),
        to: tool.schema
          .number()
          .optional()
          .describe("Target position for reorder"),
        text: tool.schema
          .string()
          .optional()
          .describe("Text content for insert/append/prepend/set"),
      },
      async execute({ action, index, to, text }: { action?: string; index?: number; to?: number; text?: string }, ctx: any) {
        const nextAction = action ?? "status";
        const queue = queueBySession.get(ctx.sessionID) ?? [];
        const pendingCount = getPendingCount(queue);
        const busy = isBusy(ctx.sessionID);
        const paused = pausedBySession.has(ctx.sessionID);
        const failedCount = queue.filter((i) => i.status === "failed").length;

        if (nextAction === "status") {
          const lines = [
            `Mode: ${currentMode}`,
            `Session busy: ${busy}`,
            `Paused: ${paused}`,
            `Queued: ${pendingCount}`,
            `Failed: ${failedCount}`,
            `Persist: ${persistQueue}${persistQueue ? ` (${resolvedPersistPath})` : ""}`,
          ];
          if (queue.length > 0) {
            lines.push("", "Queue:");
            queue.forEach((item, i) => {
              const icon = item.status === "sent" ? "[x]" : item.status === "sending" ? "[>]" : item.status === "failed" ? "[!]" : "[ ]";
              const retry = item.retries > 0 ? ` (${item.retries} retries)` : "";
              const age = Math.round((Date.now() - item.enqueuedAt) / 1000);
              lines.push(` ${i + 1}. ${icon} ${item.preview}${retry} [${age}s ago]`);
            });
          }
          return lines.join("\n");
        }

        if (nextAction === "config") {
          return [
            "Queue Configuration:",
            `  mode: ${currentMode} (default: ${defaultMode})`,
            `  maxQueueSize: ${maxQueueSize}`,
            `  maxRetries: ${maxRetries}`,
            `  retryBaseDelayMs: ${retryBaseDelayMs}`,
            `  retryMaxDelayMs: ${retryMaxDelayMs}`,
            `  drainDelayMs: ${drainDelayMs}`,
            `  autoRetryOnIdle: ${autoRetryOnIdle}`,
            `  persistQueue: ${persistQueue}`,
            `  persistPath: ${resolvedPersistPath}`,
            `  persistDebounceMs: ${persistDebounceMs}`,
            `  previewLength: ${previewLength}`,
            `  maxPreviews: ${maxPreviews}`,
            `  toastDurationMs: ${toastDurationMs}`,
            `  emptyToastDurationMs: ${emptyToastDurationMs}`,
            `  placeholderTemplate: "${placeholderTemplate}"`,
          ].join("\n");
        }

        if (nextAction === "count") {
          return `${pendingCount} messages in queue (${failedCount} failed)`;
        }

        if (nextAction === "peek") {
          const next = queue.find((item) => item.status === "queued" || item.status === "failed");
          if (!next) return "Queue is empty";
          const retry = next.retries > 0 ? ` (retried ${next.retries}x)` : "";
          const age = Math.round((Date.now() - next.enqueuedAt) / 1000);
          return `Next: ${next.preview}${retry} [waiting ${age}s]`;
        }

    if (nextAction === "hold") {
      if (currentMode === "hold") return `Mode: hold`;
      currentMode = "hold";
      schedulePersist();
      return `Mode: hold (queued messages held until manually drained)`;
    }

    if (nextAction === "immediate") {
      if (currentMode === "immediate") return `Mode: immediate`;
      currentMode = "immediate";
      schedulePersist();
      await drain(ctx.sessionID);
      return `Mode: immediate (queued messages drain automatically on idle)`;
    }

        if (nextAction === "clear") {
          const cleared = queue.length;
          queueBySession.set(ctx.sessionID, []);
          schedulePersist();
          try { await showToast(ctx.sessionID, true); } catch { /* noop */ }
          return `Cleared ${cleared} messages from queue`;
        }

        if (nextAction === "drop") {
          const idx = (index ?? 1) - 1;
          if (idx < 0 || idx >= queue.length) return `Invalid index. Queue has ${queue.length} items.`;
          const dropped = queue.splice(idx, 1)[0];
          schedulePersist();
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          return `Dropped: ${dropped.preview}`;
        }

        if (nextAction === "retry") {
          const failedItems = queue.filter((i) => i.status === "failed");
          if (failedItems.length === 0) return "No failed items to retry.";
          for (const item of failedItems) {
            item.status = "queued";
            item.retries = 0;
            item.lastError = undefined;
          }
          schedulePersist();
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          if (!paused) await drain(ctx.sessionID);
          return `Retrying ${failedItems.length} failed messages`;
        }

        if (nextAction === "pause") {
          pausedBySession.add(ctx.sessionID);
          schedulePersist();
          return "Queue paused.";
        }

        if (nextAction === "resume") {
          pausedBySession.delete(ctx.sessionID);
          schedulePersist();
          await drain(ctx.sessionID);
          return "Queue resumed. Draining pending messages.";
        }

        if (nextAction === "reorder") {
          const from = index ?? 1;
          const target = to ?? 1;
          if (from < 1 || from > queue.length || target < 1 || target > queue.length)
            return `Index out of range. Queue has ${queue.length} items.`;
          const [item] = queue.splice(from - 1, 1);
          queue.splice(target - 1, 0, item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          return `Moved item ${from} to position ${target}`;
        }

        if (nextAction === "insert") {
          const idx = index ?? 1;
          if (!text) return "No text provided.";
          if (idx < 1 || idx > queue.length + 1) return `Index out of range. Queue has ${queue.length} items.`;
          const item = makeTextItem(ctx.sessionID, text);
          queue.splice(idx - 1, 0, item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          return `Inserted at position ${idx}: ${item.preview}`;
        }

        if (nextAction === "append") {
          if (!text) return "No text provided.";
          const item = makeTextItem(ctx.sessionID, text);
          queue.push(item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          return `Appended: ${item.preview}`;
        }

        if (nextAction === "prepend") {
          if (!text) return "No text provided.";
          const item = makeTextItem(ctx.sessionID, text);
          queue.unshift(item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          return `Prepended: ${item.preview}`;
        }

        if (nextAction === "delete") {
          const idx = index ?? 1;
          if (idx < 1 || idx > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const dropped = queue.splice(idx - 1, 1)[0];
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          return `Deleted: ${dropped.preview}`;
        }

        if (nextAction === "set") {
          const idx = index ?? 1;
          if (!text) return "No text provided.";
          if (idx < 1 || idx > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const item = makeTextItem(ctx.sessionID, text);
          queue[idx - 1] = item;
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          return `Set position ${idx}: ${item.preview}`;
        }

        if (nextAction === "sort") {
          queue.sort((a, b) => a.enqueuedAt - b.enqueuedAt);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          return `Sorted ${queue.length} items by time (oldest first)`;
        }

        if (nextAction === "invert") {
          queue.reverse();
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist();
          return `Reversed ${queue.length} items`;
        }

        if (nextAction === "get") {
          const idx = index ?? 1;
          if (idx < 1 || idx > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const item = queue[idx - 1];
          const age = Math.round((Date.now() - item.enqueuedAt) / 1000);
          const retry = item.retries > 0 ? `\nRetries: ${item.retries}` : "";
          const err = item.lastError ? `\nError: ${item.lastError}` : "";
          const textParts = item.parts.filter((p: any) => p.type === "text").map((p: any) => p.text).join("\n");
          return [
            `Item ${idx}/${queue.length} status: ${item.status} age: ${age}s${retry}${err}`,
            `Preview: ${item.preview}`,
            `Content:`,
            textParts || "(no text content)",
          ].join("\n");
        }

        return `Unknown action: ${nextAction}`;
      },
    });

    const VALID_SLASH_COMMANDS = [
      "hold", "immediate", "status", "clear", "pause", "resume", "count",
      "reorder", "insert", "append", "prepend", "delete", "set", "sort", "invert", "get",
    ];

    function parseIndex(arg: string): number | null {
      const n = parseInt(arg, 10);
      return Number.isFinite(n) && n > 0 ? n : null;
    }

    function normalizeCommandName(cmd: string): string {
      let name = cmd.replace(/^\//, "").toLowerCase();
      if (name.startsWith("queue-")) name = name.slice("queue-".length);
      return name;
    }

    function handleSlashCommand(cmd: string, args: string, sessionID: string): string | null {
      const normalized = normalizeCommandName(cmd);
      if (!VALID_SLASH_COMMANDS.includes(normalized)) return null;

      const queue = queueBySession.get(sessionID) ?? [];
      const pendingCount = getPendingCount(queue);
      const busy = isBusy(sessionID);
      const paused = pausedBySession.has(sessionID);
      const failedCount = queue.filter((i) => i.status === "failed").length;

      switch (normalized) {
    case "hold": {
      if (currentMode === "hold") return `Queue mode: hold (already active)`;
      currentMode = "hold";
      schedulePersist();
      return `Queue mode: hold (queued messages held until manually drained)`;
    }
    case "immediate": {
      if (currentMode === "immediate") return `Queue mode: immediate (already active)`;
      currentMode = "immediate";
      schedulePersist();
      drain(sessionID).catch(() => {});
      return `Queue mode: immediate (queued messages drain automatically on idle)`;
    }
        case "status": {
          const lines = [
            `Mode: ${currentMode}`,
            `Session busy: ${busy}`,
            `Paused: ${paused}`,
            `Queued: ${pendingCount}`,
            `Failed: ${failedCount}`,
          ];
          if (queue.length > 0) {
            lines.push("", "Queue:");
            queue.forEach((item, i) => {
              const icon = item.status === "sent" ? "[x]" : item.status === "sending" ? "[>]" : item.status === "failed" ? "[!]" : "[ ]";
              lines.push(` ${i + 1}. ${icon} ${item.preview}`);
            });
          }
          return lines.join("\n");
        }
        case "clear": {
          const cleared = queue.length;
          queueBySession.set(sessionID, []);
          schedulePersist();
          showToast(sessionID, true).catch(() => {});
          return `Cleared ${cleared} messages from queue`;
        }
        case "pause": {
          pausedBySession.add(sessionID);
          schedulePersist();
          return "Queue paused.";
        }
        case "resume": {
          pausedBySession.delete(sessionID);
          schedulePersist();
          drain(sessionID).catch(() => {});
          return "Queue resumed. Draining pending messages.";
        }
        case "count": {
          return `${pendingCount} messages in queue (${failedCount} failed)`;
        }
        case "reorder": {
          const parts = args.trim().split(/\s+/);
          const from = parseIndex(parts[0]);
          const to = parseIndex(parts[1]);
          if (!from || !to) return "Usage: /reorder <from> <to> (1-based indices)";
          if (from > queue.length || to > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const [item] = queue.splice(from - 1, 1);
          queue.splice(to - 1, 0, item);
          queueBySession.set(sessionID, queue);
          schedulePersist();
          return `Moved item ${from} to position ${to}`;
        }
        case "insert": {
          const firstSpace = args.indexOf(" ");
          if (firstSpace === -1) return "Usage: /insert <index> <text>";
          const idx = parseIndex(args.slice(0, firstSpace));
          const insertText = args.slice(firstSpace + 1).trim();
          if (!idx) return "Invalid index. Must be a positive number.";
          if (!insertText) return "No text provided.";
          if (idx > queue.length + 1) return `Index out of range. Queue has ${queue.length} items.`;
          const item = makeTextItem(sessionID, insertText);
          queue.splice(idx - 1, 0, item);
          queueBySession.set(sessionID, queue);
          schedulePersist();
          return `Inserted at position ${idx}: ${item.preview}`;
        }
        case "append": {
          const appendText = args.trim();
          if (!appendText) return "Usage: /append <text>";
          const item = makeTextItem(sessionID, appendText);
          queue.push(item);
          queueBySession.set(sessionID, queue);
          schedulePersist();
          return `Appended: ${item.preview}`;
        }
        case "prepend": {
          const prependText = args.trim();
          if (!prependText) return "Usage: /prepend <text>";
          const item = makeTextItem(sessionID, prependText);
          queue.unshift(item);
          queueBySession.set(sessionID, queue);
          showToast(sessionID).catch(() => {});
          return `Prepended: ${item.preview}`;
        }
        case "delete": {
          const idx = parseIndex(args.trim());
          if (!idx) return "Usage: /delete <index> (1-based)";
          if (idx < 1 || idx > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const dropped = queue.splice(idx - 1, 1)[0];
          queueBySession.set(sessionID, queue);
          schedulePersist();
          showToast(sessionID).catch(() => {});
          return `Deleted: ${dropped.preview}`;
        }
        case "set": {
          const firstSpace = args.indexOf(" ");
          if (firstSpace === -1) return "Usage: /set <index> <text>";
          const idx = parseIndex(args.slice(0, firstSpace));
          const setText = args.slice(firstSpace + 1).trim();
          if (!idx) return "Invalid index.";
          if (!setText) return "No text provided.";
          if (idx < 1 || idx > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const item = makeTextItem(sessionID, setText);
          queue[idx - 1] = item;
          queueBySession.set(sessionID, queue);
          schedulePersist();
          return `Set position ${idx}: ${item.preview}`;
        }
        case "sort": {
          queue.sort((a, b) => a.enqueuedAt - b.enqueuedAt);
          queueBySession.set(sessionID, queue);
          schedulePersist();
          return `Sorted ${queue.length} items by time (oldest first)`;
        }
        case "invert": {
          queue.reverse();
          queueBySession.set(sessionID, queue);
          schedulePersist();
          return `Reversed ${queue.length} items`;
        }
        case "get": {
          const idx = parseIndex(args.trim());
          if (!idx) return "Usage: /get <index> (1-based)";
          if (idx < 1 || idx > queue.length) return `Index out of range. Queue has ${queue.length} items.`;
          const item = queue[idx - 1];
          const age = Math.round((Date.now() - item.enqueuedAt) / 1000);
          const retry = item.retries > 0 ? `\nRetries: ${item.retries}` : "";
          const err = item.lastError ? `\nError: ${item.lastError}` : "";
          const textParts = item.parts.filter((p: any) => p.type === "text").map((p: any) => p.text).join("\n");
          return [
            `Item ${idx}/${queue.length} status: ${item.status} age: ${age}s${retry}${err}`,
            `Preview: ${item.preview}`,
            `Content:`,
            textParts || "(no text content)",
          ].join("\n");
        }
        default:
          return null;
      }
    }

    const queueCommands: Record<string, { template: string; description: string }> = {
  "queue-hold": { template: "$ARGUMENTS", description: "Hold queued messages until manually drained" },
  "queue-immediate": { template: "$ARGUMENTS", description: "Auto-drain queued messages on idle" },
      "queue-status": { template: "$ARGUMENTS", description: "Show queue status" },
      "queue-clear": { template: "$ARGUMENTS", description: "Clear the queue" },
      "queue-pause": { template: "$ARGUMENTS", description: "Pause auto-drain" },
      "queue-resume": { template: "$ARGUMENTS", description: "Resume auto-drain" },
      "queue-count": { template: "$ARGUMENTS", description: "Show pending item count" },
      "queue-reorder": { template: "$ARGUMENTS", description: "Reorder: /queue-reorder <from> <to>" },
      "queue-insert": { template: "$ARGUMENTS", description: "Insert: /queue-insert <pos> <text>" },
      "queue-append": { template: "$ARGUMENTS", description: "Append text to queue" },
      "queue-prepend": { template: "$ARGUMENTS", description: "Prepend text to queue" },
      "queue-delete": { template: "$ARGUMENTS", description: "Delete: /queue-delete <index>" },
      "queue-set": { template: "$ARGUMENTS", description: "Set: /queue-set <index> <text>" },
      "queue-sort": { template: "$ARGUMENTS", description: "Sort queue by time" },
      "queue-invert": { template: "$ARGUMENTS", description: "Reverse queue order" },
      "queue-get": { template: "$ARGUMENTS", description: "Get: /queue-get <index>" },
    };

    return {
      tool: { queue: queueTool },

      config: async (inputConfig: any) => {},

      "command.execute.before": async (
        input: { command: string; sessionID: string; arguments: string },
        output: { parts: any[] },
      ) => {
        const result = handleSlashCommand(input.command, input.arguments ?? "", input.sessionID);
        if (result === null) return;
        output.parts = markInternalParts([{ type: "text", text: result }]);
      },

  event: async ({ event }: { event: any }) => {
    if (event.type === "session.status") {
      const { sessionID, status } = event.properties;
      const busy = status.type !== "idle";
      busyBySession.set(sessionID, busy);
      if (!busy && currentMode === "immediate" && !pausedBySession.has(sessionID)) {
        await drain(sessionID);
      }
      return;
    }

    if (event.type === "session.idle") {
      const { sessionID } = event.properties;
      busyBySession.set(sessionID, false);
      if (currentMode === "immediate" && !pausedBySession.has(sessionID)) {
        await drain(sessionID);
      }
    }
  },

  "chat.message": async (input: any, output: any) => {
    if (isInternalMessage(output.parts)) return;

    const parts = output.parts ?? [];
    const firstText = parts.find((p: any) => p.type === "text" && typeof p.text === "string" && p.text.length > 0);
    if (firstText) {
      const trimmed = firstText.text.trim();
      if (trimmed.startsWith("/queue-") || trimmed.startsWith("/queue ")) {
        const cmdName = trimmed.replace(/^\//, "").split(/\s+/)[0];
        const cmdArgs = trimmed.replace(/^\//, "").slice(cmdName.length).trim();
        const result = handleSlashCommand(cmdName, cmdArgs, input.sessionID);
        if (result !== null) {
          output.parts.length = 0;
          output.parts.push(...markInternalParts([{ type: "text", text: result }]));
          return;
        }
      }
    }

    if (draining.has(input.sessionID)) return;

    const textParts = parts.filter((p: any) => p.type === "text");
    const allSystemReminders = textParts.every((p: any) =>
      typeof p.text === "string" && (p.text.startsWith("<system-reminder") || p.text.includes("Instructions from:"))
    );
    if (allSystemReminders) return;

    const allSynthetic = parts.every((p: any) => p.synthetic || p.ignored || p.type !== "text");
    if (allSynthetic) return;

    const busy = isBusy(input.sessionID);
    if (!busy) {
      markBusy(input.sessionID);
      return;
    }

    const queue = getQueue(input.sessionID);
    if (queue.length >= maxQueueSize) {
      try {
        await client.tui.showToast({
          body: {
            title: "Message Queue",
            message: `Queue full (${maxQueueSize} max). Message dropped.`,
            variant: "error",
            duration: emptyToastDurationMs * 3,
          },
        });
      } catch { /* noop */ }
      const placeholder = makePlaceholder(output.parts, queue.length, `Queue full; dropped.`);
      if (placeholder) {
        output.parts.length = 0;
        output.parts.push(placeholder);
      }
      return;
    }

    const originalParts = [...output.parts];
    const queuedParts = originalParts.map(toPromptPart).filter((p: any) => p !== null);
    const preview = extractPreview(queuedParts);

    queue.push({
      sessionID: input.sessionID,
      agent: input.agent ?? output.message.agent,
      model: input.model ?? output.message.model,
      system: output.message.system,
      tools: output.message.tools,
      messageID: input.messageID,
      variant: input.variant,
      parts: queuedParts,
      preview,
      status: "queued",
      retries: 0,
      enqueuedAt: Date.now(),
    });

    const queueSize = getPendingCount(queue);
    const placeholder = makePlaceholder(originalParts, queueSize, placeholderTemplate);
    if (placeholder) {
      output.parts.length = 0;
      output.parts.push(placeholder);
    }

    schedulePersist();
    try {
      await showToast(input.sessionID);
    } catch { /* TUI may not be active */ }
  },
    };
  },
};

export default AutoQueuePlugin;
