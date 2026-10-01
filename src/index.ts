import { tool } from "@opencode-ai/plugin";
import { watchFile, unwatchFile, existsSync, mkdirSync, statSync } from "node:fs";
import { readFile, writeFile, mkdir, rename, open, unlink, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

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
  failedToastDurationMs?: number;
  queueToastHeartbeatMs?: number;
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

async function defaultPersistPath(directory: string): Promise<string> {
  const gitEntry = join(directory, ".git");
  try {
    const stat = statSync(gitEntry);
    if (stat.isDirectory()) return join(gitEntry, "queue.json");

    // Git worktrees use a .git *file* pointing at the real per-worktree gitdir.
    // Writing <worktree>/.git/queue.json treats that file as a directory and
    // fails ENOTDIR (persistence then silently stays memory-only).
    const pointer = await readFile(gitEntry, "utf-8");
    const match = pointer.match(/^gitdir:\s*(.+?)\s*$/m);
    if (!match) throw new Error(`Invalid Git worktree pointer: ${gitEntry}`);
    return join(resolve(directory, match[1]), "queue.json");
  } catch (error: any) {
    if (error?.code === "ENOENT") return join(gitEntry, "queue.json");
    throw error;
  }
}

// Retry helper for Windows file locks: rename can hit EPERM/EBUSY while AV,
// indexer or a concurrent reader holds the target open. Retrying with backoff
// resolves virtually all transient lock collisions.
async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function renameWithRetry(tmp: string, target: string): Promise<void> {
  // 8 attempts x 25ms*attempt backoff = ~700ms window: proven necessary — a
  // 180ms external lock exhausts 5 attempts (~200ms) and still fails.
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      await rename(tmp, target);
      return;
    } catch (err: any) {
      const code = err?.code ?? "";
      if (code !== "EPERM" && code !== "EACCES" && code !== "EBUSY") throw err;
      if (attempt === 8) throw err;
      await sleep(25 * attempt);
    }
  }
}

let saveTempSequence = 0;

async function saveState(filePath: string, state: PersistedState, serialized?: string): Promise<void> {
  const tmp = `${filePath}.${process.pid}.${Date.now()}.${++saveTempSequence}.tmp`;
  // Atomic write: a crash mid-write must never leave a truncated (unparseable)
  // queue file behind — that would silently discard the persisted queue.
  await mkdir(dirname(filePath), { recursive: true });
  try {
    await writeFile(tmp, serialized ?? JSON.stringify(state, null, 2), "utf-8");
    // Let EPERM/EBUSY/ENOSPC propagate: persistState guards its dedup hash with
    // this success. A swallowed failure here froze the file on disk while memory
    // moved on — stale disk state resurrected sent items on restart (proven live).
    await renameWithRetry(tmp, filePath);
  } catch (error) {
    await unlink(tmp).catch(() => {});
    throw error;
  }
}

async function withFileLock<T>(filePath: string, action: () => Promise<T>, timeoutMs = 30_000): Promise<T> {
  const lockPath = `${filePath}.lock`;
  const deadline = Date.now() + timeoutMs;
  const token = `${process.pid}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
  await mkdir(dirname(filePath), { recursive: true });

  let handle: Awaited<ReturnType<typeof open>> | null = null;
  while (!handle) {
    try {
      handle = await open(lockPath, "wx");
      await handle.writeFile(JSON.stringify({ pid: process.pid, token, createdAt: Date.now() }), "utf8");
    } catch (error: any) {
      if (handle) {
        await handle.close().catch(() => {});
        handle = null;
        await unlink(lockPath).catch(() => {});
      }
      if (error?.code !== "EEXIST") throw error;

      // Recover a lock left by a crashed process; never steal one from a live
      // writer. Re-read before unlinking so a replaced lock owner is preserved.
      try {
        const ownerText = await readFile(lockPath, "utf8");
        const owner = JSON.parse(ownerText);
        let alive = false;
        if (Number.isInteger(owner.pid) && owner.pid > 0) {
          try { process.kill(owner.pid, 0); alive = true; }
          catch (probeError: any) { alive = probeError?.code === "EPERM"; }
        }
        if (!alive) {
          const currentText = await readFile(lockPath, "utf8");
          if (currentText === ownerText) await unlink(lockPath).catch(() => {});
          continue;
        }
      } catch (readError: any) {
        if (readError?.code === "ENOENT") continue;
        if (readError instanceof SyntaxError) {
          const lockStat = await stat(lockPath).catch(() => null);
          if (lockStat && Date.now() - lockStat.mtimeMs > timeoutMs) await unlink(lockPath).catch(() => {});
          continue;
        }
      }
      if (Date.now() >= deadline) throw new Error(`Timed out waiting for queue persistence lock: ${lockPath}`);
      await sleep(25);
    }
  }

  try {
    return await action();
  } finally {
    await handle.close().catch(() => {});
    try {
      const owner = JSON.parse(await readFile(lockPath, "utf8"));
      if (owner.token === token) await unlink(lockPath);
    } catch { /* lock may have been reclaimed after process failure */ }
  }
}

// Exported for behavior tests of the lock/retry mechanics (RULE #6: verify by
// execution, not by reading).
export const persistInternals = { sleep, renameWithRetry, saveState, loadState, withFileLock };

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

// Replacement content for an intercepted /queue command message.
//
// Upstream facts (v1.18.34, commit aec0b9a6):
// - Server: command() -> command.execute.before -> prompt() -> loop() always
//   runs the agent turn; no plugin hook can cancel it (noReply is not settable
//   for commands).
// - TUI (routes/session/index.tsx): user messages whose text parts are ALL
//   synthetic/ignored are NOT rendered in the transcript.
// - Model prompt (session/message-v2.ts): only `ignored`/empty text parts are
//   excluded from the LLM call; `synthetic` text parts ARE sent.
//
// Therefore the ack part is synthetic (invisible in TUI — the command "does
// not appear in the session") but NOT ignored (the model gets a harmless
// inert instruction instead of an empty user message, which would be dropped
// from the prompt and could trigger a provider error or a stale-context
// continuation). The action itself is executed plugin-side and its result is
// surfaced as a toast; the model never sees command text or results, so it
// cannot re-execute the action.
const COMMAND_ACK_TEXT =
  "<system-reminder>message-queue plugin: the user's /queue command was handled internally by the plugin and is already complete. The result was shown to the user as a toast. Do not call any queue tool. Do not take any action. Do not repeat or summarize the command. Reply with nothing or a single short acknowledgment.</system-reminder>";

function makeCommandAckPart(): any {
  return { type: "text", text: COMMAND_ACK_TEXT, synthetic: true, metadata: { [INTERNAL_KEY]: true } };
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
  const status = error?.cause?.status ?? error?.response?.status;
  if (typeof status === "number") return status === 408 || status === 425 || status === 429 || status >= 500;
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

function promptResultError(result: any): Error | null {
  if (!result?.error) return null;
  const body = result.error;
  const message = typeof body === "string"
    ? body
    : body?.data?.message ?? body?.message ?? body?.name ?? JSON.stringify(body);
  return new Error(`Prompt rejected: ${message}`, { cause: { status: result.response?.status, body } });
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
      toastDurationMs = 10_000,
      emptyToastDurationMs = 4_000,
      failedToastDurationMs = 60_000,
      // OpenCode TUI toasts cannot be sticky or manually dismissed (v1.18.x:
      // unconditional setTimeout, no dismiss handler, duration=0 rejected by
      // schema). To keep the queue visible while items are pending, the queue
      // toast is re-posted on this interval ("heartbeat"). 0 disables.
      queueToastHeartbeatMs = 20_000,
      maxPreviews = 3,
      previewLength = 28,
      placeholderTemplate = "Queued; {count} pending",
      defaultMode = "immediate",
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

    const resolvedPersistPath = persistPath || await defaultPersistPath(ctx.directory);

    let currentMode: string = defaultMode;
    const busyBySession = new Map<string, boolean>();
    // Timestamp of the last observed idle event per session. The deferred
    // drain continuation only continues when an idle was observed AFTER the
    // send — the busy map alone can be stale if no status event fired.
    const lastIdleAt = new Map<string, number>();
    const queueBySession = new Map<string, QueuedItem[]>();
    const draining = new Set<string>();
    const pausedBySession = new Set<string>();
    let persistTimer: ReturnType<typeof setTimeout> | null = null;
    let lastWrittenHash: string = "";
    // Track active save for watcher coordination; lock-protected saves merge
    // only mutated keys into latest disk state.
    let savePromise: Promise<void> | null = null;
    let saveChain: Promise<void> = Promise.resolve();
    let mutationRevision = 0;
    let globalRevision = 0;
    // Revisioned dirty tracking skips read-only operations and cannot clear a
    // mutation newer than the snapshot that just finished saving.
    const dirtySessions = new Map<string, number>();
    const dirtyPausedSessions = new Map<string, number>();

    function markSessionDirty(sessionID: string, pausedChanged = false) {
      const revision = ++mutationRevision;
      dirtySessions.set(sessionID, revision);
      if (pausedChanged) dirtyPausedSessions.set(sessionID, revision);
    }

    async function persistState() {
      if (!persistQueue) return;
      const save = saveChain.then(() => withFileLock(resolvedPersistPath, async () => {
        const queueRevisions = new Map(dirtySessions);
        const pausedRevisions = new Map(dirtyPausedSessions);
        const savedGlobalRevision = globalRevision;
        if (queueRevisions.size === 0 && pausedRevisions.size === 0 && savedGlobalRevision === 0) return;

        const diskState = await loadState(resolvedPersistPath) ?? {
          version: 1, mode: currentMode, queues: {}, pausedSessions: [],
        };
        const queues = { ...(diskState.queues ?? {}) };
        for (const sessionID of queueRevisions.keys()) {
          const serializable = serializeQueue(queueBySession.get(sessionID) ?? []);
          if (serializable.length > 0) queues[sessionID] = serializable;
          else delete queues[sessionID];
        }
        const paused = new Set(diskState.pausedSessions ?? []);
        for (const sessionID of pausedRevisions.keys()) {
          if (pausedBySession.has(sessionID)) paused.add(sessionID);
          else paused.delete(sessionID);
        }
        const state: PersistedState = {
          version: 1,
          mode: savedGlobalRevision !== 0 ? currentMode : diskState.mode ?? currentMode,
          queues,
          pausedSessions: [...paused],
        };
        const json = JSON.stringify(state, null, 2);
        const hash = Bun.hash(json).toString();
        const diskHash = Bun.hash(JSON.stringify(diskState, null, 2)).toString();
        if (hash !== diskHash) await saveState(resolvedPersistPath, state, json);
        const protectedQueues = new Set([...dirtySessions.keys(), ...draining]);
        const savedSessionIDs = new Set(Object.keys(queues));
        for (const sessionID of [...queueBySession.keys()]) {
          if (!protectedQueues.has(sessionID) && !savedSessionIDs.has(sessionID)) queueBySession.delete(sessionID);
        }
        for (const [sessionID, items] of Object.entries(queues)) {
          if (protectedQueues.has(sessionID)) continue;
          const queue = deserializeQueue(items);
          if (queue.length > 0) queueBySession.set(sessionID, queue);
          else queueBySession.delete(sessionID);
        }
        for (const sessionID of pausedBySession) {
          if (!dirtyPausedSessions.has(sessionID) && !paused.has(sessionID)) pausedBySession.delete(sessionID);
        }
        for (const sessionID of paused) {
          if (!dirtyPausedSessions.has(sessionID)) pausedBySession.add(sessionID);
        }
        // Pin only state observed on disk or successfully written.
        lastWrittenHash = hash;
        for (const [sessionID, revision] of queueRevisions) {
          if (dirtySessions.get(sessionID) === revision) dirtySessions.delete(sessionID);
        }
        for (const [sessionID, revision] of pausedRevisions) {
          if (dirtyPausedSessions.get(sessionID) === revision) dirtyPausedSessions.delete(sessionID);
        }
        if (globalRevision === savedGlobalRevision) globalRevision = 0;
      })).finally(() => {
        if (savePromise === save) savePromise = null;
      });
      saveChain = save.catch(() => {});
      savePromise = save;
      await save;
    }

    function schedulePersist(sessionID?: string, pausedChanged = false, globalChanged = false) {
      if (!persistQueue) return;
      if (sessionID !== undefined) markSessionDirty(sessionID, pausedChanged);
      else if (globalChanged) globalRevision = ++mutationRevision;
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
      lastWrittenHash = Bun.hash(JSON.stringify(state, null, 2)).toString();
      // Mode is runtime-only: the configured default wins on startup. A stale
      // persisted "hold" must not disable auto-drain after an upgrade.
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
      // Never race a local snapshot write. Any mutation arriving during this
      // await is protected by revisioned dirty tracking below.
      if (savePromise) await savePromise;
      const state = await loadState(resolvedPersistPath);
      if (!state) return;
      const hash = Bun.hash(JSON.stringify(state, null, 2)).toString();
      if (hash === lastWrittenHash) return;

      const protectedQueues = new Set([...dirtySessions.keys(), ...draining]);
      const diskSessionIDs = new Set(Object.keys(state.queues ?? {}));
      for (const sid of [...queueBySession.keys()]) {
        if (!protectedQueues.has(sid) && !diskSessionIDs.has(sid)) queueBySession.delete(sid);
      }
      for (const [sessionID, items] of Object.entries(state.queues ?? {})) {
        if (protectedQueues.has(sessionID)) continue;
        const queue = deserializeQueue(items);
        if (queue.length > 0) queueBySession.set(sessionID, queue);
        else queueBySession.delete(sessionID);
      }
      if (dirtyPausedSessions.size === 0) {
        pausedBySession.clear();
        for (const sessionID of state.pausedSessions ?? []) pausedBySession.add(sessionID);
      }

      // Re-pin to the external content observed. Locally dirty sessions are
      // persisted through the lock-protected merge, never a stale full snapshot.
      lastWrittenHash = Bun.hash(JSON.stringify(state, null, 2)).toString();
      if (dirtySessions.size > 0 || dirtyPausedSessions.size > 0 || globalRevision !== 0) schedulePersist();
      // No toast here: a disk reload is not a user-visible queue event. The
      // old forced empty-toast fired "Queue empty. All queued messages sent."
      // on EVERY watcher reload — a false success signal (proven live).
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

    function hasQueueCapacity(queue: QueuedItem[], additional = 1): boolean {
      return getPendingCount(queue) + additional <= maxQueueSize;
    }

    function queueFullMessage(): string {
      return `Queue full (${maxQueueSize} max). Remove or send queued items first.`;
    }

    async function showToast(sessionID: string, forceEmpty = false) {
      const queue = queueBySession.get(sessionID) ?? [];
      const pending = getPendingCount(queue);
      syncHeartbeat(sessionID);
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

    // Command results are surfaced ONLY as a toast: the intercepted command
    // message is hidden from the transcript (synthetic part), so the toast is
    // the user's feedback channel for the action's output.
    async function showResultToast(result: string) {
      const message = result.length > 400 ? `${result.slice(0, 397)}...` : result;
      try {
        await client.tui.showToast({
          body: { title: "Message Queue", message, variant: "info", duration: toastDurationMs },
        });
      } catch {
        // TUI may not be active
      }
    }

    // Pinned queue toast: while a session has pending items, re-post the queue
    // listing toast every queueToastHeartbeatMs so it stays on screen (the TUI
    // has no sticky toast and no manual dismissal). Stops when the queue
    // empties or the user runs /queue hide.
    const heartbeatTimers = new Map<string, ReturnType<typeof setInterval>>();
    const toastHiddenBySession = new Set<string>();

    function syncHeartbeat(sessionID: string) {
      if (queueToastHeartbeatMs <= 0) return;
      const queue = queueBySession.get(sessionID);
      const want = !!queue && queue.length > 0 && !toastHiddenBySession.has(sessionID);
      const has = heartbeatTimers.has(sessionID);
      if (want && !has) {
        const t = setInterval(() => {
          void showToast(sessionID);
        }, queueToastHeartbeatMs);
        (t as any)?.unref?.();
        heartbeatTimers.set(sessionID, t);
      } else if (!want && has) {
        clearInterval(heartbeatTimers.get(sessionID)!);
        heartbeatTimers.delete(sessionID);
      }
    }

    async function drain(sessionID: string) {
      if (draining.has(sessionID)) return;
      if (pausedBySession.has(sessionID)) return;
      // Never prompt a busy session: OpenCode accepts the request but no turn
      // is created, so items would be marked "sent" while silently lost.
      if (isBusy(sessionID)) return;
      const initialQueue = queueBySession.get(sessionID) ?? [];
      if (initialQueue.length === 0) return;
      draining.add(sessionID);
      markSessionDirty(sessionID);
      try {
        if (drainDelayMs > 0) await sleep(drainDelayMs);
        let showedEmptyToast = false;
        while (true) {
          // Re-fetch EVERY iteration: clear() swaps the map entry for a new
          // array. Holding the pre-clear reference resurrected cleared
          // messages and wiped items enqueued after the clear (review F2).
          // Orphaned item objects are simply never re-picked.
          const queue = queueBySession.get(sessionID) ?? [];
          if (queue.length === 0) break;
          if (pausedBySession.has(sessionID)) break;
          // Failed items are retried across drains (autoRetryOnIdle) but only
          // up to maxRetries total attempts; otherwise one permanently failing
          // item (e.g. 400 Bad Request) would be re-picked in a tight infinite
          // loop, wedge the drain, and block the whole queue.
          const next = queue.find(
            (item) =>
              item.status === "queued" ||
              (item.status === "failed" && autoRetryOnIdle && (item.retries ?? 0) < maxRetries),
          );
          if (!next) {
            const failedOnly = queue.find((item) => item.status === "failed");
            if (failedOnly) break;
            break;
          }
          next.status = "sending";
          schedulePersist(sessionID);
          try {
            await showToast(sessionID);
          } catch { /* TUI may not be active */ }

          let sent = false;
          let attempts = 0;
          const maxAttempts = maxRetries + 1;
          // Captured BEFORE the prompt await: any idle event observed while
          // the prompt is in flight (turn completing server-side) must count
          // as "idle after this send" for the deferred continuation.
          const sendStart = Date.now();

          while (!sent && attempts < maxAttempts) {
            attempts++;
            try {
              const result = await client.session.prompt({
                path: { id: sessionID },
                body: {
                  agent: next.agent,
                  model: next.model,
                  system: next.system,
                  tools: next.tools,
                  parts: markInternalParts(next.parts),
                },
                throwOnError: true,
              });
              const resultError = promptResultError(result);
              if (resultError) throw resultError;
        next.status = "sent";
        next.lastError = undefined;
        sent = true;
      } catch (error: any) {
        const errMsg = error instanceof Error ? error.message : String(error);
        if (isTransientError(error) && attempts < maxAttempts) {
          next.retries = (next.retries ?? 0) + attempts;
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
          // Monotonic total-attempt counter: the outer loop re-picks failed
          // items, so a reset-per-visit counter would never reach the cap.
          next.retries = (next.retries ?? 0) + attempts;
          next.lastError = errMsg;
          next.status = "failed";
          try {
            await client.tui.showToast({
              body: {
                title: "Message Queue",
                message: `Failed "${truncatePreview(next.preview)}" after ${attempts} attempts: ${errMsg.slice(0, 100)}`,
                variant: "error",
                duration: failedToastDurationMs,
              },
            });
          } catch { /* TUI may not be active */ }
          break;
        }
        }
      }
      // ^ close catch, then close the INNER retry while — the if(sent) below
      // must sit BETWEEN the loops so its break exits the OUTER send loop.

      if (sent) {
        // One item per pass: session.prompt may resolve before the agent turn
        // completes. Sending the next item immediately would target a busy
        // session (accepted but no turn created = silent loss — the original
        // production incident). The next idle event re-triggers the drain;
        // the deferred attempt covers prompt-resolves-after-completion, where
        // no new idle event will fire — but only when an idle event was
        // observed AFTER this send (busy map alone can be stale).
        // The one-send-per-pass break skips the post-send toast block below,
        // so flag the empty state here — otherwise a fully drained queue never
        // announces "Queue empty" (regression introduced with this pass logic).
        if (getPendingCount(queue) === 0) showedEmptyToast = true;
        setTimeout(() => {
          if (
            !pausedBySession.has(sessionID) &&
            !isBusy(sessionID) &&
            (lastIdleAt.get(sessionID) ?? 0) > sendStart
          ) {
            drain(sessionID).catch(() => {});
          }
        }, Math.max(drainDelayMs, 50) + 50);
        break;
      }

    schedulePersist(sessionID);
    const pendingAfterSend = getPendingCount(queue);
    if (pendingAfterSend > 0) {
      try {
        await showToast(sessionID);
      } catch { /* TUI may not be active */ }
    } else {
      showedEmptyToast = true;
    }
    }

  const remaining = (queueBySession.get(sessionID) ?? []).filter((item) => item.status !== "sent");
  queueBySession.set(sessionID, remaining);

  if (showedEmptyToast) {
    await sleep(1500);
    try {
      await showToast(sessionID, remaining.length === 0);
    } catch { /* TUI may not be active */ }
  }
  schedulePersist(sessionID);
      } finally {
        draining.delete(sessionID);
      }
    }

    function isBusy(sessionID: string): boolean {
      // An in-flight drain implies the session is (or is about to be) busy:
      // messages arriving meanwhile must be queued, not passed through raw
      // into a session with a turn starting (silent-loss class).
      return !!busyBySession.get(sessionID) || draining.has(sessionID);
    }

    function markBusy(sessionID: string) {
      busyBySession.set(sessionID, true);
    }

    function makeTextItem(sessionID: string, text: string): QueuedItem {
      // Any enqueue (tool or slash command) is a queue change: re-show the
      // pinned toast if the user hid it.
      toastHiddenBySession.delete(sessionID);
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
        "Control message queue. Actions: hold, immediate, status, clear, drop, peek, retry, pause, resume, count, config, reorder, insert, append, prepend, delete, set, sort, invert, get, hide. Both modes queue messages while session is busy; hold = queued messages held until manual drain, immediate = queued messages auto-drain on idle. Only switch modes when explicitly requested. hide = stop the pinned queue toast until the queue changes (the TUI cannot dismiss toasts by click).",
      args: {
        action: tool.schema
          .enum(["hold", "immediate", "status", "clear", "drop", "peek", "retry", "pause", "resume", "count", "config", "reorder", "insert", "append", "prepend", "delete", "set", "sort", "invert", "get", "hide"])
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
            `  queueToastHeartbeatMs: ${queueToastHeartbeatMs}`,
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
      schedulePersist(undefined, false, true);
      return `Mode: hold (queued messages held until manually drained)`;
    }

    if (nextAction === "immediate") {
      if (currentMode === "immediate") return `Mode: immediate`;
      currentMode = "immediate";
      schedulePersist(undefined, false, true);
      await drain(ctx.sessionID);
      return `Mode: immediate (queued messages drain automatically on idle)`;
    }

        if (nextAction === "clear") {
          const cleared = queue.length;
          queueBySession.set(ctx.sessionID, []);
          schedulePersist(ctx.sessionID);
          // No forced toast: the tool result reports the clear. The old
          // "Queue empty. All queued messages sent." here was a false success
          // signal — cleared items were discarded, not sent (mirrors the
          // slash-path fix in 676ed28).
          return `Cleared ${cleared} messages from queue`;
        }

        if (nextAction === "drop") {
          const idx = (index ?? 1) - 1;
          if (idx < 0 || idx >= queue.length) return outOfRangeMsg(queue);
          const dropped = queue.splice(idx, 1)[0];
          schedulePersist(ctx.sessionID);
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
          schedulePersist(ctx.sessionID);
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          if (!paused) await drain(ctx.sessionID);
          return `Retrying ${failedItems.length} failed messages`;
        }

        if (nextAction === "pause") {
          pausedBySession.add(ctx.sessionID);
          schedulePersist(ctx.sessionID, true);
          return "Queue paused.";
        }

        if (nextAction === "resume") {
          pausedBySession.delete(ctx.sessionID);
          schedulePersist(ctx.sessionID, true);
          await drain(ctx.sessionID);
          return "Queue resumed. Draining pending messages.";
        }

        if (nextAction === "reorder") {
          const from = index ?? 1;
          const target = to ?? 1;
          if (from < 1 || from > queue.length || target < 1 || target > queue.length)
            return outOfRangeMsg(queue);
          const [item] = queue.splice(from - 1, 1);
          queue.splice(target - 1, 0, item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          return `Moved item ${from} to position ${target}`;
        }

        if (nextAction === "insert") {
          const idx = index ?? 1;
          if (!text) return "No text provided.";
          if (!hasQueueCapacity(queue)) return queueFullMessage();
          if (idx < 1 || idx > queue.length + 1) return outOfRangeMsg(queue);
          const item = makeTextItem(ctx.sessionID, text);
          queue.splice(idx - 1, 0, item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          return `Inserted at position ${idx}: ${item.preview}`;
        }

        if (nextAction === "append") {
          if (!text) return "No text provided.";
          if (!hasQueueCapacity(queue)) return queueFullMessage();
          const item = makeTextItem(ctx.sessionID, text);
          queue.push(item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          return `Appended: ${item.preview}`;
        }

        if (nextAction === "prepend") {
          if (!text) return "No text provided.";
          if (!hasQueueCapacity(queue)) return queueFullMessage();
          const item = makeTextItem(ctx.sessionID, text);
          queue.unshift(item);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          return `Prepended: ${item.preview}`;
        }

        if (nextAction === "delete") {
          const idx = index ?? 1;
          if (idx < 1 || idx > queue.length) return outOfRangeMsg(queue);
          const dropped = queue.splice(idx - 1, 1)[0];
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          try { await showToast(ctx.sessionID); } catch { /* noop */ }
          return `Deleted: ${dropped.preview}`;
        }

        if (nextAction === "set") {
          const idx = index ?? 1;
          if (!text) return "No text provided.";
          if (idx < 1 || idx > queue.length) return outOfRangeMsg(queue);
          const item = makeTextItem(ctx.sessionID, text);
          queue[idx - 1] = item;
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          return `Set position ${idx}: ${item.preview}`;
        }

        if (nextAction === "sort") {
          queue.sort((a, b) => a.enqueuedAt - b.enqueuedAt);
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          return `Sorted ${queue.length} items by time (oldest first)`;
        }

        if (nextAction === "invert") {
          queue.reverse();
          queueBySession.set(ctx.sessionID, queue);
          schedulePersist(ctx.sessionID);
          return `Reversed ${queue.length} items`;
        }

        if (nextAction === "get") {
          const idx = index ?? 1;
          if (idx < 1 || idx > queue.length) return outOfRangeMsg(queue);
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
      "hide",
    ];

    function parseIndex(arg: string): number | null {
      const n = parseInt(arg, 10);
      return Number.isFinite(n) && n > 0 ? n : null;
    }

    // Index errors must explain WHY the queue can be shorter than the user
    // expects: in immediate mode the drain sends items on idle, so messages
    // queued moments ago may already be gone (live-reported confusion:
    // "added 3, rm 3 said out of range with 2 items" — one had drained).
    function outOfRangeMsg(queue: { length: number }[]): string {
      const base = `Index out of range. Queue has ${queue.length} items.`;
      return currentMode === "immediate"
        ? `${base} (Items may have been sent already — mode is immediate.)`
        : base;
    }

    const COMMAND_ALIASES: Record<string, string> = {
      list: "status",
      ls: "status",
      drop: "delete",
      rm: "delete",
    };

    // Sentinel returned by handleSlashCommand when the command was already
    // executed <2s ago through the other hook. Callers must ack silently
    // (replace parts, NO toast) — the first execution already toasted.
    const COMMAND_DUP_RESULT = "\u0000auto-queue-dup";
    let lastCommandRun: { key: string; ts: number } | null = null;

    function normalizeCommandName(cmd: string): string {
      let name = cmd.replace(/^\//, "").toLowerCase();
      if (name.startsWith("queue-")) name = name.slice("queue-".length);
      return name;
    }

    function handleSlashCommand(cmd: string, args: string, sessionID: string): string | null {
      let normalized = normalizeCommandName(cmd);
      if (normalized === "queue") {
        // Registered "/queue" command (or typed "/queue ..."): run the
        // sub-action directly in the plugin so it works while the session is
        // busy instead of being queued as an agent prompt.
        const trimmed = (args ?? "").trim();
        const first = trimmed.split(/\s+/)[0]?.toLowerCase() ?? "";
        if (!first) return null; // bare /queue -> agent-mediated via queue tool
        normalized = COMMAND_ALIASES[first] ?? first;
        args = trimmed.slice(first.length).trim();
      }
      if (!VALID_SLASH_COMMANDS.includes(normalized)) return null;

      // Single-flight guard (review F1): the same logical command reaches the
      // plugin through TWO hooks — command.execute.before (registered command)
      // and chat.message (raw text AND the queue.md template expansion, which
      // discards the before-hook's replaced parts). Without dedup every
      // /queue <action> executed twice: append queued two items, delete
      // destroyed the WRONG second item, clear produced conflicting toasts.
      // The 2s window only catches near-simultaneous hook deliveries; a human
      // re-running the same command lands outside it.
      const cmdKey = `${sessionID}|${normalized}|${args}`;
      if (lastCommandRun && lastCommandRun.key === cmdKey && Date.now() - lastCommandRun.ts < 2000) {
        return COMMAND_DUP_RESULT;
      }
      lastCommandRun = { key: cmdKey, ts: Date.now() };
      const queue = queueBySession.get(sessionID) ?? [];
      const pendingCount = getPendingCount(queue);
      const busy = isBusy(sessionID);
      const paused = pausedBySession.has(sessionID);
      const failedCount = queue.filter((i) => i.status === "failed").length;

      switch (normalized) {
    case "hold": {
      if (currentMode === "hold") return `Queue mode: hold (already active)`;
      currentMode = "hold";
      schedulePersist(undefined, false, true);
      return `Queue mode: hold (queued messages held until manually drained)`;
    }
    case "immediate": {
      if (currentMode === "immediate") return `Queue mode: immediate (already active)`;
      currentMode = "immediate";
      schedulePersist(undefined, false, true);
      drain(sessionID).catch(() => {});
      return `Queue mode: immediate (queued messages drain automatically on idle)`;
    }
        case "status": {
          // Explicitly asking for the queue listing also re-enables the
          // pinned toast (the user wants to see the queue).
          toastHiddenBySession.delete(sessionID);
          syncHeartbeat(sessionID);
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
        case "hide": {
          // The TUI toast cannot be dismissed by click (no handler exists in
          // the TUI), so this command is the manual "close" for the pinned
          // queue toast. It stays hidden until the queue changes or the user
          // runs /queue status.
          toastHiddenBySession.add(sessionID);
          syncHeartbeat(sessionID);
          return "Queue toast hidden. It will reappear when the queue changes (/queue status to show it again).";
        }
        case "clear": {
          const cleared = queue.length;
          queueBySession.set(sessionID, []);
          schedulePersist(sessionID);
          // No toast here: the command result toast reports the clear. The old
          // forced "Queue empty. All queued messages sent." here was a false
          // success signal — cleared items were discarded, not sent.
          return `Cleared ${cleared} messages from queue`;
        }
        case "pause": {
          pausedBySession.add(sessionID);
          schedulePersist(sessionID, true);
          return "Queue paused.";
        }
        case "resume": {
          pausedBySession.delete(sessionID);
          schedulePersist(sessionID, true);
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
          if (from > queue.length || to > queue.length) return outOfRangeMsg(queue);
          const [item] = queue.splice(from - 1, 1);
          queue.splice(to - 1, 0, item);
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          return `Moved item ${from} to position ${to}`;
        }
        case "insert": {
          const firstSpace = args.indexOf(" ");
          if (firstSpace === -1) return "Usage: /insert <index> <text>";
          const idx = parseIndex(args.slice(0, firstSpace));
          const insertText = args.slice(firstSpace + 1).trim();
          if (!idx) return "Invalid index. Must be a positive number.";
          if (!insertText) return "No text provided.";
          if (!hasQueueCapacity(queue)) return queueFullMessage();
          if (idx > queue.length + 1) return outOfRangeMsg(queue);
          const item = makeTextItem(sessionID, insertText);
          queue.splice(idx - 1, 0, item);
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          return `Inserted at position ${idx}: ${item.preview}`;
        }
        case "append": {
          const appendText = args.trim();
          if (!appendText) return "Usage: /append <text>";
          if (!hasQueueCapacity(queue)) return queueFullMessage();
          const item = makeTextItem(sessionID, appendText);
          queue.push(item);
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          return `Appended: ${item.preview}`;
        }
        case "prepend": {
          const prependText = args.trim();
          if (!prependText) return "Usage: /prepend <text>";
          if (!hasQueueCapacity(queue)) return queueFullMessage();
          const item = makeTextItem(sessionID, prependText);
          queue.unshift(item);
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          showToast(sessionID).catch(() => {});
          return `Prepended: ${item.preview}`;
        }
        case "delete": {
          const idx = parseIndex(args.trim());
          if (!idx) return "Usage: /delete <index> (1-based)";
          if (idx < 1 || idx > queue.length) return outOfRangeMsg(queue);
          const dropped = queue.splice(idx - 1, 1)[0];
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
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
          if (idx < 1 || idx > queue.length) return outOfRangeMsg(queue);
          const item = makeTextItem(sessionID, setText);
          queue[idx - 1] = item;
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          return `Set position ${idx}: ${item.preview}`;
        }
        case "sort": {
          queue.sort((a, b) => a.enqueuedAt - b.enqueuedAt);
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          return `Sorted ${queue.length} items by time (oldest first)`;
        }
        case "invert": {
          queue.reverse();
          queueBySession.set(sessionID, queue);
          schedulePersist(sessionID);
          return `Reversed ${queue.length} items`;
        }
        case "get": {
          const idx = parseIndex(args.trim());
          if (!idx) return "Usage: /get <index> (1-based)";
          if (idx < 1 || idx > queue.length) return outOfRangeMsg(queue);
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
        // Execute plugin-side, surface the result as a toast, and leave only
        // the hidden ack part in the message (see makeCommandAckPart). The
        // command text/result must never reach the model — otherwise the model
        // re-executes the action via the queue tool (double execution).
        // DUP: the chat.message hook re-delivered an already-executed command
        // — ack silently, the first execution already toasted.
        if (result !== COMMAND_DUP_RESULT) void showResultToast(result);
        output.parts = [makeCommandAckPart()];
      },

  event: async ({ event }: { event: any }) => {
    try {
      if (event.type === "session.status") {
        const { sessionID, status } = event.properties ?? {};
        if (typeof sessionID !== "string") return;
        const busy = status?.type !== "idle";
        busyBySession.set(sessionID, busy);
        if (!busy) lastIdleAt.set(sessionID, Date.now());
        if (!busy && currentMode === "immediate" && !pausedBySession.has(sessionID)) {
          await drain(sessionID);
        }
        return;
      }

      if (event.type === "session.idle") {
        const { sessionID } = event.properties ?? {};
        if (typeof sessionID !== "string") return;
        busyBySession.set(sessionID, false);
        lastIdleAt.set(sessionID, Date.now());
        if (currentMode === "immediate" && !pausedBySession.has(sessionID)) {
          await drain(sessionID);
        }
      }
    } catch {
      // A malformed event must not kill the handler (subsequent events would be lost).
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
          // Same contract as command.execute.before: result goes to a toast
          // only; the stored message keeps just the hidden ack part.
          if (result !== COMMAND_DUP_RESULT) void showResultToast(result);
          output.parts.length = 0;
          output.parts.push(makeCommandAckPart());
          return;
        }
      }
      // Command-FILE expansion: OpenCode expands /queue <action> (from
      // queue.md) into the template text BEFORE this hook runs, so the
      // startsWith("/queue") check above never matches and the expanded
      // prompt gets enqueued as a literal message while busy. Intercept the
      // template signature and run the action in the plugin instead.
      const templateMatch = firstText.text.match(/^Use the queue tool with action:\s*(.+)$/im);
      if (templateMatch) {
        const rawArgs = templateMatch[1].replace(/\.\s*$/, "").trim();
        const result = handleSlashCommand("queue", rawArgs, input.sessionID);
        if (result !== null) {
          // DUP: the before-hook already executed this command — ack silently.
          if (result !== COMMAND_DUP_RESULT) void showResultToast(result);
          output.parts.length = 0;
          output.parts.push(makeCommandAckPart());
          return;
        }
      }
    }

    // NOTE: no early return for draining sessions — with isBusy() covering the
    // drain window, messages arriving mid-drain are queued like any busy-turn
    // message instead of passing through raw (silent-loss class).

    const textParts = parts.filter((p: any) => p.type === "text");
    const allSystemReminders = textParts.every((p: any) =>
      typeof p.text === "string" && (p.text.startsWith("<system-reminder") || p.text.includes("Instructions from:"))
    );
    // A message with NO text parts at all must not take this early return:
    // [].every() is true, which let attachment-only messages pass through raw
    // into a busy session (accepted but no turn created = silent loss class,
    // review F7).
    if (textParts.length > 0 && allSystemReminders) return;

    const allSynthetic = parts.every((p: any) => p.synthetic || p.ignored || p.type !== "text");
    if (allSynthetic) return;

    const busy = isBusy(input.sessionID);
    if (!busy) {
      markBusy(input.sessionID);
      return;
    }

    const queue = getQueue(input.sessionID);
    if (!hasQueueCapacity(queue)) {
      try {
        await client.tui.showToast({
          body: {
            title: "Message Queue",
            message: queueFullMessage() + " Incoming message dropped.",
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

    // A newly queued message means the user cares again: un-hide the pinned
    // toast (it also refreshes via showToast below).
    toastHiddenBySession.delete(input.sessionID);
    schedulePersist(input.sessionID);
    try {
      await showToast(input.sessionID);
    } catch { /* TUI may not be active */ }
  },
    };
  },
};

export default AutoQueuePlugin;
