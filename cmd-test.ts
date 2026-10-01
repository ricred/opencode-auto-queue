// Bug regression tests: (1) command-file expansion of /queue <action> must be
// intercepted in chat.message (not self-enqueue); (2) reloadFromDisk must not
// fire the forced-empty success toast.
import { mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const repo = join(tmpdir(), "aq-cmd-" + Date.now());
mkdirSync(join(repo, ".git"), { recursive: true });

const mod = await import("./dist/index.js");
const factory = (mod.AutoQueuePlugin ?? mod.default);

const toasts: any[] = [];
const prompts: any[] = [];
let pass = 0, fail = 0;
const check = (name: string, ok: boolean, info = "") => {
  if (ok) { pass++; console.log(`PASS  ${name}`); }
  else { fail++; console.log(`FAIL  ${name}  -> ${info}`); }
};

const client = {
  tui: { showToast: async (c: any) => { toasts.push(c.body); } },
  session: { prompt: async (c: any) => { prompts.push(c.body); } },
  app: { info: async () => ({}) },
};

setTimeout(() => { console.log("WATCHDOG: hung. toasts=" + JSON.stringify(toasts.map(t=>t.message)) + " prompts=" + prompts.length); process.exit(2); }, 12000);
const hooks = await factory.server({ client, directory: repo, worktree: repo } as any);

const h = typeof hooks === "function" ? await hooks({ client } as any) : hooks;

// seed a busy session with 1 queued item
const mockOut = () => ({ message: { system: undefined, tools: undefined } });
await h["chat.message"](
  { sessionID: "s1", agent: "build", messageID: "m0", model: { providerID: "p", modelID: "m" } },
  { parts: [{ type: "text", text: "seed task" }], ...mockOut() },
);
await h["chat.message"](
  { sessionID: "s1", agent: "build", messageID: "m1", model: { providerID: "p", modelID: "m" } },
  { parts: [{ type: "text", text: "queued task A" }], ...mockOut() },
);

// ── Bug 1: command-file expansion while busy must NOT self-enqueue ──
const expandedStatus = {
  parts: [{
    type: "text",
    text: "Use the queue tool with action: status.\n\nActions: status, config, hold, immediate, clear, drop, peek, retry, pause, resume, count, reorder, insert, and more.",
  }],
};
await h["chat.message"]({ sessionID: "s1", agent: "build", messageID: "m2" }, { ...expandedStatus, ...mockOut() },
);
const statusParts = expandedStatus.parts as any[];
const ackOk = (parts: any[]) =>
  parts.length === 1 && parts[0].type === "text" && parts[0].synthetic === true && parts[0].text.includes("/queue command was handled internally");
const selfEnqueued = statusParts.some((p: any) => p.type === "text" && p.text.includes("Use the queue tool"));
check("expanded /queue status intercepted, not enqueued", !selfEnqueued, JSON.stringify(statusParts).slice(0, 120));
check("status replaced by hidden ack part (synthetic, no result text)", ackOk(statusParts), JSON.stringify(statusParts).slice(0, 200));
const statusResultToasted = toasts.some((t) => typeof t.message === "string" && t.message.includes("Queued:"));
check("status result surfaced as toast, not model content", statusResultToasted, JSON.stringify(toasts.map((t) => t.message)).slice(0, 200));

// ── raw "/queue clear" must execute + hide from session, model never acts ──
// enqueue a second item so clear has something to remove
await h["chat.message"](
  { sessionID: "s1", agent: "build", messageID: "m1b", model: { providerID: "p", modelID: "m" } },
  { parts: [{ type: "text", text: "queued task B" }], ...mockOut() },
);
const clearCmd = { parts: [{ type: "text", text: "/queue clear" }] };
await h["chat.message"]({ sessionID: "s1", agent: "build", messageID: "m3" }, { ...clearCmd, ...mockOut() });
const clearParts = clearCmd.parts as any[];
check("/queue clear replaced by hidden ack part", ackOk(clearParts), JSON.stringify(clearParts).slice(0, 200));
check("/queue clear result shown as toast", toasts.some((t) => typeof t.message === "string" && t.message.includes("Cleared 2")), JSON.stringify(toasts.map((t) => t.message)).slice(0, 200));
const cmdOut = { parts: [{ type: "text", text: "template never used" }] };
await h["command.execute.before"](
  { command: "queue", sessionID: "s1", arguments: "clear" },
  cmdOut as any,
);
check("command.execute.before replaces template parts with ack", ackOk(cmdOut.parts as any[]), JSON.stringify(cmdOut.parts).slice(0, 200));

// ── Bug 2: no forced-empty toast from reload ──
// (reloadFromDisk fires via watcher; force it deterministically)
// watcher watches repo/.git/queue.json — writes happen on every persist
await fsSleep();
function fsSleep() { return new Promise((r) => setTimeout(r, 400)); }

const emptyLie = toasts.filter((t) => t.message?.includes("All queued messages sent")).length;
check("no phantom 'queue empty' toast from watcher reload", emptyLie === 0, `toasts=${JSON.stringify(toasts)}`);

// ── sanity: cleared queue delivers nothing on idle ──
// free the session (idle event); drain must find nothing (clear removed both items)
await h.event({ event: { type: "session.idle", properties: { sessionID: "s1" } } });
await new Promise((r) => setTimeout(r, 500));
check("clear removed queued items (no delivery on idle)", prompts.length === 0, `prompts=${prompts.length}`);

rmSync(repo, { recursive: true, force: true });

// ── Pinned queue toast (heartbeat) ──
// Fresh plugin instance with a fast heartbeat so timers are testable.
const repo2 = join(tmpdir(), "aq-hb-" + Date.now());
mkdirSync(join(repo2, ".git"), { recursive: true });
const toasts2: any[] = [];
const client2 = {
  tui: { showToast: async (c: any) => { toasts2.push(c.body); } },
  session: { prompt: async () => {} },
  app: { info: async () => ({}) },
};
const hooks2 = await factory.server({ client: client2, directory: repo2, worktree: repo2 } as any, { queueToastHeartbeatMs: 50 });
const h2 = typeof hooks2 === "function" ? await hooks2({ client: client2 } as any) : hooks2;

await h2["chat.message"](
  { sessionID: "s2", agent: "build", messageID: "n0", model: { providerID: "p", modelID: "m" } },
  { parts: [{ type: "text", text: "hb busy-maker" }], ...mockOut() },
);
await h2["chat.message"](
  { sessionID: "s2", agent: "build", messageID: "n0b", model: { providerID: "p", modelID: "m" } },
  { parts: [{ type: "text", text: "hb task" }], ...mockOut() },
);
const afterEnqueue = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
await new Promise((r) => setTimeout(r, 170));
const pinnedCount = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
check("heartbeat re-posts queue toast while pending", pinnedCount >= afterEnqueue + 2, `posts=${pinnedCount} (enqueue=${afterEnqueue})`);

// /queue hide = the manual "close" (TUI has no toast dismissal)
await h2["chat.message"](
  { sessionID: "s2", agent: "build", messageID: "n1" },
  { parts: [{ type: "text", text: "/queue hide" }], ...mockOut() },
);
const afterHide = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
await new Promise((r) => setTimeout(r, 170));
const hiddenCount = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
check("/queue hide stops the heartbeat", hiddenCount === afterHide, `before=${afterHide} after=${hiddenCount}`);

// /queue status re-enables it
await h2["chat.message"](
  { sessionID: "s2", agent: "build", messageID: "n2" },
  { parts: [{ type: "text", text: "/queue status" }], ...mockOut() },
);
const afterStatus = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
await new Promise((r) => setTimeout(r, 170));
const resumedCount = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
check("/queue status re-enables heartbeat", resumedCount > afterStatus, `before=${afterStatus} after=${resumedCount}`);

// drain to empty -> heartbeat must stop (no posts after the empty toast).
// The drain path sleeps 1500ms before posting the empty toast — wait past it.
await h2.event({ event: { type: "session.idle", properties: { sessionID: "s2" } } });
await new Promise((r) => setTimeout(r, 2400));
const finalCount = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
const hasEmpty = toasts2.some((t) => t.message?.includes("All queued messages sent"));
await new Promise((r) => setTimeout(r, 300));
const stoppedCount = toasts2.filter((t) => t.message?.startsWith("Queue (")).length;
check("drain to empty stops heartbeat", stoppedCount === finalCount && hasEmpty, `final=${finalCount} stopped=${stoppedCount} emptyToast=${hasEmpty}`);

rmSync(repo2, { recursive: true, force: true });

console.log(`\n${pass}/${pass + fail}`);
process.exit(fail === 0 ? 0 : 1);




