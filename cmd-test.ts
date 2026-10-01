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
const selfEnqueued = statusParts.some((p) => p.type === "text" && p.text.includes("Use the queue tool"));
check("expanded /queue status intercepted, not enqueued", !selfEnqueued, JSON.stringify(statusParts).slice(0, 120));
check("status result injected as internal part", statusParts.some((p) => p.type === "text" && p.text.includes("Queued:")), JSON.stringify(statusParts).slice(0, 120));

// ── Bug 2: no forced-empty toast from reload ──
// (reloadFromDisk fires via watcher; force it deterministically)
// watcher watches repo/.git/queue.json — writes happen on every persist
await fsSleep();
function fsSleep() { return new Promise((r) => setTimeout(r, 400)); }

const emptyLie = toasts.filter((t) => t.message?.includes("All queued messages sent")).length;
check("no phantom 'queue empty' toast from watcher reload", emptyLie === 0, `toasts=${JSON.stringify(toasts)}`);

// ── sanity: real drain still fires success toast when queue empties ──
// free the session (idle event) and let drain send the queued item
await h.event({ event: { type: "session.idle", properties: { sessionID: "s1" } } });
await new Promise((r) => setTimeout(r, 500));
check("real drain delivered queued item", prompts.length === 1, `prompts=${prompts.length}`);

rmSync(repo, { recursive: true, force: true });
console.log(`\n${pass}/${pass + fail}`);
process.exit(fail === 0 ? 0 : 1);




