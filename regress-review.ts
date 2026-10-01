// Regression tests for code-review findings F1, F2, F3 (commit after f25ca40).
// F1: slash command executed via BOTH hooks must run ONCE (single-flight).
// F2: /queue clear during an in-flight drain must not resurrect cleared items.
// F3: reloadFromDisk must not wipe sessions with un-persisted mutations.
import { mkdirSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let pass = 0;
let fail = 0;
function check(name: string, cond: boolean, extra?: string) {
  if (cond) { pass++; console.log(`PASS  ${name}`); }
  else { fail++; console.log(`FAIL  ${name}${extra ? " -> " + extra : ""}`); }
}

async function makePlugin(repo: string, promptImpl?: (call: number, body: any) => Promise<any>) {
  const mod = await import("./dist/index.js");
  const factory = mod.AutoQueuePlugin ?? mod.default;
  let promptCalls = 0;
  const client: any = {
    tui: { showToast: async () => {} },
    session: {
      prompt: async (p: any) => {
        promptCalls++;
        if (promptImpl) return promptImpl(promptCalls, p);
      },
    },
    app: { info: async () => ({}) },
  };
  const hooks = await factory.server({ client, directory: repo, worktree: repo } as any);
  return { hooks, client, getPromptCalls: () => promptCalls };
}
const out = () => ({ parts: [{ type: "text", text: "placeholder" }] } as any);

// ---------- F1: sequential hook delivery executes once ----------
{
  const repo = join(tmpdir(), "aq-f1-" + Date.now());
  mkdirSync(join(repo, ".git"), { recursive: true });
  const { hooks } = await makePlugin(repo);
  const tool = hooks.tool.queue;

  // First hook: registered command path executes the action.
  const o1 = out();
  await hooks["command.execute.before"]({ command: "queue", sessionID: "s1", arguments: "append task X" }, o1);
  // Second hook, immediately: queue.md template expansion of the SAME command
  // arrives as the OUTPUT of chat.message (this is the object the hook mutates).
  const o2: any = {
    parts: [{ type: "text", text: "Use the queue tool with action: append task X." }],
    message: { agent: "a", model: "m" },
  };
  await hooks["chat.message"]({ sessionID: "s1", agent: "a", model: "m", messageID: "mid1" }, o2);
  const st1 = JSON.parse(JSON.stringify(await tool.execute({ action: "count" }, { sessionID: "s1" })));
  const countText1 = st1?.content?.[0]?.text ?? JSON.stringify(st1);
  check("F1: double-hook delivery queues item ONCE", /1/.test(countText1) && !/2/.test(countText1), countText1);
  check("F1: dup delivery got silent ack part", o2.parts.length === 1 && o2.parts[0].metadata?.__auto_queue_internal === true);

  // Control: a DIFFERENT command immediately after must NOT be blocked.
  const o3 = out();
  await hooks["command.execute.before"]({ command: "queue", sessionID: "s1", arguments: "append task Y" }, o3);
  const st2 = JSON.parse(JSON.stringify(await tool.execute({ action: "count" }, { sessionID: "s1" })));
  const countText2 = st2?.content?.[0]?.text ?? JSON.stringify(st2);
  check("F1: different command still executes", /2/.test(countText2), countText2);
  rmSync(repo, { recursive: true, force: true });
}

// ---------- F2: clear during in-flight drain (stale-reference resurrection) ----------
{
  const repo = join(tmpdir(), "aq-f2-" + Date.now());
  mkdirSync(join(repo, ".git"), { recursive: true });
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const { hooks, getPromptCalls } = await makePlugin(repo, async () => { await gate; });
  const tool = hooks.tool.queue;

  await tool.execute({ action: "append", text: "task A" }, { sessionID: "s2" });
  await tool.execute({ action: "append", text: "task B" }, { sessionID: "s2" });

  // session.idle in immediate mode starts the drain; drainDelayMs=500 delay
  // runs BEFORE the first pick, so wait past it. The prompt then blocks on `gate`.
  const ev = hooks["event"];
  const evDone = ev({ event: { type: "session.idle", properties: { sessionID: "s2" } } });
  await sleep(700); // 500ms drain delay + margin
  check("F2: drain in flight (A sending)", getPromptCalls() === 1, `prompt calls: ${getPromptCalls()}`);

  await tool.execute({ action: "clear" }, { sessionID: "s2" });
  release(); // A's prompt resolves
  await evDone;
  await sleep(150);
  check("F2: cleared item B was NOT sent", getPromptCalls() === 1, `prompt calls: ${getPromptCalls()}`);
  const st = JSON.parse(JSON.stringify(await tool.execute({ action: "count" }, { sessionID: "s2" })));
  const t = st?.content?.[0]?.text ?? JSON.stringify(st);
  check("F2: queue empty after clear+drain", /0/.test(t), t);
  rmSync(repo, { recursive: true, force: true });
}

// ---------- F3: reload must not wipe a dirty session ----------
{
  const repo = join(tmpdir(), "aq-f3-" + Date.now());
  mkdirSync(join(repo, ".git"), { recursive: true });
  const { hooks } = await makePlugin(repo);
  const tool = hooks.tool.queue;
  const persistPath = join(repo, ".git", "queue.json");

  await tool.execute({ action: "append", text: "survivor" }, { sessionID: "s3" });
  await sleep(1600); // let the debounced persist land (item now on disk)
  check("F3: item persisted to disk", readFileSync(persistPath, "utf8").includes("survivor"));

  // Simulate another instance writing a file WITHOUT the item.
  const external = JSON.stringify({ version: 1, mode: "immediate", queues: {}, pausedSessions: [] }, null, 2);
  writeFileSync(persistPath, external);

  // Watcher (poll-based) reloads; the session is dirty -> memory must win.
  let survived = false;
  for (let i = 0; i < 30; i++) {
    await sleep(100);
    const st = JSON.parse(JSON.stringify(await tool.execute({ action: "peek" }, { sessionID: "s3" })));
    const t = st?.content?.[0]?.text ?? JSON.stringify(st);
    if (!t.includes("survivor")) { survived = false; break; }
    survived = true;
    // Convergence: disk healed back to memory's view?
    if (readFileSync(persistPath, "utf8").includes("survivor")) break;
  }
  check("F3: enqueued item survives external reload", survived);
  check("F3: disk converged back to memory", readFileSync(persistPath, "utf8").includes("survivor"));
  rmSync(repo, { recursive: true, force: true });
}

console.log(`\n${pass + fail} checks, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
