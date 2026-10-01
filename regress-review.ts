// Regression tests for code-review findings F1, F2, F3 (commit after f25ca40).
// F1: slash command executed via BOTH hooks must run ONCE (single-flight).
// F2: /queue clear during an in-flight drain must not resurrect cleared items.
// F3: dirty mutations survive reload; clean sessions reconcile external changes.
import { mkdirSync, rmSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let pass = 0;
let fail = 0;
function check(name: string, cond: boolean, extra?: string) {
  if (cond) { pass++; console.log(`PASS  ${name}`); }
  else { fail++; console.log(`FAIL  ${name}${extra ? " -> " + extra : ""}`); }
}

async function makePlugin(repo: string, promptImpl?: (call: number, body: any) => Promise<any>, options: any = {}) {
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
  const hooks = await factory.server({ client, directory: repo, worktree: repo } as any, options);
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

// ---------- F3: persisted sessions reconcile external changes ----------
{
  const repo = join(tmpdir(), "aq-f3-" + Date.now());
  mkdirSync(join(repo, ".git"), { recursive: true });
  const { hooks } = await makePlugin(repo, undefined, { persistDebounceMs: 30, watchDebounceMs: 30, queueToastHeartbeatMs: 0 });
  const tool = hooks.tool.queue;
  const persistPath = join(repo, ".git", "queue.json");

  // Pre-write an unrelated session so both dirty-mutation survival and clean
  // session reconciliation remain covered after revisioned dirty tracking.
  writeFileSync(persistPath, JSON.stringify({ version: 1, mode: "immediate", queues: {
    other: [{ sessionID: "other", parts: [{ type: "text", text: "old-other" }], preview: "old-other", status: "queued", retries: 0, enqueuedAt: Date.now() }],
  }, pausedSessions: [] }, null, 2));

  await tool.execute({ action: "append", text: "survivor" }, { sessionID: "s3" });
  await sleep(150); // let the debounced persist land (item now on disk)
  check("F3: item persisted to disk", readFileSync(persistPath, "utf8").includes("survivor"));

  // Re-add a new s3 mutation after prior commit was persisted. The watcher
  // sees an external state without it, but must preserve this active mutation.
  await tool.execute({ action: "append", text: "dirty-s3" }, { sessionID: "s3" });
  const external = JSON.stringify({
    version: 1,
    mode: "immediate",
    queues: { other: [{ sessionID: "other", parts: [{ type: "text", text: "external" }], preview: "external", status: "queued", retries: 0, enqueuedAt: Date.now() }] },
    pausedSessions: [],
  }, null, 2);
  writeFileSync(persistPath, external);

  // Dirty s3 survives; clean other session accepts external add/change.
  let dirtySurvived = false;
  let cleanReconciled = false;
  for (let i = 0; i < 30; i++) {
    await sleep(100);
    const s3 = await tool.execute({ action: "status" }, { sessionID: "s3" });
    const other = await tool.execute({ action: "status" }, { sessionID: "other" });
    if (/dirty-s3/.test(s3)) dirtySurvived = true;
    if (/external/.test(other)) cleanReconciled = true;
    if (dirtySurvived && cleanReconciled && readFileSync(persistPath, "utf8").includes("dirty-s3")) break;
  }
  check("F3: in-flight dirty session survives external replacement", dirtySurvived);
  check("F3: clean session accepts external addition", cleanReconciled);
  check("F3: dirty session merge preserves external session", readFileSync(persistPath, "utf8").includes("dirty-s3") && readFileSync(persistPath, "utf8").includes("external"));
  await sleep(150);
  const currentOther = await tool.execute({ action: "status" }, { sessionID: "other" });
  check("F3: clean session marker clears for future edits", /external/.test(currentOther));
  rmSync(repo, { recursive: true, force: true });
}

// ---------- Concurrent plugin instances merge separate sessions ----------
{
  const repo = join(tmpdir(), "aq-concurrent-saves-" + Date.now());
  mkdirSync(join(repo, ".git"), { recursive: true });
  const persistPath = join(repo, ".git", "queue.json");
  const options = { persistDebounceMs: 25, watchDebounceMs: 10_000, queueToastHeartbeatMs: 0 };
  const [first, second] = await Promise.all([
    makePlugin(repo, undefined, options),
    makePlugin(repo, undefined, options),
  ]);
  await Promise.all([
    first.hooks.tool.queue.execute({ action: "append", text: "from-first" }, { sessionID: "first" }),
    second.hooks.tool.queue.execute({ action: "append", text: "from-second" }, { sessionID: "second" }),
  ]);
  await sleep(200);
  const saved = readFileSync(persistPath, "utf8");
  const state = JSON.parse(saved);
  check("concurrency: both sessions survive concurrent saves", /from-first/.test(saved) && /from-second/.test(saved));
  check("concurrency: both session keys retained", !!state.queues.first && !!state.queues.second);

  const moduleUrl = new URL("./dist/index.js", import.meta.url).href;
  const childWriter = (sessionID: string, text: string) => [
    `import { AutoQueuePlugin } from ${JSON.stringify(moduleUrl)};`,
    `const repo = ${JSON.stringify(repo)};`,
    `const client = { tui: { showToast: async () => {} }, session: { prompt: async () => {} }, app: { info: async () => ({}) } };`,
    `const hooks = await AutoQueuePlugin.server({ client, directory: repo, worktree: repo }, { persistDebounceMs: 25, watchDebounceMs: 10000, queueToastHeartbeatMs: 0 });`,
    `await hooks.tool.queue.execute({ action: "append", text: ${JSON.stringify(text)} }, { sessionID: ${JSON.stringify(sessionID)} });`,
    `await new Promise((resolve) => setTimeout(resolve, 150));`,
    `process.exit(0);`,
  ].join("\n");
  const children = [
    Bun.spawn([process.execPath, "-e", childWriter("child-a", "from-child-a")], { cwd: repo, stdout: "ignore", stderr: "pipe" }),
    Bun.spawn([process.execPath, "-e", childWriter("child-b", "from-child-b")], { cwd: repo, stdout: "ignore", stderr: "pipe" }),
  ];
  const childResults = await Promise.all(children.map(async (child) => ({
    code: await child.exited,
    stderr: await new Response(child.stderr).text(),
  })));
  const afterChildren = readFileSync(persistPath, "utf8");
  const childState = JSON.parse(afterChildren);
  check("concurrency: independent processes exit cleanly", childResults.every((result) => result.code === 0), childResults.map((r) => r.stderr).join("\n"));
  check("concurrency: independent process sessions merge", /from-child-a/.test(afterChildren) && /from-child-b/.test(afterChildren));
  check("concurrency: all process and local sessions retained", ["first", "second", "child-a", "child-b"].every((sid) => !!childState.queues[sid]));
  rmSync(repo, { recursive: true, force: true });
}

// ---------- Worktree: .git pointer file resolves to real gitdir ----------
{
  const root = join(tmpdir(), "aq-worktree-" + Date.now());
  const worktree = join(root, "project-worktree");
  const gitdir = join(root, "main-repo", ".git", "worktrees", "project-worktree");
  mkdirSync(worktree, { recursive: true });
  mkdirSync(gitdir, { recursive: true });
  writeFileSync(join(worktree, ".git"), `gitdir: ${gitdir}\n`, "utf8");

  const mod = await import("./dist/index.js");
  const factory = mod.AutoQueuePlugin ?? mod.default;
  const client: any = {
    tui: { showToast: async () => {} },
    session: { prompt: async () => {} },
    app: { info: async () => ({}) },
  };
  const hooks = await factory.server({ client, directory: worktree, worktree } as any, { persistDebounceMs: 30 });
  await hooks.tool.queue.execute({ action: "append", text: "worktree-persisted" }, { sessionID: "s4" });
  await sleep(150);

  const realQueueFile = join(gitdir, "queue.json");
  check("worktree: queue state written to resolved gitdir", existsSync(realQueueFile));
  check("worktree: queue state survives (not written under .git file)",
    existsSync(realQueueFile) && readFileSync(realQueueFile, "utf8").includes("worktree-persisted"));
  check("worktree: no invalid .git/queue.json path", !existsSync(join(worktree, ".git", "queue.json")));
  rmSync(root, { recursive: true, force: true });
}

// ---------- Prompt errors and queue capacity ----------
{
  const repo = join(tmpdir(), "aq-review-limits-" + Date.now());
  mkdirSync(join(repo, ".git"), { recursive: true });
  let promptOptions: any;
  const { hooks, getPromptCalls } = await makePlugin(repo, async (_call, request) => {
    promptOptions = request;
    return { error: { name: "BadRequestError", data: { message: "invalid prompt" } }, response: { status: 400 } };
  }, { maxQueueSize: 2, maxRetries: 1, drainDelayMs: 5, persistDebounceMs: 10, queueToastHeartbeatMs: 0 });
  const tool = hooks.tool.queue;
  const sid = "limits";

  await tool.execute({ action: "append", text: "one" }, { sessionID: sid });
  await tool.execute({ action: "insert", index: 1, text: "two" }, { sessionID: sid });
  check("capacity: tool insert/add fills cap", /2 messages/.test(await tool.execute({ action: "count" }, { sessionID: sid })));
  check("capacity: tool append rejected at cap", /Queue full/.test(await tool.execute({ action: "append", text: "overflow" }, { sessionID: sid })));
  check("capacity: tool prepend rejected at cap", /Queue full/.test(await tool.execute({ action: "prepend", text: "overflow" }, { sessionID: sid })));

  const slash = (args: string) => hooks["command.execute.before"]({ command: "queue", sessionID: sid, arguments: args }, out());
  await slash("delete 1");
  await slash("append allowed-again");
  check("capacity: slash append accepted below cap", /2 messages/.test(await tool.execute({ action: "count" }, { sessionID: sid })));
  await slash("insert 1 overflow");
  check("capacity: slash insert rejected at cap", /2 messages/.test(await tool.execute({ action: "count" }, { sessionID: sid })));
  await slash("prepend overflow");
  check("capacity: slash prepend rejected at cap", /2 messages/.test(await tool.execute({ action: "count" }, { sessionID: sid })));

  // Drive item through real chat ingress, then idle drain gets SDK-style 400 result.
  await tool.execute({ action: "clear" }, { sessionID: sid });
  await hooks.event({ event: { type: "session.status", properties: { sessionID: sid, status: { type: "busy" } } } });
  const incoming = { parts: [{ type: "text", text: "prompt-400" }] , message: { agent: "a", model: "m" } };
  await hooks["chat.message"]({ sessionID: sid, agent: "a", model: "m" }, incoming);
  await hooks.event({ event: { type: "session.idle", properties: { sessionID: sid } } });
  await sleep(50);
  const status = await tool.execute({ action: "status" }, { sessionID: sid });
  check("prompt error: SDK throwOnError requested", promptOptions?.throwOnError === true);
  check("prompt error: resolved 400 retained as failed", /Queued: 1/.test(status) && /Failed: 1/.test(status));
  check("prompt error: 400 not auto-retried", getPromptCalls() === 1, `calls=${getPromptCalls()}`);
  rmSync(repo, { recursive: true, force: true });
}

// ---------- Restored overflow remains intact and blocks new additions ----------
{
  const repo = join(tmpdir(), "aq-review-restore-cap-" + Date.now());
  const git = join(repo, ".git");
  mkdirSync(git, { recursive: true });
  writeFileSync(join(git, "queue.json"), JSON.stringify({
    version: 1,
    mode: "immediate",
    pausedSessions: ["restore"],
    queues: { restore: ["a", "b", "c"].map((text) => ({ sessionID: "restore", parts: [{ type: "text", text }], preview: text, status: "queued", retries: 0, enqueuedAt: Date.now() })) },
  }));
  const { hooks } = await makePlugin(repo, undefined, { maxQueueSize: 2, queueToastHeartbeatMs: 0 });
  const tool = hooks.tool.queue;
  check("restore cap: preserve existing oversized work", /Queued: 3/.test(await tool.execute({ action: "status" }, { sessionID: "restore" })));
  check("restore cap: reject further append", /Queue full/.test(await tool.execute({ action: "append", text: "four" }, { sessionID: "restore" })));
  rmSync(repo, { recursive: true, force: true });
}

console.log(`\n${pass + fail} checks, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
