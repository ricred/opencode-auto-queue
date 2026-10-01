/**
 * Smoke test for opencode-auto-queue (local build).
 * Executes the real plugin lifecycle: enqueue while busy -> tool controls ->
 * idle drain with transient-failure retry -> instant /queue-clear interception.
 * Run: bun smoke-test.ts
 */
import { AutoQueuePlugin } from "./dist/index.js";
import { existsSync, rmSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";

const dir = "C:\\Users\\ricka\\AppData\\Local\\Temp\\opencode\\aq-smoke";
rmSync(dir, { recursive: true, force: true });
mkdirSync(`${dir}\\.git`, { recursive: true });

const prompts: any[] = [];
let failFirstPrompt = true;
const toasts: any[] = [];

const client: any = {
  session: {
    prompt: async (args: any) => {
      if (failFirstPrompt) {
        failFirstPrompt = false;
        throw new Error("network error: connection reset");
      }
      prompts.push(args);
      return {};
    },
  },
  tui: {
    showToast: async (args: any) => {
      toasts.push(args);
      return {};
    },
  },
};

const hooks: any = await (AutoQueuePlugin as any).server(
  { client, directory: dir },
  { drainDelayMs: 5, retryBaseDelayMs: 10, retryMaxDelayMs: 50, persistDebounceMs: 10, toastDurationMs: 100 },
);

let failures = 0;
function check(name: string, cond: boolean, detail = "") {
  console.log(`${cond ? "PASS" : "FAIL"}  ${name}${detail ? "  -> " + detail : ""}`);
  if (!cond) failures++;
}

function mkOut(text: string): any {
  return { parts: [{ type: "text", text, id: "px", messageID: "m1", sessionID: "s1" }], message: { agent: "build", model: { providerID: "zai-proxy", modelID: "glm5.3-flash" } } };
}

// 0) DEFAULT MODE: immediate (regression for "queue never deploys" hold-mode bug)
{
  const out = { parts: [{ type: "text", text: "x", id: "p", messageID: "m", sessionID: "sX" }] };
  await hooks["command.execute.before"]({ command: "queue", sessionID: "sX", arguments: "modecheck" }, out as any).catch(() => {});
  const cfgText = await hooks.tool.queue.execute({ action: "config" }, { sessionID: "sX" });
  check("default mode is immediate", /mode: immediate/.test(cfgText), cfgText.split("\n").find((l: string) => l.includes("mode:")));
}

// 0b) /queue list + /queue drop N intercepted in command.execute.before (busy-safe),
//     aliases list->status, drop->delete; unknown action falls through to agent.
{
  const q = hooks.tool.queue;
  await q.execute({ action: "append", text: "queued task A" }, { sessionID: "sC" });
  await q.execute({ action: "append", text: "queued task B" }, { sessionID: "sC" });
  const outList: any = { parts: [{ type: "text", text: "placeholder" }] };
  await hooks["command.execute.before"]({ command: "queue", sessionID: "sC", arguments: "list" }, outList);
  check("/queue list intercepted (alias -> status), shows 2 items",
    outList.parts[0]?.metadata?.__auto_queue_internal === true && /Queued: 2/.test(outList.parts[0].text), outList.parts[0].text.split("\n")[3]);
  const outDrop: any = { parts: [{ type: "text", text: "placeholder" }] };
  await hooks["command.execute.before"]({ command: "queue", sessionID: "sC", arguments: "drop 1" }, outDrop);
  check("/queue drop 1 intercepted (alias -> delete), removed task A",
    outDrop.parts[0]?.metadata?.__auto_queue_internal === true && /Deleted: queued task A/.test(outDrop.parts[0].text), outDrop.parts[0].text);
  const outBogus: any = { parts: [{ type: "text", text: "untouched" }] };
  await hooks["command.execute.before"]({ command: "queue", sessionID: "sC", arguments: "frobnicate" }, outBogus);
  check("/queue <unknown> falls through to agent (parts untouched)", outBogus.parts[0].text === "untouched");
  const cnt = await q.execute({ action: "count" }, { sessionID: "sC" });
  check("drop actually removed item", /^1 messages/.test(cnt), cnt);
}

// 0c) REGRESSION: stale persisted mode "hold" must NOT override immediate default
{
  const dir2 = dir + "-hold";
  rmSync(dir2, { recursive: true, force: true });
  mkdirSync(`${dir2}\\.git`, { recursive: true });
  const { writeFileSync } = await import("node:fs");
  writeFileSync(`${dir2}\\.git\\queue.json`, JSON.stringify({ version: 1, mode: "hold", queues: {}, pausedSessions: [] }));
  const hooks2: any = await (AutoQueuePlugin as any).server({ client, directory: dir2 }, { drainDelayMs: 5 });
  const cfg2 = await hooks2.tool.queue.execute({ action: "config" }, { sessionID: "sY" });
  check("persisted hold does not override immediate default", /mode: immediate/.test(cfg2), cfg2.split("\n").find((l: string) => l.includes("mode:")));
}

// 0d) REGRESSION: drain must refuse while busy (silent message loss path),
//     /queue-immediate while busy sends nothing, idle event then drains,
//     malformed events do not throw
{
  const s = "sBusy";
  await hooks.event({ event: { type: "session.status", properties: { sessionID: s, status: { type: "busy" } } } });
  await hooks["chat.message"]({ sessionID: s }, mkOut("busy msg 1"));
  await hooks.event({ event: { type: "session.status", properties: {} } });
  await hooks.event({ event: { type: "session.idle", properties: {} } });
  const before = prompts.length;
  await hooks["command.execute.before"]({ command: "queue", sessionID: s, arguments: "hold" }, { parts: [] } as any);
  await hooks["command.execute.before"]({ command: "queue", sessionID: s, arguments: "immediate" }, { parts: [] } as any);
  await new Promise((r) => setTimeout(r, 30));
  check("drain refuses while busy (no silent send)", prompts.length === before, `prompts=${prompts.length}`);
  await hooks.event({ event: { type: "session.status", properties: { sessionID: s, status: { type: "idle" } } } });
  await new Promise((r) => setTimeout(r, 40));
  check("idle after busy drains queued items", prompts.length === before + 1, `prompts ${before}->${prompts.length}`);
}

// 0e) REGRESSION: toast durations - enqueue toast 10s default (was 24h sticky), failed toast 60s
{
  const toasts2: any[] = [];
  const client2: any = {
    session: { prompt: async () => { throw new Error("bad request"); } },
    tui: { showToast: async (args: any) => { toasts2.push(args); return {}; } },
  };
  const dir3 = dir + "-toast";
  rmSync(dir3, { recursive: true, force: true });
  mkdirSync(`${dir3}\\.git`, { recursive: true });
  const h3: any = await (AutoQueuePlugin as any).server(
    { client: client2, directory: dir3 },
    { drainDelayMs: 5, retryBaseDelayMs: 5, retryMaxDelayMs: 10, persistDebounceMs: 10 },
  );
  await h3.event({ event: { type: "session.status", properties: { sessionID: "sT", status: { type: "busy" } } } });
  await h3["chat.message"]({ sessionID: "sT" }, mkOut("toast dur check"));
  await new Promise((r) => setTimeout(r, 10));
  const queueToast = toasts2.find((t) => (t.body?.message ?? "").includes("toast dur check"));
  check("enqueue toast uses 10s default (was 24h)", !!queueToast && queueToast.body.duration === 10_000, `duration=${queueToast?.body?.duration}`);
  await h3.event({ event: { type: "session.status", properties: { sessionID: "sT", status: { type: "idle" } } } });
  await new Promise((r) => setTimeout(r, 80));
  const failedToast = toasts2.find((t) => /Failed/.test(t.body?.message ?? ""));
  check("failed toast uses 60s default", !!failedToast && failedToast.body.duration === 60_000, `duration=${failedToast?.body?.duration}`);
}

// 0f) REGRESSION: external file change must not revert unsaved in-memory state
//     (reloadFromDisk now flushes pending changes before reading)
{
  const dirW = dir + "-watch";
  rmSync(dirW, { recursive: true, force: true });
  mkdirSync(`${dirW}\\.git`, { recursive: true });
  const hW: any = await (AutoQueuePlugin as any).server(
    { client, directory: dirW },
    { drainDelayMs: 5, persistDebounceMs: 50, watchDebounceMs: 20 },
  );
  const sW = "sW";
  const qFile = `${dirW}\\.git\\queue.json`;
  await hW.event({ event: { type: "session.status", properties: { sessionID: sW, status: { type: "busy" } } } });
  await hW["chat.message"]({ sessionID: sW }, mkOut("flush A"));
  await new Promise((r) => setTimeout(r, 150)); // persisted; watcher reload no-ops
  const stale = readFileSync(qFile, "utf-8"); // A-only state
  await hW["chat.message"]({ sessionID: sW }, mkOut("flush B")); // pending, not yet persisted
  writeFileSync(qFile, stale, "utf-8"); // external touch with STALE content
  await new Promise((r) => setTimeout(r, 250)); // watcher reload cycle
  const st = await hW.tool.queue.execute({ action: "status" }, { sessionID: sW });
  check("external change does not revert unsaved enqueue", /Queued: 2/.test(st) && st.includes("flush B"), (st.split("\n").find((l: string) => l.includes("messages")) ?? st).trim());
  await new Promise((r) => setTimeout(r, 100)); // let debounce flush settle
  check("flushed state persisted (B on disk)", readFileSync(qFile, "utf-8").includes("flush B"));
  check("no .tmp residue after persist", !existsSync(`${qFile}.tmp`));
}

// 0g) REGRESSION: one item per drain pass - prompt must never land while a
//     previous turn is still running (mock models 150ms turns; a second call
//     inside the window = the silent-loss signature)
{
  const lost: string[] = [];
  const sent: string[] = [];
  let turnActiveUntil = 0;
  const clientM: any = {
    session: {
      prompt: async (args: any) => {
        const text = args.body?.parts?.find((p: any) => p.type === "text")?.text ?? "?";
        if (Date.now() < turnActiveUntil) lost.push(text);
        sent.push(text);
        turnActiveUntil = Date.now() + 150;
        await new Promise((r) => setTimeout(r, 10)); // resolves before turn ends
        return {};
      },
    },
    tui: { showToast: async () => ({}) },
  };
  const dirM = dir + "-multi";
  rmSync(dirM, { recursive: true, force: true });
  mkdirSync(`${dirM}\\.git`, { recursive: true });
  const hM: any = await (AutoQueuePlugin as any).server(
    { client: clientM, directory: dirM },
    { drainDelayMs: 5, persistDebounceMs: 10 },
  );
  const sM = "sM";
  await hM.event({ event: { type: "session.status", properties: { sessionID: sM, status: { type: "busy" } } } });
  for (const t of ["m1", "m2", "m3"]) {
    await hM["chat.message"]({ sessionID: sM }, mkOut(t));
  }
  // three turn-end events, spaced past the 150ms turn window + deferred retry
  for (let i = 0; i < 3; i++) {
    await new Promise((r) => setTimeout(r, 260));
    await hM.event({ event: { type: "session.idle", properties: { sessionID: sM } } });
  }
  await new Promise((r) => setTimeout(r, 200));
  check("all 3 items delivered", sent.length === 3 && ["m1", "m2", "m3"].every((t) => sent.includes(t)), `sent=${sent.length}`);
  check("no prompt landed inside an active turn (silent-loss signature)", lost.length === 0, `lost=[${lost.join(",")}]`);
}

// 0h) REGRESSION: message arriving DURING a slow drain is queued, not lost
{
  // First 2 prompt calls fail transiently, 3rd succeeds -> slow drain with
  // real backoff windows; item recovers (bounded retries), stays delivered.
  let calls = 0;
  const sentTexts: string[] = [];
  const clientS: any = {
    session: {
      prompt: async (args: any) => {
        const text = args.body?.parts?.find((p: any) => p.type === "text")?.text ?? "?";
        sentTexts.push(text);
        if (++calls <= 2) throw new Error("network error: reset");
        return {};
      },
    },
    tui: { showToast: async () => ({}) },
  };
  const dirS = dir + "-slowdrain";
  rmSync(dirS, { recursive: true, force: true });
  mkdirSync(`${dirS}\\.git`, { recursive: true });
  const hS: any = await (AutoQueuePlugin as any).server(
    { client: clientS, directory: dirS },
    { drainDelayMs: 5, retryBaseDelayMs: 120, retryMaxDelayMs: 150, maxRetries: 2, persistDebounceMs: 10 },
  );
  const sS = "sS";
  await hS.event({ event: { type: "session.status", properties: { sessionID: sS, status: { type: "busy" } } } });
  await hS["chat.message"]({ sessionID: sS }, mkOut("slow item"));
  // Fire WITHOUT await: the idle handler awaits the full drain (~300ms with
  // backoff); awaiting it here would mean the drain is over before we inject.
  const drainRun = hS.event({ event: { type: "session.idle", properties: { sessionID: sS } } }).catch(() => {});
  await new Promise((r) => setTimeout(r, 40)); // attempt 1 failed, drain in backoff (draining=true)
  // Turn "ends" mid-drain: busy map goes false, drain still in flight
  await hS.event({ event: { type: "session.status", properties: { sessionID: sS, status: { type: "idle" } } } });
  const outMid = mkOut("mid-drain msg");
  await hS["chat.message"]({ sessionID: sS }, outMid);
  check("mid-drain message queued via draining state (busy map false)", outMid.parts.length === 1 && outMid.parts[0].ignored === true, JSON.stringify(outMid.parts[0]?.text ?? "passthrough"));
  await drainRun;
  await new Promise((r) => setTimeout(r, 400)); // attempt 2 (fail), 3 (ok), deferred continuation sends mid-drain msg
  check("mid-drain message delivered after drain settled", sentTexts.includes("mid-drain msg"), `sent=[${sentTexts.join(",")}]`);
  const slowAttempts = sentTexts.filter((t) => t === "slow item").length;
  check("slow item attempts bounded by maxRetries+1 (3)", slowAttempts === 3, `slow attempts=${slowAttempts}`);
  const st = await hS.tool.queue.execute({ action: "status" }, { sessionID: sS });
  check("queue fully drained after recovery", /Queued: 0/.test(st) && /Failed: 0/.test(st), (st.split("\n").slice(0, 4).join(" | ")));
}

// 1) First message while idle: passes through (marks session busy)
const out1: any = { parts: [{ type: "text", text: "first task", id: "p1", messageID: "m0", sessionID: "s1" }], message: { agent: "build", model: { providerID: "zai-proxy", modelID: "glm5.3-flash" } } };
await hooks["chat.message"]({ sessionID: "s1", agent: "build", model: out1.message.model }, out1);
check("idle message passes through untouched", out1.parts.length === 1 && out1.parts[0].text === "first task");

// 2) Two messages while busy: get queued + replaced by ignored placeholder
const out2 = mkOut("second task");
await hooks["chat.message"]({ sessionID: "s1" }, out2);
check("busy message #1 queued (parts replaced by ignored placeholder)", out2.parts.length === 1 && out2.parts[0].ignored === true, JSON.stringify(out2.parts[0]?.text ?? ""));
const out3 = mkOut("third task");
await hooks["chat.message"]({ sessionID: "s1" }, out3);
check("busy message #2 queued", out3.parts[0].ignored === true);

// 3) Queue tool: status / drop / count
const tool = hooks.tool.queue;
const status1 = await tool.execute({ action: "status" }, { sessionID: "s1" });
check("tool status shows 2 queued", /Queued: 2/.test(status1), status1.split("\n")[3]);
const dropRes = await tool.execute({ action: "drop", index: 1 }, { sessionID: "s1" });
check("tool drop #1 removes 'second task'", /Dropped: second task/.test(dropRes), dropRes);
const cnt = await tool.execute({ action: "count" }, { sessionID: "s1" });
check("tool count shows 1 pending", /^1 messages/.test(cnt), cnt);

// 4) Instant /queue-clear interception while busy (chat.message hook)
const out4 = mkOut("/queue-status");
await hooks["chat.message"]({ sessionID: "s1" }, out4);
check("/queue-status intercepted instantly, not queued", out4.parts.length === 1 && out4.parts[0].metadata?.__auto_queue_internal === true && /Queued: 1/.test(out4.parts[0].text), out4.parts[0].text.split("\n")[0]);

// 5) Idle drain with transient failure -> retry succeeds (exercises backoffDelay fix)
failFirstPrompt = true;
const beforeRetry = prompts.length;
await hooks.event({ event: { type: "session.idle", properties: { sessionID: "s1" } } });
await Bun.sleep(400); // drainDelay 5ms + backoff ~10-20ms + margin
check("drain sent 1 prompt after retry", prompts.length === beforeRetry + 1, `prompts=${prompts.length}`);
if (prompts.length === beforeRetry + 1) {
  const body = prompts[beforeRetry].body;
  check("prompt targets session s1", prompts[beforeRetry].path?.id === "s1");
  check("prompt parts carry internal marker", body.parts.some((p: any) => p.metadata?.__auto_queue_internal === true));
  check("prompt preserves queued text", body.parts.some((p: any) => p.type === "text" && p.text === "third task"));
  check("prompt preserves agent/model", body.agent === "build" && body.model?.modelID === "glm5.3-flash");
}
check("retry toast emitted (proves backoffDelay fix ran)", toasts.some((t) => /Retrying/.test(t.body?.message ?? "")));

// 6) Queue now empty
const cnt2 = await tool.execute({ action: "count" }, { sessionID: "s1" });
check("queue empty after drain", /^0 messages/.test(cnt2), cnt2);

// 7) Persistence file written
await Bun.sleep(50);
check("state persisted to disk", existsSync(`${dir}\\.git\\queue.json`));

console.log(failures === 0 ? "\nALL CHECKS PASSED" : `\n${failures} CHECK(S) FAILED`);
process.exit(failures === 0 ? 0 : 1);
