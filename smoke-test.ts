/**
 * Smoke test for opencode-auto-queue (local build).
 * Executes the real plugin lifecycle: enqueue while busy -> tool controls ->
 * idle drain with transient-failure retry -> instant /queue-clear interception.
 * Run: bun smoke-test.ts
 */
import { AutoQueuePlugin } from "./dist/index.js";
import { existsSync, rmSync, mkdirSync } from "node:fs";

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
  { defaultMode: "immediate", drainDelayMs: 5, retryBaseDelayMs: 10, retryMaxDelayMs: 50, persistDebounceMs: 10, toastDurationMs: 100 },
);

let failures = 0;
function check(name: string, cond: boolean, detail = "") {
  console.log(`${cond ? "PASS" : "FAIL"}  ${name}${detail ? "  -> " + detail : ""}`);
  if (!cond) failures++;
}

// 1) First message while idle: passes through (marks session busy)
const out1: any = { parts: [{ type: "text", text: "first task", id: "p1", messageID: "m0", sessionID: "s1" }], message: { agent: "build", model: { providerID: "zai-proxy", modelID: "glm5.3-flash" } } };
await hooks["chat.message"]({ sessionID: "s1", agent: "build", model: out1.message.model }, out1);
check("idle message passes through untouched", out1.parts.length === 1 && out1.parts[0].text === "first task");

// 2) Two messages while busy: get queued + replaced by ignored placeholder
function mkOut(text: string): any {
  return { parts: [{ type: "text", text, id: "px", messageID: "m1", sessionID: "s1" }], message: { agent: "build", model: { providerID: "zai-proxy", modelID: "glm5.3-flash" } } };
}
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
await hooks.event({ event: { type: "session.idle", properties: { sessionID: "s1" } } });
await Bun.sleep(400); // drainDelay 5ms + backoff ~10-20ms + margin
check("drain sent 1 prompt after retry", prompts.length === 1, `prompts=${prompts.length}`);
if (prompts.length === 1) {
  const body = prompts[0].body;
  check("prompt targets session s1", prompts[0].path?.id === "s1");
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
