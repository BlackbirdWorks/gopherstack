import { describe, expect, it, vi } from "vitest";

import {
  LOGIC_ACCESS_BASE,
  LOGIC_PROXY_BASE,
  buildTimeline,
  cancelRun,
  durationMs,
  formatDuration,
  getContent,
  getRun,
  isRequestTrigger,
  isRunning,
  listActions,
  listRuns,
  listTriggers,
  listWorkflows,
  maskSignature,
  nextToken,
  parseAction,
  parseRun,
  parseTrigger,
  parseWorkflow,
  prettyJson,
  revealCallback,
  runTrigger,
  setWorkflowEnabled,
  statusTone,
  tokenizeJson,
  triggerTypes,
  validatePayload,
} from "./azurelogic";
import { GcpRestError } from "./gcprest";

const sub = "00000000-0000-0000-0000-000000000000";
const target = { subscription: sub, group: "demo", workflow: "greeter" };
const flow = `/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.Logic/workflows/greeter`;
const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const timed = (name: string, start: string, end: string) =>
  parseAction({ name, properties: { status: "Succeeded", startTime: start, endTime: end } });
const answer = (make: () => Response) => vi.fn(() => Promise.resolve(make()));
const callUrl = (f: { mock: { calls: unknown[] } }, i: number): string =>
  (f.mock.calls[i] as [string])[0];

const definition = {
  triggers: {
    manual: { type: "Request", kind: "Http" },
    tick: { type: "Recurrence", recurrence: { frequency: "Month", interval: 1 } },
  },
};

describe("parsing", () => {
  it("parses a workflow", () => {
    expect(
      parseWorkflow({
        id: flow,
        name: "greeter",
        location: "westus",
        properties: { state: "Enabled", version: "v1", changedTime: "t", definition },
      }),
    ).toMatchObject({ name: "greeter", state: "Enabled", version: "v1", changed: "t" });
  });

  it("reads trigger types from the definition", () => {
    expect(triggerTypes(definition)).toEqual({ manual: "Request", tick: "Recurrence" });
    expect(triggerTypes({})).toEqual({});
  });

  it("parses triggers with type and schedule", () => {
    const types = triggerTypes(definition);
    expect(
      parseTrigger(
        {
          name: "tick",
          properties: {
            state: "Enabled",
            recurrence: { frequency: "Month", interval: 2 },
            nextExecutionTime: "n",
          },
        },
        types,
      ),
    ).toMatchObject({ type: "Recurrence", recurrence: "every 2 months", next: "n" });
    expect(parseTrigger({ name: "manual", properties: {} }, types).recurrence).toBe("");
    expect(
      parseTrigger({ name: "x", properties: { recurrence: { frequency: "Day" } } }, {}).recurrence,
    ).toBe("every 1 day");
  });

  it("parses runs and notes which trigger content exists", () => {
    const r = parseRun({
      name: "r1",
      properties: {
        status: "Failed",
        startTime: "s",
        endTime: "e",
        error: { code: "ActionFailed", message: "boom" },
        trigger: { name: "manual", status: "Succeeded", outputsLink: { contentSize: 12 } },
      },
    });
    expect(r).toMatchObject({
      name: "r1",
      status: "Failed",
      trigger: "manual",
      error: { code: "ActionFailed", message: "boom" },
      hasTriggerInputs: false,
      hasTriggerOutputs: true,
    });
    expect(parseRun({ name: "r2", properties: {} }).error).toBeUndefined();
  });

  it("parses actions with link sizes", () => {
    expect(
      parseAction({
        name: "Compose",
        properties: {
          status: "Succeeded",
          inputsLink: { contentSize: 3 },
          outputsLink: { contentSize: 9 },
        },
      }),
    ).toMatchObject({ hasInputs: true, hasOutputs: true, inputsSize: 3, outputsSize: 9 });
    expect(parseAction({ name: "x", properties: {} })).toMatchObject({
      hasInputs: false,
      hasOutputs: false,
    });
  });
});

describe("status and time", () => {
  it.each([
    ["Succeeded", "good"],
    ["Failed", "bad"],
    ["TimedOut", "bad"],
    ["Running", "busy"],
    ["Waiting", "busy"],
    ["Cancelled", "muted"],
    ["Skipped", "muted"],
    ["", "muted"],
  ])("tone of %s", (status, tone) => {
    expect(statusTone(status)).toBe(tone);
  });

  it("knows running statuses", () => {
    expect(isRunning("Running")).toBe(true);
    expect(isRunning("Succeeded")).toBe(false);
  });

  it("computes and formats durations", () => {
    expect(durationMs("2026-01-01T00:00:00Z", "2026-01-01T00:00:02.500Z")).toBe(2500);
    expect(durationMs("", "2026-01-01T00:00:00Z")).toBeUndefined();
    expect(durationMs("2026-01-01T00:00:05Z", "2026-01-01T00:00:00Z")).toBe(0);
    expect(formatDuration(durationMs("", ""))).toBe("—");
    expect(formatDuration(250)).toBe("250 ms");
    expect(formatDuration(2500)).toBe("2.5 s");
    expect(formatDuration(125000)).toBe("2 min 5 s");
  });

  it("lays actions out on a shared axis ordered by start", () => {
    const tl = buildTimeline([
      timed("Response", "2026-01-01T00:00:02Z", "2026-01-01T00:00:04Z"),
      timed("Compose", "2026-01-01T00:00:00Z", "2026-01-01T00:00:02Z"),
    ]);
    expect(tl.map((e) => e.name)).toEqual(["Compose", "Response"]);
    expect(tl[0]).toMatchObject({ offsetPct: 0, widthPct: 50, duration: 2000 });
    expect(tl[1]).toMatchObject({ offsetPct: 50, widthPct: 50 });
  });

  it("copes with empty and unfinished timelines", () => {
    expect(buildTimeline([])).toEqual([]);
    const [e] = buildTimeline([
      parseAction({
        name: "A",
        properties: { status: "Running", startTime: "2026-01-01T00:00:00Z" },
      }),
    ]);
    expect(e.widthPct).toBeGreaterThanOrEqual(1);
    expect(e.duration).toBeUndefined();
  });
});

describe("json view", () => {
  it("tokenizes keys, strings, numbers and literals", () => {
    const kinds = tokenizeJson(`{"a": [1, "x", true, null]}`)
      .filter((t) => t.kind !== "space" && t.kind !== "punct")
      .map((t) => `${t.kind}:${t.text}`);
    expect(kinds).toEqual(['key:"a"', "number:1", 'string:"x"', "literal:true", "literal:null"]);
  });

  it("keeps the text intact", () => {
    const text = prettyJson({ a: { b: [1, 2, { c: "d\\n" }] }, e: -1.5e3 });
    expect(
      tokenizeJson(text)
        .map((t) => t.text)
        .join(""),
    ).toBe(text);
  });

  it("pretty-prints JSON strings and passes other text through", () => {
    expect(prettyJson('{"a":1}')).toBe('{\n  "a": 1\n}');
    expect(prettyJson("hello bob")).toBe("hello bob");
    expect(prettyJson(() => 1)).toBe("");
  });
});

describe("secrets", () => {
  it("masks the signature of a callback URL", () => {
    const url =
      "https://h/_logic/workflows/f/triggers/manual/paths/invoke?api-version=2016-10-01&sp=x&sig=SECRET123&se=1";
    const masked = maskSignature(url);
    expect(masked).not.toContain("SECRET123");
    expect(masked).toContain("sig=••••••••&se=1");
    expect(maskSignature("https://h/path")).toBe("https://h/path");
  });

  it("only asks for a callback URL when called", async () => {
    const f = answer(() => json({ value: "https://h/x?sig=S", method: "POST" }));
    expect(f).not.toHaveBeenCalled();
    const cb = await revealCallback(target, "manual", asFetch(f));
    expect(cb).toEqual({ url: "https://h/x?sig=S", method: "POST" });
    expect(callUrl(f, 0)).toBe(`${LOGIC_ACCESS_BASE}/callback/${sub}/demo/greeter/manual`);
    expect((f.mock.calls[0] as unknown as [string, RequestInit])[1].method).toBe("POST");
  });
});

describe("paging", () => {
  it("extracts the skip token from a next link", () => {
    expect(nextToken("https://x/y?api-version=1&$skiptoken=abc%3D")).toBe("abc=");
    expect(nextToken("https://x/y")).toBe("");
    expect(nextToken(null)).toBe("");
    expect(nextToken("")).toBe("");
  });
});

describe("requests", () => {
  it("lists workflows across pages", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({ value: [{ name: "b", properties: {} }], nextLink: "https://x/y?$skiptoken=t2" }),
      )
      .mockResolvedValueOnce(json({ value: [{ name: "a", properties: {} }] }));
    expect((await listWorkflows(sub, "demo", asFetch(f))).map((w) => w.name)).toEqual(["a", "b"]);
    expect(callUrl(f, 0)).toBe(
      `${LOGIC_PROXY_BASE}/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.Logic/workflows?api-version=2019-05-01`,
    );
    expect(callUrl(f, 1)).toContain("&$skiptoken=t2");
  });

  it("toggles state with POST", async () => {
    const f = answer(() => new Response(null, { status: 200 }));
    await setWorkflowEnabled(target, false, asFetch(f));
    await setWorkflowEnabled(target, true, asFetch(f));
    const calls = f.mock.calls as unknown as [string, RequestInit][];
    expect(calls[0][0]).toBe(`${LOGIC_PROXY_BASE}${flow}/disable?api-version=2019-05-01`);
    expect(calls[1][0]).toBe(`${LOGIC_PROXY_BASE}${flow}/enable?api-version=2019-05-01`);
    expect(calls[0][1].method).toBe("POST");
  });

  it("lists triggers with their definition types", async () => {
    const f = answer(() => json({ value: [{ name: "manual", properties: { state: "Enabled" } }] }));
    const out = await listTriggers(target, definition, asFetch(f));
    expect(out[0]).toMatchObject({ name: "manual", type: "Request" });
    expect(callUrl(f, 0)).toContain(`${flow}/triggers?`);
  });

  it("runs a recurrence trigger through ARM and a request trigger through the access route", async () => {
    const arm = answer(() => new Response(null, { status: 200 }));
    expect(
      await runTrigger(target, { name: "tick", type: "Recurrence" }, "", asFetch(arm)),
    ).toBeUndefined();
    expect(callUrl(arm, 0)).toBe(
      `${LOGIC_PROXY_BASE}${flow}/triggers/tick/run?api-version=2019-05-01`,
    );

    const access = answer(() =>
      json({ status: 200, contentType: "application/json", runId: "r1", body: '{"ok":true}' }),
    );
    const out = await runTrigger(
      target,
      { name: "manual", type: "Request" },
      ' {"name":"bob"} ',
      asFetch(access),
    );
    expect(out).toEqual({
      status: 200,
      contentType: "application/json",
      runId: "r1",
      body: '{"ok":true}',
    });
    const [url, init] = access.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${LOGIC_ACCESS_BASE}/invoke/${sub}/demo/greeter/manual`);
    expect(init.body).toBe('{"name":"bob"}');
  });

  it("recognises request triggers and validates payloads", () => {
    expect(isRequestTrigger("Request")).toBe(true);
    expect(isRequestTrigger("Recurrence")).toBe(false);
    expect(validatePayload("")).toBe("");
    expect(validatePayload('{"a":1}')).toBe("");
    expect(validatePayload("{")).toBe("The payload must be valid JSON");
  });

  it("lists runs newest first and reads one run with its actions", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({
          value: [
            { name: "old", properties: { startTime: "2026-01-01T00:00:00Z" } },
            { name: "new", properties: { startTime: "2026-02-01T00:00:00Z" } },
          ],
        }),
      )
      .mockResolvedValueOnce(json({ name: "new", properties: { status: "Succeeded" } }))
      .mockResolvedValueOnce(
        json({ value: [{ name: "Compose", properties: { status: "Succeeded" } }] }),
      );
    expect((await listRuns(target, asFetch(f))).map((r) => r.name)).toEqual(["new", "old"]);
    expect(callUrl(f, 0)).toContain("&$top=50");
    expect((await getRun(target, "new", asFetch(f))).status).toBe("Succeeded");
    expect((await listActions(target, "new", asFetch(f)))[0].name).toBe("Compose");
    expect(callUrl(f, 2)).toContain(`${flow}/runs/new/actions?`);
  });

  it("cancels a run", async () => {
    const f = answer(() => new Response(null, { status: 200 }));
    await cancelRun(target, "r 1", asFetch(f));
    expect(callUrl(f, 0)).toBe(
      `${LOGIC_PROXY_BASE}${flow}/runs/r%201/cancel?api-version=2019-05-01`,
    );
  });

  it("reads stored content through the access route", async () => {
    const f = answer(() => new Response('"hello bob"', { status: 200 }));
    expect(await getContent(target, "r1", { action: "Compose" }, "outputs", asFetch(f))).toBe(
      '"hello bob"',
    );
    expect(await getContent(target, "r1", { trigger: true }, "inputs", asFetch(f))).toBe(
      '"hello bob"',
    );
    expect(callUrl(f, 0)).toBe(
      `${LOGIC_ACCESS_BASE}/content/${sub}/demo/greeter/r1/action/Compose/outputs`,
    );
    expect(callUrl(f, 1)).toBe(
      `${LOGIC_ACCESS_BASE}/content/${sub}/demo/greeter/r1/trigger/inputs`,
    );
  });

  it("surfaces proxy errors", async () => {
    const f = answer(() => json({ error: { code: "RunNotRunning", message: "not running" } }, 400));
    await expect(cancelRun(target, "r1", asFetch(f))).rejects.toMatchObject({
      httpStatus: 400,
      code: "RunNotRunning",
      message: "not running",
    });
    const down = vi.fn(() => Promise.reject(new Error("offline")));
    await expect(cancelRun(target, "r1", asFetch(down))).rejects.toBeInstanceOf(GcpRestError);
  });
});
