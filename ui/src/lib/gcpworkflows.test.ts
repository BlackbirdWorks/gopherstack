import { describe, expect, it, vi } from "vitest";

import {
  WORKFLOWS_PROXY_BASE,
  cancelExecution,
  listExecutions,
  listRevisions,
  listWorkflows,
  parseExecution,
  parseWorkflow,
  prettyJson,
  runExecution,
  validateArgument,
} from "./gcpworkflows";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

const WF = "projects/p/locations/us-central1/workflows/hello";

function reply(f: (url: string) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string]))));
}

const bodyOf = (call: unknown[] | undefined): unknown =>
  (call?.[1] as RequestInit | undefined)?.body;

describe("parsing", () => {
  it("parses a workflow with its source", () => {
    expect(
      parseWorkflow({
        name: WF,
        state: "ACTIVE",
        revisionId: "000001-abc",
        sourceContents: "main:",
      }),
    ).toMatchObject({
      id: "hello",
      revisionId: "000001-abc",
      source: "main:",
    });
  });

  it("parses a failed execution", () => {
    const e = parseExecution({
      name: `${WF}/executions/e1`,
      state: "FAILED",
      error: { payload: '{"message":"boom"}', context: "step main" },
    });
    expect(e).toMatchObject({ id: "e1", state: "FAILED", errorContext: "step main" });
    expect(prettyJson(e.errorPayload)).toContain('"message": "boom"');
  });
});

describe("arguments", () => {
  it.each([
    ["", null],
    ["  ", null],
    ['{"a":1}', "ok"],
    ["[1,2]", "ok"],
    ["{", "error"],
    ["not json", "error"],
  ])("validates %j", (text, want) => {
    const got = validateArgument(text);
    expect(got === null ? "ok-or-empty" : "error").toBe(want === "error" ? "error" : "ok-or-empty");
  });

  it("pretty-prints JSON and passes other text through", () => {
    expect(prettyJson('{"a":1}')).toBe('{\n  "a": 1\n}');
    expect(prettyJson("plain")).toBe("plain");
    expect(prettyJson("")).toBe("");
  });
});

describe("requests", () => {
  it("lists workflows, revisions and executions", async () => {
    const fetchFn = reply((url: string) => {
      if (url.includes(":listRevisions"))
        return json({ workflows: [{ name: WF, revisionId: "r2" }] });
      if (url.includes("/executions"))
        return json({
          executions: [{ name: `${WF}/executions/e1`, state: "SUCCEEDED", result: '"hi"' }],
        });
      return json({ workflows: [{ name: WF }] });
    }) as unknown as typeof fetch;
    expect((await listWorkflows({ project: "p", location: "us-central1" }, fetchFn))[0]?.id).toBe(
      "hello",
    );
    expect((await listRevisions(WF, fetchFn))[0]?.revisionId).toBe("r2");
    expect((await listExecutions(WF, fetchFn))[0]).toMatchObject({
      state: "SUCCEEDED",
      result: '"hi"',
    });
  });

  it("runs an execution with the argument and cancels by name", async () => {
    const fetchFn = reply(() => json({ name: `${WF}/executions/e2`, state: "ACTIVE" }));
    await runExecution(WF, ' {"name":"x"} ', fetchFn as unknown as typeof fetch);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${WORKFLOWS_PROXY_BASE}/${WF}/executions`);
    expect(JSON.parse(String(bodyOf(fetchFn.mock.calls[0])))).toEqual({
      argument: '{"name":"x"}',
    });
    await runExecution(WF, "", fetchFn as unknown as typeof fetch);
    expect(JSON.parse(String(bodyOf(fetchFn.mock.calls[1])))).toEqual({});
    await cancelExecution(`${WF}/executions/e2`, fetchFn as unknown as typeof fetch);
    expect(fetchFn.mock.calls[2]?.[0]).toBe(`${WORKFLOWS_PROXY_BASE}/${WF}/executions/e2:cancel`);
  });
});
