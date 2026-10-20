import { describe, expect, it, vi } from "vitest";

import {
  RUN_PROXY_BASE,
  createJob,
  createService,
  deployRevision,
  jobBody,
  parseExecution,
  parseJob,
  parseService,
  parseTask,
  redeployTemplate,
  runJob,
  runtimeUnavailable,
  serviceBody,
  setTraffic,
  trafficBody,
  trafficRows,
  validateImage,
  validatePort,
  validateRunId,
  validateTraffic,
} from "./gcprun";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const row = (percent: number) => ({ revision: "r", latest: false, percent });
const scope = { project: "p", location: "l" };
const loc = `${RUN_PROXY_BASE}/projects/p/locations/l`;
const svcRaw = {
  name: "projects/p/locations/l/services/web",
  uri: "http://web.localhost:8127",
  latestReadyRevision: "projects/p/locations/l/services/web/revisions/web-00002-abc",
  template: {
    revision: "web-00002-abc",
    containers: [
      { image: "img:2", ports: [{ containerPort: 8080 }], env: [{ name: "A", value: "1" }] },
    ],
  },
  trafficStatuses: [
    { type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", revision: "web-00002-abc", percent: 60 },
    { type: "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", revision: "web-00001-xyz", percent: 40 },
  ],
  conditions: [{ type: "Ready", state: "CONDITION_SUCCEEDED" }],
};

describe("validators", () => {
  it.each([
    ["", false],
    ["Web", false],
    ["1web", false],
    ["web-1", true],
  ])("run id %j ok=%s", (id, ok) => {
    expect(validateRunId(id) === null).toBe(ok);
  });

  it("validates image and port", () => {
    expect(validateImage("")).not.toBeNull();
    expect(validateImage("a b")).not.toBeNull();
    expect(validateImage("alpine:3.20")).toBeNull();
    expect(validatePort("")).toBeNull();
    expect(validatePort("70000")).not.toBeNull();
    expect(validatePort("8080")).toBeNull();
  });

  it("requires traffic to sum to 100", () => {
    expect(validateTraffic([row(50), row(50)])).toBeNull();
    expect(validateTraffic([row(50), row(40)])).toContain("90");
    expect(validateTraffic([row(150), row(-50)])).toContain("whole number");
  });
});

describe("parsers", () => {
  it("parses a service", () => {
    const s = parseService(svcRaw);
    expect(s).toMatchObject({
      id: "web",
      image: "img:2",
      port: 8080,
      latestReady: "web-00002-abc",
      state: "ok",
    });
    expect(s.traffic.map((t) => [t.percent, t.latest])).toEqual([
      [60, true],
      [40, false],
    ]);
  });

  it("parses a job", () => {
    const j = parseJob({
      name: "projects/p/locations/l/jobs/batch",
      executionCount: 2,
      template: {
        taskCount: 3,
        parallelism: 1,
        template: { maxRetries: 0, containers: [{ image: "a" }] },
      },
    });
    expect(j).toMatchObject({
      id: "batch",
      image: "a",
      taskCount: 3,
      parallelism: 1,
      maxRetries: 0,
    });
  });

  it.each([
    [{ runningCount: 1 }, "running"],
    [
      {
        completionTime: "t",
        succeededCount: 2,
        conditions: [{ type: "Completed", state: "CONDITION_SUCCEEDED" }],
      },
      "succeeded",
    ],
    [
      {
        completionTime: "t",
        failedCount: 1,
        conditions: [{ type: "Completed", state: "CONDITION_FAILED" }],
      },
      "failed",
    ],
    [
      {
        completionTime: "t",
        cancelledCount: 1,
        conditions: [{ type: "Completed", state: "CONDITION_FAILED" }],
      },
      "cancelled",
    ],
  ])("execution state %j", (raw, state) => {
    expect(parseExecution({ name: "x/e1", ...raw }).state).toBe(state);
  });

  it("parses task exit status", () => {
    const failed = parseTask({
      name: "x/t0",
      index: 1,
      completionTime: "t",
      lastAttemptResult: { status: { message: "Container called exit(7)." }, exitCode: 7 },
      conditions: [
        { type: "Completed", state: "CONDITION_FAILED", message: "Container called exit(7)." },
      ],
    });
    expect(failed).toMatchObject({ state: "failed", exitCode: 7, index: 1 });
    expect(
      parseTask({
        name: "x/t1",
        completionTime: "t",
        conditions: [{ type: "Completed", state: "CONDITION_SUCCEEDED" }],
      }),
    ).toMatchObject({ state: "succeeded", exitCode: 0 });
    expect(parseTask({ name: "x/t2" })).toMatchObject({ state: "pending", exitCode: null });
  });

  it("detects a missing runtime", () => {
    expect(
      runtimeUnavailable([
        "ok",
        "Revision is deployed but cannot serve: container runtime is unavailable",
      ]),
    ).toBe(true);
    expect(runtimeUnavailable(["fine"])).toBe(false);
  });
});

describe("request bodies", () => {
  it("builds a service", () => {
    expect(
      serviceBody({ id: "web", image: " img ", port: "8080", env: [{ name: "A", value: "1" }] }),
    ).toEqual({
      template: {
        containers: [
          { image: "img", env: [{ name: "A", value: "1" }], ports: [{ containerPort: 8080 }] },
        ],
      },
    });
  });

  it("builds a job with retries of zero", () => {
    const body = jobBody({
      id: "j",
      image: "a",
      taskCount: "4",
      parallelism: "2",
      maxRetries: "0",
      command: "sh",
      args: "-c\nexit 1",
      env: [],
    });
    expect(body).toEqual({
      template: {
        taskCount: 4,
        parallelism: 2,
        template: {
          maxRetries: 0,
          containers: [{ image: "a", command: ["sh"], args: ["-c", "exit 1"] }],
        },
      },
    });
  });

  it("redeploys without the revision name", () => {
    const t = redeployTemplate(parseService(svcRaw), "img:3", [{ name: "B", value: "2" }]);
    expect(t.revision).toBeUndefined();
    expect(t.containers).toEqual([
      { image: "img:3", ports: [{ containerPort: 8080 }], env: [{ name: "B", value: "2" }] },
    ]);
  });

  it("redeploys a reactive proxy template", () => {
    const svc = { ...parseService(svcRaw), template: new Proxy(parseService(svcRaw).template, {}) };
    expect(redeployTemplate(svc, "img:4", []).containers).toEqual([
      { image: "img:4", ports: [{ containerPort: 8080 }] },
    ]);
  });

  it("builds traffic and seeds rows", () => {
    const svc = parseService(svcRaw);
    const rows = trafficRows(svc, [
      { name: "a", id: "web-00001-xyz", image: "", createTime: "1", conditions: [], state: "ok" },
      { name: "b", id: "web-00002-abc", image: "", createTime: "2", conditions: [], state: "ok" },
    ]);
    expect(rows.map((r) => [r.label, r.percent])).toEqual([
      ["Latest revision", 60],
      ["web-00002-abc", 0],
      ["web-00001-xyz", 40],
    ]);
    expect(trafficBody(rows).traffic).toEqual([
      { type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", percent: 60 },
      { type: "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", revision: "web-00001-xyz", percent: 40 },
    ]);
  });
});

describe("calls", () => {
  it("issues the expected requests", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({})));
    await createService(scope, { id: "web", image: "i", port: "", env: [] }, f);
    await createJob(
      scope,
      {
        id: "j",
        image: "i",
        taskCount: "",
        parallelism: "",
        maxRetries: "",
        command: "",
        args: "",
        env: [],
      },
      f,
    );
    await runJob(scope, "j", f);
    await deployRevision(scope, parseService(svcRaw), "i2", [], f);
    await setTraffic(scope, "web", [{ revision: "", latest: true, percent: 100 }], f);
    expect(f.mock.calls.map((c) => [c[1].method, c[0]])).toEqual([
      ["POST", `${loc}/services?serviceId=web`],
      ["POST", `${loc}/jobs?jobId=j`],
      ["POST", `${loc}/jobs/j:run`],
      ["PATCH", `${loc}/services/web?updateMask=template`],
      ["PATCH", `${loc}/services/web?updateMask=traffic`],
    ]);
  });
});
