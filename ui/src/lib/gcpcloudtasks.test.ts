import { describe, expect, it, vi } from "vitest";

import {
  TASKS_PROXY_BASE,
  attemptOk,
  createQueue,
  createTask,
  describeAttempt,
  optionalNumber,
  parseQueue,
  parseTask,
  queueAction,
  runTask,
  taskBody,
  validateHeaderKey,
  validateQueueId,
  validateTaskId,
  validateTaskUrl,
} from "./gcpcloudtasks";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const scope = { project: "p", location: "l" };
const qUrl = `${TASKS_PROXY_BASE}/projects/p/locations/l/queues`;

describe("validation", () => {
  it.each([
    ["", "name is required"],
    ["bad_name", "use letters, digits and - (max 100)"],
    ["my-queue1", null],
  ])("queue id %j", (id, want) => expect(validateQueueId(id)).toBe(want));

  it.each([
    ["", null],
    ["a b", "use letters, digits, - and _ (max 500)"],
    ["t_1-x", null],
  ])("task id %j", (id, want) => expect(validateTaskId(id)).toBe(want));

  it.each([
    ["", "URL is required"],
    ["localhost:9000", "URL must be http or https"],
    ["not a url", "enter an absolute URL such as http://localhost:9000/hook"],
    ["http://localhost:9000/x", null],
  ])("url %j", (u, want) => expect(validateTaskUrl(u)).toBe(want));

  it("header names", () => {
    expect(validateHeaderKey("X-Id")).toBeNull();
    expect(validateHeaderKey("bad name")).not.toBeNull();
    expect(validateHeaderKey("")).not.toBeNull();
  });

  it("optional numbers", () => {
    expect(optionalNumber(" ")).toEqual({ value: null });
    expect(optionalNumber("2.5")).toEqual({ value: 2.5 });
    expect(optionalNumber("2.5", { integer: true })).toHaveProperty("error");
    expect(optionalNumber("-1")).toHaveProperty("error");
    expect(optionalNumber("-1", { min: -1 })).toEqual({ value: -1 });
  });
});

describe("parsing", () => {
  it("reads a queue", () => {
    const q = parseQueue({
      name: "projects/p/locations/l/queues/q",
      rateLimits: {
        maxDispatchesPerSecond: 500,
        maxConcurrentDispatches: 1000,
      },
      retryConfig: { maxAttempts: 100, minBackoff: "0.100s" },
    });
    expect(q).toMatchObject({
      id: "q",
      state: "RUNNING",
      maxDispatchesPerSecond: 500,
      maxAttempts: 100,
      minBackoff: "0.100s",
      maxBurstSize: null,
    });
  });

  it("reads a task and decodes the body", () => {
    const t = parseTask({
      name: "projects/p/locations/l/queues/q/tasks/t1",
      httpRequest: { url: "http://x/y", body: "aGk=", headers: { A: "b" } },
      dispatchCount: 2,
      lastAttempt: {
        responseStatus: { code: 5, message: "HTTP status code Not Found" },
      },
    });
    expect(t).toMatchObject({
      id: "t1",
      method: "POST",
      body: "hi",
      dispatchCount: 2,
    });
    expect(describeAttempt(t.lastAttempt)).toBe(
      "NOT_FOUND (HTTP status code Not Found)",
    );
    expect(attemptOk(t.lastAttempt)).toBe(false);
  });

  it("describes attempts", () => {
    expect(describeAttempt(null)).toBe("no attempts");
    expect(
      describeAttempt({
        dispatchTime: "",
        responseTime: "",
        code: null,
        message: "",
      }),
    ).toBe("in flight");
    expect(
      describeAttempt({
        dispatchTime: "",
        responseTime: "",
        code: 0,
        message: "",
      }),
    ).toBe("OK");
  });
});

describe("calls", () => {
  it("builds task bodies", () => {
    const now = new Date("2026-01-01T00:00:00Z");
    expect(
      taskBody(
        {
          id: "t1",
          url: " http://x/y ",
          method: "POST",
          headers: { A: "b" },
          body: "hi",
          delaySeconds: "30",
        },
        now,
      ),
    ).toEqual({
      task: {
        name: "t1",
        httpRequest: {
          url: "http://x/y",
          httpMethod: "POST",
          headers: { A: "b" },
          body: "aGk=",
        },
        scheduleTime: "2026-01-01T00:00:30.000Z",
      },
    });
    expect(
      taskBody({
        id: "",
        url: "http://x",
        method: "GET",
        headers: {},
        body: "",
        delaySeconds: "",
      }).task,
    ).toEqual({ httpRequest: { url: "http://x", httpMethod: "GET" } });
  });

  it("creates a queue with the full name", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ name: "projects/p/locations/l/queues/q" }));
    await createQueue(
      scope,
      {
        id: "q",
        maxDispatchesPerSecond: "5",
        maxConcurrentDispatches: "",
        maxAttempts: "3",
      },
      f,
    );
    expect(f.mock.calls[0][0]).toBe(qUrl);
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({
      name: "projects/p/locations/l/queues/q",
      rateLimits: { maxDispatchesPerSecond: 5 },
      retryConfig: { maxAttempts: 3 },
    });
  });

  it("posts queue verbs", async () => {
    const f = vi.fn().mockResolvedValue(json({ name: "x", state: "PAUSED" }));
    await queueAction(scope, "q", "pause", f);
    expect(f.mock.calls[0][0]).toBe(`${qUrl}/q:pause`);
  });

  it("qualifies a custom task id and runs tasks", async () => {
    const f = vi
      .fn()
      .mockImplementation(() => Promise.resolve(json({ name: "n" })));
    await createTask(
      scope,
      "q",
      {
        id: "t1",
        url: "http://x",
        method: "POST",
        headers: {},
        body: "",
        delaySeconds: "",
      },
      f,
    );
    expect(f.mock.calls[0][0]).toBe(`${qUrl}/q/tasks`);
    expect(JSON.parse(f.mock.calls[0][1].body).task.name).toBe(
      "projects/p/locations/l/queues/q/tasks/t1",
    );
    await runTask(scope, "q", "t1", f);
    expect(f.mock.calls[1][0]).toBe(`${qUrl}/q/tasks/t1:run`);
  });
});
