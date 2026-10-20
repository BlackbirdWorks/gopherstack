import { describe, expect, it, vi } from "vitest";

import {
  SCHEDULER_PROXY_BASE,
  countdown,
  createJob,
  describeLastAttempt,
  jobAction,
  jobBody,
  lastAttemptOk,
  parseJob,
  updateSchedule,
  validateJobId,
  validateSchedule,
  validateTimeZone,
  validateTopic,
  validateUrl,
} from "./gcpscheduler";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const scope = { project: "p", location: "l" };
const jobsUrl = `${SCHEDULER_PROXY_BASE}/projects/p/locations/l/jobs`;
const base = {
  id: "j1",
  description: "",
  schedule: " */5 * * * * ",
  timeZone: "",
  kind: "http" as const,
  url: " http://x/y ",
  method: "POST" as const,
  headers: {},
  body: "hi",
  topic: "t",
  data: "",
};

describe("validation", () => {
  it.each([
    ["", "name is required"],
    ["bad name", "use letters, digits, - and _ (max 500)"],
    ["job_1-x", null],
  ])("job id %j", (id, want) => expect(validateJobId(id)).toBe(want));

  it.each([
    ["", "schedule is required"],
    ["* * * *", "use five cron fields, e.g. */5 * * * *"],
    ["61 * * * *", "not a valid cron expression"],
    ["* * * * *", null],
    ["*/5 0-12 1,15 * 1-5", null],
  ])("schedule %j", (s, want) => expect(validateSchedule(s)).toBe(want));

  it("time zones", () => {
    expect(validateTimeZone("America/New_York")).toBeNull();
    expect(validateTimeZone("Nope/Zone")).not.toBeNull();
    expect(validateTimeZone(" ")).not.toBeNull();
  });

  it("urls and topics", () => {
    expect(validateUrl("")).toBe("URL is required");
    expect(validateUrl("ftp://x")).toBe("URL must be http or https");
    expect(validateUrl("http://localhost:9000/x")).toBeNull();
    expect(validateTopic("", "p")).toBe("topic is required");
    expect(validateTopic("my-topic", "p")).toBeNull();
    expect(validateTopic("projects/p/topics/t", "p")).toBeNull();
    expect(validateTopic("a/b", "p")).not.toBeNull();
  });
});

describe("parsing", () => {
  it("reads an http job", () => {
    const j = parseJob({
      name: "projects/p/locations/l/jobs/j1",
      schedule: "* * * * *",
      state: "ENABLED",
      httpTarget: { uri: "http://x/y", body: "aGk=", headers: { A: "b" } },
      lastAttemptTime: "2026-01-01T00:00:00Z",
      status: { code: 5, message: "HTTP status code Not Found" },
    });
    expect(j).toMatchObject({
      id: "j1",
      targetType: "HTTP",
      target: "http://x/y",
      method: "POST",
      body: "hi",
      timeZone: "Etc/UTC",
    });
    expect(describeLastAttempt(j)).toBe("NOT_FOUND (HTTP status code Not Found)");
    expect(lastAttemptOk(j)).toBe(false);
  });

  it("reads a pubsub job and a never-run job", () => {
    const j = parseJob({
      name: "projects/p/locations/l/jobs/j2",
      state: "PAUSED",
      pubsubTarget: { topicName: "projects/p/topics/t", data: "aGk=" },
    });
    expect(j).toMatchObject({ targetType: "Pub/Sub", target: "projects/p/topics/t", body: "hi" });
    expect(describeLastAttempt(j)).toBe("never run");
    expect(lastAttemptOk(j)).toBeNull();
  });

  it("treats an empty status after an attempt as OK", () => {
    const j = parseJob({
      name: "projects/p/locations/l/jobs/j3",
      httpTarget: { uri: "http://x" },
      lastAttemptTime: "2026-01-01T00:00:00Z",
      status: {},
    });
    expect(describeLastAttempt(j)).toBe("OK");
    expect(lastAttemptOk(j)).toBe(true);
  });
});

describe("countdown", () => {
  const now = Date.parse("2026-01-01T00:00:00Z");
  it.each([
    ["", "—"],
    ["2025-12-31T23:59:59Z", "due now"],
    ["2026-01-01T00:00:42Z", "in 42s"],
    ["2026-01-01T00:03:05Z", "in 3m 5s"],
    ["2026-01-01T02:04:00Z", "in 2h 4m"],
    ["2026-01-04T05:00:00Z", "in 3d 5h"],
  ])("%j", (iso, want) => expect(countdown(iso, now)).toBe(want));
});

describe("calls", () => {
  it("builds http and pubsub bodies", () => {
    expect(jobBody(scope, base)).toEqual({
      name: "projects/p/locations/l/jobs/j1",
      schedule: "*/5 * * * *",
      timeZone: "Etc/UTC",
      httpTarget: { uri: "http://x/y", httpMethod: "POST", body: "aGk=" },
    });
    expect(
      jobBody(scope, { ...base, kind: "pubsub", topic: "t", data: "hi", headers: { k: "v" } }),
    ).toMatchObject({
      pubsubTarget: { topicName: "projects/p/topics/t", data: "aGk=", attributes: { k: "v" } },
    });
  });

  it("creates, acts and updates through the proxy", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ name: "projects/p/locations/l/jobs/j1" })));
    await createJob(scope, base, fetchFn);
    await jobAction(scope, "j1", "run", fetchFn);
    await updateSchedule(scope, "j1", " 0 * * * * ", "Etc/UTC", fetchFn);
    const calls = fetchFn.mock.calls as unknown as [string, RequestInit][];
    expect(calls[0][0]).toBe(jobsUrl);
    expect(calls[0][1].method).toBe("POST");
    expect(calls[1][0]).toBe(`${jobsUrl}/j1:run`);
    expect(calls[2][0]).toBe(`${jobsUrl}/j1?updateMask=schedule,timeZone`);
    expect(calls[2][1].method).toBe("PATCH");
    expect(JSON.parse(calls[2][1].body as string)).toEqual({
      schedule: "0 * * * *",
      timeZone: "Etc/UTC",
    });
  });
});
