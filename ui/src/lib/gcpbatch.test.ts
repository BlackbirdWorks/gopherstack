import { describe, expect, it, vi } from "vitest";

import {
  BATCH_PROXY_BASE,
  buildJobBody,
  canCancel,
  cancelJob,
  createJob,
  deleteJob,
  formatDuration,
  isSettled,
  jobLogFilter,
  listJobs,
  listProjects,
  listTasks,
  parseJob,
  parseTask,
  statePill,
  validEnvName,
  validJobId,
} from "./gcpbatch";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;

const jobName = "projects/p/locations/us-central1/jobs/hello";

const rawJob = {
  name: jobName,
  createTime: "2026-01-01T00:00:00Z",
  labels: { team: "data" },
  taskGroups: [
    {
      name: `${jobName}/taskGroups/group0`,
      taskCount: "3",
      parallelism: "2",
      taskSpec: {
        maxRetryCount: 1,
        runnables: [
          { container: { imageUri: "alpine:3", commands: ["echo", "hi"] } },
          { script: { text: "echo script" }, environment: { variables: { X: "1" } } },
        ],
        environment: {
          variables: { A: "b" },
          secretVariables: { TOKEN: "projects/p/secrets/s/versions/1" },
        },
      },
    },
  ],
  status: {
    state: "RUNNING",
    runDuration: "75s",
    statusEvents: [{ type: "STATUS_CHANGED", description: "Job state is set", eventTime: "t1" }],
    taskGroups: { group0: { counts: { RUNNING: "2", PENDING: "1" } } },
  },
};

describe("parsing", () => {
  it("reads a job with its task group and runnables", () => {
    const job = parseJob(rawJob);
    expect(job).toMatchObject({
      id: "hello",
      project: "p",
      location: "us-central1",
      state: "RUNNING",
      taskCount: 3,
      counts: { RUNNING: 2, PENDING: 1 },
      labels: { team: "data" },
    });
    const g = job.groups[0];
    expect(g).toMatchObject({ taskCount: 3, parallelism: 2, maxRetryCount: 1 });
    expect(g.runnables[0]).toMatchObject({ kind: "container", image: "alpine:3" });
    expect(g.runnables[1]).toMatchObject({ kind: "script", scriptText: "echo script" });
  });

  it("keeps secret variable names but never their references", () => {
    const g = parseJob(rawJob).groups[0];
    expect(g.secretNames).toEqual(["TOKEN"]);
    expect(JSON.stringify(g)).not.toContain("secrets/s");
  });

  it("derives exit code and attempts of a task", () => {
    const t = parseTask({
      name: `${jobName}/taskGroups/group0/tasks/1`,
      status: {
        state: "SUCCEEDED",
        statusEvents: [
          { type: "STATUS_CHANGED", description: "Task state is updated from ASSIGNED to RUNNING" },
          { type: "TASK_RETRY", description: "retry" },
          { type: "STATUS_CHANGED", description: "to SUCCEEDED", taskExecution: { exitCode: 0 } },
        ],
      },
    });
    expect(t).toMatchObject({ index: "1", state: "SUCCEEDED", exitCode: 0, attempts: 2 });
  });

  it("reads a zero exit code from an empty taskExecution", () => {
    const t = parseTask({
      name: "x/0",
      status: { state: "SUCCEEDED", statusEvents: [{ type: "STATUS_CHANGED", taskExecution: {} }] },
    });
    expect(t.exitCode).toBe(0);
  });

  it("leaves the exit code empty before a task ends", () => {
    expect(parseTask({ name: "x/0", status: { state: "PENDING" } })).toMatchObject({
      exitCode: null,
      attempts: 0,
    });
  });
});

describe("helpers", () => {
  it("validates ids and variable names", () => {
    expect(validJobId("my-job1")).toBe(true);
    expect(validJobId("My")).toBe(false);
    expect(validJobId("1a")).toBe(false);
    expect(validEnvName("A_1")).toBe(true);
    expect(validEnvName("1A")).toBe(false);
  });

  it("classifies states", () => {
    expect(canCancel({ state: "RUNNING" })).toBe(true);
    expect(canCancel({ state: "SUCCEEDED" })).toBe(false);
    expect(isSettled({ state: "CANCELLATION_IN_PROGRESS" })).toBe(false);
    expect(isSettled({ state: "FAILED" })).toBe(true);
    expect(statePill("SUCCEEDED")).toContain("green");
    expect(statePill("FAILED")).toContain("red");
  });

  it("formats durations", () => {
    expect(formatDuration("12.5s")).toBe("12.5s");
    expect(formatDuration("75s")).toBe("1m 15s");
    expect(formatDuration("")).toBe("—");
  });

  it("builds the log filter", () => {
    expect(jobLogFilter("p", "hello")).toBe(
      'logName="projects/p/logs/batch_task_logs" AND resource.labels.job="hello"',
    );
  });

  it("builds a body from fixed fields only", () => {
    const body = buildJobBody({
      id: "j",
      image: " alpine:3 ",
      commands: "echo\n\nhi",
      taskCount: 3,
      parallelism: 2,
      env: [
        { name: "A", value: "b" },
        { name: " ", value: "skip" },
      ],
    });
    expect(body).toEqual({
      taskGroups: [
        {
          taskCount: "3",
          parallelism: "2",
          taskSpec: {
            runnables: [{ container: { imageUri: "alpine:3", commands: ["echo", "hi"] } }],
            environment: { variables: { A: "b" } },
          },
        },
      ],
    });
  });
});

describe("calls", () => {
  it("lists projects, jobs and tasks", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ projects: ["p"] }))
      .mockResolvedValueOnce(json({ jobs: [rawJob] }))
      .mockResolvedValueOnce(json({ tasks: [{ name: `${jobName}/taskGroups/group0/tasks/0` }] }));
    expect(await listProjects(asFetch(f))).toEqual(["p"]);
    expect((await listJobs("p", "-", asFetch(f)))[0].id).toBe("hello");
    expect(urlOf(f, 1)).toBe(`${BATCH_PROXY_BASE}/projects/p/locations/-/jobs`);
    expect(await listTasks(jobName, asFetch(f))).toHaveLength(1);
    expect(urlOf(f, 2)).toBe(`${BATCH_PROXY_BASE}/${jobName}/taskGroups/group0/tasks`);
  });

  it("creates, cancels and deletes", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({})));
    await createJob(
      "p",
      "us-central1",
      { id: "j", image: "alpine:3", commands: "", taskCount: 1, parallelism: 1, env: [] },
      asFetch(f),
    );
    expect(urlOf(f, 0)).toBe(`${BATCH_PROXY_BASE}/projects/p/locations/us-central1/jobs?jobId=j`);
    expect(initOf(f, 0).method).toBe("POST");
    await cancelJob(jobName, asFetch(f));
    expect(urlOf(f, 1)).toBe(`${BATCH_PROXY_BASE}/${jobName}:cancel`);
    await deleteJob(jobName, asFetch(f));
    expect(initOf(f, 2).method).toBe("DELETE");
  });

  it("surfaces proxy errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(json({ error: { message: "nope", status: "NOT_FOUND" } }, 404));
    await expect(deleteJob(jobName, asFetch(f))).rejects.toMatchObject({ code: "NOT_FOUND" });
  });
});
