import { describe, expect, it, vi } from "vitest";

import {
  ACTIVE_BUILD_STATUSES,
  CLOUDBUILD_PROXY_BASE,
  approveBuild,
  cancelBuild,
  deleteTrigger,
  describeSource,
  describeTriggerEvent,
  durationMs,
  formatDuration,
  getBuildLog,
  lastRun,
  listBuilds,
  listSecretRefs,
  listWorkerPools,
  parseBuild,
  parseSubstitutions,
  parseTrigger,
  retryBuild,
  runTrigger,
  runTriggerSource,
} from "./gcpcloudbuild";

const scope = { project: "p", location: "global" };
const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("parsers", () => {
  it("parses a build", () => {
    const b = parseBuild(
      {
        id: "abc",
        status: "SUCCESS",
        steps: [
          { id: "s0", name: "alpine", args: ["echo", "hi"], status: "SUCCESS" },
          { name: "x" },
        ],
        buildTriggerId: "t1",
      },
      scope,
    );
    expect(b.name).toBe("projects/p/locations/global/builds/abc");
    expect(b.steps).toHaveLength(2);
    expect(b.steps[1].status).toBe("PENDING");
    expect(b.triggerId).toBe("t1");
    expect(parseBuild({}, scope).status).toBe("STATUS_UNKNOWN");
  });

  it.each([
    [{ github: { push: { branch: "main" } } }, "GitHub push main"],
    [{ github: { pullRequest: { branch: "dev" } } }, "GitHub pull request dev"],
    [{ pubsubConfig: { topic: "projects/p/topics/t" } }, "Pub/Sub t"],
    [{ webhookConfig: {} }, "Webhook"],
    [{}, "Manual"],
  ])("trigger event %j", (raw, want) => expect(describeTriggerEvent(raw)).toBe(want));

  it("parses a trigger", () => {
    const t = parseTrigger({ id: "t1", name: "nightly", disabled: true });
    expect(t).toMatchObject({ id: "t1", displayName: "nightly", disabled: true });
  });

  it("knows which statuses can be cancelled", () => {
    expect(ACTIVE_BUILD_STATUSES.has("WORKING")).toBe(true);
    expect(ACTIVE_BUILD_STATUSES.has("SUCCESS")).toBe(false);
  });
});

describe("requests", () => {
  it("lists builds across pages", async () => {
    const pages = [{ builds: [{ id: "a" }], nextPageToken: "n" }, { builds: [{ id: "b" }] }];
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json(pages.shift())));
    expect((await listBuilds(scope, f)).map((b) => b.id)).toEqual(["a", "b"]);
    expect(f.mock.calls[0][0]).toBe(`${CLOUDBUILD_PROXY_BASE}/projects/p/locations/global/builds`);
    expect(String(f.mock.calls[1][0])).toContain("pageToken=n");
  });

  it("reads the log", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({ available: true, log: "hi" })));
    expect(await getBuildLog(scope, "abc", f)).toEqual({ available: true, log: "hi" });
    expect(f.mock.calls[0][0]).toContain("/builds/abc/log");
  });

  it("posts cancel, retry and run", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({})));
    await cancelBuild(scope, "abc", f);
    await retryBuild(scope, "abc", f);
    await runTrigger(scope, "t1", "main", f);
    await runTrigger(scope, "t1", "", f);
    expect(f.mock.calls.map((c) => c[0])).toEqual([
      `${CLOUDBUILD_PROXY_BASE}/projects/p/locations/global/builds/abc:cancel`,
      `${CLOUDBUILD_PROXY_BASE}/projects/p/locations/global/builds/abc:retry`,
      `${CLOUDBUILD_PROXY_BASE}/projects/p/locations/global/triggers/t1:run`,
      `${CLOUDBUILD_PROXY_BASE}/projects/p/locations/global/triggers/t1:run`,
    ]);
    expect((f.mock.calls[2][1] as RequestInit).body).toBe(
      JSON.stringify({ source: { branchName: "main" } }),
    );
    expect((f.mock.calls[3][1] as RequestInit).body).toBe("{}");
  });

  it("deletes a trigger and surfaces errors", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ error: { message: "nope", status: "NOT_FOUND" } }, 404)),
    );
    await expect(deleteTrigger(scope, "t1", f)).rejects.toThrow("nope");
  });
});

const mk = (id: string, t: string, trig: string) =>
  parseBuild({ id, createTime: t, buildTriggerId: trig }, scope);

describe("build details", () => {
  it("describes sources", () => {
    expect(describeSource({ source: { storageSource: { bucket: "b", object: "o.tgz" } } })).toBe(
      "gs://b/o.tgz",
    );
    expect(describeSource({ source: { repoSource: { repoName: "r", branchName: "main" } } })).toBe(
      "r@main",
    );
    expect(
      describeSource({ source: { gitSource: { url: "https://x/y.git", revision: "v1" } } }),
    ).toBe("https://x/y.git@v1");
    expect(describeSource({})).toBe("");
  });

  it("computes and formats durations", () => {
    expect(durationMs("", "")).toBeNull();
    expect(durationMs("2026-01-01T00:00:00Z", "2026-01-01T00:01:05Z")).toBe(65000);
    expect(durationMs("2026-01-01T00:00:00Z", "", Date.parse("2026-01-01T00:00:03Z"))).toBe(3000);
    expect(formatDuration(null)).toBe("");
    expect(formatDuration(400)).toBe("400 ms");
    expect(formatDuration(65000)).toBe("1m 5s");
    expect(formatDuration(7000)).toBe("7s");
  });

  it("parses results, outputs, approval and step timing", () => {
    const b = parseBuild(
      {
        id: "b1",
        approval: { state: "PENDING" },
        steps: [
          {
            name: "x",
            status: "SUCCESS",
            timing: { startTime: "2026-01-01T00:00:00Z", endTime: "2026-01-01T00:00:02Z" },
            exitCode: 1,
          },
        ],
        results: {
          images: [{ name: "gcr.io/p/i", digest: "sha256:1" }],
          buildStepOutputs: [btoa("out")],
        },
        artifacts: { images: ["a"], objects: { location: "gs://b/", paths: ["x"] } },
      },
      scope,
    );
    expect(b.approval).toBe("PENDING");
    expect(b.steps[0]).toMatchObject({ startTime: "2026-01-01T00:00:00Z", exitCode: 1 });
    expect(b.resultImages).toEqual([{ name: "gcr.io/p/i", digest: "sha256:1" }]);
    expect(b.stepOutputs).toEqual(["out"]);
    expect(b.artifacts).toEqual(["image a", "gs://b/ (x)"]);
  });

  it("lists secret names without values", () => {
    const refs = listSecretRefs({
      secrets: [{ kmsKeyName: "k", secretEnv: { TOKEN: "" } }],
      availableSecrets: {
        secretManager: [{ versionName: "projects/p/secrets/s/versions/1", env: "DB" }],
        inline: [{ envMap: { INL: "" } }],
      },
    });
    expect(refs.map((r) => r.name)).toEqual(["TOKEN", "DB", "INL"]);
    expect(JSON.stringify(refs)).not.toContain("ciphertext");
  });

  it("finds the last run of a trigger", () => {
    const builds = [
      mk("a", "2026-01-01T00:00:00Z", "t"),
      mk("b", "2026-01-02T00:00:00Z", "t"),
      mk("c", "2026-01-03T00:00:00Z", "u"),
    ];
    expect(lastRun("t", builds)?.id).toBe("b");
    expect(lastRun("zz", builds)).toBeNull();
  });
});

describe("approval, pools and runs", () => {
  it("posts only the decision and comment", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({})));
    await approveBuild(scope, "abc", true, "ok", f);
    await approveBuild(scope, "abc", false, "", f);
    expect(f.mock.calls[0][0]).toBe(
      `${CLOUDBUILD_PROXY_BASE}/projects/p/locations/global/builds/abc:approve`,
    );
    expect((f.mock.calls[0][1] as RequestInit).body).toBe(
      JSON.stringify({ approvalResult: { decision: "APPROVED", comment: "ok" } }),
    );
    expect(String((f.mock.calls[1][1] as RequestInit).body)).toContain("REJECTED");
  });

  it("lists worker pools", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(
        json({
          workerPools: [
            {
              name: "projects/p/locations/global/workerPools/wp",
              state: "RUNNING",
              privatePoolV1Config: {
                workerConfig: { machineType: "e2-standard-4", diskSizeGb: "100" },
              },
            },
          ],
        }),
      ),
    );
    const pools = await listWorkerPools(scope, f);
    expect(pools[0]).toMatchObject({
      id: "wp",
      state: "RUNNING",
      machineType: "e2-standard-4",
      diskSizeGb: "100",
    });
  });

  it("runs a trigger on a tag or commit with substitutions", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({})));
    await runTriggerSource(scope, "t", { tag: "v1", substitutions: { _A: "1" } }, f);
    await runTriggerSource(scope, "t", { commit: "abc" }, f);
    expect((f.mock.calls[0][1] as RequestInit).body).toBe(
      JSON.stringify({ source: { tagName: "v1", substitutions: { _A: "1" } } }),
    );
    expect((f.mock.calls[1][1] as RequestInit).body).toBe(
      JSON.stringify({ source: { commitSha: "abc" } }),
    );
  });

  it("parses substitution overrides", () => {
    expect(parseSubstitutions("_A=1\n\n_B=x=y")).toEqual({
      values: { _A: "1", _B: "x=y" },
      error: "",
    });
    expect(parseSubstitutions("bad").error).toContain("_KEY=value");
    expect(parseSubstitutions("_a=1").error).toContain("_a=1");
  });
});
