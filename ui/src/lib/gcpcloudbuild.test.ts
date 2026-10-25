import { describe, expect, it, vi } from "vitest";

import {
  ACTIVE_BUILD_STATUSES,
  CLOUDBUILD_PROXY_BASE,
  cancelBuild,
  deleteTrigger,
  describeTriggerEvent,
  getBuildLog,
  listBuilds,
  parseBuild,
  parseTrigger,
  retryBuild,
  runTrigger,
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
