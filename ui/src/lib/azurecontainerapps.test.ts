import { describe, expect, it, vi } from "vitest";

import {
  ACA_API_VERSION,
  ACA_PROXY_BASE,
  appAction,
  appUrl,
  listApps,
  listEnvironments,
  listExecutions,
  listJobs,
  listRevisions,
  parseApp,
  parseRevision,
  revisionAction,
  revisionActions,
  startJob,
} from "./azurecontainerapps";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;

const sub = "00000000-0000-0000-0000-000000000000";
const idOf = (rg: string, type: string, name: string) =>
  `/subscriptions/${sub}/resourceGroups/${rg}/providers/Microsoft.App/${type}/${name}`;
const api = `?api-version=${ACA_API_VERSION}`;
const base = `${ACA_PROXY_BASE}${idOf("demo", "containerApps", "web")}`;

const app = (name: string, rg = "demo", extra: Record<string, unknown> = {}) => ({
  id: idOf(rg, "containerApps", name),
  name,
  location: "eastus",
  properties: {
    provisioningState: "Succeeded",
    runningStatus: "Running",
    managedEnvironmentId: idOf(rg, "managedEnvironments", "env"),
    latestRevisionName: `${name}--abc`,
    configuration: {
      activeRevisionsMode: "Multiple",
      ingress: {
        fqdn: `${name}.env.example.io`,
        external: true,
        targetPort: 80,
        traffic: [
          { revisionName: `${name}--abc`, weight: 70 },
          { latestRevision: true, weight: 30 },
        ],
      },
    },
    template: { scale: { minReplicas: 1, maxReplicas: 3 }, containers: [{ image: "nginx:1" }] },
    ...extra,
  },
});

describe("parsing", () => {
  it("parses an app with ingress, traffic and scale", () => {
    expect(parseApp(app("web"))).toMatchObject({
      name: "web",
      resourceGroup: "demo",
      runningStatus: "Running",
      latestRevision: "web--abc",
      fqdn: "web.env.example.io",
      external: true,
      targetPort: 80,
      revisionMode: "Multiple",
      minReplicas: 1,
      maxReplicas: 3,
      images: ["nginx:1"],
      traffic: [
        { revision: "web--abc", weight: 70, latest: false },
        { revision: "", weight: 30, latest: true },
      ],
    });
  });

  it("links only external ingress", () => {
    expect(appUrl({ fqdn: "a.io", external: true })).toBe("https://a.io");
    expect(appUrl({ fqdn: "a.io", external: false })).toBe("");
    expect(appUrl({ fqdn: "", external: true })).toBe("");
  });

  it("parses a revision", () => {
    expect(
      parseRevision({
        name: "web--abc",
        properties: {
          active: true,
          runningState: "Running",
          healthState: "Healthy",
          replicas: 2,
          trafficWeight: 70,
        },
      }),
    ).toMatchObject({
      name: "web--abc",
      active: true,
      running: "Running",
      replicas: 2,
      traffic: 70,
    });
  });

  it.each([
    [true, ["restart", "deactivate"]],
    [false, ["activate"]],
  ])("revision actions for active=%s", (active, want) => {
    expect(revisionActions({ active })).toEqual(want);
  });
});

describe("client", () => {
  it("lists apps, environments and jobs of a subscription", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json({
          value: url.includes("/containerApps")
            ? [app("zeta", "b"), app("alpha", "b"), app("web")]
            : url.includes("/jobs")
              ? [
                  {
                    id: idOf("demo", "jobs", "j"),
                    name: "j",
                    properties: { configuration: { triggerType: "Manual" } },
                  },
                ]
              : [
                  {
                    id: idOf("demo", "managedEnvironments", "env"),
                    name: "env",
                    properties: { defaultDomain: "env.example.io" },
                  },
                ],
        }),
      ),
    );
    const apps = await listApps(sub, asFetch(fetchFn));
    expect(apps.map((a) => `${a.resourceGroup}/${a.name}`)).toEqual([
      "b/alpha",
      "b/zeta",
      "demo/web",
    ]);
    expect((await listEnvironments(sub, asFetch(fetchFn)))[0]?.defaultDomain).toBe(
      "env.example.io",
    );
    expect((await listJobs(sub, asFetch(fetchFn)))[0]?.trigger).toBe("Manual");
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${ACA_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.App/containerApps${api}`,
    );
  });

  it("lists revisions newest first and executions", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json({
          value: url.includes("/executions")
            ? [
                {
                  name: "e1",
                  properties: { status: "Succeeded", startTime: "2026-10-01T00:00:00Z" },
                },
              ]
            : [
                { name: "old", properties: { createdTime: "2026-10-01T00:00:00Z" } },
                { name: "new", properties: { createdTime: "2026-10-02T00:00:00Z" } },
              ],
        }),
      ),
    );
    const target = { resourceGroup: "demo", name: "web" };
    expect((await listRevisions(sub, target, asFetch(fetchFn))).map((r) => r.name)).toEqual([
      "new",
      "old",
    ]);
    expect(
      (await listExecutions(sub, { resourceGroup: "demo", name: "j" }, asFetch(fetchFn)))[0]
        ?.status,
    ).toBe("Succeeded");
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${base}/revisions${api}`);
  });

  it("posts app, revision and job actions", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(json({ name: "exec-1" })),
    );
    const target = { resourceGroup: "demo", name: "web" };
    await appAction(sub, target, "stop", asFetch(fetchFn));
    await revisionAction(sub, target, "web--abc", "restart", asFetch(fetchFn));
    const started = await startJob(sub, { resourceGroup: "demo", name: "j" }, asFetch(fetchFn));
    expect(started).toBe("exec-1");
    expect(fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit).method])).toEqual([
      [`${base}/stop${api}`, "POST"],
      [`${base}/revisions/web--abc/restart${api}`, "POST"],
      [`${ACA_PROXY_BASE}${idOf("demo", "jobs", "j")}/start${api}`, "POST"],
    ]);
  });

  it("surfaces an ARM failure", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ error: { code: "Conflict", message: "already stopped" } }, 409));
    await expect(
      appAction(sub, { resourceGroup: "demo", name: "web" }, "stop", asFetch(fetchFn)),
    ).rejects.toMatchObject({
      httpStatus: 409,
      message: "already stopped",
    });
  });
});
