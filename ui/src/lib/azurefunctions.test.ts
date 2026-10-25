import { describe, expect, it, vi } from "vitest";

import {
  FUNCTIONS_PROXY_BASE,
  appAction,
  deleteFunctionApp,
  describeRuntime,
  listAppFunctions,
  listDeployments,
  listFunctionApps,
  listSettings,
  revealSetting,
} from "./azurefunctions";

const SUB = "00000000-0000-0000-0000-000000000000";
const SITE_ID = `/subscriptions/${SUB}/resourceGroups/rg1/providers/Microsoft.Web/sites/app1`;
const PLAN_ID = `/subscriptions/${SUB}/resourceGroups/rg1/providers/Microsoft.Web/serverfarms/plan1`;

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function router(routes: Record<string, unknown>) {
  return vi.fn((url: string) => {
    const hit = Object.entries(routes).find(([k]) => url.includes(k));
    return Promise.resolve(hit ? json(hit[1]) : json({ error: { message: "no route" } }, 404));
  });
}

describe("runtime", () => {
  it.each([
    ["NODE|20", "Node 20"],
    ["PYTHON|3.11", "Python 3.11"],
    ["DOCKER|img:1", "Docker img:1"],
    ["custom", "custom"],
    ["", ""],
  ])("%s", (fx, want) => expect(describeRuntime(fx)).toBe(want));
});

describe("function apps", () => {
  it("joins ARM sites, plans and the data plane", async () => {
    const f = router({
      "/subscriptions?": { value: [{ subscriptionId: SUB }] },
      "providers/Microsoft.Web/sites": {
        value: [
          {
            id: SITE_ID,
            name: "app1",
            kind: "functionapp,linux",
            location: "eastus",
            properties: {
              state: "Running",
              serverFarmId: PLAN_ID,
              defaultHostName: "app1.azurewebsites.localhost:10012",
              siteConfig: { linuxFxVersion: "NODE|20" },
            },
          },
          { id: "x", name: "web", kind: "app" },
        ],
      },
      "providers/Microsoft.Web/serverfarms": { value: [{ id: PLAN_ID, sku: { name: "Y1" } }] },
      "/apps": { apps: [{ name: "app1", deployed: true, worker: "node" }] },
    });
    const apps = await listFunctionApps(f as unknown as typeof fetch);
    expect(apps).toHaveLength(1);
    expect(apps[0]).toMatchObject({
      name: "app1",
      resourceGroup: "rg1",
      plan: "plan1",
      planSku: "Y1",
      runtimeStack: "Node 20",
      state: "Running",
      deployed: true,
      worker: "node",
    });
  });
});

describe("data plane", () => {
  it("lists settings without values", async () => {
    const f = router({ "/settings": { settings: [{ name: "FOO", value: "••••" }] } });
    const s = await listSettings("app1", f as unknown as typeof fetch);
    expect(s).toEqual([{ name: "FOO", value: "••••" }]);
    expect(f.mock.calls[0][0]).toBe(`${FUNCTIONS_PROXY_BASE}/apps/app1/settings`);
  });

  it("reveals one setting with a POST body", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({ name: "FOO", value: "secret" })));
    expect(await revealSetting("app1", "FOO", f)).toBe("secret");
    expect(f.mock.calls[0]).toEqual([
      `${FUNCTIONS_PROXY_BASE}/apps/app1/settings/reveal`,
      {
        method: "POST",
        body: JSON.stringify({ name: "FOO" }),
        headers: { "Content-Type": "application/json" },
      },
    ]);
  });

  it("parses functions and deployments", async () => {
    const fns = router({
      "/functions": {
        functions: [
          {
            name: "hello",
            triggerType: "httpTrigger",
            authLevel: "anonymous",
            invokeUrl: "http://h/api/hello",
          },
        ],
      },
    });
    expect((await listAppFunctions("a", fns as unknown as typeof fetch))[0]).toMatchObject({
      name: "hello",
      authLevel: "anonymous",
      disabled: false,
    });
    const deps = router({
      "/deployments": { deployments: [{ id: "d1", succeeded: true, message: "m" }] },
    });
    expect((await listDeployments("a", deps as unknown as typeof fetch))[0]).toMatchObject({
      id: "d1",
      succeeded: true,
    });
  });

  it("posts lifecycle actions", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(new Response(null, { status: 204 })));
    await appAction("app1", "restart", f);
    expect(f.mock.calls[0]).toEqual([
      `${FUNCTIONS_PROXY_BASE}/apps/app1/restart`,
      { method: "POST" },
    ]);
  });

  it("deletes through ARM using the site id", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({})));
    await deleteFunctionApp({ id: SITE_ID } as never, f);
    expect(f.mock.calls[0][0]).toContain(`/dashboard/api/v1/azure/arm${SITE_ID}?api-version=`);
    expect(f.mock.calls[0][1]).toEqual({ method: "DELETE" });
  });

  it("surfaces proxy errors", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ error: { message: "gone", status: "NOT_FOUND" } }, 404)),
    );
    await expect(appAction("x", "stop", f)).rejects.toThrow("gone");
  });
});
