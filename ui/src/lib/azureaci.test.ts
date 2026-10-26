import { describe, expect, it, vi } from "vitest";

import {
  ACI_API_VERSION,
  ACI_PROXY_BASE,
  byResourceGroup,
  containerLogs,
  deleteGroup,
  groupAction,
  listGroups,
  parseGroup,
} from "./azureaci";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
const asFetch = (f: unknown) => f as typeof fetch;

const sub = "00000000-0000-0000-0000-000000000000";
const group = (name: string, rg: string) => ({
  id: `/subscriptions/${sub}/resourceGroups/${rg}/providers/Microsoft.ContainerInstance/containerGroups/${name}`,
  name,
  location: "eastus",
  properties: {
    osType: "Linux",
    provisioningState: "Succeeded",
    restartPolicy: "Always",
    ipAddress: {
      type: "Public",
      ip: "127.0.0.1",
      fqdn: "x.eastus.azurecontainer.io",
      ports: [{ port: 80 }],
    },
    imageRegistryCredentials: [{ server: "r.example.com", username: "u" }],
    instanceView: {
      state: "Running",
      events: [{ name: "PortPublished", type: "Normal", message: "m" }],
    },
    containers: [
      {
        name: "app",
        properties: {
          image: "alpine:3",
          environmentVariables: [{ name: "A", value: "1" }, { name: "TOKEN" }],
          resources: { requests: { cpu: 0.5, memoryInGB: 1 } },
          instanceView: {
            restartCount: 2,
            currentState: { state: "Running", startTime: "t1" },
            previousState: {
              state: "Terminated",
              exitCode: 3,
              detailStatus: "Error",
            },
            events: [
              {
                name: "Started",
                type: "Normal",
                message: "ok",
                count: 2,
                lastTimestamp: "t",
              },
            ],
          },
        },
      },
    ],
  },
});

describe("parseGroup", () => {
  it("reads state, restart count, exit code and events", () => {
    const g = parseGroup(group("web", "demo"));
    expect(g).toMatchObject({
      resourceGroup: "demo",
      provisioningState: "Succeeded",
      instanceState: "Running",
      ports: ["80/TCP"],
      registries: ["r.example.com (u)"],
    });
    expect(g.containers[0]).toMatchObject({
      restartCount: 2,
      cpu: 0.5,
      current: { state: "Running", exitCode: null },
      previous: { state: "Terminated", exitCode: 3 },
    });
    expect(g.containers[0]?.events[0]?.count).toBe(2);
  });

  it("flags env vars without a value as secure and never invents one", () => {
    const env = parseGroup(group("web", "demo")).containers[0]?.env;
    expect(env).toEqual([
      { name: "A", value: "1", secure: false },
      { name: "TOKEN", value: "", secure: true },
    ]);
  });

  it("tolerates a group with no instance view", () => {
    const g = parseGroup({
      id: "/subscriptions/s/resourceGroups/r/providers/p/x/y",
      name: "y",
      properties: {},
    });
    expect(g).toMatchObject({ instanceState: "", containers: [], events: [] });
  });
});

describe("requests", () => {
  it("lists groups sorted by resource group then name and buckets them", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        value: [group("b", "z-rg"), group("z", "a-rg"), group("a", "a-rg")],
      }),
    );
    const groups = await listGroups(sub, asFetch(fetchFn));
    expect(groups.map((g) => `${g.resourceGroup}/${g.name}`)).toEqual([
      "a-rg/a",
      "a-rg/z",
      "z-rg/b",
    ]);
    expect(byResourceGroup(groups).map((b) => [b.resourceGroup, b.groups.length])).toEqual([
      ["a-rg", 2],
      ["z-rg", 1],
    ]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${ACI_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.ContainerInstance/containerGroups?api-version=${ACI_API_VERSION}`,
    );
  });

  it("posts actions, deletes and fetches tailed logs", async () => {
    const fetchFn = vi.fn((url: string, _init?: RequestInit) =>
      Promise.resolve(json(url.includes("/logs") ? { content: "hello\n" } : {})),
    );
    const g = { resourceGroup: "my rg", name: "web" };
    await groupAction(sub, g, "restart", asFetch(fetchFn));
    await deleteGroup(sub, g, asFetch(fetchFn));
    expect(await containerLogs(sub, g, "app", 25, asFetch(fetchFn))).toBe("hello\n");
    const base = `${ACI_PROXY_BASE}/subscriptions/${sub}/resourceGroups/my%20rg/providers/Microsoft.ContainerInstance/containerGroups/web`;
    expect(fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit).method])).toEqual([
      [`${base}/restart?api-version=${ACI_API_VERSION}`, "POST"],
      [`${base}?api-version=${ACI_API_VERSION}`, "DELETE"],
      [`${base}/containers/app/logs?api-version=${ACI_API_VERSION}&tail=25`, "GET"],
    ]);
  });

  it("surfaces the not-running log error", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(
        json({ error: { code: "ContainerGroupNotRunning", message: "no logs" } }, 400),
      );
    await expect(
      containerLogs(sub, { resourceGroup: "r", name: "g" }, "c", 10, asFetch(fetchFn)),
    ).rejects.toThrow("no logs");
  });
});
