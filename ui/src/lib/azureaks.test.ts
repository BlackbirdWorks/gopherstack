import { describe, expect, it, vi } from "vitest";

import {
  AKS_API_VERSION,
  AKS_PROXY_BASE,
  credentialsCommand,
  deleteCluster,
  getUpgradeProfile,
  listClusters,
  parseCluster,
  parsePool,
  parseUpgradeProfile,
  powerAction,
  scalePool,
  totalNodes,
  validPoolCount,
} from "./azureaks";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;

const sub = "00000000-0000-0000-0000-000000000000";
const api = `?api-version=${AKS_API_VERSION}`;
const idOf = (rg: string, name: string) =>
  `/subscriptions/${sub}/resourceGroups/${rg}/providers/Microsoft.ContainerService/managedClusters/${name}`;
const base = `${AKS_PROXY_BASE}${idOf("demo", "prod")}`;
const target = { resourceGroup: "demo", name: "prod" };

const cluster = (name: string, rg = "demo", extra: Record<string, unknown> = {}) => ({
  id: idOf(rg, name),
  name,
  location: "eastus",
  sku: { name: "Base", tier: "Free" },
  properties: {
    kubernetesVersion: "1.35",
    currentKubernetesVersion: "1.35.2",
    provisioningState: "Succeeded",
    powerState: { code: "Running" },
    fqdn: `${name}.hcp.eastus.azmk8s.io`,
    nodeResourceGroup: `MC_${rg}_${name}_eastus`,
    networkProfile: {
      networkPlugin: "azure",
      networkPluginMode: "overlay",
      serviceCidr: "10.0.0.0/16",
      dnsServiceIP: "10.0.0.10",
      loadBalancerSku: "standard",
    },
    agentPoolProfiles: [
      {
        name: "system",
        mode: "System",
        vmSize: "Standard_D4s_v3",
        count: 2,
        availabilityZones: ["1", "2"],
      },
      { name: "burst", mode: "User", count: 1, enableAutoScaling: true, minCount: 1, maxCount: 5 },
    ],
    ...extra,
  },
});

describe("parsing", () => {
  it("parses a cluster with network profile and pools", () => {
    expect(parseCluster(cluster("prod"))).toMatchObject({
      name: "prod",
      resourceGroup: "demo",
      version: "1.35",
      currentVersion: "1.35.2",
      power: "Running",
      fqdn: "prod.hcp.eastus.azmk8s.io",
      private: false,
      nodeResourceGroup: "MC_demo_prod_eastus",
      tier: "Free",
      network: { plugin: "azure", pluginMode: "overlay", dnsServiceIp: "10.0.0.10" },
    });
  });

  it("flags a private cluster by its private FQDN", () => {
    const c = parseCluster(
      cluster("p", "demo", { fqdn: undefined, privateFQDN: "p.privatelink.x" }),
    );
    expect(c).toMatchObject({ fqdn: "p.privatelink.x", private: true });
  });

  it.each([
    [
      { name: "a", properties: { count: 3, enableAutoScaling: true, minCount: 1, maxCount: 4 } },
      3,
      true,
    ],
    [{ name: "b", mode: "User", count: 2 }, 2, false],
    [{ name: "c" }, 0, false],
  ])("parses pool %#", (raw, count, autoscale) => {
    expect(parsePool(raw)).toMatchObject({ count, autoscale });
  });

  it("sums pool counts", () => {
    expect(totalNodes(parseCluster(cluster("prod")))).toBe(3);
  });

  it("parses an upgrade profile", () => {
    const p = parseUpgradeProfile({
      properties: {
        controlPlaneProfile: {
          kubernetesVersion: "1.35.2",
          upgrades: [
            { kubernetesVersion: "1.36.1" },
            { kubernetesVersion: "1.38.0", isPreview: true },
          ],
        },
        agentPoolProfiles: [{ name: "system", kubernetesVersion: "1.35.2", upgrades: [] }],
      },
    });
    expect(p.controlPlane.upgrades).toEqual([
      { version: "1.36.1", preview: false },
      { version: "1.38.0", preview: true },
    ]);
    expect(p.pools).toEqual([{ name: "system", version: "1.35.2", upgrades: [] }]);
  });

  it.each([
    ["0", true],
    ["12", true],
    ["1000", true],
    ["1001", false],
    ["-1", false],
    ["1.5", false],
    ["", false],
    ["abc", false],
  ])("validates pool count %j", (v, ok) => {
    expect(validPoolCount(v)).toBe(ok);
  });

  it("offers the CLI command instead of credentials", () => {
    expect(credentialsCommand(target)).toBe(
      "az aks get-credentials --resource-group demo --name prod",
    );
  });
});

describe("client", () => {
  it("lists clusters sorted by resource group and name", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(
        json({ value: [cluster("zeta", "b"), cluster("alpha", "b"), cluster("prod")] }),
      ),
    );
    const out = await listClusters(sub, asFetch(fetchFn));
    expect(out.map((c) => `${c.resourceGroup}/${c.name}`)).toEqual([
      "b/alpha",
      "b/zeta",
      "demo/prod",
    ]);
    expect(fetchFn.mock.calls[0]).toEqual([
      `${AKS_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.ContainerService/managedClusters${api}`,
      { method: "GET" },
    ]);
  });

  it("reads the upgrade profile", async () => {
    const fetchFn = vi.fn((_u: string) =>
      Promise.resolve(
        json({ properties: { controlPlaneProfile: { kubernetesVersion: "1.35.2" } } }),
      ),
    );
    expect((await getUpgradeProfile(sub, target, asFetch(fetchFn))).controlPlane.version).toBe(
      "1.35.2",
    );
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${base}/upgradeProfiles/default${api}`);
  });

  it.each([
    ["start", "POST"],
    ["stop", "POST"],
  ] as const)("posts %s", async (action, method) => {
    const fetchFn = vi.fn((_u: string, _i?: RequestInit) => Promise.resolve(json({}, 202)));
    await powerAction(sub, target, action, asFetch(fetchFn));
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${base}/${action}${api}`);
    expect(initOf(fetchFn, 0).method).toBe(method);
  });

  it("deletes a cluster", async () => {
    const fetchFn = vi.fn((_u: string, _i?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    await deleteCluster(sub, target, asFetch(fetchFn));
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${base}${api}`);
    expect(initOf(fetchFn, 0).method).toBe("DELETE");
  });

  it("scales a pool by writing back its current properties with the new count", async () => {
    const pool = {
      properties: { mode: "User", vmSize: "Standard_D4s_v3", count: 1, osType: "Linux" },
    };
    const fetchFn = vi.fn((_u: string, init?: RequestInit) =>
      Promise.resolve(json(init?.method === "PUT" ? {} : pool)),
    );
    await scalePool(sub, target, "burst", 6, asFetch(fetchFn));
    const url = `${base}/agentPools/burst${api}`;
    expect(fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit).method])).toEqual([
      [url, "GET"],
      [url, "PUT"],
    ]);
    expect(JSON.parse(initOf(fetchFn, 1).body as string)).toEqual({
      properties: { mode: "User", vmSize: "Standard_D4s_v3", count: 6, osType: "Linux" },
    });
  });

  it("surfaces an ARM failure", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ error: { code: "Conflict", message: "cluster is stopped" } }, 409));
    await expect(scalePool(sub, target, "burst", 2, asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 409,
      message: "cluster is stopped",
    });
  });
});
