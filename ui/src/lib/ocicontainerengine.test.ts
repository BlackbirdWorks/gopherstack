import { describe, expect, it, vi } from "vitest";

import {
  OKE_PROXY_BASE,
  deleteCluster,
  kubeconfigCommand,
  listClusters,
  listCompartments,
  listNodePools,
  listWorkRequests,
  parseCluster,
  parseNodePool,
  parseWorkRequest,
  scaleNodePool,
  validPoolSize,
} from "./ocicontainerengine";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;

const comp = "ocid1.compartment.oc1..aaaa";
const cluster = (id: string, name: string) => ({
  id,
  name,
  compartmentId: comp,
  lifecycleState: "ACTIVE",
  kubernetesVersion: "v1.34.2",
  type: "BASIC_CLUSTER",
  vcnId: "ocid1.vcn.oc1.iad.vcna",
  endpoints: { kubernetes: "10.0.0.5:6443", publicEndpoint: "203.0.113.7:6443" },
  availableKubernetesUpgrades: ["v1.35.2"],
  metadata: { timeCreated: "2026-10-01T00:00:00Z" },
});
const pool = (id: string, name: string, extra: Record<string, unknown> = {}) => ({
  id,
  clusterId: "c1",
  name,
  lifecycleState: "ACTIVE",
  kubernetesVersion: "v1.34.2",
  nodeShape: "VM.Standard.E4.Flex",
  nodeShapeConfig: { ocpus: 2, memoryInGBs: 16 },
  nodeConfigDetails: {
    size: 2,
    placementConfigs: [{ availabilityDomain: "Uocm:US-ASHBURN-AD-1", subnetId: "s1" }],
  },
  nodes: [
    { id: "n1", name: "node-1", lifecycleState: "ACTIVE", privateIp: "10.0.1.4" },
    { id: "n2", name: "node-2", lifecycleState: "ACTIVE" },
  ],
  ...extra,
});

describe("parsing", () => {
  it("parses a cluster", () => {
    expect(parseCluster(cluster("c1", "prod"))).toMatchObject({
      id: "c1",
      name: "prod",
      state: "ACTIVE",
      version: "v1.34.2",
      publicEndpoint: "203.0.113.7:6443",
      kubernetesEndpoint: "10.0.0.5:6443",
      upgrades: ["v1.35.2"],
      created: "2026-10-01T00:00:00Z",
    });
  });

  it("parses a node pool with placements and nodes", () => {
    const p = parseNodePool(pool("p1", "workers"));
    expect(p).toMatchObject({
      shape: "VM.Standard.E4.Flex",
      ocpus: 2,
      memoryGbs: 16,
      size: 2,
      legacySize: false,
    });
    expect(p.placements).toEqual([{ availabilityDomain: "Uocm:US-ASHBURN-AD-1", subnetId: "s1" }]);
    expect(p.nodes.map((n) => n.name)).toEqual(["node-1", "node-2"]);
  });

  it("reads quantityPerSubnet for a legacy pool", () => {
    const p = parseNodePool({ id: "p", name: "old", quantityPerSubnet: 3, subnetIds: ["s"] });
    expect(p).toMatchObject({ size: 3, legacySize: true });
  });

  it("parses a work request", () => {
    expect(
      parseWorkRequest({
        id: "wr",
        operationType: "CLUSTER_CREATE",
        status: "SUCCEEDED",
        timeAccepted: "a",
        timeFinished: "b",
        resources: [{ identifier: "c1" }],
      }),
    ).toEqual({
      id: "wr",
      operation: "CLUSTER_CREATE",
      status: "SUCCEEDED",
      accepted: "a",
      finished: "b",
      resource: "c1",
    });
  });

  it.each([
    ["0", true],
    ["5", true],
    ["1000", true],
    ["1001", false],
    ["-2", false],
    ["2.5", false],
    ["", false],
  ])("validates pool size %j", (v, ok) => {
    expect(validPoolSize(v)).toBe(ok);
  });

  it("offers the CLI command instead of credentials", () => {
    expect(kubeconfigCommand({ id: "ocid1.cluster.oc1..c" }, "http://localhost:8158")).toBe(
      "oci ce cluster create-kubeconfig --cluster-id ocid1.cluster.oc1..c --file $HOME/.kube/config --endpoint http://localhost:8158",
    );
  });
});

describe("client", () => {
  it("lists compartments", async () => {
    const fetchFn = vi.fn((_u: string) => Promise.resolve(json({ compartments: [comp] })));
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${OKE_PROXY_BASE}/compartments`);
  });

  it("follows opc-next-page when listing clusters", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json([cluster("c1", "a")], 200, { "opc-next-page": "tok" }))
      .mockResolvedValueOnce(json([cluster("c2", "b")]));
    const out = await listClusters(comp, asFetch(fetchFn));
    expect(out.map((c) => c.name)).toEqual(["a", "b"]);
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([
      `${OKE_PROXY_BASE}/api/clusters?compartmentId=${encodeURIComponent(comp)}`,
      `${OKE_PROXY_BASE}/api/clusters?compartmentId=${encodeURIComponent(comp)}&page=tok`,
    ]);
  });

  it("reads each pool to get its nodes, sorted by name", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("/api/nodePools/")
          ? json(pool(url.endsWith("/p2") ? "p2" : "p1", url.endsWith("/p2") ? "alpha" : "zeta"))
          : json([{ id: "p1" }, { id: "p2" }]),
      ),
    );
    const out = await listNodePools(comp, "c1", asFetch(fetchFn));
    expect(out.map((p) => p.name)).toEqual(["alpha", "zeta"]);
    expect(out[0]?.nodes).toHaveLength(2);
    expect(fetchFn.mock.calls[0]?.[0]).toContain("clusterId=c1");
  });

  it("lists work requests of a cluster newest first", async () => {
    const fetchFn = vi.fn((_u: string) =>
      Promise.resolve(json([{ id: "wr", operationType: "CLUSTER_CREATE" }])),
    );
    expect((await listWorkRequests(comp, "c1", asFetch(fetchFn)))[0]?.operation).toBe(
      "CLUSTER_CREATE",
    );
    expect(fetchFn.mock.calls[0]?.[0]).toContain(
      "clusterId=c1&sortBy=TIME_ACCEPTED&sortOrder=DESC",
    );
  });

  it("deletes a cluster", async () => {
    const fetchFn = vi.fn((_u: string, _i?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    await deleteCluster("c1", asFetch(fetchFn));
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${OKE_PROXY_BASE}/api/clusters/c1`);
    expect(initOf(fetchFn, 0).method).toBe("DELETE");
  });

  it.each([
    [false, { nodeConfigDetails: { size: 4 } }],
    [true, { quantityPerSubnet: 4 }],
  ])("scales a pool (legacy=%s) sending only the size", async (legacySize, want) => {
    const fetchFn = vi.fn((_u: string, _i?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    await scaleNodePool({ id: "p1", legacySize }, 4, asFetch(fetchFn));
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${OKE_PROXY_BASE}/api/nodePools/p1`);
    expect(initOf(fetchFn, 0).method).toBe("PUT");
    expect(JSON.parse(initOf(fetchFn, 0).body as string)).toEqual(want);
  });

  it("surfaces an OCI failure", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ code: "Conflict", message: "pool is updating" }, 409));
    await expect(
      scaleNodePool({ id: "p1", legacySize: false }, 1, asFetch(fetchFn)),
    ).rejects.toMatchObject({
      httpStatus: 409,
      message: "pool is updating",
    });
  });
});
