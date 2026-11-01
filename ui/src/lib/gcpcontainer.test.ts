import { describe, expect, it, vi } from "vitest";

import {
  GKE_PROXY_BASE,
  deleteCluster,
  getPoolSizes,
  isBusy,
  listClusters,
  listOperations,
  parseCluster,
  parseNodePool,
  releaseChannelLabel,
  resizeNodePool,
  validNodeCount,
} from "./gcpcontainer";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;

const pool = (name: string, extra: Record<string, unknown> = {}) => ({
  name,
  status: "RUNNING",
  version: "1.30.1-gke.100",
  initialNodeCount: 3,
  config: { machineType: "e2-standard-4", diskSizeGb: 100 },
  autoscaling: { enabled: true, minNodeCount: 1, maxNodeCount: 5 },
  locations: ["us-central1-a"],
  ...extra,
});
const cluster = (name: string, location = "us-central1-a") => ({
  name,
  location,
  status: "RUNNING",
  currentMasterVersion: "1.30.1-gke.100",
  endpoint: "34.1.2.3",
  releaseChannel: { channel: "REGULAR" },
  network: "default",
  subnetwork: "default",
  currentNodeCount: 3,
  createTime: "2026-10-01T00:00:00Z",
  nodePools: [pool("default-pool")],
});
const path = "projects/p/locations/us-central1-a/clusters/c1";

describe("parsing", () => {
  it("parses a cluster with its pools", () => {
    expect(parseCluster(cluster("c1"), "p")).toMatchObject({
      name: "c1",
      path,
      status: "RUNNING",
      endpoint: "34.1.2.3",
      releaseChannel: "REGULAR",
      currentNodeCount: 3,
      pools: [{ name: "default-pool", machineType: "e2-standard-4" }],
    });
  });

  it("parses autoscaling and defaults", () => {
    expect(parseNodePool(pool("a")).autoscaling).toEqual({ enabled: true, min: 1, max: 5 });
    expect(parseNodePool({ name: "b" }).autoscaling).toEqual({ enabled: false, min: 0, max: 0 });
  });

  it.each([
    ["REGULAR", "regular"],
    ["UNSPECIFIED", "none"],
    ["", "none"],
  ])("labels release channel %s", (channel, want) => {
    expect(releaseChannelLabel(channel)).toBe(want);
  });

  it.each([
    ["RUNNING", false],
    ["RECONCILING", true],
    ["PROVISIONING", true],
  ])("busy state %s", (status, want) => {
    expect(isBusy(status)).toBe(want);
  });

  it.each([
    ["3", true],
    ["0", true],
    ["1000", true],
    ["1001", false],
    ["-1", false],
    ["x", false],
    ["", false],
    [7, true],
  ])("validates node count %j", (value, want) => {
    expect(validNodeCount(value)).toBe(want);
  });
});

describe("client", () => {
  it("lists clusters across locations, sorted", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ clusters: [cluster("b", "us-east1-b"), cluster("a")] })),
    );
    const out = await listClusters("p", asFetch(fetchFn));
    expect(out.map((c) => `${c.location}/${c.name}`)).toEqual(["us-central1-a/a", "us-east1-b/b"]);
    expect(fetchFn.mock.calls[0]).toEqual([
      `${GKE_PROXY_BASE}/projects/p/locations/-/clusters`,
      { method: "GET" },
    ]);
  });

  it("reads pool sizes with the poolSizes verb", async () => {
    const fetchFn = vi.fn((_url: string) => Promise.resolve(json({ "default-pool": 4 })));
    expect(await getPoolSizes(path, asFetch(fetchFn))).toEqual({ "default-pool": 4 });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${GKE_PROXY_BASE}/${path}:poolSizes`);
  });

  it("resizes a pool and deletes a cluster", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(
        json({ name: "operation-1", operationType: "SET_NODE_POOL_SIZE", status: "RUNNING" }),
      ),
    );
    const op = await resizeNodePool(path, "default-pool", 5, asFetch(fetchFn));
    await deleteCluster(path, asFetch(fetchFn));
    expect(op).toMatchObject({ name: "operation-1", type: "SET_NODE_POOL_SIZE" });
    const [resize, del] = fetchFn.mock.calls;
    expect(resize?.[0]).toBe(`${GKE_PROXY_BASE}/${path}/nodePools/default-pool:setSize`);
    expect(JSON.parse(String((resize![1] as RequestInit).body))).toEqual({ nodeCount: 5 });
    expect([del?.[0], (del![1] as RequestInit).method]).toEqual([
      `${GKE_PROXY_BASE}/${path}`,
      "DELETE",
    ]);
  });

  it("lists operations newest first", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(
        json({
          operations: [
            {
              name: "o1",
              startTime: "2026-10-01T00:00:00Z",
              status: "DONE",
              targetLink: "x/clusters/c1",
            },
            { name: "o2", startTime: "2026-10-02T00:00:00Z", status: "RUNNING" },
          ],
        }),
      ),
    );
    const ops = await listOperations("p", asFetch(fetchFn));
    expect(ops.map((o) => o.name)).toEqual(["o2", "o1"]);
    expect(ops[1]?.target).toBe("c1");
  });

  it("surfaces a REST error", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(
        json({ error: { message: "autoscaling enabled", status: "FAILED_PRECONDITION" } }, 400),
      );
    await expect(resizeNodePool(path, "p", 1, asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 400,
      code: "FAILED_PRECONDITION",
    });
  });
});
