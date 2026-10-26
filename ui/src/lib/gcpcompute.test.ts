import { describe, expect, it, vi } from "vitest";

import {
  COMPUTE_PROXY_BASE,
  deleteAddress,
  deleteInstance,
  instanceAction,
  labelText,
  listAddresses,
  listInstances,
  listOperations,
  listZoneNames,
  parseAddress,
  parseFirewall,
  parseInstance,
  parseOperation,
  parseRoute,
  parseRouter,
  regionOfZone,
  scopeOf,
} from "./gcpcompute";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
const asFetch = (f: unknown) => f as typeof fetch;

describe("parsing", () => {
  it("derives instance fields from the compute resource", () => {
    const i = parseInstance({
      name: "web-1",
      status: "RUNNING",
      zone: "https://x/projects/p/zones/us-central1-a",
      machineType: "https://x/projects/p/zones/us-central1-a/machineTypes/e2-medium",
      labels: { env: "dev" },
      metadata: {
        items: [
          { key: "startup-script", value: "echo" },
          { key: "owner", value: "me" },
        ],
      },
      disks: [
        {
          source: "https://x/zones/z/disks/data",
          boot: true,
          diskSizeGb: "20",
        },
      ],
      networkInterfaces: [{ networkIP: "10.0.0.2", accessConfigs: [{ natIP: "34.1.1.1" }] }],
    });
    expect(i).toMatchObject({
      zone: "us-central1-a",
      machineType: "e2-medium",
      internalIps: ["10.0.0.2"],
      externalIps: ["34.1.1.1"],
      metadataKeys: ["startup-script", "owner"],
      disks: [{ name: "data", sizeGb: 20, boot: true }],
    });
    expect(labelText(i.labels)).toBe("env=dev");
  });

  it("summarises firewall rules by direction", () => {
    const f = parseFirewall({
      name: "ssh",
      network: "x/global/networks/vpc",
      allowed: [{ IPProtocol: "tcp", ports: ["22", "80"] }, { IPProtocol: "icmp" }],
      sourceRanges: ["0.0.0.0/0"],
    });
    expect(f).toMatchObject({
      direction: "INGRESS",
      rules: ["allow tcp:22,80", "allow icmp"],
      ranges: ["0.0.0.0/0"],
    });
    expect(
      parseFirewall({ direction: "EGRESS", destinationRanges: ["10.0.0.0/8"] }).ranges,
    ).toEqual(["10.0.0.0/8"]);
  });

  it("finds the route next hop and the router NATs", () => {
    expect(parseRoute({ nextHopIp: "x/10.0.0.9", destRange: "1.0.0.0/8" }).nextHop).toBe(
      "10.0.0.9",
    );
    expect(
      parseRouter({
        name: "r",
        nats: [
          {
            name: "n",
            natIpAllocateOption: "AUTO_ONLY",
            sourceSubnetworkIpRangesToNat: "ALL",
          },
        ],
      }).nats,
    ).toEqual(["n (AUTO_ONLY, ALL)"]);
  });

  it("treats region-less addresses as global and joins operation errors", () => {
    expect(parseAddress({ name: "a" }).scope).toBe("global");
    expect(parseAddress({ name: "a", region: "https://x/regions/us-east1" }).scope).toBe(
      "us-east1",
    );
    const op = parseOperation({
      name: "o",
      zone: "x/zones/z1",
      error: { errors: [{ message: "a" }, { message: "b" }] },
    });
    expect(op).toMatchObject({ scope: "z1", error: "a; b" });
  });

  it("maps zones to regions and scopes", () => {
    expect(regionOfZone("us-central1-a")).toBe("us-central1");
    expect(scopeOf("https://x/zones/europe-west1-b/instances/i", "zones")).toBe("europe-west1-b");
  });
});

describe("requests", () => {
  it("flattens aggregated lists and skips scopes with only warnings", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        items: {
          "zones/a": { instances: [{ name: "i1" }] },
          "zones/b": { warning: { code: "NO_RESULTS_ON_PAGE" } },
          "zones/c": { instances: [{ name: "i2" }] },
        },
      }),
    );
    const got = await listInstances("p", asFetch(fetchFn));
    expect(got.map((i) => i.name)).toEqual(["i1", "i2"]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${COMPUTE_PROXY_BASE}/projects/p/aggregated/instances`,
    );
  });

  it("follows page tokens", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ items: [{ name: "us-a" }], nextPageToken: "t" }))
      .mockResolvedValueOnce(json({ items: [{ name: "asia-a" }] }));
    expect(await listZoneNames("p", asFetch(fetchFn))).toEqual(["asia-a", "us-a"]);
    expect(String(fetchFn.mock.calls[1]?.[0])).toContain("pageToken=t");
  });

  it("reads regional and global addresses from one aggregated list", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        items: {
          "regions/r": { addresses: [{ name: "r-ip", region: "x/regions/r" }] },
          global: { addresses: [{ name: "g-ip" }] },
        },
      }),
    );
    const got = await listAddresses("p", asFetch(fetchFn));
    expect(got.map((a) => `${a.scope}/${a.name}`)).toEqual(["r/r-ip", "global/g-ip"]);
    expect(fetchFn).toHaveBeenCalledTimes(1);
  });

  it("lists operations newest first", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        items: {
          global: {
            operations: [
              { name: "old", insertTime: "2026-01-01" },
              { name: "new", insertTime: "2026-02-01" },
            ],
          },
        },
      }),
    );
    expect((await listOperations("p", asFetch(fetchFn))).map((o) => o.name)).toEqual([
      "new",
      "old",
    ]);
  });

  it("encodes names in actions and deletes", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    await instanceAction("p q", "z", "vm/1", "stop", asFetch(fetchFn));
    await deleteInstance("p", "z", "vm1", asFetch(fetchFn));
    await deleteAddress("p", "global", "a", asFetch(fetchFn));
    await deleteAddress("p", "us-east1", "a", asFetch(fetchFn));
    expect(fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit).method])).toEqual([
      [`${COMPUTE_PROXY_BASE}/projects/p%20q/zones/z/instances/vm%2F1/stop`, "POST"],
      [`${COMPUTE_PROXY_BASE}/projects/p/zones/z/instances/vm1`, "DELETE"],
      [`${COMPUTE_PROXY_BASE}/projects/p/global/addresses/a`, "DELETE"],
      [`${COMPUTE_PROXY_BASE}/projects/p/regions/us-east1/addresses/a`, "DELETE"],
    ]);
  });

  it("surfaces proxy errors", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ error: { message: "nope", status: "NOT_FOUND" } }, 404));
    await expect(listInstances("p", asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 404,
      code: "NOT_FOUND",
    });
  });
});
