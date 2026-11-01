import { describe, expect, it, vi } from "vitest";

import {
  COMPUTE_PROXY_BASE,
  createInstance,
  createSnapshot,
  deleteAddress,
  deleteInstance,
  imageFamilies,
  inRegionScope,
  inZoneScope,
  instanceAction,
  instanceTransitioning,
  labelText,
  listImages,
  listSecurityPolicies,
  listTargetProxies,
  metadataRevealable,
  operationInScope,
  operationRunning,
  parseBackendService,
  parseForwardingRule,
  parseNetwork,
  parseSecurityPolicy,
  parseUrlMap,
  revealMetadata,
  shouldPoll,
  POLL_MAX,
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
const bodyOf = (call: unknown[] | undefined): unknown =>
  JSON.parse(String(((call ?? [])[1] as RequestInit).body));

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

describe("metadata redaction on the client", () => {
  it("keeps keys and lengths and never offers SSH keys for reveal", () => {
    const i = parseInstance({
      name: "vm",
      metadata: {
        items: [
          { key: "startup-script", value: "", valueLength: 42, redacted: true },
          { key: "ssh-keys", value: "", valueLength: 300, redacted: true },
          { key: "sshKeys", valueLength: 1 },
          { key: "short", value: "abc" },
        ],
      },
    });
    expect(i.metadata).toEqual([
      { key: "startup-script", length: 42, revealable: true },
      { key: "ssh-keys", length: 300, revealable: false },
      { key: "sshKeys", length: 1, revealable: false },
      { key: "short", length: 3, revealable: true },
    ]);
    expect(metadataRevealable("block-project-ssh-keys")).toBe(false);
    expect(metadataRevealable("")).toBe(false);
  });

  it("requests a reveal only for permitted keys", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(
        json({ metadata: { items: [{ key: "startup-script", value: "echo hi" }] } }),
      );
    expect(await revealMetadata("p", "z", "vm", "startup-script", asFetch(fetchFn))).toBe(
      "echo hi",
    );
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${COMPUTE_PROXY_BASE}/projects/p/zones/z/instances/vm?reveal=startup-script`,
    );
    expect(await revealMetadata("p", "z", "vm", "ssh-keys", asFetch(fetchFn))).toBe("");
    expect(fetchFn).toHaveBeenCalledTimes(1);
  });
});

describe("new resources", () => {
  it("reads instance nics, disk modes and tags", () => {
    const i = parseInstance({
      name: "vm",
      tags: { items: ["web"] },
      disks: [{ source: "x/disks/d", boot: true, mode: "READ_WRITE", interface: "SCSI" }],
      networkInterfaces: [
        {
          name: "nic0",
          network: "x/global/networks/vpc",
          subnetwork: "x/regions/r/subnetworks/sub",
          networkIP: "10.0.0.2",
          accessConfigs: [{ natIP: "34.0.0.1" }],
        },
      ],
    });
    expect(i.tags).toEqual(["web"]);
    expect(i.disks[0]).toMatchObject({ mode: "READ_WRITE", iface: "SCSI" });
    expect(i.nics).toEqual([
      {
        name: "nic0",
        network: "vpc",
        subnetwork: "sub",
        internalIp: "10.0.0.2",
        externalIps: ["34.0.0.1"],
      },
    ]);
  });

  it("reads VPC peering state", () => {
    const n = parseNetwork({
      name: "a",
      peerings: [
        { name: "p", network: "x/global/networks/b", state: "ACTIVE", exportCustomRoutes: true },
      ],
    });
    expect(n.peerings).toEqual([
      {
        name: "p",
        peer: "b",
        state: "ACTIVE",
        detail: "",
        exportCustomRoutes: true,
        importCustomRoutes: false,
      },
    ]);
  });

  it("marks subnet routes as system routes", () => {
    expect(parseRoute({ name: "default-route-1", routeType: "SUBNET" }).routeType).toBe(
      "system (subnet)",
    );
    expect(parseRoute({ name: "r" }).routeType).toBe("static");
  });

  it("parses load balancing resources with the attached Cloud Armor policy", () => {
    const b = parseBackendService({
      name: "bs",
      region: "x/regions/r",
      protocol: "HTTP",
      loadBalancingScheme: "EXTERNAL",
      backends: [{ group: "x/instanceGroups/ig" }],
      healthChecks: ["x/healthChecks/hc"],
      securityPolicy: "x/securityPolicies/waf",
    });
    expect(b).toMatchObject({
      scope: "r",
      backends: ["ig"],
      healthChecks: ["hc"],
      securityPolicy: "waf",
    });
    expect(parseBackendService({ name: "g" }).scope).toBe("global");
    expect(
      parseUrlMap({
        name: "um",
        defaultService: "x/backendServices/bs",
        hostRules: [{ hosts: ["a.com"], pathMatcher: "pm" }],
        pathMatchers: [{ name: "pm" }],
      }),
    ).toMatchObject({ defaultService: "bs", hostRules: ["a.com -> pm"], pathMatchers: ["pm"] });
    expect(
      parseForwardingRule({
        name: "fr",
        IPAddress: "1.2.3.4",
        IPProtocol: "TCP",
        portRange: "80-80",
        target: "x/targetHttpProxies/tp",
      }),
    ).toMatchObject({ ip: "1.2.3.4", ports: "80-80", target: "tp" });
  });

  it("sorts Cloud Armor rules by priority and renders matches", () => {
    const p = parseSecurityPolicy({
      name: "waf",
      rules: [
        {
          priority: 2147483647,
          action: "allow",
          match: { versionedExpr: "SRC_IPS_V1", config: { srcIpRanges: ["*"] } },
        },
        {
          priority: 10,
          action: "deny(403)",
          preview: true,
          match: { expr: { expression: "origin.region_code == 'XX'" } },
        },
      ],
    });
    expect(p.rules.map((r) => [r.priority, r.action, r.match, r.preview])).toEqual([
      [10, "deny(403)", "origin.region_code == 'XX'", true],
      [2147483647, "allow", "*", false],
    ]);
  });

  it("lists images across projects and groups them by family", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("/projects/debian-cloud/")
          ? json({
              items: [
                { name: "d12-v1", family: "debian-12" },
                { name: "d12-v2", family: "debian-12" },
              ],
            })
          : url.includes("/projects/mine/")
            ? json({ items: [{ name: "custom" }] })
            : json({ error: { message: "x" } }, 404),
      ),
    );
    const images = await listImages("mine", asFetch(fetchFn));
    expect(images.map((i) => `${i.project}/${i.name}`)).toEqual([
      "debian-cloud/d12-v1",
      "debian-cloud/d12-v2",
      "mine/custom",
    ]);
    expect(imageFamilies(images)).toEqual([
      {
        project: "debian-cloud",
        family: "debian-12",
        source: "projects/debian-cloud/global/images/family/debian-12",
        label: "debian-12 (debian-cloud)",
      },
    ]);
  });

  it("merges http and https proxies and security policies", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("targetHttpsProxies")
          ? json({
              items: {
                global: {
                  targetHttpsProxies: [{ name: "s", sslCertificates: ["x/sslCertificates/c"] }],
                },
              },
            })
          : url.includes("targetHttpProxies")
            ? json({ items: { global: { targetHttpProxies: [{ name: "h" }] } } })
            : json({ items: [{ name: "waf" }] }),
      ),
    );
    const proxies = await listTargetProxies("p", asFetch(fetchFn));
    expect(proxies.map((p) => [p.name, p.kind, p.certificates])).toEqual([
      ["h", "HTTP", []],
      ["s", "HTTPS", ["c"]],
    ]);
    expect((await listSecurityPolicies("p", asFetch(fetchFn)))[0]?.name).toBe("waf");
  });

  it("creates an instance and a snapshot with fixed-shape bodies", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    await createInstance(
      "p",
      "z",
      {
        name: "vm",
        machineType: "e2-small",
        sourceImage: "projects/a/global/images/family/f",
        diskSizeGb: " 20 ",
        network: "vpc",
        subnetwork: "",
        externalIp: true,
      },
      asFetch(fetchFn),
    );
    await createSnapshot("p", "z", "disk 1", "snap", asFetch(fetchFn));
    const [first, second] = fetchFn.mock.calls;
    expect(first?.[0]).toBe(`${COMPUTE_PROXY_BASE}/projects/p/zones/z/instances`);
    expect(bodyOf(first)).toEqual({
      name: "vm",
      machineType: "e2-small",
      disks: [
        {
          boot: true,
          autoDelete: true,
          initializeParams: { sourceImage: "projects/a/global/images/family/f", diskSizeGb: "20" },
        },
      ],
      networkInterfaces: [{ network: "vpc", accessConfigs: [{ type: "ONE_TO_ONE_NAT" }] }],
    });
    expect(second?.[0]).toBe(
      `${COMPUTE_PROXY_BASE}/projects/p/zones/z/disks/disk%201/createSnapshot`,
    );
    expect(bodyOf(second)).toEqual({ name: "snap" });
  });

  it("supports suspend and resume actions", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    await instanceAction("p", "z", "vm", "suspend", asFetch(fetchFn));
    await instanceAction("p", "z", "vm", "resume", asFetch(fetchFn));
    expect(fetchFn.mock.calls.map((c) => String(c[0]).split("/").pop())).toEqual([
      "suspend",
      "resume",
    ]);
  });
});

describe("scope and polling", () => {
  it("filters zonal, regional and operation scopes", () => {
    expect(inZoneScope("us-central1-a", "", "")).toBe(true);
    expect(inZoneScope("us-central1-a", "us-central1-a", "us-central1")).toBe(true);
    expect(inZoneScope("us-central1-b", "us-central1-a", "us-central1")).toBe(false);
    expect(inZoneScope("us-central1-b", "", "us-central1")).toBe(true);
    expect(inZoneScope("us-east1-b", "", "us-central1")).toBe(false);
    expect(inRegionScope("global", "us-east1")).toBe(true);
    expect(inRegionScope("us-west1", "us-east1")).toBe(false);
    expect(operationInScope("global", "z-a", "")).toBe(true);
    expect(operationInScope("us-central1-a", "", "us-central1")).toBe(true);
    expect(operationInScope("us-central1", "us-central1-a", "us-central1")).toBe(true);
    expect(operationInScope("europe-west1-b", "", "us-central1")).toBe(false);
  });

  it("polls only while operations or instances are unsettled, and only so long", () => {
    expect(operationRunning({ status: "RUNNING" })).toBe(true);
    expect(operationRunning({ status: "PENDING" })).toBe(true);
    expect(operationRunning({ status: "DONE" })).toBe(false);
    expect(instanceTransitioning({ status: "STOPPING" })).toBe(true);
    expect(instanceTransitioning({ status: "RUNNING" })).toBe(false);
    expect(shouldPoll(true, 0)).toBe(true);
    expect(shouldPoll(false, 0)).toBe(false);
    expect(shouldPoll(true, POLL_MAX)).toBe(false);
  });
});
