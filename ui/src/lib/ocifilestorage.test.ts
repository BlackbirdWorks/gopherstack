import { describe, expect, it, vi } from "vitest";

import {
  FS_PROXY_BASE,
  cloneFromSnapshot,
  createExport,
  createFileSystem,
  createMountTarget,
  createSnapshot,
  deleteExport,
  deleteFileSystem,
  deleteMountTarget,
  deleteSnapshot,
  emptyOption,
  exportBody,
  fileSystemBody,
  formatBytes,
  getExport,
  getNfs,
  getPrivateIp,
  listAvailabilityDomains,
  listExports,
  listFileSystems,
  listMountTargets,
  listPolicies,
  listReplications,
  listSnapshots,
  listSubnets,
  mountTargetBody,
  parseExportDetail,
  parseFileSystem,
  parseMountTarget,
  parsePolicy,
  parseReplication,
  parseSnapshot,
  validateExport,
  validateExportPath,
  validateFileSystem,
  validateMountTarget,
  validateOption,
  validateSnapshotName,
  validateSource,
} from "./ocifilestorage";
import { GcpRestError } from "./gcprest";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const answer = (make: () => Response) => vi.fn(() => Promise.resolve(make()));
const callUrl = (f: { mock: { calls: unknown[] } }, i: number): string =>
  (f.mock.calls[i] as [string])[0];
const comp = "ocid1.compartment.oc1..aaaa";
const ad = "Uocm:IAD-AD-1";

describe("parsing", () => {
  it("parses a cloned file system", () => {
    expect(
      parseFileSystem({
        id: "fs1",
        displayName: "data",
        lifecycleState: "ACTIVE",
        availabilityDomain: ad,
        meteredBytes: 2048,
        isCloneParent: false,
        isHydrated: false,
        cloneAttachStatus: "ATTACHED",
        sourceDetails: { parentFileSystemId: "fs0", sourceSnapshotId: "sn0" },
      }),
    ).toMatchObject({
      id: "fs1",
      name: "data",
      meteredBytes: 2048,
      parentId: "fs0",
      sourceSnapshotId: "sn0",
      cloneStatus: "ATTACHED",
      hydrated: false,
    });
  });

  it("leaves hydration unknown for plain file systems", () => {
    expect(parseFileSystem({ id: "fs1" }).hydrated).toBeUndefined();
  });

  it("parses snapshots, mount targets, policies and replications", () => {
    expect(
      parseSnapshot({
        id: "s1",
        name: "n",
        fileSystemId: "fs1",
        snapshotType: "USER",
        isCloneSource: true,
      }),
    ).toMatchObject({ id: "s1", type: "USER", isCloneSource: true });
    expect(
      parseMountTarget({
        id: "m1",
        displayName: "mt",
        exportSetId: "es1",
        privateIpIds: ["ip1", 3],
        requestedThroughput: 1,
      }),
    ).toMatchObject({ exportSetId: "es1", privateIpIds: ["ip1"], throughput: 1 });
    expect(
      parsePolicy({
        id: "p1",
        displayName: "daily",
        schedules: [{ period: "DAILY", hourOfDay: 3, timeZone: "UTC" }],
      }).schedules,
    ).toEqual(["DAILY · hour 3 · UTC"]);
    expect(
      parseReplication({ id: "r1", sourceId: "a", targetId: "b", replicationInterval: 60 }),
    ).toMatchObject({ sourceId: "a", targetId: "b", intervalMinutes: 60 });
  });

  it("parses export options with the privileged-port default", () => {
    const d = parseExportDetail({
      id: "e1",
      path: "/data",
      exportOptions: [
        { source: "10.0.0.0/16", access: "READ_ONLY", identitySquash: "ALL" },
        {
          source: "10.1.0.1",
          access: "READ_WRITE",
          identitySquash: "NONE",
          requirePrivilegedSourcePort: false,
        },
      ],
    });
    expect(d.options).toEqual([
      { source: "10.0.0.0/16", access: "READ_ONLY", squash: "ALL", privileged: true },
      { source: "10.1.0.1", access: "READ_WRITE", squash: "NONE", privileged: false },
    ]);
  });
});

describe("formatBytes", () => {
  it.each([
    [0, "0 B"],
    [1023, "1023 B"],
    [1024, "1.0 KB"],
    [1536, "1.5 KB"],
    [5 * 1024 * 1024, "5.0 MB"],
    [-5, "0 B"],
  ])("%d -> %s", (n, text) => {
    expect(formatBytes(n)).toBe(text);
  });
});

describe("validation", () => {
  it.each([
    ["10.0.0.1", ""],
    ["10.0.0.0/16", ""],
    ["0.0.0.0/0", ""],
    ["::1", ""],
    ["fd00::/8", ""],
    ["2001:db8::1/128", ""],
    ["", "Source is required"],
    ["10.0.0/16", "Source must be an IP address or CIDR block"],
    ["999.0.0.1", "Source must be an IP address or CIDR block"],
    ["10.0.0.0/33", "Prefix length must be 0 to 32"],
    ["::1/129", "Prefix length must be 0 to 128"],
    ["10.0.0.0/8/8", "Source must be an IP address or CIDR block"],
    ["everyone", "Source must be an IP address or CIDR block"],
    ["1::2::3", "Source must be an IP address or CIDR block"],
  ])("source %j", (source, want) => {
    expect(validateSource(source)).toBe(want);
  });

  it.each([
    ["/data", ""],
    ["/", ""],
    ["/a/b", ""],
    ["", "Path is required"],
    ["data", "Path must start with a slash"],
    ["/data/", "Path must not end with a slash"],
    ["/a//b", "Path elements must not be empty, . or .."],
    ["/a/../b", "Path elements must not be empty, . or .."],
    [`/${"x".repeat(1100)}`, "Path is too long"],
  ])("path %j", (path, want) => {
    expect(validateExportPath(path)).toBe(want);
  });

  it("validates options", () => {
    expect(validateOption(emptyOption())).toBe("");
    expect(validateOption({ ...emptyOption(), access: "ADMIN" })).toBe("Pick an access mode");
    expect(validateOption({ ...emptyOption(), squash: "X" })).toBe("Pick an identity squash");
    expect(validateOption({ ...emptyOption(), source: "no" })).not.toBe("");
  });

  it("validates exports", () => {
    const ok = { exportSetId: "es", fileSystemId: "fs", path: "/d", options: [emptyOption()] };
    expect(validateExport(ok)).toBe("");
    expect(validateExport({ ...ok, exportSetId: "" })).toBe("Pick a mount target");
    expect(validateExport({ ...ok, fileSystemId: "" })).toBe("Pick a file system");
    expect(validateExport({ ...ok, path: "x" })).toBe("Path must start with a slash");
    expect(
      validateExport({ ...ok, options: [emptyOption(), { ...emptyOption(), source: "x" }] }),
    ).toMatch(/^Option 2:/);
    expect(
      validateExport({ ...ok, options: Array.from({ length: 33 }, () => emptyOption()) }),
    ).toBe("At most 32 client options");
  });

  it("validates file systems, mount targets and snapshot names", () => {
    expect(validateFileSystem({ name: "", compartmentId: "", ad, snapshotId: "" })).toBe(
      "Pick a compartment",
    );
    expect(validateFileSystem({ name: "", compartmentId: comp, ad: "", snapshotId: "" })).toBe(
      "Pick an availability domain",
    );
    expect(validateFileSystem({ name: "x", compartmentId: comp, ad, snapshotId: "" })).toBe("");
    expect(validateMountTarget({ name: "", compartmentId: comp, ad, subnetId: "" })).toBe(
      "Pick a subnet",
    );
    expect(validateMountTarget({ name: "", compartmentId: comp, ad, subnetId: "s" })).toBe("");
    expect(validateSnapshotName("")).toBe("Snapshot name is required");
    expect(validateSnapshotName("a/b")).not.toBe("");
    expect(validateSnapshotName(" nightly-1 ")).toBe("");
  });

  it("builds request bodies from fixed fields", () => {
    expect(fileSystemBody({ name: " d ", compartmentId: comp, ad, snapshotId: "sn" })).toEqual({
      compartmentId: comp,
      availabilityDomain: ad,
      displayName: "d",
      sourceSnapshotId: "sn",
    });
    expect(fileSystemBody({ name: "", compartmentId: comp, ad, snapshotId: "" })).toEqual({
      compartmentId: comp,
      availabilityDomain: ad,
    });
    expect(mountTargetBody({ name: "mt", compartmentId: comp, ad, subnetId: "s" })).toEqual({
      compartmentId: comp,
      availabilityDomain: ad,
      subnetId: "s",
      displayName: "mt",
    });
    expect(
      exportBody({
        exportSetId: "es",
        fileSystemId: "fs",
        path: " /d ",
        options: [
          { source: " 10.0.0.0/8 ", access: "READ_ONLY", squash: "ALL", privileged: false },
        ],
      }),
    ).toEqual({
      exportSetId: "es",
      fileSystemId: "fs",
      path: "/d",
      exportOptions: [
        {
          source: "10.0.0.0/8",
          access: "READ_ONLY",
          identitySquash: "ALL",
          requirePrivilegedSourcePort: false,
        },
      ],
    });
  });
});

describe("requests", () => {
  it("lists file systems for the scope and drops deleted ones", async () => {
    const f = answer(() =>
      json([
        { id: "b", displayName: "beta", lifecycleState: "ACTIVE" },
        { id: "a", displayName: "alpha", lifecycleState: "ACTIVE" },
        { id: "c", displayName: "gone", lifecycleState: "DELETED" },
      ]),
    );
    const out = await listFileSystems(comp, ad, asFetch(f));
    expect(out.map((x) => x.name)).toEqual(["alpha", "beta"]);
    expect(callUrl(f, 0)).toBe(
      `${FS_PROXY_BASE}/api/fileSystems?compartmentId=${encodeURIComponent(comp)}&availabilityDomain=${encodeURIComponent(ad)}`,
    );
  });

  it("follows opc-next-page", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json([{ id: "m1", lifecycleState: "ACTIVE" }], 200, { "opc-next-page": "p2" }),
      )
      .mockResolvedValueOnce(json([{ id: "m2", lifecycleState: "ACTIVE" }]));
    const out = await listMountTargets(comp, ad, asFetch(f));
    expect(out.map((m) => m.id)).toEqual(["m1", "m2"]);
    expect(callUrl(f, 1)).toContain("page=p2");
  });

  it("lists exports by compartment sorted by path", async () => {
    const f = answer(() =>
      json([
        { id: "2", path: "/z", lifecycleState: "ACTIVE" },
        { id: "1", path: "/a", lifecycleState: "ACTIVE" },
      ]),
    );
    expect((await listExports(comp, asFetch(f))).map((e) => e.path)).toEqual(["/a", "/z"]);
    expect(callUrl(f, 0)).toBe(
      `${FS_PROXY_BASE}/api/exports?compartmentId=${encodeURIComponent(comp)}`,
    );
  });

  it("reads an export with its options", async () => {
    const f = answer(() =>
      json({
        id: "e1",
        path: "/d",
        exportOptions: [{ source: "1.2.3.4", access: "READ_WRITE", identitySquash: "ROOT" }],
      }),
    );
    expect((await getExport("e1", asFetch(f))).options).toHaveLength(1);
    expect(callUrl(f, 0)).toBe(`${FS_PROXY_BASE}/api/exports/e1`);
  });

  it("lists snapshots newest first", async () => {
    const f = answer(() =>
      json([
        { id: "1", name: "old", timeCreated: "2026-01-01T00:00:00Z", lifecycleState: "ACTIVE" },
        { id: "2", name: "new", timeCreated: "2026-02-01T00:00:00Z", lifecycleState: "ACTIVE" },
        { id: "3", name: "gone", lifecycleState: "DELETED" },
      ]),
    );
    expect((await listSnapshots("fs1", asFetch(f))).map((s) => s.name)).toEqual(["new", "old"]);
    expect(callUrl(f, 0)).toBe(`${FS_PROXY_BASE}/api/snapshots?fileSystemId=fs1`);
  });

  it("lists policies and replications", async () => {
    const f = answer(() => json([{ id: "x", displayName: "n", lifecycleState: "ACTIVE" }]));
    expect(await listPolicies(comp, ad, asFetch(f))).toHaveLength(1);
    expect(await listReplications(comp, ad, asFetch(f))).toHaveLength(1);
    expect(callUrl(f, 0)).toContain("/api/filesystemSnapshotPolicies?");
    expect(callUrl(f, 1)).toContain("/api/replications?");
  });

  it("reads availability domains, subnets, NFS info and private IPs", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ items: [{ name: "AD-2" }, { name: "AD-1" }] }))
      .mockResolvedValueOnce(
        json({ items: [{ id: "s1", displayName: "files", cidrBlock: "10.0.1.0/24", vcnId: "v" }] }),
      )
      .mockResolvedValueOnce(json({ enabled: true, endpoints: { m1: "127.0.0.1:20049" } }))
      .mockResolvedValueOnce(json({ ipAddress: "10.0.1.5", hostnameLabel: "files" }));
    expect(await listAvailabilityDomains(asFetch(f))).toEqual(["AD-1", "AD-2"]);
    expect(await listSubnets(comp, asFetch(f))).toEqual([
      { id: "s1", name: "files", cidr: "10.0.1.0/24", vcnId: "v" },
    ]);
    expect(await getNfs(asFetch(f))).toEqual({
      enabled: true,
      endpoints: { m1: "127.0.0.1:20049" },
    });
    expect(await getPrivateIp("ip 1", asFetch(f))).toEqual({
      address: "10.0.1.5",
      hostname: "files",
    });
    expect(callUrl(f, 3)).toBe(`${FS_PROXY_BASE}/privateips/ip%201`);
  });

  it("sends writes through the proxy", async () => {
    const f = answer(() => new Response(null, { status: 204 }));
    await createFileSystem({ name: "d", compartmentId: comp, ad, snapshotId: "" }, asFetch(f));
    await deleteFileSystem("fs1", asFetch(f));
    await createMountTarget({ name: "", compartmentId: comp, ad, subnetId: "s" }, asFetch(f));
    await deleteMountTarget("m1", asFetch(f));
    await createExport(
      { exportSetId: "es", fileSystemId: "fs", path: "/d", options: [emptyOption()] },
      asFetch(f),
    );
    await deleteExport("e1", asFetch(f));
    await createSnapshot("fs1", " nightly ", asFetch(f));
    await deleteSnapshot("sn1", asFetch(f));
    await cloneFromSnapshot({ id: "sn1" }, "copy", comp, ad, asFetch(f));

    const calls = f.mock.calls as unknown as [string, RequestInit][];
    expect(calls.map(([url, init]) => `${init.method} ${url.replace(FS_PROXY_BASE, "")}`)).toEqual([
      "POST /api/fileSystems",
      "DELETE /api/fileSystems/fs1",
      "POST /api/mountTargets",
      "DELETE /api/mountTargets/m1",
      "POST /api/exports",
      "DELETE /api/exports/e1",
      "POST /api/snapshots",
      "DELETE /api/snapshots/sn1",
      "POST /api/fileSystems",
    ]);
    expect(JSON.parse(calls[6][1].body as string)).toEqual({
      fileSystemId: "fs1",
      name: "nightly",
    });
    expect(JSON.parse(calls[8][1].body as string)).toMatchObject({
      sourceSnapshotId: "sn1",
      displayName: "copy",
    });
  });

  it("surfaces proxy errors", async () => {
    const f = answer(() =>
      json({ error: { code: 409, status: "IncorrectState", message: "busy" } }, 409),
    );
    await expect(deleteFileSystem("fs1", asFetch(f))).rejects.toMatchObject({
      httpStatus: 409,
      code: "IncorrectState",
      message: "busy",
    });
    await expect(deleteFileSystem("fs1", asFetch(f))).rejects.toBeInstanceOf(GcpRestError);
  });
});
