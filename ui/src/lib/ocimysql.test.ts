import { describe, expect, it, vi } from "vitest";

import {
  MYSQL_PROXY_BASE,
  allowedActions,
  createBackup,
  deleteSystem,
  getSystem,
  listBackups,
  listCompartments,
  listConfigurations,
  listSystems,
  mysqlEndpoint,
  parseBackup,
  parseConfiguration,
  parseSystem,
  systemAction,
} from "./ocimysql";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

const system = {
  id: "s1",
  displayName: "orders",
  lifecycleState: "ACTIVE",
  mysqlVersion: "9.4.0",
  shapeName: "MySQL.2",
  isHighlyAvailable: true,
  endpoints: [{ ipAddress: "10.0.1.5", port: 3306, portX: 33060 }],
  dataStorage: {
    dataStorageSizeInGBs: 50,
    maxStorageSizeInGBs: 100,
    isAutoExpandStorageEnabled: true,
  },
  backupPolicy: {
    isEnabled: true,
    retentionInDays: 7,
    windowStartTime: "03:00",
    pitrPolicy: { isEnabled: true },
  },
  deletionPolicy: { isDeleteProtected: true, finalBackup: "SKIP_FINAL_BACKUP" },
  adminPassword: "must-not-surface",
};

describe("parsing", () => {
  it("parses a full DB system and never carries the password", () => {
    const s = parseSystem(system);
    expect(s).toMatchObject({
      id: "s1",
      name: "orders",
      state: "ACTIVE",
      version: "9.4.0",
      shape: "MySQL.2",
      ip: "10.0.1.5",
      port: 3306,
      portX: 33060,
      storageGb: 50,
      maxStorageGb: 100,
      autoExpand: true,
      highlyAvailable: true,
      backupEnabled: true,
      backupRetentionDays: 7,
      backupWindow: "03:00",
      pitr: true,
      deleteProtected: true,
    });
    expect(JSON.stringify(s)).not.toContain("must-not-surface");
  });

  it("parses a summary without storage or endpoint details", () => {
    const s = parseSystem({ id: "s2", lifecycleState: "CREATING" });
    expect(s).toMatchObject({ name: "s2", ip: "", storageGb: 0, deleteProtected: false });
    expect(mysqlEndpoint(s)).toBe("");
  });

  it("parses a backup and a configuration", () => {
    expect(
      parseBackup({
        id: "b1",
        displayName: "nightly",
        lifecycleState: "ACTIVE",
        dbSystemId: "s1",
        backupType: "FULL",
        creationType: "MANUAL",
        backupSizeInGBs: 3,
        retentionInDays: 7,
      }),
    ).toMatchObject({ name: "nightly", type: "FULL", creation: "MANUAL", sizeGb: 3 });
    expect(
      parseConfiguration({ id: "c1", displayName: "std", type: "DEFAULT", shapeName: "MySQL.2" }),
    ).toMatchObject({ name: "std", type: "DEFAULT", shape: "MySQL.2" });
  });

  it("derives the actions each state allows", () => {
    expect(allowedActions({ state: "ACTIVE", deleteProtected: false })).toEqual({
      start: false,
      stop: true,
      restart: true,
      remove: true,
    });
    expect(allowedActions({ state: "INACTIVE", deleteProtected: true })).toMatchObject({
      start: true,
      stop: false,
      remove: false,
    });
    expect(allowedActions({ state: "DELETING", deleteProtected: false }).remove).toBe(false);
  });

  it("formats the endpoint", () => {
    expect(mysqlEndpoint({ ip: "10.0.1.5", port: 3306 })).toBe("10.0.1.5:3306");
  });
});

describe("client", () => {
  it("lists compartments", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ compartments: [comp] })));
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
  });

  it("follows opc-next-page when listing DB systems", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json([system], 200, { "opc-next-page": "p2" }))
      .mockResolvedValueOnce(json([{ id: "s2" }]));
    const out = await listSystems(comp, asFetch(fetchFn));
    expect(out.map((s) => s.id)).toEqual(["s1", "s2"]);
    expect(fetchFn.mock.calls[1]?.[0]).toBe(
      `${MYSQL_PROXY_BASE}/api/dbSystems?compartmentId=${encodeURIComponent(comp)}&page=p2`,
    );
  });

  it("reads one system, its backups and the configurations", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(json(url.includes("/dbSystems/") ? system : [{ id: "x" }])),
    );
    expect((await getSystem("s1", asFetch(fetchFn))).name).toBe("orders");
    expect(await listBackups(comp, "s1", asFetch(fetchFn))).toHaveLength(1);
    expect(await listConfigurations(comp, asFetch(fetchFn))).toHaveLength(1);
    expect(fetchFn.mock.calls[1]?.[0]).toContain("&dbSystemId=s1");
  });

  it("posts lifecycle actions, deletes and creates backups", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    await systemAction("s1", "stop", asFetch(fetchFn));
    await deleteSystem("s1", asFetch(fetchFn));
    await createBackup("s1", "  manual ", asFetch(fetchFn));
    await createBackup("s1", "", asFetch(fetchFn));
    const calls = fetchFn.mock.calls;
    expect(calls[0]?.[0]).toBe(`${MYSQL_PROXY_BASE}/api/dbSystems/s1/actions/stop`);
    expect(calls[0]?.[1]?.method).toBe("POST");
    expect(calls[1]?.[1]?.method).toBe("DELETE");
    expect(JSON.parse(String(calls[2]?.[1]?.body))).toEqual({
      dbSystemId: "s1",
      displayName: "manual",
    });
    expect(JSON.parse(String(calls[3]?.[1]?.body))).toEqual({ dbSystemId: "s1" });
  });

  it("surfaces an error code from the proxy", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ code: "NotAuthorizedOrNotFound", message: "gone" }, 404)),
    );
    await expect(getSystem("s1", asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 404,
      code: "NotAuthorizedOrNotFound",
    });
  });
});
