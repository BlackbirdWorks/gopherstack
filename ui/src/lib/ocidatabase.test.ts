import { describe, expect, it, vi } from "vitest";

import {
  DB_PROXY_BASE,
  actionEnabled,
  attachmentName,
  createBackup,
  createBody,
  createDatabase,
  databaseAction,
  deleteDatabase,
  downloadWallet,
  getDatabase,
  getEngine,
  isBusy,
  listBackups,
  listDatabases,
  parseAcl,
  parseDetail,
  parseSummary,
  updateBody,
  updateDatabase,
  validAclEntry,
  validateNew,
  validatePassword,
  type NewDatabase,
} from "./ocidatabase";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

const dbRaw = {
  id: "db1",
  displayName: "orders",
  dbName: "ORDERS",
  dbWorkload: "OLTP",
  lifecycleState: "AVAILABLE",
  computeModel: "ECPU",
  computeCount: 4,
  dataStorageSizeInTBs: 2,
  isFreeTier: false,
  isMtlsConnectionRequired: true,
  dbVersion: "23ai",
  timeCreated: "2026-11-01T00:00:00Z",
  whitelistedIps: ["203.0.113.0/24"],
  isAccessControlEnabled: true,
  backupRetentionPeriodInDays: 60,
  connectionStrings: {
    allConnectionStrings: { HIGH: "high-desc" },
    profiles: [
      {
        displayName: "orders_high",
        consumerGroup: "HIGH",
        protocol: "TCPS",
        tlsAuthentication: "MUTUAL",
        sessionMode: "DIRECT",
        value: "high-desc",
      },
    ],
  },
  connectionUrls: { apexUrl: "https://apex.example", ignored: 5 },
};

const newDb: NewDatabase = {
  compartmentId: comp,
  displayName: " orders ",
  dbName: "ORDERS",
  workload: "OLTP",
  computeCount: 2,
  storageTBs: 1,
  freeTier: false,
  mtlsRequired: true,
  adminPassword: "Welcome12345!x",
};

describe("parsing", () => {
  it("parses a summary", () => {
    expect(parseSummary(dbRaw)).toEqual({
      id: "db1",
      name: "orders",
      dbName: "ORDERS",
      workload: "OLTP",
      state: "AVAILABLE",
      details: "",
      computeModel: "ECPU",
      computeCount: 4,
      storageTBs: 2,
      freeTier: false,
      mtlsRequired: true,
      version: "23ai",
      created: "2026-11-01T00:00:00Z",
    });
    expect(parseSummary({ dbName: "X" }).name).toBe("X");
  });

  it("parses connection profiles, urls and access control", () => {
    const d = parseDetail(dbRaw);
    expect(d.profiles).toEqual([
      {
        name: "orders_high",
        consumerGroup: "HIGH",
        protocol: "TCPS",
        tlsAuthentication: "MUTUAL",
        sessionMode: "DIRECT",
        value: "high-desc",
      },
    ]);
    expect(d.connectionStrings).toEqual({ HIGH: "high-desc" });
    expect(d.connectionUrls).toEqual({ apexUrl: "https://apex.example" });
    expect(d).toMatchObject({ acl: ["203.0.113.0/24"], aclEnabled: true, backupRetentionDays: 60 });
    expect(parseDetail({ id: "x" }).profiles).toEqual([]);
  });
});

describe("state helpers", () => {
  it("offers actions by state", () => {
    expect(actionEnabled("stop", "AVAILABLE")).toBe(true);
    expect(actionEnabled("restart", "AVAILABLE")).toBe(true);
    expect(actionEnabled("start", "AVAILABLE")).toBe(false);
    expect(actionEnabled("start", "STOPPED")).toBe(true);
    expect(actionEnabled("stop", "STOPPED")).toBe(false);
    expect(actionEnabled("stop", "PROVISIONING")).toBe(false);
  });

  it("detects busy states", () => {
    expect(isBusy("PROVISIONING")).toBe(true);
    expect(isBusy("STOPPING")).toBe(true);
    expect(isBusy("AVAILABLE")).toBe(false);
    expect(isBusy("STOPPED")).toBe(false);
  });
});

describe("create form", () => {
  it("builds the body from fixed fields", () => {
    expect(createBody(newDb)).toEqual({
      compartmentId: comp,
      dbName: "ORDERS",
      dbWorkload: "OLTP",
      adminPassword: "Welcome12345!x",
      isMtlsConnectionRequired: true,
      isFreeTier: false,
      displayName: "orders",
      computeModel: "ECPU",
      computeCount: 2,
      dataStorageSizeInTBs: 1,
    });
  });

  it("leaves the shape out for Always Free", () => {
    const body = createBody({ ...newDb, freeTier: true, displayName: "" });
    expect(body).not.toHaveProperty("computeCount");
    expect(body).not.toHaveProperty("displayName");
    expect(body.isFreeTier).toBe(true);
  });

  it("validates fields", () => {
    expect(validateNew(newDb)).toBe("");
    expect(validateNew({ ...newDb, dbName: "1bad" })).toMatch(/Database name/);
    expect(validateNew({ ...newDb, dbName: "has space" })).toMatch(/Database name/);
    expect(validateNew({ ...newDb, workload: "XX" })).toMatch(/workload/);
    expect(validateNew({ ...newDb, computeCount: 1 })).toMatch(/ECPU/);
    expect(validateNew({ ...newDb, storageTBs: 0 })).toMatch(/Storage/);
    expect(validateNew({ ...newDb, freeTier: true, computeCount: 0, storageTBs: 0 })).toBe("");
  });

  it("checks the admin password policy", () => {
    expect(validatePassword("Welcome12345")).toBe("");
    expect(validatePassword("short1A")).toMatch(/12 to 30/);
    expect(validatePassword("alllowercase123")).toMatch(/uppercase/);
    expect(validatePassword('Welcome1234"5')).toMatch(/cannot contain/);
  });

  it("sends the body through the proxy", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({}, 202)));
    await createDatabase(newDb, asFetch(fetchMock));
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${DB_PROXY_BASE}/api/autonomousDatabases`);
    expect(init.method).toBe("POST");
  });
});

describe("requests", () => {
  it("lists databases, hiding terminated ones", async () => {
    const fetchMock = vi.fn((_url: string) =>
      Promise.resolve(json([dbRaw, { ...dbRaw, id: "db2", lifecycleState: "TERMINATED" }])),
    );
    const got = await listDatabases(comp, asFetch(fetchMock));
    expect(got.map((d) => d.id)).toEqual(["db1"]);
    expect(fetchMock.mock.calls[0][0]).toBe(
      `${DB_PROXY_BASE}/api/autonomousDatabases?compartmentId=${encodeURIComponent(comp)}`,
    );
  });

  it("gets one database and the engine", async () => {
    expect(
      (await getDatabase("db 1", asFetch(vi.fn(() => Promise.resolve(json(dbRaw)))))).dbName,
    ).toBe("ORDERS");
    expect(await getEngine(asFetch(vi.fn(() => Promise.resolve(json({ engine: "stub" })))))).toBe(
      "stub",
    );
  });

  it("runs actions and delete", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({})));
    await databaseAction("db1", "stop", asFetch(fetchMock));
    expect(fetchMock).toHaveBeenLastCalledWith(
      `${DB_PROXY_BASE}/api/autonomousDatabases/db1/actions/stop`,
      expect.objectContaining({ method: "POST" }),
    );
    await deleteDatabase("db1", asFetch(fetchMock));
    expect(fetchMock).toHaveBeenLastCalledWith(
      `${DB_PROXY_BASE}/api/autonomousDatabases/db1`,
      expect.objectContaining({ method: "DELETE" }),
    );
  });

  it("builds update bodies from the fields set", async () => {
    expect(updateBody({ displayName: " x " })).toEqual({ displayName: "x" });
    expect(updateBody({ acl: ["1.2.3.4"], aclEnabled: true, computeCount: 4 })).toEqual({
      whitelistedIps: ["1.2.3.4"],
      isAccessControlEnabled: true,
      computeCount: 4,
    });
    const fetchMock = vi.fn(() => Promise.resolve(json({})));
    await updateDatabase("db1", { computeCount: 8 }, asFetch(fetchMock));
    expect((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].method).toBe("PUT");
  });

  it("parses the access control list", () => {
    expect(parseAcl("1.1.1.1, 2.2.2.0/24\n3.3.3.3\n")).toEqual([
      "1.1.1.1",
      "2.2.2.0/24",
      "3.3.3.3",
    ]);
    expect(parseAcl("")).toEqual([]);
    expect(validAclEntry("2001:db8::/32")).toBe(true);
    expect(validAclEntry("bad entry")).toBe(false);
  });

  it("lists backups newest first without deleted ones", async () => {
    const fetchMock = vi.fn((_url: string) =>
      Promise.resolve(
        json([
          {
            id: "b1",
            displayName: "old",
            timeStarted: "2026-10-01T00:00:00Z",
            lifecycleState: "ACTIVE",
          },
          {
            id: "b2",
            displayName: "new",
            timeStarted: "2026-11-01T00:00:00Z",
            lifecycleState: "CREATING",
            isAutomatic: true,
          },
          {
            id: "b3",
            displayName: "gone",
            timeStarted: "2026-11-02T00:00:00Z",
            lifecycleState: "DELETED",
          },
        ]),
      ),
    );
    const got = await listBackups(comp, "db1", asFetch(fetchMock));
    expect(got.map((b) => b.id)).toEqual(["b2", "b1"]);
    expect(got[0].automatic).toBe(true);
    expect(fetchMock.mock.calls[0][0]).toContain("autonomousDatabaseId=db1");
  });

  it("creates a manual backup", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({}, 202)));
    await createBackup("db1", " nightly ", asFetch(fetchMock));
    const init = (fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1];
    expect(JSON.parse(init.body as string)).toEqual({
      autonomousDatabaseId: "db1",
      displayName: "nightly",
    });
  });
});

describe("wallet", () => {
  it("reads the attachment file name", () => {
    expect(attachmentName('attachment; filename="Wallet_ORDERS.zip"', "x.zip")).toBe(
      "Wallet_ORDERS.zip",
    );
    expect(attachmentName(null, "wallet.zip")).toBe("wallet.zip");
    expect(attachmentName('attachment; filename="../evil"', "wallet.zip")).toBe("wallet.zip");
  });

  it("posts the password and returns the zip with its name", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        new Response(new Uint8Array([80, 75, 3, 4]), {
          status: 200,
          headers: {
            "Content-Type": "application/zip",
            "Content-Disposition": 'attachment; filename="Wallet_ORDERS.zip"',
          },
        }),
      ),
    );
    const got = await downloadWallet("db1", "Wallet#1234", asFetch(fetchMock));
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${DB_PROXY_BASE}/api/autonomousDatabases/db1/wallet`);
    expect(JSON.parse(init.body as string)).toEqual({ password: "Wallet#1234" });
    expect(got.filename).toBe("Wallet_ORDERS.zip");
    expect(got.blob.size).toBe(4);
  });

  it("raises the service error without echoing the password", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(json({ code: "InvalidParameter", message: "password too weak" }, 400)),
    );
    const err = await downloadWallet("db1", "Wallet#1234", asFetch(fetchMock)).catch(
      (e: unknown) => e,
    );
    expect((err as Error).message).toBe("password too weak");
    expect(JSON.stringify(err)).not.toContain("Wallet#1234");
  });
});
