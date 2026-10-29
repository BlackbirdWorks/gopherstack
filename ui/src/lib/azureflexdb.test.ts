import { describe, expect, it, vi } from "vitest";

import {
  FLEX_PROXY_BASE,
  addDatabase,
  addFirewallRule,
  canAct,
  deleteDatabase,
  deleteFirewallRule,
  isNonDefault,
  listConfigurations,
  listDatabases,
  listFirewallRules,
  listServers,
  parseConfig,
  parseServer,
  powerAction,
  validCharset,
  validChildName,
  validIpv4,
} from "./azureflexdb";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);

const sub = "00000000-0000-0000-0000-000000000000";
const server = (name: string, ns: string, extra: Record<string, unknown> = {}) => ({
  id: `/subscriptions/${sub}/resourceGroups/demo/providers/${ns}/flexibleServers/${name}`,
  name,
  location: "eastus",
  sku: { name: "Standard_B1ms", tier: "Burstable" },
  properties: {
    state: "Ready",
    version: "16",
    fullyQualifiedDomainName: `${name}.postgres.database.azure.com`,
    administratorLogin: "pgadmin",
    storage: { storageSizeGB: 32, iops: 120 },
    highAvailability: { mode: "Disabled", state: "NotEnabled" },
    ...extra,
  },
});
const target = { engine: "postgresql", resourceGroup: "demo", name: "pgone" } as const;
const base = `${FLEX_PROXY_BASE}/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.DBforPostgreSQL/flexibleServers/pgone`;

describe("parsing", () => {
  it("parses a server with the gopherstack endpoint", () => {
    expect(
      parseServer(
        server("pgone", "Microsoft.DBforPostgreSQL", { gopherstackEndpoint: "127.0.0.1:5432" }),
        "postgresql",
      ),
    ).toMatchObject({
      name: "pgone",
      resourceGroup: "demo",
      state: "Ready",
      version: "16",
      sku: "Standard_B1ms",
      tier: "Burstable",
      storageGb: 32,
      haMode: "Disabled",
      fqdn: "pgone.postgres.database.azure.com",
      endpoint: "127.0.0.1:5432",
      login: "pgadmin",
    });
  });

  it("has no password field", () => {
    const s = parseServer(
      server("x", "Microsoft.DBforMySQL", { administratorLoginPassword: "leak" }),
      "mysql",
    );
    expect(JSON.stringify(s)).not.toContain("leak");
  });

  it.each([
    [{ value: "150", defaultValue: "100", source: "user-override" }, true],
    [{ value: "100", defaultValue: "100", source: "system-default" }, false],
    [
      { value: "1", defaultValue: "1", source: "system-default", isConfigPendingRestart: "True" },
      true,
    ],
    [
      { value: "1", defaultValue: "1", source: "system-default", isConfigPendingRestart: false },
      false,
    ],
  ])("non-default %j", (props, want) => {
    expect(isNonDefault(parseConfig({ name: "p", properties: props }))).toBe(want);
  });
});

describe("validation", () => {
  it.each([
    ["10.0.0.1", true],
    ["256.0.0.1", false],
    ["1.2.3", false],
    ["a.b.c.d", false],
  ])("ip %s", (v, want) => expect(validIpv4(v)).toBe(want));

  it.each([
    ["orders", true],
    ["a b", false],
    ["", false],
    ["-x", false],
  ])("name %j", (v, want) => expect(validChildName(v)).toBe(want));

  it.each([
    ["", true],
    ["UTF8", true],
    ["a;b", false],
  ])("charset %j", (v, want) => expect(validCharset(v)).toBe(want));

  it("gates power actions on state", () => {
    expect(canAct("Ready", "stop")).toBe(true);
    expect(canAct("Ready", "start")).toBe(false);
    expect(canAct("Stopped", "start")).toBe(true);
    expect(canAct("Starting", "restart")).toBe(false);
  });
});

describe("requests", () => {
  it("merges both engines and sorts", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ value: [server("zeta", "Microsoft.DBforPostgreSQL")] }))
      .mockResolvedValueOnce(json({ value: [server("alpha", "Microsoft.DBforMySQL")] }));
    const out = await listServers(sub, asFetch(f));
    expect(out.map((s) => `${s.engine}:${s.name}`)).toEqual(["mysql:alpha", "postgresql:zeta"]);
    expect(urlOf(f, 0)).toBe(
      `${FLEX_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.DBforPostgreSQL/flexibleServers?api-version=2025-08-01`,
    );
    expect(urlOf(f, 1)).toContain("Microsoft.DBforMySQL/flexibleServers?api-version=2024-12-30");
  });

  it("keeps one engine's servers when the other fails, and throws when both fail", async () => {
    const one = vi
      .fn()
      .mockResolvedValueOnce(json({ value: [server("a", "Microsoft.DBforPostgreSQL")] }))
      .mockResolvedValueOnce(json({ error: { code: "x", message: "boom" } }, 500));
    expect(await listServers(sub, asFetch(one))).toHaveLength(1);
    const none = vi.fn().mockResolvedValue(json({ error: { code: "x", message: "boom" } }, 500));
    await expect(listServers(sub, asFetch(none))).rejects.toThrow("boom");
  });

  it("lists children", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({
          value: [{ name: "postgres", properties: { charset: "UTF8", collation: "en_US.utf8" } }],
        }),
      )
      .mockResolvedValueOnce(
        json({
          value: [
            {
              name: "office",
              properties: { startIpAddress: "10.0.0.1", endIpAddress: "10.0.0.9" },
            },
          ],
        }),
      )
      .mockResolvedValueOnce(
        json({
          value: [
            {
              name: "max_connections",
              properties: { value: "150", defaultValue: "100", source: "user-override" },
            },
          ],
        }),
      );
    expect((await listDatabases(sub, target, asFetch(f)))[0]).toMatchObject({
      name: "postgres",
      charset: "UTF8",
    });
    expect((await listFirewallRules(sub, target, asFetch(f)))[0]).toMatchObject({
      start: "10.0.0.1",
      end: "10.0.0.9",
    });
    expect((await listConfigurations(sub, target, asFetch(f)))[0]).toMatchObject({
      name: "max_connections",
      value: "150",
    });
    expect(urlOf(f, 1)).toBe(`${base}/firewallRules?api-version=2025-08-01`);
  });

  it.each(["start", "stop", "restart"] as const)("posts %s", async (action) => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({}, 202)));
    await powerAction(sub, target, action, asFetch(f));
    expect(urlOf(f, 0)).toBe(`${base}/${action}?api-version=2025-08-01`);
    expect(initOf(f, 0).method).toBe("POST");
    expect(initOf(f, 0).body).toBeUndefined();
  });

  it("writes databases and firewall rules with fixed bodies", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({})));
    await addDatabase(sub, target, "orders", " UTF8 ", "", asFetch(f));
    expect(initOf(f, 0).method).toBe("PUT");
    expect(JSON.parse(String(initOf(f, 0).body))).toEqual({ properties: { charset: "UTF8" } });
    await deleteDatabase(sub, target, "orders", asFetch(f));
    expect(initOf(f, 1).method).toBe("DELETE");
    await addFirewallRule(sub, target, "office", " 10.0.0.1", "10.0.0.9 ", asFetch(f));
    expect(JSON.parse(String(initOf(f, 2).body))).toEqual({
      properties: { startIpAddress: "10.0.0.1", endIpAddress: "10.0.0.9" },
    });
    await deleteFirewallRule(sub, target, "office", asFetch(f));
    expect(urlOf(f, 3)).toBe(`${base}/firewallRules/office?api-version=2025-08-01`);
  });
});
