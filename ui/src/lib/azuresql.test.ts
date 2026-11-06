import { describe, expect, it, vi } from "vitest";

import {
  SQL_PROXY_BASE,
  SQL_QUERY_BASE,
  deleteFirewallRule,
  formatMaxSize,
  listDatabases,
  listServers,
  parseDatabase,
  parseServer,
  putFirewallRule,
  runQuery,
  validateQuery,
  validateRule,
} from "./azuresql";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const ok = (body: unknown) => vi.fn().mockImplementation(() => Promise.resolve(json(body)));
const server = parseServer({
  id: "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Sql/servers/srv",
  name: "srv",
  properties: { state: "Ready", administratorLogin: "admin" },
});

describe("validation", () => {
  it.each([
    ["SELECT 1", ""],
    ["  select * from t", ""],
    ["", "Enter a SELECT statement"],
    ["DROP TABLE t", "Only SELECT statements are allowed"],
    ["SELECT " + "x".repeat(4000), "Query is at most 4000 characters"],
  ])("query %j", (q, want) => expect(validateQuery(q)).toBe(want));

  it.each([
    ["r1", "10.0.0.1", "10.0.0.9", ""],
    ["r 1", "10.0.0.1", "10.0.0.9", "Name: 1-128 letters, digits, . _ -"],
    ["r1", "10.0.0", "10.0.0.9", "Start and end must be IPv4 addresses"],
    ["r1", "10.0.0.1", "300.0.0.1", "Start and end must be IPv4 addresses"],
  ])("rule %j %j %j", (n, s, e, want) => expect(validateRule(n, s, e)).toBe(want));

  it.each([
    [0, "-"],
    [2 * 2 ** 30, "2 GB"],
    [100 * 2 ** 20, "100 MB"],
  ])("size %j", (n, want) => expect(formatMaxSize(n)).toBe(want));
});

describe("parsing", () => {
  it("never carries a password", () => {
    const s = parseServer({
      id: "/x",
      name: "n",
      properties: { administratorLoginPassword: "pw" },
    });
    expect(JSON.stringify(s)).not.toContain("pw");
  });

  it("reads database sku, status and size", () => {
    expect(
      parseDatabase({
        name: "d",
        sku: { name: "S0", tier: "Standard" },
        properties: { status: "Online", maxSizeBytes: 1024 },
      }),
    ).toMatchObject({
      name: "d",
      sku: "S0",
      tier: "Standard",
      status: "Online",
      maxSizeBytes: 1024,
    });
  });
});

describe("client", () => {
  it("lists servers at subscription scope", async () => {
    const f = ok({
      value: [
        { id: "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Sql/servers/b", name: "b" },
        { id: "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Sql/servers/a", name: "a" },
      ],
    });
    const out = await listServers("s", f);
    expect(f.mock.calls[0][0]).toContain(
      `${SQL_PROXY_BASE}/subscriptions/s/providers/Microsoft.Sql/servers?api-version=`,
    );
    expect(out.map((x) => x.name)).toEqual(["a", "b"]);
    expect(out[0].resourceGroup).toBe("rg");
  });

  it("lists databases under the server", async () => {
    const f = ok({ value: [{ name: "d" }] });
    expect(await listDatabases(server, "sub", f)).toHaveLength(1);
    expect(f.mock.calls[0][0]).toContain(
      "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Sql/servers/srv/databases?api-version=",
    );
  });

  it("writes firewall rules with only the fixed fields", async () => {
    const f = ok({});
    await putFirewallRule(server, "sub", "r1", " 10.0.0.1 ", "10.0.0.2", f);
    expect(f.mock.calls[0][1].method).toBe("PUT");
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({
      properties: { startIpAddress: "10.0.0.1", endIpAddress: "10.0.0.2" },
    });
    await deleteFirewallRule(server, "sub", "r1", f);
    expect(f.mock.calls[1][1].method).toBe("DELETE");
  });

  it("posts the query with the server id", async () => {
    const f = ok({ columns: ["a"], rows: [["1"]], truncated: true, maxRows: 500, elapsedMs: 4 });
    const res = await runQuery(server, "master", "SELECT 1", f);
    expect(f.mock.calls[0][0]).toBe(SQL_QUERY_BASE);
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({
      server: server.id,
      database: "master",
      query: "SELECT 1",
    });
    expect(res).toEqual({
      columns: ["a"],
      rows: [["1"]],
      truncated: true,
      maxRows: 500,
      elapsedMs: 4,
    });
  });
});
