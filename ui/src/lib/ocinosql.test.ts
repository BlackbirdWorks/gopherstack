import { describe, expect, it, vi } from "vitest";

import {
  NOSQL_PROXY_BASE,
  canDrop,
  cellText,
  createTable,
  ddlTableName,
  deleteRow,
  dropTable,
  getRow,
  getTable,
  keyParams,
  listCompartments,
  listIndexes,
  listTables,
  listUsage,
  parseIndex,
  parseTable,
  putRow,
  resultColumns,
  rowsUsable,
  runQuery,
} from "./ocinosql";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";
const q = `compartmentId=${encodeURIComponent(comp)}`;

const table = {
  id: "t1",
  name: "users",
  lifecycleState: "ACTIVE",
  schemaState: "MUTABLE",
  isAutoReclaimable: false,
  ddlStatement: "CREATE TABLE users (id INTEGER, PRIMARY KEY(id))",
  tableLimits: {
    capacityMode: "PROVISIONED",
    maxReadUnits: 10,
    maxWriteUnits: 5,
    maxStorageInGBs: 2,
  },
  schema: {
    columns: [
      { name: "id", type: "INTEGER", isNullable: false },
      { name: "uid", type: "STRING", isAsUuid: true, defaultValue: "x" },
    ],
    primaryKey: ["id"],
    shardKey: ["id"],
    ttl: 3,
  },
};

describe("parsing", () => {
  it("parses a table with schema and limits", () => {
    expect(parseTable(table)).toMatchObject({
      name: "users",
      state: "ACTIVE",
      mode: "PROVISIONED",
      readUnits: 10,
      writeUnits: 5,
      storageGb: 2,
      ttlDays: 3,
      primaryKey: ["id"],
      shardKey: ["id"],
    });
    expect(parseTable(table).columns).toEqual([
      { name: "id", type: "INTEGER", nullable: false, generated: false, defaultValue: "" },
      { name: "uid", type: "STRING", nullable: true, generated: true, defaultValue: "x" },
    ]);
  });

  it("tolerates a summary without schema", () => {
    expect(parseTable({ id: "t2" })).toMatchObject({ name: "t2", columns: [], primaryKey: [] });
  });

  it("parses an index with json paths", () => {
    expect(
      parseIndex({
        name: "ix",
        lifecycleState: "ACTIVE",
        keys: [{ columnName: "name" }, { columnName: "doc", jsonPath: "a.b" }],
      }),
    ).toEqual({ name: "ix", state: "ACTIVE", keys: ["name", "doc.a.b"] });
  });

  it("extracts the table name from DDL", () => {
    expect(ddlTableName("CREATE TABLE IF NOT EXISTS users (id INTEGER)")).toBe("users");
    expect(ddlTableName('  create table "My Tbl"(id integer)')).toBe("My Tbl");
    expect(ddlTableName("DROP TABLE users")).toBe("");
  });

  it("formats results", () => {
    expect(resultColumns([{ a: 1 }, { b: 2, a: 3 }])).toEqual(["a", "b"]);
    expect(cellText(null)).toBe("NULL");
    expect(cellText({ x: 1 })).toBe('{"x":1}');
    expect(cellText(7)).toBe("7");
    expect(keyParams({ id: "1", k: "", n: "a b" })).toBe("key=id%3A1&key=n%3Aa%20b");
  });

  it("derives table actions", () => {
    expect(canDrop({ state: "ACTIVE" })).toBe(true);
    expect(canDrop({ state: "DELETING" })).toBe(false);
    expect(rowsUsable({ state: "CREATING" })).toBe(false);
  });
});

describe("client", () => {
  it("lists compartments", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ compartments: [comp] })));
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
  });

  it("follows opc-next-page when listing tables", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ items: [table] }, 200, { "opc-next-page": "p2" }))
      .mockResolvedValueOnce(json({ items: [{ id: "t2", name: "b" }] }));
    expect((await listTables(comp, asFetch(fetchFn))).map((t) => t.name)).toEqual(["users", "b"]);
    expect(fetchFn.mock.calls[1]?.[0]).toBe(`${NOSQL_PROXY_BASE}/api/tables?${q}&page=p2`);
  });

  it("reads a table, its indexes and usage", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.includes("/indexes")
            ? { items: [{ name: "ix", keys: [] }] }
            : url.includes("/usage")
              ? { items: [{ timeStarted: "t", readUnits: 4, secondsInPeriod: 60 }] }
              : table,
        ),
      ),
    );
    expect((await getTable(comp, "users", asFetch(fetchFn))).ddl).toContain("CREATE TABLE");
    expect(await listIndexes(comp, "users", asFetch(fetchFn))).toHaveLength(1);
    expect((await listUsage(comp, "users", asFetch(fetchFn)))[0]).toMatchObject({ readUnits: 4 });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${NOSQL_PROXY_BASE}/api/tables/users?${q}`);
  });

  it("creates provisioned and on-demand tables and drops one", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    const ddl = "CREATE TABLE t (id INTEGER, PRIMARY KEY(id))";
    await createTable(
      comp,
      ddl,
      { mode: "PROVISIONED", readUnits: 3, writeUnits: 4, storageGb: 1 },
      asFetch(fetchFn),
    );
    await createTable(
      comp,
      ddl,
      { mode: "ON_DEMAND", readUnits: 3, writeUnits: 4, storageGb: 1 },
      asFetch(fetchFn),
    );
    await dropTable(comp, "t", asFetch(fetchFn));
    const calls = fetchFn.mock.calls;
    expect(JSON.parse(String(calls[0]?.[1]?.body))).toEqual({
      compartmentId: comp,
      name: "t",
      ddlStatement: ddl,
      tableLimits: {
        capacityMode: "PROVISIONED",
        maxStorageInGBs: 1,
        maxReadUnits: 3,
        maxWriteUnits: 4,
      },
    });
    expect(JSON.parse(String(calls[1]?.[1]?.body)).tableLimits).toEqual({
      capacityMode: "ON_DEMAND",
      maxStorageInGBs: 1,
    });
    expect(calls[2]?.[1]?.method).toBe("DELETE");
    await expect(
      createTable(
        comp,
        "DROP TABLE t",
        { mode: "ON_DEMAND", readUnits: 0, writeUnits: 0, storageGb: 1 },
        asFetch(fetchFn),
      ),
    ).rejects.toThrow("CREATE TABLE");
  });

  it("runs a query page and hands back the next token", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(
        json({ items: [{ id: 1, name: "ada" }], usage: { readUnitsConsumed: 2 } }, 200, {
          "opc-next-page": "tok",
        }),
      ),
    );
    const res = await runQuery(comp, "SELECT * FROM users", "p1", 5, asFetch(fetchFn));
    expect(res).toEqual({
      rows: [{ id: 1, name: "ada" }],
      columns: ["id", "name"],
      next: "tok",
      readUnits: 2,
    });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${NOSQL_PROXY_BASE}/api/query?limit=5&page=p1`);
    expect(JSON.parse(String(fetchFn.mock.calls[0]?.[1]?.body))).toEqual({
      compartmentId: comp,
      statement: "SELECT * FROM users",
    });
  });

  it("gets, puts and deletes rows", async () => {
    const fetchFn = vi.fn((_url: string, init?: RequestInit) =>
      Promise.resolve(
        json(
          init?.method === "DELETE"
            ? { isSuccess: true }
            : init?.method === "PUT"
              ? { version: "v" }
              : { value: { id: 1 }, usage: { readUnitsConsumed: 1 } },
        ),
      ),
    );
    expect((await getRow(comp, "users", { id: "1" }, asFetch(fetchFn)))?.value).toEqual({ id: 1 });
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${NOSQL_PROXY_BASE}/api/tables/users/rows?${q}&key=id%3A1`,
    );
    await putRow(comp, "users", '{"id":1}', asFetch(fetchFn));
    expect(JSON.parse(String(fetchFn.mock.calls[1]?.[1]?.body))).toEqual({
      compartmentId: comp,
      value: { id: 1 },
    });
    expect(await deleteRow(comp, "users", { id: "1" }, asFetch(fetchFn))).toBe(true);
  });

  it("returns null for a missing row and rejects bad row JSON", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ value: null })));
    expect(await getRow(comp, "users", { id: "9" }, asFetch(fetchFn))).toBeNull();
    await expect(putRow(comp, "users", "{nope", asFetch(fetchFn))).rejects.toThrow("valid JSON");
    await expect(putRow(comp, "users", "[1]", asFetch(fetchFn))).rejects.toThrow("JSON object");
    expect(fetchFn).toHaveBeenCalledTimes(1);
  });

  it("reads the proxy's own error envelope", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(
        json(
          {
            error: {
              message: "invalid NoSQL proxy request body",
              status: "InvalidParameter",
              code: 400,
            },
          },
          400,
        ),
      ),
    );
    await expect(runQuery(comp, "DROP TABLE x", "", 5, asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 400,
      code: "InvalidParameter",
      message: "invalid NoSQL proxy request body",
    });
  });

  it("surfaces an error code from the proxy", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ code: "NotAuthorizedOrNotFound", message: "gone" }, 404)),
    );
    await expect(getTable(comp, "x", asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 404,
      code: "NotAuthorizedOrNotFound",
    });
  });
});
