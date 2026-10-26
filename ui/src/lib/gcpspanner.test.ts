import { describe, expect, it, vi } from "vitest";

import {
  SPANNER_PROXY_BASE,
  deleteDatabase,
  getDdl,
  listDatabases,
  listInstances,
  parseDatabase,
  parseInstance,
} from "./gcpspanner";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("parsers", () => {
  it("parses an instance", () => {
    const i = parseInstance({
      name: "projects/p/instances/i1",
      config: "projects/p/instanceConfigs/regional-us-central1",
      displayName: "One",
      nodeCount: 2,
      state: "READY",
    });
    expect(i).toMatchObject({
      id: "i1",
      config: "regional-us-central1",
      nodeCount: 2,
      processingUnits: 0,
      state: "READY",
    });
  });

  it("defaults database fields", () => {
    expect(parseDatabase({ name: "projects/p/instances/i/databases/d" })).toMatchObject({
      id: "d",
      state: "STATE_UNSPECIFIED",
      dialect: "GOOGLE_STANDARD_SQL",
    });
  });
});

describe("requests", () => {
  it("lists instances and databases", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ instances: [{ name: "projects/p/instances/a" }] }))
      .mockResolvedValueOnce(json({ databases: [{ name: "projects/p/instances/a/databases/d" }] }));
    expect((await listInstances("p", f)).map((i) => i.id)).toEqual(["a"]);
    expect((await listDatabases("projects/p/instances/a", f)).map((d) => d.id)).toEqual(["d"]);
    expect(f.mock.calls[1][0]).toBe(`${SPANNER_PROXY_BASE}/projects/p/instances/a/databases`);
  });

  it("reads the schema", async () => {
    const f = vi.fn((..._args: unknown[]) =>
      Promise.resolve(json({ statements: ["CREATE TABLE t (id INT64) PRIMARY KEY (id)"] })),
    );
    expect(await getDdl("projects/p/instances/a/databases/d", f)).toEqual([
      "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
    ]);
    expect(f.mock.calls[0][0]).toBe(`${SPANNER_PROXY_BASE}/projects/p/instances/a/databases/d/ddl`);
  });

  it("returns no statements for an empty schema", async () => {
    expect(
      await getDdl(
        "x",
        vi.fn((..._args: unknown[]) => Promise.resolve(json({}))),
      ),
    ).toEqual([]);
  });

  it("drops a database", async () => {
    const f = vi.fn((..._args: unknown[]) => Promise.resolve(json({})));
    await deleteDatabase("projects/p/instances/a/databases/d", f);
    expect(f.mock.calls[0][1]).toEqual({ method: "DELETE" });
  });
});
