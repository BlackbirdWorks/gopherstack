import { describe, expect, it } from "vitest";

import {
  CosmosError,
  buildQueryBody,
  cellText,
  collsUrl,
  createContainer,
  createDatabase,
  dbUrl,
  deleteContainer,
  deleteDatabase,
  deleteDocument,
  docsUrl,
  documentColumns,
  editableText,
  listContainers,
  listDatabases,
  newDocumentText,
  pageCount,
  paginate,
  parseContainer,
  parseDocumentText,
  parseParamValue,
  partitionKeyHeader,
  partitionKeyValue,
  queryDocuments,
  referencedParams,
  replaceDocument,
  saveDocument,
  validateContainerId,
  validateDatabaseId,
  validateParam,
  validatePartitionKeyPath,
} from "./azurecosmos";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers });

const recorder = (res: () => Response) => {
  const calls: { url: string; init: RequestInit }[] = [];
  const fn = ((url: string, init: RequestInit) => {
    calls.push({ url, init: init ?? {} });
    return Promise.resolve(res());
  }) as unknown as typeof fetch;
  return { calls, fn };
};

describe("urls", () => {
  it.each([
    [dbUrl(), "/dashboard/api/v1/azure/cosmos/dbs"],
    [dbUrl("a b"), "/dashboard/api/v1/azure/cosmos/dbs/a%20b"],
    [collsUrl("d"), "/dashboard/api/v1/azure/cosmos/dbs/d/colls"],
    [collsUrl("d", "c"), "/dashboard/api/v1/azure/cosmos/dbs/d/colls/c"],
    [docsUrl("d", "c"), "/dashboard/api/v1/azure/cosmos/dbs/d/colls/c/docs"],
    [docsUrl("d", "c", "x y"), "/dashboard/api/v1/azure/cosmos/dbs/d/colls/c/docs/x%20y"],
  ])("%s", (got, want) => {
    expect(got).toBe(want);
  });
});

describe("validation", () => {
  it.each([
    ["ok", null],
    ["", "Database id is required"],
    ["a/b", "Database id cannot contain / \\ # ? or end with a space"],
    ["x".repeat(256), "Database id must be at most 255 characters"],
    ["trail ", "Database id cannot contain / \\ # ? or end with a space"],
  ])("database id %j", (id, want) => {
    expect(validateDatabaseId(id)).toBe(want);
  });

  it("container id uses its own label", () => {
    expect(validateContainerId("")).toBe("Container id is required");
  });

  it.each([
    ["/pk", null],
    ["/a/b", null],
    ["", "Partition key path is required"],
    ["pk", "Must start with /"],
    ["/", "Name a property after the /"],
    ["/a//b", "Path segments cannot be empty"],
    ["/a b", "Path cannot contain whitespace or backslashes"],
  ])("partition key path %j", (p, want) => {
    expect(validatePartitionKeyPath(p)).toBe(want);
  });

  it.each([
    [{ name: "@x", value: "1" }, null],
    [{ name: "x", value: "1" }, "Name must look like @name"],
    [{ name: "@1x", value: "1" }, "Name must look like @name"],
  ])("param %j", (p, want) => {
    expect(validateParam(p)).toBe(want);
  });
});

describe("partition keys", () => {
  it.each([
    [{ pk: "x" }, "/pk", "x"],
    [{ a: { b: 3 } }, "/a/b", 3],
    [{ a: 1 }, "/b", undefined],
    [{ a: [1] }, "/a/0", undefined],
    [{ a: null }, "/a/b", undefined],
  ])("value %j at %s", (doc, path, want) => {
    expect(partitionKeyValue(doc, path)).toEqual(want);
  });

  it.each([
    ["x", '["x"]'],
    [3, "[3]"],
    [true, "[true]"],
    [null, "[null]"],
    [undefined, "[null]"],
    ["é", '["\\u00e9"]'],
  ])("header for %j", (v, want) => {
    expect(partitionKeyHeader(v)).toBe(want);
  });
});

describe("documents", () => {
  it("parses a valid document", () => {
    expect(parseDocumentText('{"id":"a","n":1}')).toEqual({
      error: null,
      doc: { id: "a", n: 1 },
    });
  });

  it.each([
    ["{", "Invalid JSON"],
    ["[1]", "A document must be a JSON object"],
    ["null", "A document must be a JSON object"],
    ["{}", 'The document needs a non-empty string "id"'],
    ['{"id":3}', 'The document needs a non-empty string "id"'],
    ['{"id":""}', 'The document needs a non-empty string "id"'],
  ])("rejects %s", (text, prefix) => {
    const r = parseDocumentText(text);
    expect(r.doc).toBeNull();
    expect(r.error).toContain(prefix);
  });

  it("strips system fields for editing", () => {
    const text = editableText({
      id: "a",
      _rid: "r",
      _etag: "e",
      _ts: 1,
      _self: "s",
      _attachments: "x",
      n: 2,
    });
    expect(JSON.parse(text)).toEqual({ id: "a", n: 2 });
  });

  it("seeds a new document with the partition key field", () => {
    expect(JSON.parse(newDocumentText("/tenant/id"))).toEqual({
      id: "",
      tenant: "",
    });
    expect(JSON.parse(newDocumentText(""))).toEqual({ id: "", pk: "" });
  });

  it("derives grid columns without system fields", () => {
    expect(
      documentColumns([
        { id: "a", z: 1, _ts: 3 },
        { id: "b", a: 1 },
      ]),
    ).toEqual(["id", "a", "z"]);
    expect(documentColumns([])).toEqual(["id"]);
    expect(documentColumns([{ id: "a", c: 1, b: 1, a: 1 }], 2)).toEqual(["id", "a", "b"]);
  });

  it.each([
    [undefined, ""],
    ["s", "s"],
    [3, "3"],
    [{ a: 1 }, '{"a":1}'],
    [null, "null"],
  ])("cell %j", (v, want) => {
    expect(cellText(v)).toBe(want);
  });
});

describe("query building", () => {
  it.each([
    ["5", 5],
    ["true", true],
    ["null", null],
    ['"x"', "x"],
    ["[1,2]", [1, 2]],
    ["abc", "abc"],
    ["  ", ""],
  ])("param value %j", (text, want) => {
    expect(parseParamValue(text)).toEqual(want);
  });

  it("builds the body", () => {
    expect(buildQueryBody("q", [{ name: "@a", value: "1" }])).toEqual({
      query: "q",
      parameters: [{ name: "@a", value: 1 }],
    });
  });

  it("finds referenced params once, in order", () => {
    expect(referencedParams("SELECT * FROM c WHERE c.a = @b AND c.x = @a OR c.y = @b")).toEqual([
      "@b",
      "@a",
    ]);
    expect(referencedParams("SELECT 1")).toEqual([]);
  });
});

describe("paging", () => {
  it("slices and counts pages", () => {
    const items = [1, 2, 3, 4, 5];
    expect(paginate(items, 0, 2)).toEqual([1, 2]);
    expect(paginate(items, 2, 2)).toEqual([5]);
    expect(pageCount(5, 2)).toBe(3);
    expect(pageCount(0, 2)).toBe(1);
  });
});

describe("parseContainer", () => {
  it("reads the partition key and system fields", () => {
    const c = parseContainer({
      id: "c1",
      _rid: "r",
      _etag: '"e"',
      _ts: 7,
      partitionKey: { paths: ["/pk"], kind: "Hash", version: 2 },
      indexingPolicy: { indexingMode: "consistent" },
    });
    expect(c).toMatchObject({
      id: "c1",
      rid: "r",
      etag: '"e"',
      ts: 7,
      partitionKeyPath: "/pk",
      partitionKeyKind: "Hash",
      partitionKeyVersion: 2,
      indexingPolicy: { indexingMode: "consistent" },
    });
  });

  it("tolerates a bare container", () => {
    const c = parseContainer({ id: "c" });
    expect(c.partitionKeyPath).toBe("");
    expect(c.indexingPolicy).toBeNull();
    expect(c.ts).toBeNull();
  });
});

describe("requests", () => {
  it("lists databases and containers", async () => {
    const dbs = recorder(() => json({ Databases: [{ id: "d", _rid: "r" }] }));
    expect(await listDatabases(dbs.fn)).toEqual([{ id: "d", rid: "r" }]);
    const cs = recorder(() =>
      json({
        DocumentCollections: [{ id: "c", partitionKey: { paths: ["/p"] } }],
      }),
    );
    expect((await listContainers("d", cs.fn))[0].partitionKeyPath).toBe("/p");
    expect(cs.calls[0].url).toBe(collsUrl("d"));
  });

  it("creates and deletes databases and containers", async () => {
    const r = recorder(() => new Response("{}", { status: 201 }));
    await createDatabase("d", r.fn);
    await createContainer("d", "c", "/pk", r.fn);
    await deleteContainer("d", "c", r.fn);
    await deleteDatabase("d", r.fn);
    expect(r.calls.map((c) => c.init.method)).toEqual(["POST", "POST", "DELETE", "DELETE"]);
    expect(JSON.parse(r.calls[1].init.body as string)).toEqual({
      id: "c",
      partitionKey: { paths: ["/pk"], kind: "Hash" },
    });
  });

  it("queries with query headers and reports the charge", async () => {
    const r = recorder(() =>
      json({ Documents: [{ id: "a" }] }, 200, { "x-ms-request-charge": "2.5" }),
    );
    const res = await queryDocuments(
      "d",
      "c",
      "SELECT * FROM c WHERE c.n = @n",
      [{ name: "@n", value: "1" }],
      r.fn,
    );
    expect(res).toEqual({ documents: [{ id: "a" }], charge: "2.5" });
    const h = r.calls[0].init.headers as Record<string, string>;
    expect(h["x-ms-documentdb-isquery"]).toBe("true");
    expect(h["Content-Type"]).toBe("application/query+json");
    expect(JSON.parse(r.calls[0].init.body as string).parameters).toEqual([
      { name: "@n", value: 1 },
    ]);
  });

  it("flags upsert only when asked", async () => {
    const r = recorder(() => json({ id: "a" }, 201));
    await saveDocument("d", "c", { id: "a" }, false, r.fn);
    await saveDocument("d", "c", { id: "a" }, true, r.fn);
    expect(
      (r.calls[0].init.headers as Record<string, string>)["x-ms-documentdb-is-upsert"],
    ).toBeUndefined();
    expect((r.calls[1].init.headers as Record<string, string>)["x-ms-documentdb-is-upsert"]).toBe(
      "true",
    );
  });

  it("sends partition key and If-Match on replace and delete", async () => {
    const r = recorder(() => json({ id: "a" }));
    await replaceDocument("d", "c", "a", "x", { id: "a" }, '"e"', r.fn);
    await deleteDocument("d", "c", "a", "x", "", r.fn);
    const rep = r.calls[0].init.headers as Record<string, string>;
    expect(rep["x-ms-documentdb-partitionkey"]).toBe('["x"]');
    expect(rep["If-Match"]).toBe('"e"');
    expect((r.calls[1].init.headers as Record<string, string>)["If-Match"]).toBeUndefined();
    expect(r.calls[0].url).toBe(docsUrl("d", "c", "a"));
  });

  it("maps error bodies and network failures", async () => {
    const bad = recorder(() => json({ code: "Conflict", message: "exists" }, 409));
    await expect(createDatabase("d", bad.fn)).rejects.toMatchObject({
      status: 409,
      code: "Conflict",
      message: "exists",
    });
    const raw = recorder(() => new Response("boom", { status: 500 }));
    await expect(listDatabases(raw.fn)).rejects.toMatchObject({
      code: "HTTP500",
      message: "boom",
    });
    const down = (() => Promise.reject(new Error("offline"))) as unknown as typeof fetch;
    const err = await listDatabases(down).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(CosmosError);
    expect(err).toMatchObject({ status: 0, code: "NetworkError" });
  });
});
