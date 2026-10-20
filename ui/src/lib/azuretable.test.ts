import { describe, expect, it } from "vitest";

import {
  TableError,
  buildEntityBody,
  buildFilter,
  continuationFrom,
  createTable,
  deleteEntity,
  deleteTable,
  encodeValue,
  entityToDrafts,
  entityUrl,
  escapeOData,
  insertEntity,
  listTables,
  odataLiteral,
  parseEntity,
  propertyColumns,
  queryEntities,
  queryString,
  tableItemUrl,
  tableUrl,
  updateEntity,
  validateKey,
  validatePropertyName,
  validateTableName,
  validateTypedValue,
  type EdmType,
} from "./azuretable";

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
    [tableUrl("acct"), "/dashboard/api/v1/azure/table/acct"],
    [tableUrl("acct", "Tables"), "/dashboard/api/v1/azure/table/acct/Tables"],
    [tableUrl("acct", "t1", "$top=5"), "/dashboard/api/v1/azure/table/acct/t1?$top=5"],
    [tableItemUrl("acct", "t1"), "/dashboard/api/v1/azure/table/acct/Tables('t1')"],
    [
      entityUrl("acct", "t1", "p", "r"),
      "/dashboard/api/v1/azure/table/acct/t1(PartitionKey%3D'p'%2CRowKey%3D'r')",
    ],
    [
      entityUrl("acct", "t1", "o'b", "r"),
      "/dashboard/api/v1/azure/table/acct/t1(PartitionKey%3D'o''b'%2CRowKey%3D'r')",
    ],
  ])("%s", (got, want) => {
    expect(got).toBe(want);
  });
});

describe("validation", () => {
  it.each([
    ["", "Name is required"],
    ["ab", "Must be 3-63 characters"],
    ["1abc", "Must start with a letter"],
    ["my-table", "Use letters and numbers only"],
    ["Tables", '"tables" is a reserved name'],
    ["abc", null],
    ["MyTable01", null],
  ])("table name %j", (name, want) => {
    expect(validateTableName(name)).toBe(want);
  });

  it.each([
    ["Age", null],
    ["_x1", null],
    ["", "Property name is required"],
    ["1a", "Use letters, numbers and underscores, starting with a letter or underscore"],
    ["PartitionKey", "PartitionKey is a system property"],
  ])("property name %j", (name, want) => {
    expect(validatePropertyName(name)).toBe(want);
  });

  it("rejects forbidden key characters", () => {
    expect(validateKey("PartitionKey", "a/b")).toContain("cannot contain");
    expect(validateKey("PartitionKey", "ok key")).toBeNull();
    expect(validateKey("RowKey", "x".repeat(513))).toContain("512");
  });

  it.each<[EdmType, string, boolean]>([
    ["Edm.String", "anything", true],
    ["Edm.Int32", "2147483647", true],
    ["Edm.Int32", "2147483648", false],
    ["Edm.Int32", "1.5", false],
    ["Edm.Int64", "9223372036854775807", true],
    ["Edm.Int64", "9223372036854775808", false],
    ["Edm.Int64", "abc", false],
    ["Edm.Double", "3.14", true],
    ["Edm.Double", "1e10", true],
    ["Edm.Double", "NaN", false],
    ["Edm.Double", "", false],
    ["Edm.Boolean", "true", true],
    ["Edm.Boolean", "yes", false],
    ["Edm.DateTime", "2024-01-02T03:04:05Z", true],
    ["Edm.DateTime", "not a date", false],
    ["Edm.Guid", "550e8400-e29b-41d4-a716-446655440000", true],
    ["Edm.Guid", "550e8400", false],
    ["Edm.Binary", "aGk=", true],
    ["Edm.Binary", "aGk", false],
  ])("%s %j valid=%s", (type, value, ok) => {
    expect(validateTypedValue(type, value) === null).toBe(ok);
  });
});

describe("encodeValue", () => {
  it.each<[EdmType, string, unknown, EdmType | undefined]>([
    ["Edm.String", " hi ", " hi ", undefined],
    ["Edm.Int32", "42", 42, undefined],
    ["Edm.Boolean", "false", false, undefined],
    ["Edm.Double", "3", 3, "Edm.Double"],
    ["Edm.Int64", "9007199254740993", "9007199254740993", "Edm.Int64"],
    ["Edm.DateTime", "2024-01-02T03:04:05Z", "2024-01-02T03:04:05.000Z", "Edm.DateTime"],
    [
      "Edm.Guid",
      " 550e8400-e29b-41d4-a716-446655440000 ",
      "550e8400-e29b-41d4-a716-446655440000",
      "Edm.Guid",
    ],
    ["Edm.Binary", "aGk=", "aGk=", "Edm.Binary"],
  ])("%s", (type, value, want, annotation) => {
    expect(encodeValue(type, value)).toEqual(
      annotation ? { value: want, annotation } : { value: want },
    );
  });

  it("builds a body with annotations beside typed values", () => {
    expect(
      buildEntityBody("p", "r", [
        { name: "Age", type: "Edm.Int32", value: "3" },
        { name: "Big", type: "Edm.Int64", value: "5" },
      ]),
    ).toEqual({
      PartitionKey: "p",
      RowKey: "r",
      Age: 3,
      Big: "5",
      "Big@odata.type": "Edm.Int64",
    });
  });
});

describe("parseEntity", () => {
  const raw = {
    "odata.metadata": "http://x/$metadata#t/@Element",
    "odata.etag": "W/\"datetime'2024'\"",
    PartitionKey: "p",
    RowKey: "r",
    Timestamp: "2024-01-02T03:04:05Z",
    Name: "bob",
    Age: 30,
    Score: 3.5,
    Active: true,
    Big: "9007199254740993",
    "Big@odata.type": "Edm.Int64",
    When: "2024-01-02T03:04:05Z",
    "When@odata.type": "Edm.DateTime",
    Id: "550e8400-e29b-41d4-a716-446655440000",
    "Id@odata.type": "Edm.Guid",
    Blob: "aGk=",
    "Blob@odata.type": "Edm.Binary",
    Huge: 4000000000,
  };

  it("reads system fields, etag and typed properties", () => {
    const e = parseEntity(raw);
    expect(e.partitionKey).toBe("p");
    expect(e.rowKey).toBe("r");
    expect(e.timestamp).toBe("2024-01-02T03:04:05Z");
    expect(e.etag).toBe(raw["odata.etag"]);
    expect(e.props).toEqual({
      Name: { type: "Edm.String", value: "bob" },
      Age: { type: "Edm.Int32", value: "30" },
      Score: { type: "Edm.Double", value: "3.5" },
      Active: { type: "Edm.Boolean", value: "true" },
      Big: { type: "Edm.Int64", value: "9007199254740993" },
      When: { type: "Edm.DateTime", value: "2024-01-02T03:04:05Z" },
      Id: { type: "Edm.Guid", value: "550e8400-e29b-41d4-a716-446655440000" },
      Blob: { type: "Edm.Binary", value: "aGk=" },
      Huge: { type: "Edm.Double", value: "4000000000" },
    });
  });

  it("collects sorted dynamic columns and draft rows", () => {
    const a = parseEntity({ PartitionKey: "p", RowKey: "1", Zeta: "z" });
    const b = parseEntity({ PartitionKey: "p", RowKey: "2", Alpha: 1 });
    expect(propertyColumns([a, b])).toEqual(["Alpha", "Zeta"]);
    expect(entityToDrafts(b)).toEqual([{ name: "Alpha", type: "Edm.Int32", value: "1" }]);
  });
});

describe("filters", () => {
  it.each<[EdmType, string, string]>([
    ["Edm.String", "O'Brien", "'O''Brien'"],
    ["Edm.Int32", " 7 ", "7"],
    ["Edm.Int64", "9", "9L"],
    ["Edm.Double", "1.5", "1.5"],
    ["Edm.Boolean", "true", "true"],
    ["Edm.DateTime", "2024-01-02T03:04:05Z", "datetime'2024-01-02T03:04:05.000Z'"],
    [
      "Edm.Guid",
      "550e8400-e29b-41d4-a716-446655440000",
      "guid'550e8400-e29b-41d4-a716-446655440000'",
    ],
    ["Edm.Binary", "aGk=", "binary'aGk='"],
  ])("literal %s", (type, value, want) => {
    expect(odataLiteral(type, value)).toBe(want);
  });

  it("joins conditions", () => {
    const conds = [
      { property: "PartitionKey", op: "eq", type: "Edm.String" as const, value: "p" },
      { property: "Age", op: "gt", type: "Edm.Int32" as const, value: "3" },
    ];
    expect(buildFilter(conds)).toBe("PartitionKey eq 'p' and Age gt 3");
    expect(buildFilter(conds, "or")).toBe("PartitionKey eq 'p' or Age gt 3");
  });

  it("escapes quotes", () => {
    expect(escapeOData("a'b")).toBe("a''b");
  });

  it("builds query strings", () => {
    expect(queryString({})).toBe("");
    expect(queryString({ select: ["A", "B C"], top: 2 })).toBe("$select=A%2CB%20C&$top=2");
    expect(queryString({ filter: " Age gt 3 ", top: 10 })).toBe("$filter=Age%20gt%203&$top=10");
    expect(queryString({ top: 5, next: { partitionKey: "a b", rowKey: "r" } })).toBe(
      "$top=5&NextPartitionKey=a%20b&NextRowKey=r",
    );
  });
});

describe("continuation", () => {
  it("is null without a header", () => {
    expect(continuationFrom(new Headers())).toBeNull();
  });
  it("reads both tokens", () => {
    const h = new Headers({
      "x-ms-continuation-NextPartitionKey": "p2",
      "x-ms-continuation-NextRowKey": "r2",
    });
    expect(continuationFrom(h)).toEqual({ partitionKey: "p2", rowKey: "r2" });
  });
});

describe("requests", () => {
  it("lists tables", async () => {
    const { calls, fn } = recorder(() => json({ value: [{ TableName: "a" }, { TableName: "b" }] }));
    expect(await listTables("acct", fn)).toEqual([{ name: "a" }, { name: "b" }]);
    expect(calls[0].url).toBe("/dashboard/api/v1/azure/table/acct/Tables");
  });

  it("creates a table, reporting an existing one", async () => {
    const ok = recorder(() => json({}, 201));
    expect(await createTable("acct", "t1", ok.fn)).toBe(true);
    expect(ok.calls[0].init.method).toBe("POST");
    expect(ok.calls[0].init.body).toBe('{"TableName":"t1"}');

    const dup = recorder(() =>
      json({ "odata.error": { code: "TableAlreadyExists", message: { value: "exists" } } }, 409),
    );
    expect(await createTable("acct", "t1", dup.fn)).toBe(false);
  });

  it("deletes a table", async () => {
    const { calls, fn } = recorder(() => new Response(null, { status: 204 }));
    await deleteTable("acct", "t1", fn);
    expect(calls[0].url).toBe("/dashboard/api/v1/azure/table/acct/Tables('t1')");
    expect(calls[0].init.method).toBe("DELETE");
  });

  it("queries with filter, top and returns the continuation", async () => {
    const { calls, fn } = recorder(() =>
      json({ value: [{ PartitionKey: "p", RowKey: "r", Name: "x" }] }, 200, {
        "x-ms-continuation-NextPartitionKey": "p",
        "x-ms-continuation-NextRowKey": "s",
      }),
    );
    const page = await queryEntities("acct", "t1", { filter: "Age gt 1", top: 1 }, fn);
    expect(calls[0].url).toBe("/dashboard/api/v1/azure/table/acct/t1?$filter=Age%20gt%201&$top=1");
    expect(page.entities).toHaveLength(1);
    expect(page.next).toEqual({ partitionKey: "p", rowKey: "s" });
  });

  it("inserts, merges, replaces and deletes with the right verbs and If-Match", async () => {
    const { calls, fn } = recorder(() => new Response(null, { status: 204 }));
    const props = [{ name: "Age", type: "Edm.Int32" as const, value: "3" }];
    await insertEntity("acct", "t1", "p", "r", props, fn);
    await updateEntity("acct", "t1", "p", "r", props, "merge", 'W/"1"', fn);
    await updateEntity("acct", "t1", "p", "r", props, "replace", "", fn);
    await deleteEntity("acct", "t1", "p", "r", 'W/"1"', fn);
    expect(calls.map((c) => c.init.method)).toEqual(["POST", "PATCH", "PUT", "DELETE"]);
    const hdr = (i: number) => (calls[i].init.headers as Record<string, string>)["If-Match"];
    expect([hdr(1), hdr(2), hdr(3)]).toEqual(['W/"1"', "*", 'W/"1"']);
    expect(JSON.parse(String(calls[0].init.body))).toEqual({
      PartitionKey: "p",
      RowKey: "r",
      Age: 3,
    });
  });

  it("surfaces OData errors", async () => {
    const { fn } = recorder(() =>
      json({ "odata.error": { code: "InvalidInput", message: { value: "bad filter" } } }, 400),
    );
    const err = await queryEntities("acct", "t1", {}, fn).catch((e) => e);
    expect(err).toBeInstanceOf(TableError);
    expect(err).toMatchObject({ status: 400, code: "InvalidInput", message: "bad filter" });
  });

  it("wraps network failures", async () => {
    const fn = (() => Promise.reject(new Error("down"))) as unknown as typeof fetch;
    await expect(listTables("acct", fn)).rejects.toMatchObject({ code: "NetworkError" });
  });
});
