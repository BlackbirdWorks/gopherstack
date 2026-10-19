import { describe, expect, it, vi } from "vitest";

import {
  FIRESTORE_PROXY_BASE,
  aggregationBody,
  buildStructuredQuery,
  docsSignature,
  fieldPath,
  fieldsToNodes,
  gridColumns,
  listCollectionIds,
  newFilter,
  newNode,
  newQuerySpec,
  nodesFromJsonText,
  nodesToFields,
  parseDoc,
  resolveFields,
  runAggregation,
  runQuery,
  saveDocument,
  validateFields,
  valueSummary,
  type FsCtx,
} from "./gcpfirestore";

const ctx: FsCtx = { project: "p", database: "(default)" };

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const allTypes = {
  s: { stringValue: "hi" },
  i: { integerValue: "9007199254740993" },
  d: { doubleValue: 1.5 },
  b: { booleanValue: true },
  t: { timestampValue: "2024-01-02T03:04:05Z" },
  g: { geoPointValue: { latitude: 1.5, longitude: -2 } },
  r: { referenceValue: "projects/p/databases/(default)/documents/users/alice" },
  a: { arrayValue: { values: [{ integerValue: "1" }, { stringValue: "x" }] } },
  m: { mapValue: { fields: { k: { nullValue: "NULL_VALUE" } } } },
  n: { nullValue: "NULL_VALUE" },
  y: { bytesValue: "aGk=" },
};

describe("value conversion", () => {
  it("round-trips every value type", () => {
    const nodes = fieldsToNodes(allTypes);
    expect(validateFields(nodes, ctx)).toBeNull();
    expect(nodesToFields(nodes, ctx)).toEqual(allTypes);
  });

  it("expands short references", () => {
    const n = newNode("reference", "ref");
    n.value = "users/bob";
    expect(nodesToFields([n], ctx)).toEqual({
      ref: { referenceValue: "projects/p/databases/(default)/documents/users/bob" },
    });
  });

  it.each([
    ["integer", "1.5", "whole number"],
    ["integer", "99999999999999999999", "64-bit"],
    ["double", "abc", "number"],
    ["timestamp", "yesterday", "RFC 3339"],
    ["reference", "users", "document"],
    ["bytes", "***", "base64"],
    ["boolean", "maybe", "true or false"],
  ] as const)("rejects bad %s %s", (type, value, want) => {
    const n = newNode(type, "f");
    n.value = value;
    expect(validateFields([n], ctx)).toContain(want);
  });

  it("validates geopoint range, names and duplicates", () => {
    const g = newNode("geopoint", "g");
    g.lat = "91";
    g.lng = "0";
    expect(validateFields([g], ctx)).toContain("latitude");
    expect(validateFields([newNode("string", "")], ctx)).toContain("empty");
    expect(validateFields([newNode("string", "a"), newNode("string", "a")], ctx)).toContain(
      "duplicate",
    );
    expect(validateFields([newNode("string", "__x__")], ctx)).toContain("reserved");
  });

  it("serializes special doubles as strings", () => {
    const n = newNode("double", "x");
    n.value = "NaN";
    expect(nodesToFields([n], ctx)).toEqual({ x: { doubleValue: "NaN" } });
  });

  it("parses raw JSON and reports problems", () => {
    expect("nodes" in nodesFromJsonText('{"a":{"stringValue":"x"}}')).toBe(true);
    expect(nodesFromJsonText("{bad")).toHaveProperty("error");
    expect(nodesFromJsonText("[]")).toHaveProperty("error");
    expect(nodesFromJsonText('{"a":{"wat":1}}')).toHaveProperty("error");
    expect(resolveFields("raw", [], '{"a":{"integerValue":"x"}}', ctx)).toHaveProperty("error");
    expect(resolveFields("raw", [], '{"a":{"integerValue":"7"}}', ctx)).toEqual({
      fields: { a: { integerValue: "7" } },
    });
  });
});

describe("helpers", () => {
  it.each([
    ["name", "name"],
    ["a.b", "`a.b`"],
    ["a`b", "`a\\`b`"],
  ])("fieldPath %s", (name, want) => {
    expect(fieldPath(name)).toBe(want);
  });

  it.each([
    [{ stringValue: "x" }, "x"],
    [{ integerValue: "5" }, "5"],
    [{ arrayValue: { values: [{}, {}] } }, "[2 items]"],
    [{ mapValue: { fields: { a: {} } } }, "{1 fields}"],
    [{ geoPointValue: { latitude: 1, longitude: 2 } }, "1, 2"],
    [{ bytesValue: "aGk=" }, "bytes(2)"],
    [{ nullValue: "NULL_VALUE" }, "null"],
    [{ referenceValue: "projects/p/databases/d/documents/a/b" }, "→ a/b"],
  ])("valueSummary %j", (v, want) => {
    expect(valueSummary(v)).toBe(want);
  });

  it("derives columns and signatures", () => {
    const d1 = parseDoc({ name: "x/documents/c/1", fields: { a: {}, b: {} }, updateTime: "t1" });
    const d2 = parseDoc({ name: "x/documents/c/2", fields: { a: {} }, updateTime: "t2" });
    expect(d1.segs).toEqual(["c", "1"]);
    expect(d1.id).toBe("1");
    expect(gridColumns([d1, d2])).toEqual(["a", "b"]);
    expect(gridColumns([d1, d2], 1)).toEqual(["a"]);
    expect(docsSignature([d1, d2])).not.toBe(docsSignature([d1]));
  });
});

describe("query builder", () => {
  it("builds composite filters, ordering, limit and cursors", () => {
    const spec = newQuerySpec();
    const f1 = newFilter();
    Object.assign(f1, { field: "age", op: "GREATER_THAN", type: "integer", value: "30" });
    const f2 = newFilter();
    Object.assign(f2, { field: "city", op: "IN", type: "string", value: "a, b" });
    const f3 = newFilter();
    Object.assign(f3, { field: "gone", op: "IS_NULL" });
    spec.where.items.push(f1, {
      kind: "group",
      id: 99,
      op: "OR",
      items: [f2, f3],
    });
    spec.orderBy.push({ id: 1, field: "age", dir: "DESCENDING" });
    spec.limit = "5";
    spec.startKind = "startAfter";
    spec.startValues = '[30, "x"]';
    spec.endKind = "endBefore";
    spec.endValues = "[40]";
    const r = buildStructuredQuery("users", spec, ctx);
    expect(r).toEqual({
      ok: {
        from: [{ collectionId: "users", allDescendants: false }],
        where: {
          compositeFilter: {
            op: "AND",
            filters: [
              {
                fieldFilter: {
                  field: { fieldPath: "age" },
                  op: "GREATER_THAN",
                  value: { integerValue: "30" },
                },
              },
              {
                compositeFilter: {
                  op: "OR",
                  filters: [
                    {
                      fieldFilter: {
                        field: { fieldPath: "city" },
                        op: "IN",
                        value: {
                          arrayValue: { values: [{ stringValue: "a" }, { stringValue: "b" }] },
                        },
                      },
                    },
                    { unaryFilter: { op: "IS_NULL", field: { fieldPath: "gone" } } },
                  ],
                },
              },
            ],
          },
        },
        orderBy: [{ field: { fieldPath: "age" }, direction: "DESCENDING" }],
        limit: 5,
        startAt: { values: [{ integerValue: "30" }, { stringValue: "x" }], before: false },
        endAt: { values: [{ integerValue: "40" }], before: true },
      },
    });
  });

  it("flattens a single filter and prunes empty groups", () => {
    const spec = newQuerySpec();
    const f = newFilter();
    Object.assign(f, { field: "a", value: "x" });
    spec.where.items.push({ kind: "group", id: 7, op: "OR", items: [] }, f);
    const r = buildStructuredQuery("c", spec, ctx);
    expect(r).toMatchObject({ ok: { where: { fieldFilter: { op: "EQUAL" } } } });
  });

  it.each([
    ["missing field", (s: ReturnType<typeof newQuerySpec>) => s.where.items.push(newFilter())],
    [
      "bad limit",
      (s: ReturnType<typeof newQuerySpec>) => {
        s.limit = "-1";
      },
    ],
    [
      "bad cursor",
      (s: ReturnType<typeof newQuerySpec>) => {
        s.startKind = "startAt";
        s.startValues = "nope";
      },
    ],
  ])("rejects %s", (_name, mutate) => {
    const spec = newQuerySpec();
    mutate(spec);
    expect(buildStructuredQuery("c", spec, ctx)).toHaveProperty("error");
  });

  it("builds aggregation bodies", () => {
    expect(aggregationBody("count", "", { from: [] })).toEqual({
      structuredAggregationQuery: {
        structuredQuery: { from: [] },
        aggregations: [{ alias: "result", count: {} }],
      },
    });
    expect(aggregationBody("sum", " age ", {})).toMatchObject({
      structuredAggregationQuery: {
        aggregations: [{ alias: "result", sum: { field: { fieldPath: "age" } } }],
      },
    });
  });
});

describe("requests", () => {
  it("lists collection ids across pages", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ collectionIds: ["b"], nextPageToken: "t" }))
      .mockResolvedValueOnce(json({ collectionIds: ["a"] }));
    expect(await listCollectionIds(ctx, ["users", "alice"], fetchFn)).toEqual(["a", "b"]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${FIRESTORE_PROXY_BASE}/projects/p/databases/(default)/documents/users/alice:listCollectionIds`,
    );
  });

  it("merge sends one mask path per field plus an exists precondition", async () => {
    const fetchFn = vi.fn().mockResolvedValue(json({ name: "x/documents/c/d" }));
    await saveDocument(ctx, ["c", "d"], { "a.b": {}, n: {} }, "merge", true, fetchFn);
    const url = String(fetchFn.mock.calls[0]?.[0]);
    expect(url).toContain("updateMask.fieldPaths=%60a.b%60");
    expect(url).toContain("updateMask.fieldPaths=n");
    expect(url).toContain("currentDocument.exists=true");
    expect(fetchFn.mock.calls[0]?.[1]).toMatchObject({ method: "PATCH" });
  });

  it("replace sends no mask", async () => {
    const fetchFn = vi.fn().mockResolvedValue(json({ name: "x/documents/c/d" }));
    await saveDocument(ctx, ["c", "d"], { n: {} }, "replace", false, fetchFn);
    expect(String(fetchFn.mock.calls[0]?.[0])).not.toContain("?");
  });

  it("returns only documents from query responses", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json([{ readTime: "t" }, { document: { name: "x/documents/c/1" } }]));
    const docs = await runQuery(ctx, [], { from: [] }, fetchFn);
    expect(docs.map((d) => d.id)).toEqual(["1"]);
  });

  it("formats aggregation results", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(
        json([{ result: { aggregateFields: { result: { integerValue: "3" } } } }]),
      );
    expect(await runAggregation(ctx, [], "count", "", { from: [] }, fetchFn)).toBe("3");
  });
});
