import { describe, expect, it, vi } from "vitest";

import {
  BQ_PROXY_BASE,
  decodeRows,
  draftsToSchema,
  loadJobBody,
  newDraft,
  parseQueryResult,
  pushHistory,
  renderCell,
  runQuery,
  summarizeQuery,
  typeLabel,
  validateBqId,
  validateDrafts,
  validateGsUri,
  type SchemaField,
} from "./gcpbigquery";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });

const e = (sql: string) => ({ sql, at: 1, ok: true, summary: "" });

const schema: SchemaField[] = [
  { name: "id", type: "INTEGER", mode: "NULLABLE" },
  {
    name: "who",
    type: "RECORD",
    mode: "NULLABLE",
    fields: [
      { name: "name", type: "STRING", mode: "NULLABLE" },
      { name: "tags", type: "STRING", mode: "REPEATED" },
    ],
  },
  { name: "nums", type: "INTEGER", mode: "REPEATED" },
];

describe("validation", () => {
  it.each([
    ["", "Table is required"],
    ["bad-name", "Table: use letters, digits and _ only"],
    ["ok_1", null],
  ])("id %s", (id, want) => {
    expect(validateBqId(id, "Table")).toBe(want);
  });

  it.each([
    ["gs://b/o.csv", null],
    ["gs://b/*.csv", null],
    ["gs://b", "use gs://bucket/object (a * wildcard is allowed)"],
    ["http://x/y", "use gs://bucket/object (a * wildcard is allowed)"],
  ])("gs uri %s", (uri, want) => {
    expect(validateGsUri(uri)).toBe(want);
  });

  it("checks drafts recursively", () => {
    const rec = newDraft("r", "RECORD");
    expect(validateDrafts([rec])).toBe('RECORD "r" needs at least one field');
    rec.children = [newDraft("a"), newDraft("A")];
    expect(validateDrafts([rec])).toBe('duplicate field "A"');
    rec.children = [newDraft("a")];
    expect(validateDrafts([rec])).toBeNull();
    expect(validateDrafts([newDraft("1bad")])).toBe('invalid field name "1bad"');
  });
});

describe("schema", () => {
  it("converts drafts with nested records", () => {
    const rec = newDraft("who", "RECORD", "REPEATED");
    rec.children = [newDraft("n", "INTEGER")];
    expect(draftsToSchema([newDraft(" id ", "INTEGER", "REQUIRED"), rec])).toEqual([
      { name: "id", type: "INTEGER", mode: "REQUIRED" },
      {
        name: "who",
        type: "RECORD",
        mode: "REPEATED",
        fields: [{ name: "n", type: "INTEGER", mode: "NULLABLE" }],
      },
    ]);
  });

  it("labels repeated types", () => {
    expect(typeLabel(schema[2]!)).toBe("ARRAY<INTEGER>");
    expect(typeLabel(schema[1]!)).toBe("RECORD");
  });
});

describe("cells", () => {
  const wire = [
    {
      f: [
        { v: "1" },
        { v: { f: [{ v: "ann" }, { v: [{ v: "x" }, { v: "y" }] }] } },
        { v: [{ v: "7" }, { v: "8" }] },
      ],
    },
    { f: [{ v: null }, { v: null }, { v: [] }] },
  ];

  it("decodes records and arrays", () => {
    expect(decodeRows(schema, wire)).toEqual([
      ["1", { name: "ann", tags: ["x", "y"] }, ["7", "8"]],
      [null, null, []],
    ]);
  });

  it.each([
    [null, "NULL"],
    ["a", "a"],
    [["1", "2"], "[1, 2]"],
    [{ name: "ann", tags: ["x"] }, "{name: ann, tags: [x]}"],
  ])("renders %j", (value, want) => {
    expect(renderCell(value)).toBe(want);
  });
});

describe("queries", () => {
  it("parses a result and summarizes it", () => {
    const r = parseQueryResult(
      {
        schema: { fields: [{ name: "x", type: "INTEGER", mode: "NULLABLE" }] },
        rows: [{ f: [{ v: "1" }] }],
        totalRows: "1",
        jobReference: { jobId: "j1" },
        cacheHit: false,
      },
      12,
    );
    expect(r.rows).toEqual([["1"]]);
    expect(r.jobId).toBe("j1");
    expect(summarizeQuery(r)).toBe("1 row(s)");
    expect(summarizeQuery({ ...r, schema: [], dmlAffected: "3" })).toBe("3 row(s) affected");
  });

  it("posts the default dataset", async () => {
    const f = vi.fn(() => Promise.resolve(json({ jobComplete: true })));
    await runQuery("p", "SELECT 1", "ds", f);
    const [url, init] = f.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${BQ_PROXY_BASE}/projects/p/queries`);
    expect(JSON.parse(init.body as string).defaultDataset).toEqual({
      projectId: "p",
      datasetId: "ds",
    });
  });

  it("surfaces the API error message", async () => {
    const f = vi.fn(() =>
      Promise.resolve(
        json({ error: { message: "Syntax error", status: "INVALID_ARGUMENT" } }, 400),
      ),
    );
    await expect(runQuery("p", "x", "", f)).rejects.toThrow("Syntax error");
  });

  it("dedupes and caps history", () => {
    const out = pushHistory([e("a"), e("b")], e("a"), 2);
    expect(out.map((h) => h.sql)).toEqual(["a", "b"]);
    expect(pushHistory(out, e("c"), 2).map((h) => h.sql)).toEqual(["c", "a"]);
  });
});

describe("load jobs", () => {
  it("builds a CSV load with skip rows", () => {
    const body = loadJobBody("p", {
      uri: " gs://b/o.csv ",
      format: "CSV",
      autodetect: true,
      dataset: "d",
      table: "t",
      skipRows: 1,
      append: false,
    });
    expect(body).toEqual({
      configuration: {
        load: {
          sourceUris: ["gs://b/o.csv"],
          sourceFormat: "CSV",
          autodetect: true,
          destinationTable: { projectId: "p", datasetId: "d", tableId: "t" },
          createDisposition: "CREATE_IF_NEEDED",
          writeDisposition: "WRITE_TRUNCATE",
          skipLeadingRows: 1,
        },
      },
    });
  });
});
