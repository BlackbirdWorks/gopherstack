import { describe, expect, it, vi } from "vitest";

import {
  BIGTABLE_PROXY_BASE,
  decodeBytes,
  deleteTable,
  describeGcRule,
  listInstances,
  parseRow,
  parseTable,
  prefixRowSet,
  readRows,
  readRowsBody,
} from "./gcpbigtable";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const b64 = (s: string) => btoa(s);

describe("gc rules", () => {
  it.each([
    [undefined, "keep forever"],
    [{ maxNumVersions: 3 }, "max 3 versions"],
    [{ maxAge: "604800s" }, "max age 604800s"],
    [
      { union: { rules: [{ maxNumVersions: 1 }, { maxAge: "60s" }] } },
      "any of (max 1 versions; max age 60s)",
    ],
    [{ intersection: { rules: [{ maxNumVersions: 1 }] } }, "all of (max 1 versions)"],
  ])("%j", (rule, want) => expect(describeGcRule(rule)).toBe(want));
});

describe("tables", () => {
  it("parses families sorted with their rules", () => {
    const t = parseTable({
      name: "projects/p/instances/i/tables/t",
      columnFamilies: { b: { gcRule: { maxNumVersions: 2 } }, a: {} },
    });
    expect(t.id).toBe("t");
    expect(t.granularity).toBe("MILLIS");
    expect(t.families).toEqual([
      { name: "a", gcRule: "keep forever" },
      { name: "b", gcRule: "max 2 versions" },
    ]);
  });
});

describe("row prefix", () => {
  it("is absent for an empty prefix", () => {
    expect(prefixRowSet("")).toBeUndefined();
    expect(readRowsBody("", 5)).toEqual({ rowsLimit: "5" });
  });

  it("covers keys from the prefix to its successor", () => {
    expect(prefixRowSet("user#")).toEqual({
      rowRanges: [{ startKeyClosed: b64("user#"), endKeyOpen: b64("user$") }],
    });
  });
});

describe("bytes", () => {
  it("decodes text and flags binary", () => {
    expect(decodeBytes(b64("hi"))).toEqual({ text: "hi", binary: false });
    expect(decodeBytes("")).toEqual({ text: "", binary: false });
    expect(decodeBytes(btoa("\u00FF\u00FE"))).toEqual({ text: "0xfffe", binary: true });
  });
});

describe("rows", () => {
  it("groups cells by family and column", () => {
    const row = parseRow({
      key: b64("user#1"),
      families: [
        {
          name: "cf",
          columns: [
            {
              qualifier: b64("name"),
              cells: [
                { timestampMicros: "2000000", value: b64("new") },
                { timestampMicros: "1000000", value: b64("old") },
              ],
            },
          ],
        },
      ],
    });
    expect(row.key).toBe("user#1");
    expect(row.cellCount).toBe(2);
    expect(row.families[0].columns[0].qualifier).toBe("name");
    expect(row.families[0].columns[0].cells.map((c) => c.value)).toEqual(["new", "old"]);
    expect(row.families[0].columns[0].cells[0].timestamp?.getTime()).toBe(2000);
  });

  it("reads rows through the proxy", async () => {
    const f = vi.fn((..._args: unknown[]) =>
      Promise.resolve(json({ rows: [{ key: b64("k"), families: [] }] })),
    );
    const rows = await readRows("projects/p/instances/i/tables/t", "k", 10, f);
    expect(rows.map((r) => r.key)).toEqual(["k"]);
    const [url, init] = f.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(`${BIGTABLE_PROXY_BASE}/projects/p/instances/i/tables/t:readRows`);
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toMatchObject({ rowsLimit: "10" });
  });
});

describe("requests", () => {
  it("lists instances across pages", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({ instances: [{ name: "projects/p/instances/a" }], nextPageToken: "n" }),
      )
      .mockResolvedValueOnce(json({ instances: [{ name: "projects/p/instances/b" }] }));
    const got = await listInstances("p", f);
    expect(got.map((i) => i.id)).toEqual(["a", "b"]);
    expect(f.mock.calls[1][0]).toBe(`${BIGTABLE_PROXY_BASE}/projects/p/instances?pageToken=n`);
  });

  it("deletes a table", async () => {
    const f = vi.fn((..._args: unknown[]) => Promise.resolve(json({})));
    await deleteTable("projects/p/instances/i/tables/t", f);
    expect(f.mock.calls[0]).toEqual([
      `${BIGTABLE_PROXY_BASE}/projects/p/instances/i/tables/t`,
      { method: "DELETE" },
    ]);
  });
});
