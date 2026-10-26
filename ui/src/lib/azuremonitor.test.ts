import { describe, expect, it, vi } from "vitest";

import {
  MONITOR_PROXY_BASE,
  QUERY_HISTORY_KEY,
  QUERY_HISTORY_MAX,
  QueryError,
  addToHistory,
  formatCell,
  listRules,
  listTables,
  listWorkspaces,
  parseRule,
  parseTable,
  parseWorkspace,
  queryErrorFrom,
  readHistory,
  runQuery,
  writeHistory,
} from "./azuremonitor";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function reply(f: (url: string, init?: RequestInit) => Response) {
  return vi.fn((...a: unknown[]) => Promise.resolve(f(...(a as [string, RequestInit?]))));
}

describe("parsing", () => {
  it("reads workspace customer id, sku and retention", () => {
    const w = parseWorkspace({
      id: "/subscriptions/s/workspaces/ws",
      name: "ws",
      location: "westus",
      properties: { customerId: "cid", sku: { name: "PerGB2018" }, retentionInDays: 45 },
    });
    expect(w).toMatchObject({ name: "ws", customerId: "cid", sku: "PerGB2018", retentionDays: 45 });
  });

  it("tells custom tables from built-in ones and keeps the schema", () => {
    const t = parseTable({
      name: "App_CL",
      properties: {
        plan: "Analytics",
        retentionInDays: 30,
        totalRetentionInDays: 90,
        schema: { tableType: "CustomLog", columns: [{ name: "Level", type: "string" }] },
      },
    });
    expect(t).toMatchObject({ custom: true, plan: "Analytics", totalRetentionDays: 90 });
    expect(t.columns).toEqual([{ name: "Level", type: "string", description: "" }]);
    expect(
      parseTable({ name: "Heartbeat", properties: { schema: { tableType: "Microsoft" } } }).custom,
    ).toBe(false);
  });

  it("reads stream declarations and the transform of each data flow", () => {
    const r = parseRule({
      name: "dcr",
      properties: {
        immutableId: "dcr-1",
        streamDeclarations: { "Custom-X": { columns: [{ name: "a", type: "string" }] } },
        destinations: { logAnalytics: [{ name: "dest" }] },
        dataFlows: [
          {
            streams: ["Custom-X"],
            destinations: ["dest"],
            transformKql: "source | take 1",
            outputStream: "Custom-X_CL",
          },
        ],
      },
    });
    expect(r.streams[0]).toMatchObject({ name: "Custom-X" });
    expect(r.dataFlows[0]).toMatchObject({
      transformKql: "source | take 1",
      outputStream: "Custom-X_CL",
    });
    expect(r.destinations).toEqual(["dest"]);
  });
});

describe("listing", () => {
  it("lists workspaces, tables and rules from the proxy", async () => {
    const f = reply((url) => {
      if (url.endsWith("/tables")) return json({ value: [{ name: "T" }] });
      if (url.endsWith("/rules")) return json({ value: [{ name: "r" }] });
      return json({ value: [{ name: "ws" }] });
    });
    expect((await listWorkspaces(f))[0]?.name).toBe("ws");
    expect((await listTables("a b", f))[0]?.name).toBe("T");
    expect((await listRules(f))[0]?.name).toBe("r");
    expect(f.mock.calls[1]?.[0]).toBe(`${MONITOR_PROXY_BASE}/workspaces/a%20b/tables`);
  });
});

describe("runQuery", () => {
  it("posts the query and parses typed columns", async () => {
    const f = reply(() =>
      json({
        tables: [{ name: "PrimaryResult", columns: [{ name: "x", type: "long" }], rows: [[1]] }],
      }),
    );
    const res = await runQuery("cid", "print x=1", "PT1H", f);
    expect(res.tables[0]?.columns).toEqual([{ name: "x", type: "long" }]);
    expect(res.tables[0]?.rows).toEqual([[1]]);
    const init = f.mock.calls[0]?.[1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({ query: "print x=1", timespan: "PT1H" });
  });

  it("surfaces the BadArgumentError chain", async () => {
    const body = {
      error: {
        code: "BadArgumentError",
        message: "The request had some invalid properties",
        innererror: {
          code: "SemanticError",
          message: "A semantic error occurred.",
          innererror: { code: "SEM0100", message: "'NoTable' is not a table" },
        },
      },
    };
    const err = await runQuery(
      "cid",
      "NoTable",
      "",
      reply(() => json(body, 400)),
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(QueryError);
    const qe = err as QueryError;
    expect(qe).toMatchObject({ httpStatus: 400, code: "BadArgumentError" });
    expect(qe.details).toEqual([
      "SemanticError: A semantic error occurred.",
      "SEM0100: 'NoTable' is not a table",
    ]);
  });

  it("wraps network failures", async () => {
    const f = vi.fn(() => Promise.reject(new Error("down")));
    await expect(runQuery("cid", "q", "", f as unknown as typeof fetch)).rejects.toMatchObject({
      code: "NetworkError",
    });
  });

  it("falls back to the status for a non-JSON error body", () => {
    expect(queryErrorFrom(502, "oops")).toMatchObject({
      httpStatus: 502,
      code: "Error",
      message: "HTTP 502",
    });
  });
});

describe("formatCell", () => {
  it("renders null, objects and scalars", () => {
    expect(formatCell(null)).toBe("");
    expect(formatCell({ a: 1 })).toBe('{"a":1}');
    expect(formatCell(5)).toBe("5");
  });
});

const entry = (q: string) => ({ query: q, timespan: "", at: "t" });

describe("query history", () => {
  it("puts the newest first, dedupes and caps the list", () => {
    let h = addToHistory([], entry("a"));
    h = addToHistory(h, entry("b"));
    h = addToHistory(h, entry("a"));
    expect(h.map((e) => e.query)).toEqual(["a", "b"]);
    for (let i = 0; i < QUERY_HISTORY_MAX + 5; i++) h = addToHistory(h, entry(`q${i}`));
    expect(h).toHaveLength(QUERY_HISTORY_MAX);
  });

  it("round-trips through storage and survives junk", () => {
    const mem = new Map<string, string>();
    const storage = {
      getItem: (k: string) => mem.get(k) ?? null,
      setItem: (k: string, v: string) => void mem.set(k, v),
    };
    writeHistory([entry("a")], storage);
    expect(readHistory(storage)).toEqual([entry("a")]);
    mem.set(QUERY_HISTORY_KEY, "{not json");
    expect(readHistory(storage)).toEqual([]);
    mem.set(QUERY_HISTORY_KEY, JSON.stringify([{ query: 1 }, entry("ok")]));
    expect(readHistory(storage)).toEqual([entry("ok")]);
  });

  it("never throws when storage is unavailable", () => {
    const broken = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    };
    expect(readHistory(broken)).toEqual([]);
    expect(() => writeHistory([entry("a")], broken)).not.toThrow();
  });
});
