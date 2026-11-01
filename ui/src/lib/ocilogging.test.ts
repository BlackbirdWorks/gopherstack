import { describe, expect, it, vi } from "vitest";

import {
  LOGGING_PROXY_BASE,
  cellText,
  defaultQuery,
  getLog,
  listCompartments,
  listLogGroups,
  listLogs,
  parseLog,
  parseLogGroup,
  parseSearch,
  pushTestEntry,
  searchLogs,
  setLogEnabled,
} from "./ocilogging";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

describe("parsing", () => {
  it("parses a log group and a log", () => {
    expect(
      parseLogGroup({ id: "g1", displayName: "apps", lifecycleState: "ACTIVE", description: "d" }),
    ).toMatchObject({ id: "g1", name: "apps", state: "ACTIVE", description: "d" });
    expect(
      parseLog({
        id: "l1",
        logGroupId: "g1",
        displayName: "web",
        logType: "CUSTOM",
        lifecycleState: "ACTIVE",
        isEnabled: true,
        retentionDuration: 30,
      }),
    ).toMatchObject({ name: "web", type: "CUSTOM", enabled: true, retentionDays: 30 });
  });

  it("renders cell values", () => {
    expect(cellText("x")).toBe("x");
    expect(cellText({ a: 1 })).toBe('{"a":1}');
    expect(cellText(null)).toBe("");
  });

  it("builds table columns from the declared fields and formats datetime", () => {
    const r = parseSearch({
      results: [{ data: { datetime: 1_700_000_000_000, logContent: { level: "ERROR" } } }],
      fields: [{ fieldName: "datetime" }, { fieldName: "logContent" }],
      summary: { resultCount: 1 },
    });
    expect(r.columns).toEqual(["datetime", "logContent"]);
    expect(r.rows[0]).toEqual({
      datetime: "2023-11-14T22:13:20.000Z",
      logContent: '{"level":"ERROR"}',
    });
    expect(r.count).toBe(1);
  });

  it("falls back to the union of row keys without declared fields", () => {
    const r = parseSearch({ results: [{ data: { a: 1 } }, { data: { b: "x" } }] }, true);
    expect(r.columns).toEqual(["a", "b"]);
    expect(r.count).toBe(2);
    expect(r.more).toBe(true);
  });

  it.each([
    [["c"], 'search "c" | sort by datetime desc'],
    [["c", "g"], 'search "c/g" | sort by datetime desc'],
    [["c", "g", "l"], 'search "c/g/l" | sort by datetime desc'],
  ])("builds a default query for %j", (args, want) => {
    expect(defaultQuery(args[0] ?? "", args[1], args[2])).toBe(want);
  });
});

describe("client", () => {
  it("lists compartments, groups and logs", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.endsWith("/compartments")
            ? { compartments: [comp] }
            : url.includes("/logs")
              ? [{ id: "l1", displayName: "web" }]
              : [{ id: "g1", displayName: "apps" }],
        ),
      ),
    );
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
    expect((await listLogGroups(comp, asFetch(fetchFn)))[0]?.name).toBe("apps");
    expect((await listLogs("g1", asFetch(fetchFn)))[0]?.name).toBe("web");
    expect(fetchFn.mock.calls[2]?.[0]).toBe(`${LOGGING_PROXY_BASE}/api/logGroups/g1/logs`);
  });

  it("reads one log", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ id: "l1", displayName: "web" })));
    expect((await getLog("g1", "l1", asFetch(fetchFn))).id).toBe("l1");
  });

  it("enables and disables a log with only the flag", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 202 })),
    );
    await setLogEnabled("g1", "l1", false, asFetch(fetchFn));
    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe(`${LOGGING_PROXY_BASE}/api/logGroups/g1/logs/l1`);
    expect(init?.method).toBe("PUT");
    expect(JSON.parse(String(init?.body))).toEqual({ isEnabled: false });
  });

  it("posts a search with the time range and a result limit", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(
        json({ results: [{ data: { logContent: "hi" } }], summary: { resultCount: 1 } }, 200, {
          "opc-next-page": "n",
        }),
      ),
    );
    const start = new Date("2026-01-01T00:00:00Z");
    const end = new Date("2026-01-01T01:00:00Z");
    const r = await searchLogs('search "c"', start, end, asFetch(fetchFn));
    expect(r.more).toBe(true);
    expect(r.rows[0]?.logContent).toBe("hi");
    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe(`${LOGGING_PROXY_BASE}/api/search?limit=100`);
    expect(JSON.parse(String(init?.body))).toEqual({
      searchQuery: 'search "c"',
      timeStart: start.toISOString(),
      timeEnd: end.toISOString(),
    });
  });

  it("reports a search error from the proxy", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ code: "InvalidParameter", message: "bad query" }, 400)),
    );
    await expect(searchLogs("x", new Date(), new Date(), asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 400,
      code: "InvalidParameter",
    });
  });

  it("pushes a test entry as data only", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(new Response(null, { status: 200 })),
    );
    await pushTestEntry("l1", "hello", asFetch(fetchFn));
    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toBe(`${LOGGING_PROXY_BASE}/api/logs/l1/actions/push`);
    expect(JSON.parse(String(init?.body))).toEqual({ data: "hello" });
  });
});
