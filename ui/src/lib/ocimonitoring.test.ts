import { describe, expect, it, vi } from "vitest";

import {
  MONITORING_PROXY_BASE,
  chartPoints,
  getAlarmHistory,
  groupNamespaces,
  listAlarms,
  listCompartments,
  listMetrics,
  parseAlarm,
  parseHistory,
  parseMetric,
  parseSeries,
  seriesLabel,
  summarizeMetrics,
} from "./ocimonitoring";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

describe("parsing", () => {
  it("parses metrics and groups them by namespace", () => {
    const defs = [
      { namespace: "b", name: "z", dimensions: { host: "a" } },
      { namespace: "a", name: "y" },
      { namespace: "b", name: "x" },
      { namespace: "b", name: "x" },
    ].map((m) => parseMetric(m));
    expect(defs[0]?.dimensions).toEqual({ host: "a" });
    expect(groupNamespaces(defs)).toEqual([
      { namespace: "a", metrics: ["y"] },
      { namespace: "b", metrics: ["x", "z"] },
    ]);
  });

  it("parses a series and drops unparsable timestamps", () => {
    const s = parseSeries({
      name: "cpu",
      namespace: "app",
      resolution: "1m",
      dimensions: { host: "a" },
      aggregatedDatapoints: [
        { timestamp: "2026-01-01T00:00:00Z", value: 4 },
        { timestamp: "nope", value: 1 },
      ],
    });
    expect(s.points).toEqual([{ t: Date.parse("2026-01-01T00:00:00Z"), v: 4 }]);
    expect(seriesLabel(s)).toBe("cpu {host=a}");
    expect(seriesLabel({ name: "cpu", dimensions: {} })).toBe("cpu");
  });

  it("merges an alarm with its status", () => {
    const a = parseAlarm(
      { id: "a1", displayName: "hi", severity: "CRITICAL", query: "q", isEnabled: true },
      { status: "FIRING", timestampTriggered: "t" },
    );
    expect(a).toMatchObject({ name: "hi", status: "FIRING", triggered: "t", enabled: true });
    expect(parseAlarm({ id: "a2" }).status).toBe("");
  });

  it("parses a history entry", () => {
    expect(parseHistory({ timestamp: "t", summary: "s", timestampTriggered: "x" })).toEqual({
      timestamp: "t",
      summary: "s",
      triggered: "x",
    });
  });

  it("scales chart points into the box with zero in range", () => {
    expect(chartPoints([], 100, 50)).toBe("");
    const pts = chartPoints(
      [
        { t: 0, v: 0 },
        { t: 10, v: 10 },
      ],
      108,
      58,
    );
    expect(pts).toBe("4.0,54.0 104.0,4.0");
    expect(chartPoints([{ t: 5, v: 3 }], 108, 58)).toMatch(/^4\.0,4\.0$/);
  });
});

describe("client", () => {
  it("lists compartments", async () => {
    const fetchFn = vi.fn(() => Promise.resolve(json({ compartments: [comp] })));
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
  });

  it("pages ListMetrics through POST", async () => {
    const fetchFn = vi
      .fn<(url: string, init?: RequestInit) => Promise<Response>>()
      .mockResolvedValueOnce(json([{ namespace: "a", name: "x" }], 200, { "opc-next-page": "p2" }))
      .mockResolvedValueOnce(json([{ namespace: "a", name: "y" }]));
    const out = await listMetrics(comp, asFetch(fetchFn));
    expect(out.map((m) => m.name)).toEqual(["x", "y"]);
    const base = `${MONITORING_PROXY_BASE}/api/metrics/actions/listMetrics?compartmentId=${encodeURIComponent(comp)}`;
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([base, `${base}&page=p2`]);
    expect(fetchFn.mock.calls[0]?.[1]?.method).toBe("POST");
  });

  it("summarizes with the namespace, query and range ending shortly after now", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) =>
      Promise.resolve(
        json([
          { name: "cpu", aggregatedDatapoints: [{ timestamp: "2026-01-01T00:00:00Z", value: 1 }] },
        ]),
      ),
    );
    const now = new Date("2026-01-01T01:00:00Z");
    const out = await summarizeMetrics(comp, "app", "cpu[1m].max()", 3600, asFetch(fetchFn), now);
    expect(out[0]?.points).toHaveLength(1);
    const [url, init] = fetchFn.mock.calls[0] ?? [];
    expect(url).toContain("/summarizeMetricsData?compartmentId=");
    expect(JSON.parse(String(init?.body))).toEqual({
      namespace: "app",
      query: "cpu[1m].max()",
      startTime: "2026-01-01T00:00:00.000Z",
      endTime: "2026-01-01T01:01:00.000Z",
    });
  });

  it("surfaces an MQL error", async () => {
    const fetchFn = vi.fn(() =>
      Promise.resolve(json({ code: "InvalidParameter", message: "bad MQL" }, 400)),
    );
    await expect(summarizeMetrics(comp, "app", "x", 60, asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 400,
    });
  });

  it("joins alarms with their statuses", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.includes("/alarms/status")
            ? [{ id: "a1", status: "FIRING" }]
            : [
                { id: "a1", displayName: "hi" },
                { id: "a2", displayName: "lo" },
              ],
        ),
      ),
    );
    const out = await listAlarms(comp, asFetch(fetchFn));
    expect(out.map((a) => [a.name, a.status])).toEqual([
      ["hi", "FIRING"],
      ["lo", ""],
    ]);
  });

  it("reads alarm history entries", async () => {
    const fetchFn = vi.fn((_url: string) =>
      Promise.resolve(json({ alarmId: "a1", entries: [{ timestamp: "t", summary: "s" }] })),
    );
    expect(await getAlarmHistory("a1", asFetch(fetchFn))).toHaveLength(1);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${MONITORING_PROXY_BASE}/api/alarms/a1/history`);
  });
});
