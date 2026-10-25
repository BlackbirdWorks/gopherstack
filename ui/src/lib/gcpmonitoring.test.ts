import { describe, expect, it, vi } from "vitest";

import {
  MONITORING_PROXY_BASE,
  describeUptimeTarget,
  listMetricDescriptors,
  parseAlertPolicy,
  parseDescriptor,
  parseSeries,
  parseSnooze,
  parseUptimeCheck,
  queryTimeSeries,
  snoozeState,
  typedValue,
} from "./gcpmonitoring";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

describe("values", () => {
  it.each([
    [{ int64Value: "7" }, 7, "7"],
    [{ doubleValue: 1.5 }, 1.5, "1.5"],
    [{ boolValue: true }, 1, "true"],
    [{ stringValue: "ok" }, null, "ok"],
    [{ distributionValue: { count: "3" } }, null, "distribution (count 3)"],
    [undefined, null, ""],
  ])("%j", (raw, value, text) => expect(typedValue(raw)).toEqual({ value, text }));
});

describe("parsers", () => {
  it("parses a descriptor", () => {
    const d = parseDescriptor({
      type: "custom.googleapis.com/q",
      metricKind: "GAUGE",
      valueType: "INT64",
      labels: [{ key: "queue" }],
    });
    expect(d).toMatchObject({ type: "custom.googleapis.com/q", labels: ["queue"] });
  });

  it("sorts series points by time", () => {
    const s = parseSeries({
      metric: { labels: { queue: "a" } },
      resource: { type: "global", labels: { project_id: "p" } },
      points: [
        { interval: { endTime: "2026-01-01T00:02:00Z" }, value: { int64Value: "2" } },
        { interval: { endTime: "2026-01-01T00:01:00Z" }, value: { int64Value: "1" } },
      ],
    });
    expect(s.points.map((p) => p.value)).toEqual([1, 2]);
    expect(s.metricLabels).toEqual({ queue: "a" });
    expect(s.resourceType).toBe("global");
  });

  it("parses alert policies, checks and snoozes", () => {
    const p = parseAlertPolicy({
      name: "projects/p/alertPolicies/9",
      displayName: "deep",
      conditions: [{ displayName: "gt3" }],
      notificationChannels: ["a", "b"],
    });
    expect(p).toMatchObject({ id: "9", enabled: true, conditions: ["gt3"], channels: 2 });
    expect(parseAlertPolicy({ enabled: false }).enabled).toBe(false);

    const c = parseUptimeCheck({
      name: "projects/p/uptimeCheckConfigs/home",
      period: "300s",
      monitoredResource: { labels: { host: "example.com" } },
      httpCheck: { path: "/h", useSsl: true },
    });
    expect(c).toMatchObject({
      target: "https://example.com/h",
      protocol: "HTTP",
      periodSeconds: 300,
    });
    expect(
      describeUptimeTarget({
        monitoredResource: { labels: { host: "x" } },
        tcpCheck: { port: 22 },
      }),
    ).toEqual({
      target: "x:22",
      protocol: "TCP",
    });

    const s = parseSnooze({
      name: "projects/p/snoozes/5",
      interval: { startTime: "2026-01-01T00:00:00Z", endTime: "2026-01-01T01:00:00Z" },
      criteria: { policies: ["projects/p/alertPolicies/9"] },
    });
    expect(s.policies).toEqual(["projects/p/alertPolicies/9"]);
    expect(snoozeState(s, Date.parse("2025-12-31T00:00:00Z"))).toBe("scheduled");
    expect(snoozeState(s, Date.parse("2026-01-01T00:30:00Z"))).toBe("active");
    expect(snoozeState(s, Date.parse("2026-01-02T00:00:00Z"))).toBe("expired");
  });
});

describe("requests", () => {
  it("lists descriptors with a filter", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ metricDescriptors: [{ type: "t" }] })),
    );
    expect((await listMetricDescriptors("p", 'x="y"', f)).map((d) => d.type)).toEqual(["t"]);
    expect(String(f.mock.calls[0][0])).toContain(
      `${MONITORING_PROXY_BASE}/projects/p/metricDescriptors?filter=`,
    );
  });

  it("queries a trailing window", async () => {
    const f = vi.fn((..._a: unknown[]) => Promise.resolve(json({ timeSeries: [{ points: [] }] })));
    const now = new Date("2026-01-01T01:00:00Z");
    expect(await queryTimeSeries("p", "custom.googleapis.com/q", 3600, now, f)).toHaveLength(1);
    const url = new URL(String(f.mock.calls[0][0]), "http://x");
    expect(url.pathname).toBe(`${MONITORING_PROXY_BASE}/projects/p/timeSeries`);
    expect(url.searchParams.get("filter")).toBe('metric.type="custom.googleapis.com/q"');
    expect(url.searchParams.get("interval.startTime")).toBe("2026-01-01T00:00:00.000Z");
    expect(url.searchParams.get("interval.endTime")).toBe("2026-01-01T01:00:00.000Z");
  });

  it("surfaces proxy errors", async () => {
    const f = vi.fn((..._a: unknown[]) =>
      Promise.resolve(json({ error: { message: "bad filter", status: "INVALID_ARGUMENT" } }, 400)),
    );
    await expect(queryTimeSeries("p", "m", 60, new Date(), f)).rejects.toThrow("bad filter");
  });
});
