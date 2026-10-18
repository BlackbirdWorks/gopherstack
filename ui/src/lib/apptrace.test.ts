import { describe, expect, it } from "vitest";
import {
  buildQuery,
  buildTraceTree,
  distinctValues,
  emptyFilters,
  formatLatency,
  maxSeq,
  mergeNew,
  statusClass,
  type TraceSpan,
} from "./apptrace";

function span(over: Partial<TraceSpan>): TraceSpan {
  return {
    id: "a",
    traceId: "a",
    kind: "request",
    time: "2026-10-18T00:00:00Z",
    service: "s3",
    operation: "GetObject",
    status: 200,
    latencyMs: 1,
    requestBytes: 0,
    responseBytes: 0,
    seq: 1,
    ...over,
  };
}

describe("buildQuery", () => {
  it.each([
    ["empty", {}, ""],
    ["text", { q: "  bucket " }, "?q=bucket"],
    ["errors", { status: "errors" }, "?errorsOnly=true"],
    ["4xx", { status: "4xx" }, "?minStatus=400&maxStatus=499"],
    [
      "combo",
      { service: "sqs", kind: "internal", region: "eu-west-1" },
      "?service=sqs&region=eu-west-1&kind=internal",
    ],
  ] as const)("%s", (_n, f, want) => {
    expect(buildQuery({ ...emptyFilters, ...f })).toBe(want);
  });

  it("appends paging params", () => {
    expect(buildQuery(emptyFilters, { limit: 50, afterSeq: 7 })).toBe("?limit=50&afterSeq=7");
  });
});

describe("statusClass and formatLatency", () => {
  it.each([
    [200, "ok"],
    [301, "redirect"],
    [404, "client"],
    [503, "server"],
  ] as const)("status %i", (code, want) => {
    expect(statusClass(code)).toBe(want);
  });

  it.each([
    [0.25, "250 µs"],
    [3.14, "3.1 ms"],
    [42.4, "42 ms"],
    [2500, "2.50 s"],
    [Number.NaN, "—"],
  ])("latency %f", (ms, want) => {
    expect(formatLatency(ms)).toBe(want);
  });
});

describe("mergeNew", () => {
  it("dedups, orders newest first and caps", () => {
    const existing = [span({ id: "b", seq: 2 }), span({ id: "a", seq: 1 })];
    const incoming = [span({ id: "c", seq: 3 }), span({ id: "b", seq: 2 })];
    const merged = mergeNew(existing, incoming, 2);
    expect(merged.map((s) => s.id)).toEqual(["c", "b"]);
    expect(maxSeq(merged)).toBe(3);
  });

  it("returns existing when nothing new", () => {
    const existing = [span({})];
    expect(mergeNew(existing, [], 10)).toBe(existing);
  });
});

describe("distinctValues", () => {
  it("returns sorted unique non-empty values", () => {
    const spans = [span({ service: "sqs" }), span({ service: "s3" }), span({ service: "sqs" })];
    expect(distinctValues(spans, "service")).toEqual(["s3", "sqs"]);
    expect(distinctValues(spans, "region")).toEqual([]);
  });
});

describe("buildTraceTree", () => {
  it("nests children and treats evicted parents as roots", () => {
    const spans = [
      span({ id: "req", seq: 1 }),
      span({ id: "req.2", parentId: "req.1", seq: 3, kind: "internal" }),
      span({ id: "req.1", parentId: "req", seq: 2, kind: "internal" }),
      span({ id: "orphan", parentId: "gone", seq: 4, kind: "internal" }),
    ];
    const rows = buildTraceTree(spans).map((r) => [r.span.id, r.depth]);
    expect(rows).toEqual([
      ["req", 0],
      ["req.1", 1],
      ["req.2", 2],
      ["orphan", 0],
    ]);
  });
});
