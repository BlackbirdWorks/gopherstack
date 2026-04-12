import { describe, expect, it } from "vitest";

import { parseSSEData, toConsoleEvent, toSummaryMetricCount } from "./stream";

describe("stream helpers", () => {
  it("parses valid json and ignores empty values", () => {
    expect(parseSSEData("")).toBeNull();
    expect(parseSSEData("   ")).toBeNull();
    expect(parseSSEData('{"a":1}')).toEqual({ a: 1 });
  });

  it("returns null for invalid json", () => {
    expect(parseSSEData("{not-json}")).toBeNull();
  });

  it("counts summary metric keys", () => {
    expect(toSummaryMetricCount(null)).toBe(0);
    expect(toSummaryMetricCount("nope")).toBe(0);
    expect(toSummaryMetricCount({ one: 1, two: 2 })).toBe(2);
  });

  it("maps console payload to normalized event", () => {
    expect(toConsoleEvent(null)).toBeNull();
    expect(toConsoleEvent({ Method: "GET" })).toBeNull();
    expect(toConsoleEvent({ Path: "/" })).toBeNull();

    expect(
      toConsoleEvent({
        Method: "POST",
        Path: "/",
        StatusCode: 200,
        Service: "DynamoDB",
        Timestamp: "2026-01-01T00:00:00Z",
      }),
    ).toEqual({
      method: "POST",
      path: "/",
      statusCode: 200,
      service: "DynamoDB",
      timestamp: "2026-01-01T00:00:00Z",
    });

    expect(
      toConsoleEvent({
        Method: "GET",
        Path: "/health",
        StatusCode: "bad",
      }),
    ).toEqual({
      method: "GET",
      path: "/health",
      statusCode: 0,
      service: "",
      timestamp: "",
    });
  });
});
