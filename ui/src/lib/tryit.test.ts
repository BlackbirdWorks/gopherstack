import { describe, expect, it } from "vitest";

import { cleanQuery, parseHeaderLines, readTryResponse } from "./tryit";

describe("tryit helpers", () => {
  it("parses header lines", () => {
    expect(parseHeaderLines("A: b\n\n  C : d: e \nnocolon")).toEqual([
      { name: "A", value: "b" },
      { name: "C", value: "d: e" },
      { name: "nocolon", value: "" },
    ]);
  });

  it("normalises the proxy answer", () => {
    expect(
      readTryResponse({ status: 201, headers: { A: 1 }, body: "x", encoding: "", truncated: true }),
    ).toEqual({ status: 201, headers: { A: "1" }, body: "x", encoding: "utf8", truncated: true });
    expect(readTryResponse({ status: "404" }).status).toBe(404);
    expect(readTryResponse({}).headers).toEqual({});
  });

  it("strips a leading question mark", () => {
    expect(cleanQuery(" ?a=1&b=2 ")).toBe("a=1&b=2");
    expect(cleanQuery("")).toBe("");
  });
});
