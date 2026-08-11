import { beforeEach, describe, expect, it, vi } from "vitest";
import { regionsForFanout } from "./region-data";
import { DEFAULT_REGION } from "./region.svelte";

describe("regionsForFanout", () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns the backend's regions when the endpoint succeeds", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ regions: ["eu-west-1", "us-east-1"] }),
      }),
    );

    await expect(regionsForFanout()).resolves.toEqual(["eu-west-1", "us-east-1"]);
  });

  it("falls back to DEFAULT_REGION when the backend returns an empty list", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ regions: [] }),
      }),
    );

    await expect(regionsForFanout()).resolves.toEqual([DEFAULT_REGION]);
  });

  it("treats a 404 (endpoint not deployed yet) as empty, not an error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: false, status: 404, json: () => Promise.resolve({}) }),
    );

    await expect(regionsForFanout()).resolves.toEqual([DEFAULT_REGION]);
  });

  it("treats a network failure as empty, not an error", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("network down")));

    await expect(regionsForFanout()).resolves.toEqual([DEFAULT_REGION]);
  });

  it("ignores a malformed response body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ regions: "not-an-array" }),
      }),
    );

    await expect(regionsForFanout()).resolves.toEqual([DEFAULT_REGION]);
  });
});
