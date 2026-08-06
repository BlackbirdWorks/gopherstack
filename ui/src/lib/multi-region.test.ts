import { beforeEach, describe, expect, it, vi } from "vitest";
import { multiRegionList } from "./multi-region";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "./region.svelte";

function mockRegionsWithData(regions: string[] | "404") {
  return vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/dashboard/api/system/regions")) {
      if (regions === "404") {
        return Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) });
      }
      return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({ regions }) });
    }
    return Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) });
  });
}

type FakeItem = { name: string };
type FakeResponse = { Items: FakeItem[] };

function extractItems(res: FakeResponse): FakeItem[] {
  return res.Items;
}

describe("multiRegionList", () => {
  beforeEach(() => {
    setStoredRegion(DEFAULT_REGION);
    vi.unstubAllGlobals();
  });

  it("single-region mode issues exactly one call, against the current region", async () => {
    setStoredRegion("eu-west-1");
    vi.stubGlobal("fetch", mockRegionsWithData(["us-east-1", "eu-west-1", "ap-south-1"]));

    const regionCall = vi.fn((region: string): Promise<FakeResponse> =>
      Promise.resolve({ Items: [{ name: `table-in-${region}` }] }),
    );

    const result = await multiRegionList(regionCall, extractItems);

    expect(regionCall).toHaveBeenCalledTimes(1);
    expect(regionCall).toHaveBeenCalledWith("eu-west-1");
    expect(result.items).toEqual([{ region: "eu-west-1", item: { name: "table-in-eu-west-1" } }]);
    expect(result.errors).toEqual([]);
  });

  it("single-region mode rethrows a rejection exactly as calling regionCall directly would", async () => {
    setStoredRegion("eu-west-1");
    const failure = Object.assign(new Error("Rate exceeded."), { name: "ThrottlingException" });
    const regionCall = vi.fn((): Promise<FakeResponse> => Promise.reject(failure));

    await expect(multiRegionList(regionCall, extractItems)).rejects.toBe(failure);
  });

  it("All mode issues one call per region-with-data, tagging each row with its region", async () => {
    setStoredRegion(ALL_REGIONS);
    vi.stubGlobal("fetch", mockRegionsWithData(["us-east-1", "eu-west-1"]));

    const regionCall = vi.fn((region: string): Promise<FakeResponse> =>
      Promise.resolve({ Items: [{ name: `table-in-${region}` }] }),
    );

    const result = await multiRegionList(regionCall, extractItems);

    expect(regionCall).toHaveBeenCalledTimes(2);
    expect(regionCall).toHaveBeenCalledWith("us-east-1");
    expect(regionCall).toHaveBeenCalledWith("eu-west-1");
    expect(result.items).toEqual(
      expect.arrayContaining([
        { region: "us-east-1", item: { name: "table-in-us-east-1" } },
        { region: "eu-west-1", item: { name: "table-in-eu-west-1" } },
      ]),
    );
    expect(result.items).toHaveLength(2);
    expect(result.errors).toEqual([]);
  });

  it("calls regionCall once per region, i.e. builds a fresh client per region", async () => {
    setStoredRegion(ALL_REGIONS);
    vi.stubGlobal("fetch", mockRegionsWithData(["us-east-1", "eu-west-1"]));

    const seenRegions: string[] = [];
    const regionCall = vi.fn((region: string): Promise<FakeResponse> => {
      seenRegions.push(region);
      return Promise.resolve({ Items: [] });
    });

    await multiRegionList(regionCall, extractItems);

    expect(seenRegions.toSorted()).toEqual(["eu-west-1", "us-east-1"]);
    expect(regionCall).toHaveBeenCalledTimes(2);
  });

  it("an empty regions-with-data list falls back to the default region only", async () => {
    setStoredRegion(ALL_REGIONS);
    vi.stubGlobal("fetch", mockRegionsWithData("404"));

    const regionCall = vi.fn((region: string): Promise<FakeResponse> =>
      Promise.resolve({ Items: [{ name: `table-in-${region}` }] }),
    );

    const result = await multiRegionList(regionCall, extractItems);

    expect(regionCall).toHaveBeenCalledTimes(1);
    expect(regionCall).toHaveBeenCalledWith(DEFAULT_REGION);
    expect(result.items).toEqual([
      { region: DEFAULT_REGION, item: { name: `table-in-${DEFAULT_REGION}` } },
    ]);
  });

  it("collects a per-region error without dropping other regions' results", async () => {
    setStoredRegion(ALL_REGIONS);
    vi.stubGlobal("fetch", mockRegionsWithData(["us-east-1", "eu-west-1"]));

    const failure = new Error("boom");
    const regionCall = vi.fn((region: string): Promise<FakeResponse> =>
      region === "eu-west-1"
        ? Promise.reject(failure)
        : Promise.resolve({ Items: [{ name: "ok" }] }),
    );

    const result = await multiRegionList(regionCall, extractItems);

    expect(result.items).toEqual([{ region: "us-east-1", item: { name: "ok" } }]);
    expect(result.errors).toEqual([{ region: "eu-west-1", error: failure }]);
  });
});
