import { beforeEach, describe, expect, it } from "vitest";
import { render, screen, fireEvent } from "@testing-library/svelte";
import RegionPicker from "./RegionPicker.svelte";
import {
  ALL_REGIONS,
  DEFAULT_REGION,
  currentRegionSelection,
  setStoredRegion,
} from "$lib/region.svelte";
import { resetRegionCatalogCache } from "$lib/region-catalog";

describe("RegionPicker", () => {
  beforeEach(() => {
    setStoredRegion(ALL_REGIONS);
    resetRegionCatalogCache();
  });

  it("shows 'All' as the current selection by default", () => {
    render(RegionPicker);

    expect(screen.getByTestId("region-picker-label")).toHaveTextContent("All");
  });

  it("opens to offer an explicit 'All regions' entry", async () => {
    render(RegionPicker);
    await fireEvent.click(screen.getByTitle("Switch region"));

    expect(screen.getByText("All regions")).toBeInTheDocument();
  });

  it("selecting 'All regions' sets the ALL_REGIONS sentinel", async () => {
    setStoredRegion(DEFAULT_REGION);
    render(RegionPicker);
    await fireEvent.click(screen.getByTitle("Switch region"));
    await fireEvent.click(screen.getByText("All regions"));

    expect(currentRegionSelection()).toBe(ALL_REGIONS);
  });

  it("accepts an arbitrary typed region not in the AWS catalog", async () => {
    render(RegionPicker);
    await fireEvent.click(screen.getByTitle("Switch region"));

    const input = screen.getByPlaceholderText("Search or type a region...");
    await fireEvent.input(input, { target: { value: "mars-north-1" } });
    await fireEvent.keyDown(input, { key: "Enter" });

    expect(currentRegionSelection()).toBe("mars-north-1");
  });
});
