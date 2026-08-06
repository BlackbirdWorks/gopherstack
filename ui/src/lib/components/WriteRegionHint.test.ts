import { beforeEach, describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/svelte";
import WriteRegionHint from "./WriteRegionHint.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

describe("WriteRegionHint", () => {
  beforeEach(() => {
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders nothing when a specific region is selected", () => {
    setStoredRegion("eu-west-1");
    render(WriteRegionHint);

    expect(screen.queryByTestId("write-region-hint")).not.toBeInTheDocument();
  });

  it("shows the default write target when All is selected", () => {
    setStoredRegion(ALL_REGIONS);
    render(WriteRegionHint);

    expect(screen.getByTestId("write-region-hint")).toHaveTextContent(`using ${DEFAULT_REGION}`);
  });
});
