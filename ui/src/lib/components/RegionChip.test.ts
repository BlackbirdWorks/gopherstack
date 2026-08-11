import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/svelte";
import RegionChip from "./RegionChip.svelte";

describe("RegionChip", () => {
  it("renders the given region", () => {
    render(RegionChip, { props: { region: "eu-west-1" } });

    expect(screen.getByTestId("region-chip")).toHaveTextContent("eu-west-1");
  });

  it("renders 'global' for a resource with no region, instead of disappearing", () => {
    render(RegionChip, { props: {} });

    const chip = screen.getByTestId("region-chip");
    expect(chip).toHaveTextContent("global");
    expect(chip.title).toContain("Global service");
  });

  it("renders 'global' when region is explicitly null", () => {
    render(RegionChip, { props: { region: null } });

    expect(screen.getByTestId("region-chip")).toHaveTextContent("global");
  });
});
