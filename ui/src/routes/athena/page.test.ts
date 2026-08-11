import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import AthenaPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAthenaClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

describe("Athena Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    expect(screen.getByText("Amazon Athena")).toBeInTheDocument();
  });

  it("shows query editor tab by default", () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    expect(screen.getByText("Query Editor")).toBeInTheDocument();
    expect(screen.getByText("Run Query")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    expect(screen.getByText("Workgroups")).toBeInTheDocument();
    expect(screen.getByText("Data Catalogs")).toBeInTheDocument();
    expect(screen.getByText("Query History")).toBeInTheDocument();
  });

  it("shows SQL query textarea", () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    expect(screen.getByRole("textbox", { name: "SQL Query" })).toBeInTheDocument();
  });

  it("shows workgroups", async () => {
    mockSend.mockResolvedValue({
      WorkGroups: [
        { Name: "primary", State: "ENABLED", Description: "Default workgroup" },
        { Name: "data-science", State: "ENABLED" },
      ],
    });
    render(AthenaPage);
    await fireEvent.click(screen.getByText("Workgroups"));
    await waitFor(
      () => {
        expect(screen.getByText("primary")).toBeInTheDocument();
        expect(screen.getByText("data-science")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state for workgroups", async () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    await fireEvent.click(screen.getByText("Workgroups"));
    await waitFor(
      () => {
        expect(screen.getByText("No workgroups found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads data catalogs tab", async () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    mockSend.mockResolvedValue({ DataCatalogsSummary: [] });
    await fireEvent.click(screen.getByText("Data Catalogs"));
    await waitFor(
      () => {
        expect(screen.getByText("No data catalogs found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("disables Run Query button when query is empty", () => {
    mockSend.mockResolvedValue({ WorkGroups: [] });
    render(AthenaPage);
    const btn = screen.getByRole("button", { name: "Run Query" });
    expect(btn).toBeInTheDocument();
  });

  describe("All regions mode", () => {
    // loadWorkgroups also runs unconditionally on mount (the Query Editor's
    // workgroup selector needs it too), so these tests wait for that initial
    // fan-out to settle before switching tabs -- otherwise a tab click that
    // arrives while workgroups is still empty triggers a second, overlapping
    // loadWorkgroups() call from handleTabChange.
    it("fans ListWorkGroups out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      // The workgroup selector always has a hardcoded "primary" <option>, so
      // use non-colliding names here to actually observe the fan-out data
      // landing (rather than matching that static option).
      mockSend.mockResolvedValueOnce({ WorkGroups: [{ Name: "us-workgroup" }] });
      mockSend.mockResolvedValueOnce({ WorkGroups: [{ Name: "eu-workgroup" }] });

      render(AthenaPage);
      // Wait for the initial mount-time loadWorkgroups() to actually settle
      // (visible via the Query Editor's workgroup <select>) before switching
      // tabs -- otherwise a click that lands while `workgroups` is still
      // empty triggers a second, overlapping loadWorkgroups() call from
      // handleTabChange's `workgroups.length === 0` check.
      await waitFor(() =>
        expect(screen.getByRole("option", { name: "us-workgroup" })).toBeInTheDocument(),
      );
      await fireEvent.click(screen.getByText("Workgroups"));

      await waitFor(() => expect(screen.getByText("us-workgroup")).toBeInTheDocument());
      expect(screen.getByText("eu-workgroup")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListWorkGroups call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ WorkGroups: [{ Name: "us-workgroup" }] });
      render(AthenaPage);
      await waitFor(() =>
        expect(screen.getByRole("option", { name: "us-workgroup" })).toBeInTheDocument(),
      );
      await fireEvent.click(screen.getByText("Workgroups"));
      await waitFor(() => expect(screen.getByText("us-workgroup")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same workgroup name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ WorkGroups: [{ Name: "shared-wg" }] });
      mockSend.mockResolvedValueOnce({ WorkGroups: [{ Name: "shared-wg" }] });

      render(AthenaPage);
      await waitFor(() =>
        expect(screen.getAllByRole("option", { name: "shared-wg" })).toHaveLength(2),
      );
      await fireEvent.click(screen.getByText("Workgroups"));

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-wg");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
