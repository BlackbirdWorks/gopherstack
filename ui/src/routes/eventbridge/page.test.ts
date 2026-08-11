import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import EventBridgePage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEventBridgeClient: () => ({ send: mockSend }),
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

describe("EventBridge Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // ListEventBuses call against a single region, so pin single-region
    // mode here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    expect(screen.getByText("EventBridge")).toBeInTheDocument();
  });

  it("shows Create Bus button", () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    expect(screen.getByText("Create Bus")).toBeInTheDocument();
  });

  it("shows empty state when no buses", async () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    await waitFor(
      () => {
        expect(screen.getByText("No event buses found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded event buses", async () => {
    mockSend.mockResolvedValueOnce({
      EventBuses: [
        { Name: "default", Arn: "arn:aws:events:us-east-1:123:event-bus/default" },
        { Name: "my-custom-bus", Arn: "arn:aws:events:us-east-1:123:event-bus/my-custom-bus" },
      ],
    });
    render(EventBridgePage);
    await waitFor(
      () => {
        expect(screen.getByText("default")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("my-custom-bus")).toBeInTheDocument();
  });

  it("filters buses via search", async () => {
    mockSend.mockResolvedValueOnce({
      EventBuses: [
        { Name: "default", Arn: "arn:1" },
        { Name: "app-events", Arn: "arn:2" },
      ],
    });
    render(EventBridgePage);
    await waitFor(
      () => {
        expect(screen.getByText("app-events")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search event buses...");
    await fireEvent.input(searchInput, { target: { value: "app" } });
    await waitFor(() => {
      expect(screen.queryByText("default")).not.toBeInTheDocument();
    });
    expect(screen.getByText("app-events")).toBeInTheDocument();
  });

  it("opens create bus modal", async () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    await fireEvent.click(screen.getByText("Create Bus"));
    await waitFor(() => {
      expect(screen.getByText("Create Event Bus")).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. my-app-events")).toBeInTheDocument();
  });

  it("opens put events modal", async () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    await fireEvent.click(screen.getByText("Put Events"));
    await waitFor(() => {
      expect(screen.getByText("Put Events", { selector: "h2" })).toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(EventBridgePage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans ListEventBuses out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ EventBuses: [{ Name: "us-bus", Arn: "arn:1" }] });
      mockSend.mockResolvedValueOnce({ EventBuses: [{ Name: "eu-bus", Arn: "arn:2" }] });

      render(EventBridgePage);

      await waitFor(() => expect(screen.getByText("us-bus")).toBeInTheDocument());
      expect(screen.getByText("eu-bus")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ EventBuses: [{ Name: "solo-bus", Arn: "arn:1" }] });

      render(EventBridgePage);

      await waitFor(() => expect(screen.getByText("solo-bus")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListEventBuses call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ EventBuses: [{ Name: "solo-bus", Arn: "arn:1" }] });
      render(EventBridgePage);
      await waitFor(() => expect(screen.getByText("solo-bus")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same bus name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ EventBuses: [{ Name: "shared-bus", Arn: "arn:us" }] });
      mockSend.mockResolvedValueOnce({ EventBuses: [{ Name: "shared-bus", Arn: "arn:eu" }] });

      render(EventBridgePage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-bus");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".justify-between") as HTMLElement).getByTestId("region-chip")
            .textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
