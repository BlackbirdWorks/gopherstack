import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SFNPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSFNClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
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

describe("Step Functions Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // ListStateMachines call against a single region, so pin single-region
    // mode here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ stateMachines: [] });

    render(SFNPage);

    expect(screen.getByText("Step Functions")).toBeInTheDocument();
  });

  it("displays loaded state machines", async () => {
    mockSend.mockResolvedValueOnce({
      stateMachines: [
        { stateMachineArn: "arn:1", name: "order-workflow", type: "STANDARD" },
        { stateMachineArn: "arn:2", name: "data-pipeline", type: "EXPRESS" },
      ],
    });

    render(SFNPage);

    await waitFor(
      () => {
        expect(screen.getByText("order-workflow")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("data-pipeline")).toBeInTheDocument();
  });

  it("filters state machines via search input", async () => {
    mockSend.mockResolvedValueOnce({
      stateMachines: [
        { stateMachineArn: "arn:1", name: "order-workflow", type: "STANDARD" },
        { stateMachineArn: "arn:2", name: "data-pipeline", type: "EXPRESS" },
        { stateMachineArn: "arn:3", name: "payment-flow", type: "STANDARD" },
      ],
    });

    render(SFNPage);

    await waitFor(
      () => {
        expect(screen.getByText("order-workflow")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search workflows...");
    await fireEvent.input(searchInput, { target: { value: "data" } });

    await waitFor(() => {
      expect(screen.queryByText("order-workflow")).not.toBeInTheDocument();
    });
    expect(screen.getByText("data-pipeline")).toBeInTheDocument();
    expect(screen.queryByText("payment-flow")).not.toBeInTheDocument();
  });

  it("selects a state machine and loads executions", async () => {
    mockSend.mockResolvedValueOnce({
      stateMachines: [{ stateMachineArn: "arn:1", name: "order-workflow", type: "STANDARD" }],
    });
    // selectSM: DescribeStateMachineCommand
    mockSend.mockResolvedValueOnce({
      stateMachineArn: "arn:1",
      name: "order-workflow",
      definition: "{}",
    });
    // ListExecutionsCommand
    mockSend.mockResolvedValueOnce({ executions: [] });

    render(SFNPage);

    await waitFor(
      () => {
        expect(screen.getByText("order-workflow")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("order-workflow"));

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(3);
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no state machines exist", async () => {
    mockSend.mockResolvedValueOnce({ stateMachines: [] });

    render(SFNPage);

    await waitFor(
      () => {
        expect(screen.getByText("No state machines found.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("not authorized"));

    render(SFNPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans ListStateMachines out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        stateMachines: [{ stateMachineArn: "arn:us:1", name: "us-workflow", type: "STANDARD" }],
      });
      mockSend.mockResolvedValueOnce({
        stateMachines: [{ stateMachineArn: "arn:eu:1", name: "eu-workflow", type: "STANDARD" }],
      });

      render(SFNPage);

      await waitFor(() => expect(screen.getByText("us-workflow")).toBeInTheDocument());
      expect(screen.getByText("eu-workflow")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({
        stateMachines: [{ stateMachineArn: "arn:1", name: "solo-workflow", type: "STANDARD" }],
      });

      render(SFNPage);

      await waitFor(() => expect(screen.getByText("solo-workflow")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListStateMachines call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({
        stateMachines: [{ stateMachineArn: "arn:1", name: "solo-workflow", type: "STANDARD" }],
      });
      render(SFNPage);
      await waitFor(() => expect(screen.getByText("solo-workflow")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same workflow name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        stateMachines: [{ stateMachineArn: "arn:us:1", name: "shared-workflow", type: "STANDARD" }],
      });
      mockSend.mockResolvedValueOnce({
        stateMachines: [{ stateMachineArn: "arn:eu:1", name: "shared-workflow", type: "STANDARD" }],
      });

      render(SFNPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-workflow");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest('[role="button"]') as HTMLElement).getByTestId("region-chip")
            .textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
