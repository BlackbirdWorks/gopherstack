import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import LambdaPage from "./+page.svelte";
import { setMockPageUrl } from "$lib/mock-page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getLambdaClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
  },
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
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

// Every load* on this page (loadFunctions and loadLayers) fires on mount via
// onRegionChange, so any queued mock response also needs a second value for
// the layers call -- an empty list keeps assertions focused on functions.
const emptyLayers = { Layers: [] };

describe("Lambda Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    // Every test below predates "All" mode and assumes exactly one
    // ListFunctions call per load against a single region, so pin
    // single-region mode here; the "All regions mode" describe block below
    // opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ Functions: [] });

    render(LambdaPage);

    expect(screen.getByText("Lambda Functions")).toBeInTheDocument();
    expect(screen.getByText("Create Function")).toBeInTheDocument();
  });

  it("honors ?q= and ?runtime= from the URL on load", async () => {
    setMockPageUrl(new URL("http://localhost/lambda?q=api&runtime=python"));
    mockSend.mockResolvedValueOnce({
      Functions: [
        {
          FunctionName: "my-handler",
          FunctionArn: "arn:1",
          Runtime: "nodejs20.x",
          MemorySize: 128,
        },
        {
          FunctionName: "api-processor",
          FunctionArn: "arn:2",
          Runtime: "python3.12",
          MemorySize: 256,
        },
      ],
    });

    render(LambdaPage);

    await waitFor(() => {
      expect(screen.getByText("api-processor")).toBeInTheDocument();
    });
    expect(screen.queryByText("my-handler")).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText("Search functions...")).toHaveValue("api");
  });

  it("displays loaded functions", async () => {
    mockSend.mockResolvedValueOnce({
      Functions: [
        {
          FunctionName: "my-handler",
          FunctionArn: "arn:1",
          Runtime: "nodejs20.x",
          MemorySize: 128,
        },
        {
          FunctionName: "api-processor",
          FunctionArn: "arn:2",
          Runtime: "python3.12",
          MemorySize: 256,
        },
      ],
    });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-handler")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("api-processor")).toBeInTheDocument();
  });

  it("filters functions via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Functions: [
        { FunctionName: "my-handler", FunctionArn: "arn:1", Runtime: "nodejs20.x" },
        { FunctionName: "api-processor", FunctionArn: "arn:2", Runtime: "python3.12" },
        { FunctionName: "data-transformer", FunctionArn: "arn:3", Runtime: "go1.x" },
      ],
    });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-handler")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search functions...");
    await fireEvent.input(searchInput, { target: { value: "api" } });

    await waitFor(() => {
      expect(screen.queryByText("my-handler")).not.toBeInTheDocument();
    });
    expect(screen.getByText("api-processor")).toBeInTheDocument();
    expect(screen.queryByText("data-transformer")).not.toBeInTheDocument();
  });

  it("selects a function when row is clicked", async () => {
    mockSend.mockResolvedValueOnce({
      Functions: [
        {
          FunctionName: "my-handler",
          FunctionArn: "arn:1",
          Runtime: "nodejs20.x",
          MemorySize: 128,
        },
      ],
    });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-handler")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    // Click the function row
    await fireEvent.click(screen.getByText("my-handler"));

    // The page renders without errors after selection
    expect(screen.getByText("my-handler")).toBeInTheDocument();
  });

  it("shows empty state when no functions exist", async () => {
    mockSend.mockResolvedValueOnce({ Functions: [] });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("No functions found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));

    render(LambdaPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans ListFunctions out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "us-handler", FunctionArn: "arn:1" }],
      });
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "eu-handler", FunctionArn: "arn:2" }],
      });
      mockSend.mockResolvedValueOnce(emptyLayers);
      mockSend.mockResolvedValueOnce(emptyLayers);

      render(LambdaPage);

      await waitFor(() => expect(screen.getByText("us-handler")).toBeInTheDocument());
      expect(screen.getByText("eu-handler")).toBeInTheDocument();
      expect(
        mockSend.mock.calls.filter(([c]) => c?.constructor?.name === "ListFunctionsCommand"),
      ).toHaveLength(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "my-handler", FunctionArn: "arn:1" }],
      });
      mockSend.mockResolvedValueOnce(emptyLayers);

      render(LambdaPage);

      await waitFor(() => expect(screen.getByText("my-handler")).toBeInTheDocument());
      expect(
        mockSend.mock.calls.filter(([c]) => c?.constructor?.name === "ListFunctionsCommand"),
      ).toHaveLength(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListFunctions call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "my-handler", FunctionArn: "arn:1" }],
      });
      mockSend.mockResolvedValueOnce(emptyLayers);
      render(LambdaPage);
      await waitFor(() => expect(screen.getByText("my-handler")).toBeInTheDocument());
      expect(
        mockSend.mock.calls.filter(([c]) => c?.constructor?.name === "ListFunctionsCommand"),
      ).toHaveLength(1);
    });

    it("renders the same function name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "shared-fn", FunctionArn: "arn:us:shared-fn" }],
      });
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "shared-fn", FunctionArn: "arn:eu:shared-fn" }],
      });
      mockSend.mockResolvedValueOnce(emptyLayers);
      mockSend.mockResolvedValueOnce(emptyLayers);

      render(LambdaPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-fn");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("deletes the row's own region, not the picker's, when two regions share a function name", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "shared-fn", FunctionArn: "arn:us:shared-fn" }],
      });
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "shared-fn", FunctionArn: "arn:eu:shared-fn" }],
      });
      mockSend.mockResolvedValueOnce(emptyLayers);
      mockSend.mockResolvedValueOnce(emptyLayers);

      render(LambdaPage);
      await waitFor(() => expect(screen.getAllByText("shared-fn")).toHaveLength(2));

      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({
        Functions: [{ FunctionName: "shared-fn", FunctionArn: "arn:us:shared-fn" }],
      });
      mockSend.mockResolvedValueOnce({ Functions: [] });

      const rows = screen.getAllByText("shared-fn");
      const euRow = rows
        .map((r) => r.closest("tr") as HTMLElement)
        .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
      await fireEvent.click(within(euRow).getByTitle("Delete"));

      await waitFor(() => {
        const remaining = screen.getAllByText("shared-fn");
        expect(remaining).toHaveLength(1);
        expect(
          within(remaining[0].closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
        ).toBe("us-east-1");
      });

      vi.unstubAllGlobals();
    });
  });
});
