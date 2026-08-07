import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import FirehosePage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getFirehoseClient: () => ({ send: mockSend }),
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

describe("Firehose Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // ListDeliveryStreams call against a single region, so pin single-region
    // mode here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    expect(screen.getByText("Amazon Data Firehose")).toBeInTheDocument();
  });

  it("shows Create Stream button", () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    expect(screen.getByText("Create Stream")).toBeInTheDocument();
  });

  it("shows empty state when no streams", async () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    await waitFor(
      () => {
        expect(screen.getByText("No delivery streams found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded stream names", async () => {
    mockSend.mockResolvedValue({
      DeliveryStreamNames: ["clickstream-to-s3", "logs-to-opensearch"],
    });
    render(FirehosePage);
    await waitFor(
      () => {
        expect(screen.getByText("clickstream-to-s3")).toBeInTheDocument();
        expect(screen.getByText("logs-to-opensearch")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create stream modal", async () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    await fireEvent.click(screen.getByText("Create Stream"));
    expect(screen.getByText("Stream Name")).toBeInTheDocument();
    expect(screen.getByText("S3 Bucket Name")).toBeInTheDocument();
  });

  it("cancels create stream modal", async () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    await fireEvent.click(screen.getByText("Create Stream"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Stream Name")).not.toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    expect(screen.getByPlaceholderText("Search delivery streams...")).toBeInTheDocument();
  });

  it("navigates to stream detail on click", async () => {
    mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["my-stream"] });
    mockSend.mockResolvedValueOnce({
      DeliveryStreamDescription: {
        DeliveryStreamName: "my-stream",
        DeliveryStreamStatus: "ACTIVE",
        DeliveryStreamType: "DirectPut",
        DeliveryStreamARN: "arn:aws:firehose:us-east-1:123:deliverystream/my-stream",
        Destinations: [],
        CreateTimestamp: new Date(),
      },
    });
    render(FirehosePage);
    await waitFor(() => expect(screen.getByText("my-stream")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("my-stream"));
    await waitFor(
      () => {
        expect(screen.getByText("Put Record")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
    render(FirehosePage);
    expect(screen.getByText("Delivery Streams")).toBeInTheDocument();
    expect(screen.getByText("Selected Stream")).toBeInTheDocument();
    expect(screen.getByText("Destinations")).toBeInTheDocument();
    expect(screen.getByText("Status")).toBeInTheDocument();
  });

  describe("All regions mode", () => {
    it("fans ListDeliveryStreams out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["us-stream"] });
      mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["eu-stream"] });

      render(FirehosePage);

      await waitFor(() => expect(screen.getByText("us-stream")).toBeInTheDocument());
      expect(screen.getByText("eu-stream")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["solo-stream"] });

      render(FirehosePage);

      await waitFor(() => expect(screen.getByText("solo-stream")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListDeliveryStreams call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["solo-stream"] });
      render(FirehosePage);
      await waitFor(() => expect(screen.getByText("solo-stream")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same stream name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["shared-stream"] });
      mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ["shared-stream"] });

      render(FirehosePage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-stream");
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
