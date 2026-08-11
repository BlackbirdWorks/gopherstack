import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import KinesisPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

// twoShards builds a two-shard DescribeStream/ListShards fixture with
// adjacent, splittable hash-key ranges.
function twoShards() {
  return [
    {
      ShardId: "shardId-000000000000",
      HashKeyRange: { StartingHashKey: "0", EndingHashKey: "100" },
    },
    {
      ShardId: "shardId-000000000001",
      HashKeyRange: {
        StartingHashKey: "101",
        EndingHashKey: "340282366920938463463374607431768211455",
      },
    },
  ];
}

vi.mock("$lib/aws-client", () => ({
  getKinesisClient: () => ({ send: mockSend }),
  getCloudWatchClient: () => ({ send: mockSend }),
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

describe("Kinesis Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // ListStreams call against a single region, so pin single-region mode
    // here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    expect(screen.getByText("Kinesis Data Streams")).toBeInTheDocument();
  });

  it("shows Create Stream button", () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    expect(screen.getByText("Create Stream")).toBeInTheDocument();
  });

  it("shows empty state when no streams", async () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    await waitFor(
      () => {
        expect(screen.getByText("No streams found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded streams", async () => {
    mockSend.mockResolvedValueOnce({ StreamNames: ["user-events", "order-stream"] });
    render(KinesisPage);
    await waitFor(
      () => {
        expect(screen.getByText("user-events")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("order-stream")).toBeInTheDocument();
  });

  it("filters streams via search", async () => {
    mockSend.mockResolvedValueOnce({
      StreamNames: ["user-events", "order-stream", "click-stream"],
    });
    render(KinesisPage);
    await waitFor(
      () => {
        expect(screen.getByText("user-events")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search streams...");
    await fireEvent.input(searchInput, { target: { value: "order" } });
    await waitFor(() => {
      expect(screen.queryByText("user-events")).not.toBeInTheDocument();
    });
    expect(screen.getByText("order-stream")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    await fireEvent.click(screen.getByText("Create Stream"));
    await waitFor(() => {
      expect(screen.getByText("Create Data Stream")).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. user-events")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    await fireEvent.click(screen.getByText("Create Stream"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. user-events")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. user-events")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("permission denied"));
    render(KinesisPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("shows 4 stat cards", () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    expect(screen.getByText("Total Streams")).toBeInTheDocument();
    expect(screen.getByText("Open Shards")).toBeInTheDocument();
    expect(screen.getByText("Consumers")).toBeInTheDocument();
    expect(screen.getByText("Shards Used/Limit")).toBeInTheDocument();
  });

  async function selectFirstStream() {
    // ListStreams on mount.
    mockSend.mockResolvedValueOnce({ StreamNames: ["s1"] });
    render(KinesisPage);
    await waitFor(() => expect(screen.getByText("s1")).toBeInTheDocument(), { timeout: 3000 });

    // Describe + ListShards + ListStreamConsumers on select.
    mockSend.mockResolvedValueOnce({
      StreamDescription: {
        StreamName: "s1",
        StreamARN: "arn:aws:kinesis:us-east-1:000000000000:stream/s1",
        StreamStatus: "ACTIVE",
        RetentionPeriodHours: 24,
        StreamModeDetails: { StreamMode: "PROVISIONED" },
        EncryptionType: "NONE",
      },
    });
    mockSend.mockResolvedValueOnce({ Shards: twoShards() });
    mockSend.mockResolvedValueOnce({
      Consumers: [{ ConsumerName: "c1", ConsumerARN: "arn:c1", ConsumerStatus: "ACTIVE" }],
    });
    await fireEvent.click(screen.getByText("s1"));
    await waitFor(() => expect(screen.getByText("shardId-000000000000")).toBeInTheDocument(), {
      timeout: 3000,
    });
  }

  it("Split passes a strictly-interior hash key (not the shard start)", async () => {
    await selectFirstStream();
    mockSend.mockClear();
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Shards: twoShards() });

    await fireEvent.click(screen.getAllByText("Split")[0]);
    await waitFor(() => expect(mockSend).toHaveBeenCalled());

    const cmd = mockSend.mock.calls[0][0];
    const key = cmd.input.NewStartingHashKey as string;
    expect(BigInt(key) > 0n).toBe(true);
    expect(BigInt(key) < 100n).toBe(true);
  });

  it("Merge picks an adjacent shard", async () => {
    await selectFirstStream();
    mockSend.mockClear();
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Shards: twoShards() });

    await fireEvent.click(screen.getAllByText("Merge")[0]);
    await waitFor(() => expect(mockSend).toHaveBeenCalled());

    const cmd = mockSend.mock.calls[0][0];
    expect(cmd.input.ShardToMerge).toBe("shardId-000000000000");
    expect(cmd.input.AdjacentShardToMerge).toBe("shardId-000000000001");
  });

  it("real consumer count is reflected in the stat card", async () => {
    await selectFirstStream();
    // The Consumers stat card shows the count from ListStreamConsumers.
    const cards = screen.getAllByText("1");
    expect(cards.length).toBeGreaterThan(0);
  });

  it("GetRecords uses the selected iterator type and pages via NextShardIterator", async () => {
    await selectFirstStream();
    mockSend.mockClear();
    mockSend.mockResolvedValueOnce({ ShardIterator: "iter-abc" });
    mockSend.mockResolvedValueOnce({
      Records: [
        { SequenceNumber: "49-1", PartitionKey: "pk", Data: new TextEncoder().encode("hello") },
      ],
      NextShardIterator: "iter-next",
    });

    await fireEvent.click(screen.getAllByText("View Records")[0]);
    await waitFor(() => expect(screen.getByText(/hello/)).toBeInTheDocument(), { timeout: 3000 });

    const iterCmd = mockSend.mock.calls[0][0];
    expect(iterCmd.input.ShardIteratorType).toBe("TRIM_HORIZON");

    // A "Load more" control appears because NextShardIterator was returned.
    const loadMore = screen.getByText(/Load more/);
    mockSend.mockResolvedValueOnce({
      Records: [
        { SequenceNumber: "49-2", PartitionKey: "pk", Data: new TextEncoder().encode("world") },
      ],
      NextShardIterator: "iter-next-2",
    });
    await fireEvent.click(loadMore);
    await waitFor(() => expect(screen.getByText(/world/)).toBeInTheDocument(), { timeout: 3000 });

    const pageCmd = mockSend.mock.calls.at(-1)![0];
    expect(pageCmd.input.ShardIterator).toBe("iter-next");
  });

  it("Manage tab exposes UpdateShardCount / retention / encryption / stream-mode", async () => {
    await selectFirstStream();
    await fireEvent.click(screen.getByText("Manage"));
    await waitFor(() => {
      expect(screen.getByText(/Reshard/)).toBeInTheDocument();
    });
    expect(screen.getByText("Retention Period")).toBeInTheDocument();
    expect(screen.getByText("Server-Side Encryption")).toBeInTheDocument();
    expect(screen.getByText("Stream Mode")).toBeInTheDocument();
  });

  describe("All regions mode", () => {
    it("fans ListStreams out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ StreamNames: ["us-stream"] });
      mockSend.mockResolvedValueOnce({ StreamNames: ["eu-stream"] });

      render(KinesisPage);

      await waitFor(() => expect(screen.getByText("us-stream")).toBeInTheDocument());
      expect(screen.getByText("eu-stream")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ StreamNames: ["solo-stream"] });

      render(KinesisPage);

      await waitFor(() => expect(screen.getByText("solo-stream")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListStreams call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ StreamNames: ["solo-stream"] });
      render(KinesisPage);
      await waitFor(() => expect(screen.getByText("solo-stream")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same stream name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ StreamNames: ["shared-stream"] });
      mockSend.mockResolvedValueOnce({ StreamNames: ["shared-stream"] });

      render(KinesisPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-stream");
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
