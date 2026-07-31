import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import DynamoDBPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDynamoDBClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe("DynamoDB Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ TableNames: [] });

    render(DynamoDBPage);

    expect(screen.getByText("DynamoDB Tables")).toBeInTheDocument();
    expect(screen.getByText("+ Create Table")).toBeInTheDocument();
  });

  it("displays loaded tables", async () => {
    mockSend.mockResolvedValueOnce({ TableNames: ["Users", "Orders"] });
    mockSend.mockResolvedValueOnce({
      Table: {
        TableName: "Users",
        TableStatus: "ACTIVE",
        ItemCount: 42,
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      },
    });
    mockSend.mockResolvedValueOnce({
      Table: {
        TableName: "Orders",
        TableStatus: "ACTIVE",
        ItemCount: 7,
        KeySchema: [{ AttributeName: "orderId", KeyType: "HASH" }],
      },
    });

    render(DynamoDBPage);

    await waitFor(
      () => {
        expect(screen.getByText("Users")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("Orders")).toBeInTheDocument();
  });

  it("filters tables via search input", async () => {
    mockSend.mockResolvedValueOnce({ TableNames: ["Alpha", "Beta", "Gamma"] });
    mockSend.mockResolvedValue({
      Table: {
        TableName: "Alpha",
        TableStatus: "ACTIVE",
        ItemCount: 0,
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      },
    });

    render(DynamoDBPage);

    await waitFor(
      () => {
        expect(screen.getByText("Alpha")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search tables...");
    await fireEvent.input(searchInput, { target: { value: "Beta" } });

    await waitFor(() => {
      expect(screen.queryByText("Alpha")).not.toBeInTheDocument();
    });
    expect(screen.getByText("Beta")).toBeInTheDocument();
    expect(screen.queryByText("Gamma")).not.toBeInTheDocument();
  });

  it("opens and closes create table modal", async () => {
    mockSend.mockResolvedValueOnce({ TableNames: [] });

    render(DynamoDBPage);

    const createBtn = screen.getByText("+ Create Table");
    await fireEvent.click(createBtn);

    expect(screen.getByText("Create Table")).toBeInTheDocument();
    expect(screen.getByLabelText("Table Name")).toBeInTheDocument();
    expect(screen.getByLabelText("Partition Key")).toBeInTheDocument();

    const cancelBtn = screen.getByText("Cancel");
    await fireEvent.click(cancelBtn);

    await waitFor(() => {
      expect(screen.queryByLabelText("Table Name")).not.toBeInTheDocument();
    });
  });

  it("creates a table via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ TableNames: [] });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ TableNames: ["NewTable"] });
    mockSend.mockResolvedValueOnce({
      Table: {
        TableName: "NewTable",
        TableStatus: "ACTIVE",
        ItemCount: 0,
        KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }],
      },
    });

    render(DynamoDBPage);

    await fireEvent.click(screen.getByText("+ Create Table"));

    const nameInput = screen.getByLabelText("Table Name");
    const pkInput = screen.getByLabelText("Partition Key");

    await fireEvent.input(nameInput, { target: { value: "NewTable" } });
    await fireEvent.input(pkInput, { target: { value: "pk" } });

    const submitBtn = screen.getByRole("dialog").querySelector('button[type="submit"]')!;
    await fireEvent.click(submitBtn);

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(4);
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalled();
  });

  it("opens table detail view on card click", async () => {
    mockSend.mockResolvedValueOnce({ TableNames: ["MyTable"] });
    mockSend.mockResolvedValueOnce({
      Table: {
        TableName: "MyTable",
        TableStatus: "ACTIVE",
        ItemCount: 5,
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
      },
    });

    render(DynamoDBPage);

    await waitFor(
      () => {
        expect(screen.getByText("MyTable")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const tableBtn = screen.getByText("MyTable");
    await fireEvent.click(tableBtn);

    await waitFor(() => {
      expect(screen.getByText("Delete Table")).toBeInTheDocument();
    });
    expect(screen.getByText("New Item")).toBeInTheDocument();
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network failure"));

    render(DynamoDBPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  describe("Streams tab", () => {
    const streamEnabledTable = {
      TableName: "StreamsTable",
      TableStatus: "ACTIVE",
      ItemCount: 0,
      KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
      LatestStreamArn:
        "arn:aws:dynamodb:us-east-1:000000000000:table/StreamsTable/stream/2024-01-01",
    };

    it("shows 'not enabled' banner when streams are disabled", async () => {
      mockSend.mockResolvedValueOnce({ TableNames: ["StreamsTable"] });
      mockSend.mockResolvedValueOnce({
        Table: { ...streamEnabledTable, StreamSpecification: { StreamEnabled: false } },
      });

      render(DynamoDBPage);

      await waitFor(() => expect(screen.getByText("StreamsTable")).toBeInTheDocument(), {
        timeout: 3000,
      });
      await fireEvent.click(screen.getByText("StreamsTable"));

      // Wait for table detail to open (Overview tab is default)
      await waitFor(() => expect(screen.getByText("Stream Events")).toBeInTheDocument(), {
        timeout: 3000,
      });
      await fireEvent.click(screen.getByText("Stream Events"));

      await waitFor(
        () =>
          expect(
            screen.getByText("DynamoDB Streams are not enabled for this table."),
          ).toBeInTheDocument(),
        { timeout: 3000 },
      );
    });

    it("shows stats cards and filter buttons when streams are enabled", async () => {
      const mockStreamInfo = {
        available: true,
        eventCount: 3,
        shards: [{ shardId: "shardId-00000000000000000001-00000001", startSeq: "1", endSeq: "3" }],
        latestEventUnix: Math.floor(Date.now() / 1000) - 2,
        lagSeconds: 2,
        oldestSeq: "1",
        latestSeq: "3",
        eventTypes: { insert: 2, modify: 1, remove: 0 },
      };

      // Mock fetch for stream-info and stream-events
      const mockFetch = vi.fn().mockImplementation((url: string) => {
        if (url.includes("stream-info")) {
          return Promise.resolve({
            ok: true,
            json: () => Promise.resolve(mockStreamInfo),
          });
        }
        if (url.includes("stream-events")) {
          return Promise.resolve({ ok: true, text: () => Promise.resolve("") });
        }
        return Promise.resolve({ ok: false });
      });
      vi.stubGlobal("fetch", mockFetch);

      // loadTables: ListTables + DescribeTable
      mockSend.mockResolvedValueOnce({ TableNames: ["StreamsTable"] });
      mockSend.mockResolvedValueOnce({ Table: streamEnabledTable });
      // openTable: DescribeTable + DescribeTimeToLive (TTL rejects silently)
      mockSend.mockResolvedValueOnce({ Table: streamEnabledTable });
      mockSend.mockRejectedValueOnce(new Error("TTL not supported"));

      render(DynamoDBPage);

      await waitFor(() => expect(screen.getByText("StreamsTable")).toBeInTheDocument(), {
        timeout: 3000,
      });
      await fireEvent.click(screen.getByText("StreamsTable"));

      await waitFor(() => expect(screen.getByText("Stream Events")).toBeInTheDocument(), {
        timeout: 3000,
      });
      await fireEvent.click(screen.getByText("Stream Events"));

      // Stats cards should appear (streamsEnabled=true triggers fetch)
      await waitFor(() => expect(screen.getByText("Buffered Events")).toBeInTheDocument(), {
        timeout: 3000,
      });
      await waitFor(() => expect(screen.getByText("Active Shards")).toBeInTheDocument(), {
        timeout: 3000,
      });
      await waitFor(() => expect(screen.getByText("Shard Breakdown")).toBeInTheDocument(), {
        timeout: 3000,
      });

      // Filter buttons should be present
      expect(screen.getByRole("button", { name: "ALL" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "INSERT" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "MODIFY" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "REMOVE" })).toBeInTheDocument();

      // Iterator Expiry card shows "15 min"
      expect(screen.getByText("15 min")).toBeInTheDocument();

      vi.unstubAllGlobals();
    });
  });
});
