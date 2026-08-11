import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DynamoDBPage from "./+page.svelte";
import { setMockPageUrl, getMockPage } from "$lib/mock-page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDynamoDBClient: () => ({ send: mockSend }),
  getDynamoDBStreamsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

function commandName(command: unknown): string {
  return (command as { constructor: { name: string } }).constructor.name;
}

function findCall(name: string, index = 0): { input: Record<string, unknown> } {
  const calls = mockSend.mock.calls.filter((c) => commandName(c[0]) === name);
  return calls[index][0] as { input: Record<string, unknown> };
}

function callCount(name: string): number {
  return mockSend.mock.calls.filter((c) => commandName(c[0]) === name).length;
}

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

const baseTable = {
  TableName: "Orders",
  TableStatus: "ACTIVE",
  ItemCount: 2,
  TableSizeBytes: 2048,
  TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/Orders",
  KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
  AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
  BillingModeSummary: { BillingMode: "PAY_PER_REQUEST" },
};

// Opens the DynamoDB table list, then drills into "Orders" so activeTab === 'overview'.
// Queues exactly the 4 calls that flow requires: ListTables, DescribeTable (loadTables),
// DescribeTable, DescribeTimeToLive (openTable) -- callers queue whatever comes after.
async function openOrdersTable(): Promise<void> {
  mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });
  mockSend.mockResolvedValueOnce({ Table: baseTable });
  render(DynamoDBPage);
  await waitFor(() => expect(screen.getByText("Orders")).toBeInTheDocument(), { timeout: 3000 });

  mockSend.mockResolvedValueOnce({ Table: baseTable });
  mockSend.mockResolvedValueOnce({ TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" } });
  await fireEvent.click(screen.getByText("Orders"));
  await waitFor(() => expect(screen.getByText("Delete Table")).toBeInTheDocument(), {
    timeout: 3000,
  });
}

describe("DynamoDB Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    // Region defaults to "All" for a fresh user (see region.svelte.ts) --
    // every existing test in this file below predates "All" mode and
    // assumes exactly one ListTables/DescribeTable call sequence against a
    // single region, so pin single-region mode here. Tests that actually
    // exercise All mode call setStoredRegion(ALL_REGIONS) themselves.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ TableNames: [] });

    render(DynamoDBPage);

    expect(screen.getByText("DynamoDB Tables")).toBeInTheDocument();
    expect(screen.getByText("+ Create Table")).toBeInTheDocument();
  });

  it("honors ?q= and ?sort= from the URL on load", async () => {
    setMockPageUrl(new URL("http://localhost/dynamodb?q=Order&sort=alpha"));
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

    await waitFor(() => {
      expect(screen.getByText("Orders")).toBeInTheDocument();
    });
    expect(screen.queryByText("Users")).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText("Search tables...")).toHaveValue("Order");
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

  describe("Table list mutations", () => {
    it("sends the exact CreateTable input for a provisioned table with a sort key", async () => {
      mockSend.mockResolvedValueOnce({ TableNames: [] });
      render(DynamoDBPage);
      await waitFor(() => expect(screen.getByText("+ Create Table")).toBeInTheDocument());

      await fireEvent.click(screen.getByText("+ Create Table"));
      const dialog = screen.getByRole("dialog");
      await fireEvent.input(within(dialog).getByLabelText("Table Name"), {
        target: { value: "Orders" },
      });
      await fireEvent.input(within(dialog).getByLabelText("Partition Key"), {
        target: { value: "id" },
      });
      await fireEvent.input(within(dialog).getByLabelText("Sort Key (Optional)"), {
        target: { value: "ts" },
      });
      await fireEvent.click(within(dialog).getByLabelText("Provisioned"));
      await fireEvent.input(document.querySelector("#create-rcu")!, { target: { value: "10" } });
      await fireEvent.input(document.querySelector("#create-wcu")!, { target: { value: "20" } });

      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });
      mockSend.mockResolvedValueOnce({ Table: baseTable });

      await fireEvent.click(dialog.querySelector('button[type="submit"]')!);

      await waitFor(() => expect(callCount("CreateTableCommand")).toBe(1));
      expect(findCall("CreateTableCommand").input).toEqual({
        TableName: "Orders",
        KeySchema: [
          { AttributeName: "id", KeyType: "HASH" },
          { AttributeName: "ts", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "id", AttributeType: "S" },
          { AttributeName: "ts", AttributeType: "S" },
        ],
        BillingMode: "PROVISIONED",
        ProvisionedThroughput: { ReadCapacityUnits: 10, WriteCapacityUnits: 20 },
      });
    });

    it("shows the AWS error code in a toast when CreateTable fails", async () => {
      mockSend.mockResolvedValueOnce({ TableNames: [] });
      render(DynamoDBPage);
      await waitFor(() => expect(screen.getByText("+ Create Table")).toBeInTheDocument());
      await fireEvent.click(screen.getByText("+ Create Table"));
      const dialog = screen.getByRole("dialog");
      await fireEvent.input(within(dialog).getByLabelText("Table Name"), {
        target: { value: "Dup" },
      });
      await fireEvent.input(within(dialog).getByLabelText("Partition Key"), {
        target: { value: "id" },
      });

      const err = Object.assign(new Error("Table already exists"), {
        name: "ResourceInUseException",
        $metadata: { httpStatusCode: 400 },
      });
      mockSend.mockRejectedValueOnce(err);
      await fireEvent.click(dialog.querySelector('button[type="submit"]')!);

      const { toast } = await import("svelte-sonner");
      await waitFor(() => {
        expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
          "Failed to create table: ResourceInUseException (HTTP 400): Table already exists",
        );
      });
    });

    it("deletes a table via the exact DeleteTable input once confirmDestructive resolves true", async () => {
      await openOrdersTable();

      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({ TableNames: [] });

      await fireEvent.click(screen.getByText("Delete Table"));

      await waitFor(() => expect(callCount("DeleteTableCommand")).toBe(1));
      expect(confirmDestructive).toHaveBeenCalled();
      expect(findCall("DeleteTableCommand").input).toEqual({ TableName: "Orders" });
    });

    it("shows the AWS error code in a toast when DeleteTable fails", async () => {
      await openOrdersTable();

      const err = Object.assign(new Error("Deletion protection is enabled"), {
        name: "ValidationException",
        $metadata: { httpStatusCode: 400 },
      });
      mockSend.mockRejectedValueOnce(err);

      await fireEvent.click(screen.getByText("Delete Table"));

      const { toast } = await import("svelte-sonner");
      await waitFor(() => {
        expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
          "Failed to delete table: ValidationException (HTTP 400): Deletion protection is enabled",
        );
      });
    });

    it("does not call DeleteTable when confirmDestructive resolves false", async () => {
      confirmDestructive.mockResolvedValue(false);
      await openOrdersTable();

      await fireEvent.click(screen.getByText("Delete Table"));

      await waitFor(() => expect(confirmDestructive).toHaveBeenCalled());
      expect(callCount("DeleteTableCommand")).toBe(0);
    });
  });

  describe("Item CRUD", () => {
    it("creates an item via PutItem and reads it back through the Items tab with attribute types intact", async () => {
      await openOrdersTable();

      // Items tab auto-load
      mockSend.mockResolvedValueOnce({ Items: [] });
      await fireEvent.click(screen.getByText("Items"));
      await waitFor(() =>
        expect(screen.getByText("No items found in this table.")).toBeInTheDocument(),
      );

      await fireEvent.click(screen.getByText("New Item"));
      const dialog = screen.getByRole("dialog");
      await fireEvent.input(within(dialog).getByLabelText(/Item JSON/), {
        target: { value: JSON.stringify({ id: "1", age: 30, active: true }) },
      });

      // PutItem
      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({
        Items: [{ id: { S: "1" }, age: { N: "30" }, active: { BOOL: true } }],
        // reload after create (items tab)
      });

      await fireEvent.click(dialog.querySelector('button[type="submit"]')!);

      await waitFor(() => expect(callCount("PutItemCommand")).toBe(1));
      expect(findCall("PutItemCommand").input).toEqual({
        TableName: "Orders",
        Item: { id: { S: "1" }, age: { N: "30" }, active: { BOOL: true } },
      });

      // Round trip: the Number and Boolean types survive the write + re-read, rendered as
      // "30" and "true" rather than collapsing to strings or disappearing.
      await waitFor(() => expect(screen.getByText("30")).toBeInTheDocument());
      expect(screen.getByText("true")).toBeInTheDocument();
    });

    it("shows the AWS error code in a toast when PutItem (create item) fails", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("New Item"));
      const dialog = screen.getByRole("dialog");
      await fireEvent.input(within(dialog).getByLabelText(/Item JSON/), {
        target: { value: JSON.stringify({ id: "1" }) },
      });

      const err = Object.assign(new Error("Item size exceeds limit"), {
        name: "ValidationException",
        $metadata: { httpStatusCode: 400 },
      });
      mockSend.mockRejectedValueOnce(err);
      await fireEvent.click(dialog.querySelector('button[type="submit"]')!);

      const { toast } = await import("svelte-sonner");
      await waitFor(() => {
        expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
          "Failed: ValidationException (HTTP 400): Item size exceeds limit",
        );
      });
    });

    it("deletes an item via the exact DeleteItem Key and reloads the Items list", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({ Items: [{ id: { S: "row-1" } }] });
      await fireEvent.click(screen.getByText("Items"));
      await waitFor(() => expect(screen.getByText("row-1")).toBeInTheDocument());

      // DeleteItem
      mockSend.mockResolvedValueOnce({});
      // reload
      mockSend.mockResolvedValueOnce({ Items: [] });
      await fireEvent.click(screen.getByText("Delete"));

      await waitFor(() => expect(callCount("DeleteItemCommand")).toBe(1));
      expect(findCall("DeleteItemCommand").input).toEqual({
        TableName: "Orders",
        Key: { id: { S: "row-1" } },
      });
    });

    it("batch-deletes selected items via the exact BatchWriteItem DeleteRequest keys", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({
        Items: [{ id: { S: "a" } }, { id: { S: "b" } }],
      });
      await fireEvent.click(screen.getByText("Items"));
      await waitFor(() => expect(screen.getByText("a")).toBeInTheDocument());

      // Select-all checkbox is the first checkbox in the header row.
      const checkboxes = screen.getAllByRole("checkbox");
      await fireEvent.click(checkboxes[0]);
      await waitFor(() => expect(screen.getByText("Delete Selected (2)")).toBeInTheDocument());

      // BatchWriteItem
      mockSend.mockResolvedValueOnce({ UnprocessedItems: {} });
      // reload
      mockSend.mockResolvedValueOnce({ Items: [] });
      await fireEvent.click(screen.getByText("Delete Selected (2)"));

      await waitFor(() => expect(callCount("BatchWriteItemCommand")).toBe(1));
      expect(findCall("BatchWriteItemCommand").input).toEqual({
        RequestItems: {
          Orders: [
            { DeleteRequest: { Key: { id: { S: "a" } } } },
            { DeleteRequest: { Key: { id: { S: "b" } } } },
          ],
        },
      });
    });

    it("applies a partial UpdateExpression via the Edit modal's advanced panel, with ExpressionAttributeValues threaded through", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({ Items: [{ id: { S: "row-1" }, score: { N: "1" } }] });
      await fireEvent.click(screen.getByText("Items"));
      await waitFor(() => expect(screen.getByText("row-1")).toBeInTheDocument());

      await fireEvent.click(screen.getByText("Edit"));
      const dialog = screen.getByRole("dialog");
      await fireEvent.input(document.querySelector("#update-exp")!, {
        target: { value: "SET score = :v" },
      });
      await fireEvent.input(document.querySelector("#update-exp-values")!, {
        target: { value: JSON.stringify({ ":v": 42 }) },
      });

      // UpdateItem
      mockSend.mockResolvedValueOnce({});
      // reload
      mockSend.mockResolvedValueOnce({ Items: [{ id: { S: "row-1" }, score: { N: "42" } }] });
      await fireEvent.click(within(dialog).getByText("Apply Expression Update"));

      await waitFor(() => expect(callCount("UpdateItemCommand")).toBe(1));
      expect(findCall("UpdateItemCommand").input).toEqual({
        TableName: "Orders",
        Key: { id: { S: "row-1" } },
        UpdateExpression: "SET score = :v",
        ConditionExpression: undefined,
        ExpressionAttributeValues: { ":v": { N: "42" } },
      });
    });

    it("runs BatchGetItem from the Items tab's Batch Get panel with keys converted through the table's key schema", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({ Items: [] });
      await fireEvent.click(screen.getByText("Items"));
      await waitFor(() => expect(screen.getByText("Batch Get by Keys")).toBeInTheDocument());
      await fireEvent.click(screen.getByText("Batch Get by Keys"));

      await fireEvent.input(document.querySelector("#batch-get-keys")!, {
        target: { value: JSON.stringify([{ id: "1" }, { id: "2" }]) },
      });

      mockSend.mockResolvedValueOnce({
        Responses: { Orders: [{ id: { S: "1" } }] },
      });
      await fireEvent.click(screen.getByText("Run BatchGetItem"));

      await waitFor(() => expect(callCount("BatchGetItemCommand")).toBe(1));
      expect(findCall("BatchGetItemCommand").input).toEqual({
        RequestItems: {
          Orders: { Keys: [{ id: { S: "1" } }, { id: { S: "2" } }] },
        },
      });

      const { toast } = await import("svelte-sonner");
      await waitFor(() => {
        expect(vi.mocked(toast.success)).toHaveBeenCalledWith("BatchGet completed. Found: 1");
      });
    });
  });

  describe("Query and Scan", () => {
    it("paginates Query via Load More with the exact ExclusiveStartKey, appending rather than replacing results", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("Query"));

      await fireEvent.input(document.querySelector("#q-pk")!, { target: { value: "abc" } });

      mockSend.mockResolvedValueOnce({
        Items: [{ id: { S: "abc" } }],
        Count: 1,
        LastEvaluatedKey: { id: { S: "abc" } },
      });
      await fireEvent.click(screen.getByText("Execute Query"));
      await waitFor(() => expect(screen.getByText("abc")).toBeInTheDocument());

      const firstCall = findCall("QueryCommand", 0);
      expect(firstCall.input).toEqual({
        TableName: "Orders",
        KeyConditionExpression: "#pk = :pkval",
        ExpressionAttributeNames: { "#pk": "id" },
        ExpressionAttributeValues: { ":pkval": { S: "abc" } },
        Limit: 100,
        ScanIndexForward: true,
      });

      mockSend.mockResolvedValueOnce({
        Items: [{ id: { S: "xyz" } }],
        Count: 1,
        LastEvaluatedKey: undefined,
      });
      await fireEvent.click(screen.getByText("Load More"));

      await waitFor(() => expect(callCount("QueryCommand")).toBe(2));
      const secondCall = findCall("QueryCommand", 1);
      expect(secondCall.input).toEqual({
        TableName: "Orders",
        KeyConditionExpression: "#pk = :pkval",
        ExpressionAttributeNames: { "#pk": "id" },
        ExpressionAttributeValues: { ":pkval": { S: "abc" } },
        Limit: 100,
        ScanIndexForward: true,
        ExclusiveStartKey: { id: { S: "abc" } },
      });

      // Appended, not replaced -- both pages' rows are visible, and Load More is gone.
      expect(screen.getByText("abc")).toBeInTheDocument();
      await waitFor(() => expect(screen.getByText("xyz")).toBeInTheDocument());
      expect(screen.queryByText("Load More")).not.toBeInTheDocument();
    });

    it("shows the AWS error code in a toast when Query fails", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("Query"));
      await fireEvent.input(document.querySelector("#q-pk")!, { target: { value: "abc" } });

      const err = Object.assign(new Error("Requested resource not found"), {
        name: "ResourceNotFoundException",
        $metadata: { httpStatusCode: 400 },
      });
      mockSend.mockRejectedValueOnce(err);
      await fireEvent.click(screen.getByText("Execute Query"));

      const { toast } = await import("svelte-sonner");
      await waitFor(() => {
        expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
          "Query failed: ResourceNotFoundException (HTTP 400): Requested resource not found",
        );
      });
    });

    it("sends Scan's ExpressionAttributeValues parsed from the Filter Expression Values field", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("Scan"));

      await fireEvent.input(document.querySelector("#s-filter")!, {
        target: { value: "age > :minAge" },
      });
      await fireEvent.input(document.querySelector("#s-filter-values")!, {
        target: { value: JSON.stringify({ ":minAge": 18 }) },
      });

      mockSend.mockResolvedValueOnce({ Items: [], Count: 0, ScannedCount: 5 });
      await fireEvent.click(screen.getByText("Execute Scan"));

      await waitFor(() => expect(callCount("ScanCommand")).toBe(1));
      expect(findCall("ScanCommand").input).toEqual({
        TableName: "Orders",
        Limit: 100,
        FilterExpression: "age > :minAge",
        ExpressionAttributeValues: { ":minAge": { N: "18" } },
      });
    });

    it("renders a Scan result's boolean and number cells through the resultsTable snippet with exact text", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("Scan"));

      mockSend.mockResolvedValueOnce({
        Items: [{ id: { S: "row-1" }, count: { N: "7" }, active: { BOOL: false } }],
        Count: 1,
        ScannedCount: 1,
      });
      await fireEvent.click(screen.getByText("Execute Scan"));

      await waitFor(() => expect(screen.getByText("row-1")).toBeInTheDocument());
      // These come from the shared {#snippet resultsTable} block's {item[col] ?? ''} cell --
      // an `as Column<T>[]` cast or an arrow-function `render` elsewhere in this codebase has
      // silently rendered blank cells for exactly this class of value, so assert the literal
      // text landed in the DOM rather than merely checking the mock was called.
      expect(screen.getByText("7")).toBeInTheDocument();
      expect(screen.getByText("false")).toBeInTheDocument();
    });
  });

  describe("Table config tabs", () => {
    it("adds and removes a tag with the exact TagResource/UntagResource input", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({ Tags: [] });
      await fireEvent.click(screen.getByText("Tags"));
      await waitFor(() => expect(screen.getByText("No tags on this table.")).toBeInTheDocument());

      await fireEvent.input(document.querySelector("#tag-key")!, { target: { value: "Env" } });
      await fireEvent.input(document.querySelector("#tag-value")!, { target: { value: "prod" } });

      // TagResource
      mockSend.mockResolvedValueOnce({});
      // reload
      mockSend.mockResolvedValueOnce({ Tags: [{ Key: "Env", Value: "prod" }] });
      await fireEvent.click(screen.getByText("Add Tag"));

      await waitFor(() => expect(callCount("TagResourceCommand")).toBe(1));
      expect(findCall("TagResourceCommand").input).toEqual({
        ResourceArn: baseTable.TableArn,
        Tags: [{ Key: "Env", Value: "prod" }],
      });
      await waitFor(() => expect(screen.getByText("Env")).toBeInTheDocument());

      // UntagResource
      mockSend.mockResolvedValueOnce({});
      // reload
      mockSend.mockResolvedValueOnce({ Tags: [] });
      await fireEvent.click(screen.getByText("Remove"));

      await waitFor(() => expect(callCount("UntagResourceCommand")).toBe(1));
      expect(findCall("UntagResourceCommand").input).toEqual({
        ResourceArn: baseTable.TableArn,
        TagKeys: ["Env"],
      });
    });

    it("creates and deletes a backup with the exact CreateBackup/DeleteBackup input", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({ BackupSummaries: [] });
      await fireEvent.click(screen.getByText("Backups"));
      await waitFor(() =>
        expect(screen.getByText("No backups found for this table.")).toBeInTheDocument(),
      );

      await fireEvent.input(document.querySelector("#backup-name")!, {
        target: { value: "nightly" },
      });
      // CreateBackup
      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({
        BackupSummaries: [
          { BackupName: "nightly", BackupArn: "arn:aws:dynamodb:::backup/nightly" },
        ],
      });
      await fireEvent.click(screen.getByText("Create Backup"));

      await waitFor(() => expect(callCount("CreateBackupCommand")).toBe(1));
      expect(findCall("CreateBackupCommand").input).toEqual({
        TableName: "Orders",
        BackupName: "nightly",
      });
      await waitFor(() => expect(screen.getByText("nightly")).toBeInTheDocument());

      // DeleteBackup
      mockSend.mockResolvedValueOnce({});
      // reload
      mockSend.mockResolvedValueOnce({ BackupSummaries: [] });
      await fireEvent.click(screen.getByText("Delete"));

      await waitFor(() => expect(callCount("DeleteBackupCommand")).toBe(1));
      expect(findCall("DeleteBackupCommand").input).toEqual({
        BackupArn: "arn:aws:dynamodb:::backup/nightly",
      });
    });

    it("toggles PITR via the exact UpdateContinuousBackups PointInTimeRecoverySpecification", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({
        ContinuousBackupsDescription: {
          PointInTimeRecoveryDescription: { PointInTimeRecoveryStatus: "DISABLED" },
        },
      });
      await fireEvent.click(screen.getByText("PITR"));
      await waitFor(() => expect(screen.getByText("Enable PITR")).toBeInTheDocument());

      // UpdateContinuousBackups
      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({
        ContinuousBackupsDescription: {
          PointInTimeRecoveryDescription: { PointInTimeRecoveryStatus: "ENABLED" },
        },
        // reload
      });
      await fireEvent.click(screen.getByText("Enable PITR"));

      await waitFor(() => expect(callCount("UpdateContinuousBackupsCommand")).toBe(1));
      expect(findCall("UpdateContinuousBackupsCommand").input).toEqual({
        TableName: "Orders",
        PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
      });
    });

    it("hydrates the table and renders the PITR panel from a ?table=X&tab=pitr deep link, with no click at all", async () => {
      setMockPageUrl(new URL("http://localhost/dynamodb?table=Orders&tab=pitr"));
      // Both the table-list load and the deep-link hydration effect fire on
      // mount, so their DDB calls interleave rather than following the
      // strict sequence openOrdersTable()'s click-driven flow relies on.
      // Dispatch by command type instead of by call order.
      mockSend.mockImplementation((command: unknown) => {
        switch (commandName(command)) {
          case "ListTablesCommand":
            return Promise.resolve({ TableNames: ["Orders"] });
          case "DescribeTableCommand":
            return Promise.resolve({ Table: baseTable });
          case "DescribeTimeToLiveCommand":
            return Promise.resolve({ TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" } });
          case "DescribeContinuousBackupsCommand":
            return Promise.resolve({
              ContinuousBackupsDescription: {
                PointInTimeRecoveryDescription: { PointInTimeRecoveryStatus: "ENABLED" },
              },
            });
          default:
            return Promise.resolve({});
        }
      });

      render(DynamoDBPage);

      // The PITR panel is visible straight from the URL: no row click, no tab click.
      await waitFor(() => expect(screen.getByText("Enabled")).toBeInTheDocument(), {
        timeout: 3000,
      });
      expect(screen.getByText("Delete Table")).toBeInTheDocument();
      expect(callCount("DescribeContinuousBackupsCommand")).toBe(1);
    });

    it("shows an error alert and an 'Unknown' status when DescribeContinuousBackups rejects, not a false 'Disabled'", async () => {
      await openOrdersTable();
      mockSend.mockRejectedValueOnce(new Error("Throttled"));
      await fireEvent.click(screen.getByText("PITR"));

      await waitFor(() =>
        expect(screen.getByText("Failed to load continuous-backups status")).toBeInTheDocument(),
      );
      expect(screen.getByText(/Throttled/)).toBeInTheDocument();
      expect(screen.getByText("Unknown")).toBeInTheDocument();
      expect(screen.queryByText("Disabled")).not.toBeInTheDocument();
    });

    it("clicking a table row sets ?table= in the URL and resets the tab to overview", async () => {
      mockSend.mockResolvedValueOnce({ TableNames: ["Orders", "Users"] });
      mockSend.mockResolvedValueOnce({ Table: baseTable });
      mockSend.mockResolvedValueOnce({
        Table: {
          TableName: "Users",
          TableStatus: "ACTIVE",
          ItemCount: 1,
          KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        },
      });

      render(DynamoDBPage);
      await waitFor(() => expect(screen.getByText("Orders")).toBeInTheDocument(), {
        timeout: 3000,
      });

      mockSend.mockResolvedValueOnce({ Table: baseTable });
      mockSend.mockResolvedValueOnce({ TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" } });
      await fireEvent.click(screen.getByText("Orders"));
      await waitFor(() => expect(screen.getByText("Delete Table")).toBeInTheDocument());

      mockSend.mockResolvedValueOnce({ BackupSummaries: [] });
      await fireEvent.click(screen.getByText("Backups"));
      await waitFor(() => expect(getMockPage().url.searchParams.get("tab")).toBe("backups"));
      expect(getMockPage().url.searchParams.get("table")).toBe("Orders");

      await fireEvent.click(screen.getByText("Tables"));
      await waitFor(() => expect(screen.getByText("Users")).toBeInTheDocument());

      mockSend.mockResolvedValueOnce({
        Table: {
          TableName: "Users",
          TableStatus: "ACTIVE",
          ItemCount: 1,
          KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        },
      });
      mockSend.mockResolvedValueOnce({ TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" } });
      await fireEvent.click(screen.getByText("Users"));
      await waitFor(() => expect(getMockPage().url.searchParams.get("table")).toBe("Users"));
      expect(getMockPage().url.searchParams.get("tab")).toBeNull();
    });

    // The global `goto` mock (vitest.setup.ts) defers its `page.url` write
    // by a microtask, matching the real `@sveltejs/kit` client runtime:
    // `goto()` reassigns `page.url` only after `await navigate(...)`
    // resolves, deep inside its async pipeline -- never synchronously
    // before returning. A row-click handler that calls `.set()` on two
    // different urlState instances back to back (no `await` between them)
    // has both `set()` calls compute their next URL from the SAME
    // `page.url` snapshot, since neither `goto()` has had a chance to
    // land before the second snapshot is taken -- so the second `goto()`
    // clobbers the first instead of composing with it. This test proves
    // the fix (`setUrlParams()`, which computes one URL from one snapshot
    // and fires a single `goto()`) actually closes that gap, using the
    // exact browser repro that surfaced the bug: land on `?tab=pitr` with
    // no table selected, then click a row.
    it("does not lose the tab reset to a same-tick goto() race when selecting a table from a URL that already has ?tab=", async () => {
      setMockPageUrl(new URL("http://localhost/dynamodb?tab=pitr"));

      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });
      mockSend.mockResolvedValueOnce({ Table: baseTable });
      render(DynamoDBPage);
      await waitFor(() => expect(screen.getByText("Orders")).toBeInTheDocument(), {
        timeout: 3000,
      });

      mockSend.mockResolvedValueOnce({ Table: baseTable });
      mockSend.mockResolvedValueOnce({ TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" } });
      await fireEvent.click(screen.getByText("Orders"));

      await waitFor(() => expect(getMockPage().url.searchParams.get("table")).toBe("Orders"));
      // The real bug this reproduces: a stale-snapshot second set() call
      // leaves the old `tab=pitr` in the URL instead of resetting it, so
      // the table page opens back onto the PITR tab instead of Overview.
      expect(getMockPage().url.searchParams.get("tab")).toBeNull();
    });

    it("updates TTL via the exact UpdateTimeToLive TimeToLiveSpecification", async () => {
      await openOrdersTable();
      await fireEvent.input(document.querySelector("#ttl-attr")!, {
        target: { value: "expiresAt" },
      });
      await fireEvent.click(document.querySelector("#ttl-enabled")!);

      mockSend.mockResolvedValueOnce({});
      await fireEvent.click(screen.getByText("Update TTL"));

      await waitFor(() => expect(callCount("UpdateTimeToLiveCommand")).toBe(1));
      expect(findCall("UpdateTimeToLiveCommand").input).toEqual({
        TableName: "Orders",
        TimeToLiveSpecification: { Enabled: true, AttributeName: "expiresAt" },
      });
    });

    it("creates a GSI via the exact UpdateTable GlobalSecondaryIndexUpdates", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("Indexes"));
      await fireEvent.click(screen.getByText("Create GSI"));

      const dialog = screen.getByRole("dialog");
      await fireEvent.input(within(dialog).getByLabelText(/Index Name/), {
        target: { value: "byEmail" },
      });
      await fireEvent.input(within(dialog).getByLabelText(/Partition Key/), {
        target: { value: "email" },
      });

      // UpdateTable
      mockSend.mockResolvedValueOnce({});
      // refreshTable's DescribeTable
      mockSend.mockResolvedValueOnce({ Table: baseTable });
      await fireEvent.click(dialog.querySelector('button[type="submit"]')!);

      await waitFor(() => expect(callCount("UpdateTableCommand")).toBe(1));
      expect(findCall("UpdateTableCommand").input).toEqual({
        TableName: "Orders",
        AttributeDefinitions: [{ AttributeName: "email", AttributeType: "S" }],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: "byEmail",
              KeySchema: [{ AttributeName: "email", KeyType: "HASH" }],
              Projection: { ProjectionType: "ALL" },
            },
          },
        ],
      });
    });

    it("adds a replica via the exact UpdateTable ReplicaUpdates", async () => {
      await openOrdersTable();
      mockSend.mockResolvedValueOnce({ TableAutoScalingDescription: { Replicas: [] } });
      await fireEvent.click(screen.getByText("Replicas"));
      await waitFor(() =>
        expect(
          screen.getByText("No replicas configured. This table is not a global table."),
        ).toBeInTheDocument(),
      );

      await fireEvent.input(document.querySelector("#new-replica-region")!, {
        target: { value: "eu-west-1" },
      });
      // UpdateTable
      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({
        TableAutoScalingDescription: {
          Replicas: [{ RegionName: "eu-west-1", ReplicaStatus: "CREATING" }],
        },
        // reload
      });
      await fireEvent.click(screen.getByRole("button", { name: "Add Replica" }));

      await waitFor(() => expect(callCount("UpdateTableCommand")).toBe(1));
      expect(findCall("UpdateTableCommand").input).toEqual({
        TableName: "Orders",
        ReplicaUpdates: [{ Create: { RegionName: "eu-west-1" } }],
      });
    });

    it("updates capacity via the exact UpdateTable BillingMode/ProvisionedThroughput", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByLabelText("Provisioned"));
      await fireEvent.input(document.querySelector("#edit-rcu")!, { target: { value: "15" } });
      await fireEvent.input(document.querySelector("#edit-wcu")!, { target: { value: "25" } });

      // UpdateTable
      mockSend.mockResolvedValueOnce({});
      // loadTables
      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });
      mockSend.mockResolvedValueOnce({ Table: baseTable });
      await fireEvent.click(screen.getByText("Update Capacity"));

      await waitFor(() => expect(callCount("UpdateTableCommand")).toBe(1));
      expect(findCall("UpdateTableCommand").input).toEqual({
        TableName: "Orders",
        BillingMode: "PROVISIONED",
        ProvisionedThroughput: { ReadCapacityUnits: 15, WriteCapacityUnits: 25 },
      });
    });

    it("runs PartiQL via the exact ExecuteStatement Statement", async () => {
      await openOrdersTable();
      await fireEvent.click(screen.getByText("PartiQL"));

      mockSend.mockResolvedValueOnce({ Items: [{ id: { S: "p-1" } }] });
      await fireEvent.click(screen.getByText("Execute"));

      await waitFor(() => expect(callCount("ExecuteStatementCommand")).toBe(1));
      expect(findCall("ExecuteStatementCommand").input).toEqual({
        Statement: 'SELECT * FROM "Orders"',
      });
      await waitFor(() => expect(screen.getByText("p-1")).toBeInTheDocument());
    });

    it("exports the table to S3 via ExportTableToPointInTime using the prompted bucket name", async () => {
      await openOrdersTable();
      vi.stubGlobal("prompt", vi.fn().mockReturnValue("my-export-bucket"));

      mockSend.mockResolvedValueOnce({});
      await fireEvent.click(screen.getByText("Export to S3"));

      await waitFor(() => expect(callCount("ExportTableToPointInTimeCommand")).toBe(1));
      expect(findCall("ExportTableToPointInTimeCommand").input).toEqual({
        TableArn: baseTable.TableArn,
        S3Bucket: "my-export-bucket",
        ExportFormat: "DYNAMODB_JSON",
      });

      vi.unstubAllGlobals();
    });
  });

  describe("All regions mode", () => {
    it("fans ListTables out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });
      mockSend.mockResolvedValueOnce({ TableNames: ["Users"] });

      render(DynamoDBPage);

      await waitFor(() => expect(screen.getByText("Orders")).toBeInTheDocument());
      expect(screen.getByText("Users")).toBeInTheDocument();
      expect(callCount("ListTablesCommand")).toBe(2);
      // The table list is fanned out only -- no per-table DescribeTable
      // fan-out yet, so exactly the 2 ListTables calls above are expected.
      expect(callCount("DescribeTableCommand")).toBe(0);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });

      render(DynamoDBPage);

      await waitFor(() => expect(screen.getByText("Orders")).toBeInTheDocument());
      expect(callCount("ListTablesCommand")).toBe(1);

      vi.unstubAllGlobals();
    });

    it("opening a row pins the picker to that row's region and single-region-loads its detail", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["eu-west-1"]);
      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });

      render(DynamoDBPage);
      await waitFor(() => expect(screen.getByText("Orders")).toBeInTheDocument());

      mockSend.mockResolvedValueOnce({ TableNames: ["Orders"] });
      mockSend.mockResolvedValueOnce({ Table: baseTable });
      mockSend.mockResolvedValueOnce({ Table: baseTable });
      mockSend.mockResolvedValueOnce({ TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" } });

      await fireEvent.click(screen.getByText("Orders"));

      await waitFor(() => expect(screen.getByText("Delete Table")).toBeInTheDocument(), {
        timeout: 3000,
      });

      vi.unstubAllGlobals();
    });
  });
});
