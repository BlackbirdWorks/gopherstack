import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import TableDetailPage from "./+page.svelte";

const mockSend = vi.fn();
let mockParams = { tableName: "TestTable" };

function commandName(command: unknown): string {
  return (command as { constructor: { name: string } }).constructor.name;
}

function findCall(name: string, index = 0): { input: Record<string, unknown> } {
  const calls = mockSend.mock.calls.filter((c) => commandName(c[0]) === name);
  return calls[index][0] as { input: Record<string, unknown> };
}

vi.mock("$app/state", () => ({
  get page() {
    return { params: mockParams };
  },
}));

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDynamoDBClient: () => ({ send: mockSend }),
}));

vi.mock("$lib/dynamodb", () => ({
  itemToJson: (item: Record<string, unknown>) => item,
  avToJson: (av: unknown) => {
    if (av && typeof av === "object" && "S" in av) return (av as { S: string }).S;
    if (av && typeof av === "object" && "N" in av) return Number((av as { N: string }).N);
    return String(av);
  },
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const tableResponse = {
  Table: {
    TableName: "TestTable",
    TableStatus: "ACTIVE",
    ItemCount: 5,
    TableSizeBytes: 1024,
    TableArn: "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable",
    TableId: "abc123",
    BillingModeSummary: { BillingMode: "PAY_PER_REQUEST" },
    KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
    AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    GlobalSecondaryIndexes: [],
    LocalSecondaryIndexes: [],
    CreationDateTime: new Date("2024-01-01"),
  },
};

const ttlDisabledResponse = {
  TimeToLiveDescription: { TimeToLiveStatus: "DISABLED" },
};

const ttlEnabledResponse = {
  TimeToLiveDescription: { TimeToLiveStatus: "ENABLED", AttributeName: "expiry" },
};

function defaultMocks(): void {
  mockSend.mockResolvedValueOnce(ttlDisabledResponse);
  mockSend.mockResolvedValueOnce(tableResponse);
}

describe("DynamoDB Table Detail Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockParams = { tableName: "TestTable" };
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders table name in heading", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("TestTable")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("renders breadcrumb link back to DynamoDB tables", () => {
    defaultMocks();
    render(TableDetailPage);
    expect(screen.getByText("DynamoDB Tables")).toBeInTheDocument();
  });

  it("shows Delete Table button", () => {
    defaultMocks();
    render(TableDetailPage);
    expect(screen.getByText("Delete Table")).toBeInTheDocument();
  });

  it("shows TTL DISABLED in status card initially", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        const card = document.querySelector("#ttl-status-card");
        expect(card).toBeInTheDocument();
        expect(card?.textContent).toContain("DISABLED");
      },
      { timeout: 3000 },
    );
  });

  it("shows TTL ENABLED with attribute name when TTL is enabled", async () => {
    mockSend.mockResolvedValueOnce(ttlEnabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);
    render(TableDetailPage);
    await waitFor(
      () => {
        const card = document.querySelector("#ttl-status-card");
        expect(card?.textContent).toContain("ENABLED");
        expect(card?.textContent).toContain("expiry");
      },
      { timeout: 3000 },
    );
  });

  it("shows item count and table size stat cards", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("5")).toBeInTheDocument();
        expect(screen.getByText("1.0 KB")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows ACTIVE status badge", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("ACTIVE")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows PAY_PER_REQUEST billing mode", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("PAY_PER_REQUEST")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows table ARN", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText(/arn:aws:dynamodb/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("renders Overview, Items, Query, Backups, Stream Events, PartiQL tabs", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Overview")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("Items")).toBeInTheDocument();
    expect(screen.getByText("Query")).toBeInTheDocument();
    expect(screen.getByText("Backups")).toBeInTheDocument();
    expect(screen.getByText("Stream Events")).toBeInTheDocument();
    expect(screen.getByText("PartiQL")).toBeInTheDocument();
  });

  it("shows TTL form controls in Overview tab", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByLabelText("Enable TTL")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("Update TTL")).toBeInTheDocument();
    expect(document.querySelector("#ttl-enabled")).toBeInTheDocument();
    expect(document.querySelector('input[name="attributeName"]')).toBeInTheDocument();
  });

  it("shows streams form controls in Overview tab", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Update Streams")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(document.querySelector("#streams-enabled")).toBeInTheDocument();
  });

  it("calls UpdateTimeToLive and shows success toast when TTL enabled", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce(ttlEnabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);

    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Update TTL")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(document.querySelector("#ttl-enabled")!);
    await fireEvent.click(screen.getByText("Update TTL"));

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(toast.success).toHaveBeenCalledWith("TTL enabled successfully");
      },
      { timeout: 3000 },
    );
  });

  it("calls UpdateTimeToLive and shows disabled toast when TTL disabled", async () => {
    mockSend.mockResolvedValueOnce(ttlEnabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce(ttlDisabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);

    render(TableDetailPage);
    await waitFor(
      () => {
        expect(document.querySelector("#ttl-status-card")?.textContent).toContain("ENABLED");
      },
      { timeout: 3000 },
    );

    // uncheck the checkbox (already checked because TTL is enabled)
    await fireEvent.click(document.querySelector("#ttl-enabled")!);
    await fireEvent.click(screen.getByText("Update TTL"));

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(toast.success).toHaveBeenCalledWith("TTL disabled successfully");
      },
      { timeout: 3000 },
    );
  });

  it("loads and displays items when Items tab is clicked", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      Items: [{ id: { S: "item-1" }, name: { S: "Alice" } }],
      Count: 1,
      LastEvaluatedKey: undefined,
    });

    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Items")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("Items"));
    await waitFor(
      () => {
        expect(screen.getByText("item-1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it('shows "No items found" empty state', async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({ Items: [], Count: 0 });

    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Items")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    await fireEvent.click(screen.getByText("Items"));
    await waitFor(
      () => {
        expect(screen.getByText(/No items found/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Query tab with PK input", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Query")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    await fireEvent.click(screen.getByText("Query"));
    expect(screen.getByText("Run Query")).toBeInTheDocument();
    expect(screen.getByText("Partition Key Value")).toBeInTheDocument();
  });

  it("runs query and displays results", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      Items: [{ id: { S: "q-1" }, val: { S: "result" } }],
      Count: 1,
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Query")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Query"));

    const pkInput = screen.getAllByPlaceholderText("value")[0];
    await fireEvent.input(pkInput, { target: { value: "mykey" } });
    await fireEvent.click(screen.getByText("Run Query"));

    await waitFor(
      () => {
        expect(screen.getByText("q-1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads backups when Backups tab is clicked", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      BackupSummaries: [
        {
          BackupName: "backup-1",
          BackupStatus: "AVAILABLE",
          BackupCreationDateTime: new Date("2024-01-01"),
          BackupSizeBytes: 512,
        },
      ],
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Backups")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Backups"));
    await waitFor(
      () => {
        expect(screen.getByText("backup-1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens and cancels delete confirmation dialog", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(
      () => {
        expect(screen.getByText("Delete Table")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("Delete Table"));
    expect(screen.getByPlaceholderText("TestTable")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(
      () => {
        expect(screen.queryByPlaceholderText("TestTable")).not.toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows the AWS error code and status in the inline banner on load failure", async () => {
    const err = Object.assign(new Error("Requested resource not found"), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(err);

    render(TableDetailPage);
    await waitFor(
      () => {
        expect(
          screen.getByText("ResourceNotFoundException (HTTP 400): Requested resource not found"),
        ).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("Retry")).toBeInTheDocument();
  });

  it("shows PartiQL editor with execute button", async () => {
    defaultMocks();
    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("PartiQL")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("PartiQL"));
    expect(document.querySelector("#partiql-execute")).toBeInTheDocument();
    expect(document.querySelector("#partiql-output")).toBeInTheDocument();
  });

  it("executes PartiQL and shows output", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({ Items: [{ id: { S: "partiql-item" } }] });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("PartiQL")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("PartiQL"));

    await fireEvent.click(document.querySelector("#partiql-execute")!);
    await waitFor(
      () => {
        expect(document.querySelector("#partiql-output")?.textContent).toContain("partiql-item");
      },
      { timeout: 3000 },
    );
  });

  it("shows Export CSV button when items are loaded", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      Items: [{ id: { S: "row-1" }, val: { N: "42" } }],
      Count: 1,
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Items")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Items"));
    await waitFor(
      () => {
        expect(screen.getByText("Export CSV")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("filters items client-side using the filter input", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      Items: [{ id: { S: "apple" } }, { id: { S: "banana" } }],
      Count: 2,
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Items")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Items"));
    await waitFor(() => expect(screen.getByText("apple")).toBeInTheDocument(), { timeout: 3000 });

    const filterInput = screen.getByPlaceholderText("Filter…");
    await fireEvent.input(filterInput, { target: { value: "apple" } });
    await waitFor(
      () => {
        expect(screen.getByText("apple")).toBeInTheDocument();
        expect(screen.queryByText("banana")).not.toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Backups tab and loads backups", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      BackupSummaries: [
        {
          BackupName: "my-backup",
          BackupStatus: "AVAILABLE",
          BackupArn: "arn:aws:dynamodb:::table/t/backup/b1",
        },
      ],
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Backups")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Backups"));
    await waitFor(
      () => {
        expect(screen.getByText("my-backup")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("sends the exact UpdateTimeToLive input when enabling TTL", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce(ttlEnabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Update TTL")).toBeInTheDocument(), {
      timeout: 3000,
    });

    await fireEvent.input(document.querySelector('input[name="attributeName"]')!, {
      target: { value: "expiry" },
    });
    await fireEvent.click(document.querySelector("#ttl-enabled")!);
    await fireEvent.click(screen.getByText("Update TTL"));

    await waitFor(() => {
      expect(mockSend.mock.calls.some((c) => commandName(c[0]) === "UpdateTimeToLiveCommand")).toBe(
        true,
      );
    });
    const call = findCall("UpdateTimeToLiveCommand");
    expect(call.input).toEqual({
      TableName: "TestTable",
      TimeToLiveSpecification: { AttributeName: "expiry", Enabled: true },
    });
  });

  it("sends the exact UpdateTable StreamSpecification when enabling streams", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce(ttlDisabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Update Streams")).toBeInTheDocument(), {
      timeout: 3000,
    });

    await fireEvent.click(document.querySelector("#streams-enabled")!);
    await fireEvent.click(screen.getByText("Update Streams"));

    await waitFor(() => {
      expect(mockSend.mock.calls.some((c) => commandName(c[0]) === "UpdateTableCommand")).toBe(
        true,
      );
    });
    const call = findCall("UpdateTableCommand");
    expect(call.input).toEqual({
      TableName: "TestTable",
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
    });
  });

  it("shows the AWS error code in a toast when UpdateTable (streams) fails", async () => {
    defaultMocks();
    const err = Object.assign(new Error("Streaming is already enabled"), {
      name: "ResourceInUseException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(err);
    mockSend.mockResolvedValueOnce(ttlDisabledResponse);
    mockSend.mockResolvedValueOnce(tableResponse);

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Update Streams")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Update Streams"));

    const { toast } = await import("svelte-sonner");
    await waitFor(() => {
      expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
        "ResourceInUseException (HTTP 400): Streaming is already enabled",
      );
    });
  });

  it("sends the exact CreateBackup input and shows the created backup", async () => {
    defaultMocks();
    // loadBackups() on tab switch
    mockSend.mockResolvedValueOnce({ BackupSummaries: [] });
    // CreateBackup response
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      BackupSummaries: [{ BackupName: "nightly-1", BackupStatus: "AVAILABLE" }],
      // loadBackups() after create
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Backups")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Backups"));

    const nameInput = screen.getByPlaceholderText("Backup name (optional)");
    await fireEvent.input(nameInput, { target: { value: "nightly-1" } });
    await fireEvent.click(screen.getByText("Create Backup"));

    await waitFor(() => {
      expect(mockSend.mock.calls.some((c) => commandName(c[0]) === "CreateBackupCommand")).toBe(
        true,
      );
    });
    const call = findCall("CreateBackupCommand");
    expect(call.input).toEqual({ TableName: "TestTable", BackupName: "nightly-1" });

    await waitFor(() => expect(screen.getByText("nightly-1")).toBeInTheDocument());
  });

  it("shows the AWS error code in a toast when CreateBackup fails", async () => {
    defaultMocks();
    // loadBackups() on tab switch
    mockSend.mockResolvedValueOnce({ BackupSummaries: [] });
    const err = Object.assign(new Error("Backup already exists"), {
      name: "BackupInUseException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(err);

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Backups")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Backups"));
    await fireEvent.input(screen.getByPlaceholderText("Backup name (optional)"), {
      target: { value: "dup" },
    });
    await fireEvent.click(screen.getByText("Create Backup"));

    const { toast } = await import("svelte-sonner");
    await waitFor(() => {
      expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
        "BackupInUseException (HTTP 400): Backup already exists",
      );
    });
  });

  it("sends the exact key-typed Query input (Number partition key) and shows results", async () => {
    const numericTable = {
      Table: {
        ...tableResponse.Table,
        AttributeDefinitions: [{ AttributeName: "id", AttributeType: "N" }],
      },
    };
    mockSend.mockResolvedValueOnce(ttlDisabledResponse);
    mockSend.mockResolvedValueOnce(numericTable);
    mockSend.mockResolvedValueOnce({ Items: [{ id: { N: "42" } }], Count: 1 });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Query")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Query"));

    const pkInput = screen.getAllByPlaceholderText("value")[0];
    await fireEvent.input(pkInput, { target: { value: "42" } });
    await fireEvent.click(screen.getByText("Run Query"));

    await waitFor(() => {
      expect(mockSend.mock.calls.some((c) => commandName(c[0]) === "QueryCommand")).toBe(true);
    });
    const call = findCall("QueryCommand");
    expect(call.input).toEqual({
      TableName: "TestTable",
      IndexName: undefined,
      KeyConditionExpression: "#pk = :pkVal",
      ExpressionAttributeNames: { "#pk": "id" },
      ExpressionAttributeValues: { ":pkVal": { N: "42" } },
    });
  });

  it("shows the AWS error code in a toast when Query fails", async () => {
    defaultMocks();
    const err = Object.assign(new Error("KeyConditionExpression is invalid"), {
      name: "ValidationException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(err);

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Query")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Query"));
    await fireEvent.input(screen.getAllByPlaceholderText("value")[0], {
      target: { value: "x" },
    });
    await fireEvent.click(screen.getByText("Run Query"));

    const { toast } = await import("svelte-sonner");
    await waitFor(() => {
      expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
        "ValidationException (HTTP 400): KeyConditionExpression is invalid",
      );
    });
  });

  it("paginates Items via Load more: sends ExclusiveStartKey and appends rather than replaces", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({
      Items: [{ id: { S: "item-1" } }],
      LastEvaluatedKey: { id: { S: "item-1" } },
    });
    mockSend.mockResolvedValueOnce({
      Items: [{ id: { S: "item-2" } }],
      LastEvaluatedKey: undefined,
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Items")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Items"));
    await waitFor(() => expect(screen.getByText("item-1")).toBeInTheDocument());

    const loadMore = screen.getByText("Load more");
    await fireEvent.click(loadMore);

    await waitFor(() => {
      expect(mockSend.mock.calls.filter((c) => commandName(c[0]) === "ScanCommand").length).toBe(2);
    });
    const secondScan = findCall("ScanCommand", 1);
    expect(secondScan.input).toEqual({
      TableName: "TestTable",
      Limit: 50,
      ExclusiveStartKey: { id: { S: "item-1" } },
    });

    // Both pages' items are visible -- appended, not replaced.
    expect(screen.getByText("item-1")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("item-2")).toBeInTheDocument());
    expect(screen.queryByText("Load more")).not.toBeInTheDocument();
  });

  it("submits DeleteTable with the exact TableName once the name is typed to confirm", async () => {
    defaultMocks();
    mockSend.mockResolvedValueOnce({});
    const originalLocation = window.location;
    // jsdom's window.location isn't directly assignable; stub it for this test so the
    // page's `window.location.href = ...` redirect after delete doesn't throw.
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { ...originalLocation, href: "" },
    });

    render(TableDetailPage);
    await waitFor(() => expect(screen.getByText("Delete Table")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Delete Table"));

    const input = screen.getByPlaceholderText("TestTable");
    await fireEvent.input(input, { target: { value: "TestTable" } });
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    await waitFor(() => {
      expect(mockSend.mock.calls.some((c) => commandName(c[0]) === "DeleteTableCommand")).toBe(
        true,
      );
    });
    expect(findCall("DeleteTableCommand").input).toEqual({ TableName: "TestTable" });

    Object.defineProperty(window, "location", { configurable: true, value: originalLocation });
  });
});
