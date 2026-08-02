import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import TimestreamQueryPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getTimestreamQueryClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleScheduledQuery = {
  Name: "my-scheduled-query",
  Arn: "arn:aws:timestream:us-east-1:123456789012:scheduled-query/my-scheduled-query-abc123",
  State: "ENABLED",
};

// Renders the page and resolves the two calls fired unconditionally by the
// mount-time onRegionChange effect (ListScheduledQueries, then
// DescribeAccountSettings), regardless of which tab is active.
async function renderPage(scheduled: unknown[] = [], settings: Record<string, unknown> = {}) {
  mockSend.mockResolvedValueOnce({ ScheduledQueries: scheduled });
  mockSend.mockResolvedValueOnce(settings);
  render(TimestreamQueryPage);
  await waitFor(() => {
    expect(mockSend).toHaveBeenCalledTimes(2);
  });
}

describe("Timestream Query Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title and all tabs", async () => {
    await renderPage();
    expect(screen.getByText("Timestream Query")).toBeInTheDocument();
    expect(screen.getByText("SQL Query")).toBeInTheDocument();
    expect(screen.getByText("Scheduled Queries")).toBeInTheDocument();
    expect(screen.getByText("Account Settings")).toBeInTheDocument();
  });

  it("loads scheduled queries and account settings unconditionally on mount", async () => {
    await renderPage();
    expect(mockSend).toHaveBeenNthCalledWith(1, expect.objectContaining({ input: {} }));
    expect(mockSend).toHaveBeenNthCalledWith(2, expect.objectContaining({ input: {} }));
  });

  it("runs a SQL query and renders the result columns and rows", async () => {
    await renderPage();

    mockSend.mockResolvedValueOnce({
      QueryId: "query-1",
      ColumnInfo: [{ Name: "time", Type: { ScalarType: "TIMESTAMP" } }],
      Rows: [{ Data: [{ ScalarValue: "2024-01-01 00:00:00.000000000" }] }],
      QueryStatus: {
        ProgressPercentage: 100,
        CumulativeBytesScanned: 1048576,
        CumulativeBytesMetered: 2097152,
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: /Run Query/ }));

    await waitFor(() => {
      expect(screen.getByText("2024-01-01 00:00:00.000000000")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          QueryString: 'SELECT * FROM "mydb"."mytable" ORDER BY time DESC LIMIT 100',
          MaxRows: 100,
          QueryInsights: { Mode: "ENABLED_WITH_RATE_CONTROL" },
        },
      }),
    );
    expect(screen.getByText("TIMESTAMP")).toBeInTheDocument();
    expect(screen.getByText("Progress: 100%")).toBeInTheDocument();
    expect(screen.getByText("Scanned: 1.00 MB")).toBeInTheDocument();
  });

  it("shows an inline error and a toast when a query fails", async () => {
    await renderPage();

    const error = Object.assign(new Error("Syntax error at line 1."), {
      name: "InvalidEndpointException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: /Run Query/ }));

    await waitFor(() => {
      expect(
        screen.getByText("InvalidEndpointException: Syntax error at line 1."),
      ).toBeInTheDocument();
    });
    expect(toast.error).toHaveBeenCalledWith(
      "Query failed: InvalidEndpointException: Syntax error at line 1.",
    );
  });

  it("pages forward with the Next button, replacing the result rows with the new page and carrying the NextToken", async () => {
    await renderPage();

    mockSend.mockResolvedValueOnce({
      QueryId: "query-1",
      ColumnInfo: [{ Name: "time", Type: { ScalarType: "TIMESTAMP" } }],
      Rows: [{ Data: [{ ScalarValue: "row-1" }] }],
      NextToken: "page-2-token",
    });
    await fireEvent.click(screen.getByRole("button", { name: /Run Query/ }));
    await waitFor(() => screen.getByText("row-1"));

    mockSend.mockResolvedValueOnce({
      Rows: [{ Data: [{ ScalarValue: "row-2" }] }],
      NextToken: null,
    });
    await fireEvent.click(screen.getByRole("button", { name: "Next" }));

    await waitFor(() => {
      expect(screen.getByText("row-2")).toBeInTheDocument();
    });
    // The page swaps to the new page rather than appending.
    expect(screen.queryByText("row-1")).not.toBeInTheDocument();
    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: {
          QueryString: 'SELECT * FROM "mydb"."mytable" ORDER BY time DESC LIMIT 100',
          NextToken: "page-2-token",
          MaxRows: 100,
        },
      }),
    );
  });

  it("prepares a query and reports the discovered column and parameter counts", async () => {
    await renderPage();

    mockSend.mockResolvedValueOnce({
      Columns: [{ Name: "time" }, { Name: "value" }],
      Parameters: [{ Name: "p1" }],
    });
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Prepare" }));

    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith("Prepared: 2 columns, 1 parameters");
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { QueryString: 'SELECT * FROM "mydb"."mytable" ORDER BY time DESC LIMIT 100' },
      }),
    );
  });

  it("lists scheduled queries, rendering name, state, and ARN", async () => {
    await renderPage([exampleScheduledQuery]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));

    expect(screen.getByText("my-scheduled-query")).toBeInTheDocument();
    expect(screen.getByText("ENABLED")).toBeInTheDocument();
    expect(screen.getByText(exampleScheduledQuery.Arn)).toBeInTheDocument();
  });

  it("shows empty state when there are no scheduled queries", async () => {
    await renderPage([]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));

    expect(screen.getByText("No scheduled queries")).toBeInTheDocument();
  });

  it("shows a toast error including the AWS error code when the initial scheduled-query load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), { name: "ThrottlingException" });
    mockSend.mockRejectedValueOnce(error);
    mockSend.mockResolvedValueOnce({});
    const { toast } = await import("svelte-sonner");

    render(TimestreamQueryPage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load scheduled queries: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("refuses to create a scheduled query with blank required fields", async () => {
    await renderPage();
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "New" }));
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(toast.error).toHaveBeenCalledWith("Name, query, and schedule are required");
    // Only the two mount-time calls -- no CreateScheduledQuery.
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("creates a scheduled query, omitting notification/error-report/target configuration when left blank", async () => {
    await renderPage([]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("No scheduled queries"));

    await fireEvent.click(screen.getByRole("button", { name: "New" }));
    await fireEvent.input(screen.getByLabelText("Name *"), {
      target: { value: "my-scheduled-query" },
    });
    await fireEvent.input(screen.getByLabelText("Query String *"), {
      target: { value: 'SELECT * FROM "mydb"."mytable"' },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ScheduledQueries: [exampleScheduledQuery] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-scheduled-query")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Name: "my-scheduled-query",
          QueryString: 'SELECT * FROM "mydb"."mytable"',
          ScheduledQueryExecutionRoleArn: "",
          ScheduleConfiguration: { ScheduleExpression: "rate(1 hour)" },
          NotificationConfiguration: undefined,
          ErrorReportConfiguration: undefined,
          TargetConfiguration: undefined,
        },
      }),
    );
  });

  it("creates a scheduled query with notification, error-report, and target configuration when filled in", async () => {
    await renderPage([]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("No scheduled queries"));

    await fireEvent.click(screen.getByRole("button", { name: "New" }));
    await fireEvent.input(screen.getByLabelText("Name *"), {
      target: { value: "my-scheduled-query" },
    });
    await fireEvent.input(screen.getByLabelText("Query String *"), {
      target: { value: 'SELECT * FROM "mydb"."mytable"' },
    });
    await fireEvent.input(screen.getByLabelText("Execution Role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/tsq-role" },
    });
    await fireEvent.input(screen.getByLabelText("SNS Topic ARN"), {
      target: { value: "arn:aws:sns:us-east-1:123456789012:my-topic" },
    });
    await fireEvent.input(screen.getByLabelText("Error Report S3 Bucket"), {
      target: { value: "my-error-bucket" },
    });
    await fireEvent.input(screen.getByLabelText("Target Database"), {
      target: { value: "mydb" },
    });
    await fireEvent.input(screen.getByLabelText("Target Table"), {
      target: { value: "mytable" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ScheduledQueries: [exampleScheduledQuery] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-scheduled-query")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Name: "my-scheduled-query",
          QueryString: 'SELECT * FROM "mydb"."mytable"',
          ScheduledQueryExecutionRoleArn: "arn:aws:iam::123456789012:role/tsq-role",
          ScheduleConfiguration: { ScheduleExpression: "rate(1 hour)" },
          NotificationConfiguration: {
            SnsConfiguration: { TopicArn: "arn:aws:sns:us-east-1:123456789012:my-topic" },
          },
          ErrorReportConfiguration: { S3Configuration: { BucketName: "my-error-bucket" } },
          TargetConfiguration: {
            TimestreamConfiguration: {
              DatabaseName: "mydb",
              TableName: "mytable",
              TimeColumn: "time",
              DimensionMappings: [],
            },
          },
        },
      }),
    );
  });

  it("shows a toast error including the AWS error code when creating a scheduled query fails", async () => {
    await renderPage([]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("No scheduled queries"));

    await fireEvent.click(screen.getByRole("button", { name: "New" }));
    await fireEvent.input(screen.getByLabelText("Name *"), {
      target: { value: "my-scheduled-query" },
    });
    await fireEvent.input(screen.getByLabelText("Query String *"), {
      target: { value: 'SELECT * FROM "mydb"."mytable"' },
    });

    const error = Object.assign(new Error("Limit exceeded."), {
      name: "ServiceQuotaExceededException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Create failed: ServiceQuotaExceededException: Limit exceeded.",
      );
    });
  });

  it("deletes a scheduled query after confirming", async () => {
    await renderPage([exampleScheduledQuery]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("my-scheduled-query"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ScheduledQueries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { ScheduledQueryArn: exampleScheduledQuery.Arn } }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No scheduled queries")).toBeInTheDocument();
    });
  });

  it("does not delete a scheduled query when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderPage([exampleScheduledQuery]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("my-scheduled-query"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the two mount-time calls -- no DeleteScheduledQuery, no reload.
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(screen.getByText("my-scheduled-query")).toBeInTheDocument();
  });

  it("disables an ENABLED scheduled query", async () => {
    await renderPage([exampleScheduledQuery]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("my-scheduled-query"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ScheduledQueries: [{ ...exampleScheduledQuery, State: "DISABLED" }],
    });

    await fireEvent.click(screen.getByTitle("Disable"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { ScheduledQueryArn: exampleScheduledQuery.Arn, State: "DISABLED" },
        }),
      );
    });
  });

  it("runs a scheduled query immediately", async () => {
    await renderPage([exampleScheduledQuery]);
    await fireEvent.click(screen.getByText("Scheduled Queries"));
    await waitFor(() => screen.getByText("my-scheduled-query"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ScheduledQueries: [exampleScheduledQuery] });

    await fireEvent.click(screen.getByTitle("Run now"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { ScheduledQueryArn: exampleScheduledQuery.Arn, InvocationTime: expect.any(Date) },
        }),
      );
    });
  });

  it("switches to Account Settings and shows the loaded pricing model and last-updated time", async () => {
    await renderPage([], {
      QueryPricingModel: "BYTES_SCANNED",
      MaxQueryTCU: 10,
      LastUpdatedTime: 1704067200,
    });
    await fireEvent.click(screen.getByText("Account Settings"));

    expect(screen.getByLabelText("Query Pricing Model")).toHaveValue("BYTES_SCANNED");
    expect(screen.getByLabelText("Max Query TCU (optional)")).toHaveValue(10);
    expect(screen.getByText(/Last updated:/)).toBeInTheDocument();
  });

  it("saves account settings, omitting MaxQueryTCU when left blank", async () => {
    await renderPage([], { QueryPricingModel: "COMPUTE_UNITS", MaxQueryTCU: null });
    await fireEvent.click(screen.getByText("Account Settings"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ QueryPricingModel: "COMPUTE_UNITS" });

    await fireEvent.click(screen.getByRole("button", { name: "Save Settings" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { QueryPricingModel: "COMPUTE_UNITS", MaxQueryTCU: undefined },
        }),
      );
    });
  });

  it("shows a toast error including the AWS error code when saving account settings fails", async () => {
    await renderPage([], { QueryPricingModel: "COMPUTE_UNITS" });
    await fireEvent.click(screen.getByText("Account Settings"));

    const error = Object.assign(new Error("Access denied."), { name: "AccessDeniedException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Save Settings" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Update failed: AccessDeniedException: Access denied.",
      );
    });
  });
});
