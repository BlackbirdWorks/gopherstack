import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import RDSDataPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getRDSDataClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
    info: vi.fn(),
  },
}));

async function fillConnection(): Promise<void> {
  await fireEvent.input(screen.getByLabelText("Resource ARN *"), {
    target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster" },
  });
  await fireEvent.input(screen.getByLabelText("Secret ARN *"), {
    target: { value: "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret" },
  });
}

describe("RDSData Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    try {
      localStorage.clear();
    } catch {
      // jsdom always has localStorage; ignore if not
    }
  });

  it("renders page title", () => {
    render(RDSDataPage);
    expect(screen.getByText("RDS Data")).toBeInTheDocument();
  });

  it("shows all four tabs", () => {
    render(RDSDataPage);
    expect(screen.getByRole("tab", { name: "Query Console" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Transactions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Statement History" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "ExecuteSql (legacy)" })).toBeInTheDocument();
  });

  it("shows Query Console tab by default with SQL textarea", () => {
    render(RDSDataPage);
    expect(screen.getByRole("textbox", { name: "SQL Query" })).toBeInTheDocument();
    expect(screen.getByText("Run Query")).toBeInTheDocument();
  });

  it("Run Query is disabled until both Resource ARN and Secret ARN are filled", async () => {
    render(RDSDataPage);
    const btn = screen.getByText("Run Query").closest("button")!;
    expect(btn).toBeDisabled();

    await fireEvent.input(screen.getByLabelText("Resource ARN *"), {
      target: { value: "arn:aws:rds:us-east-1:123456789012:cluster:c" },
    });
    expect(btn).toBeDisabled();

    await fireEvent.input(screen.getByLabelText("Secret ARN *"), {
      target: { value: "arn:aws:secretsmanager:us-east-1:123456789012:secret:s" },
    });
    expect(btn).not.toBeDisabled();
  });

  it("shows Ctrl+Enter keyboard hint", () => {
    render(RDSDataPage);
    expect(screen.getByText(/Ctrl\+Enter/)).toBeInTheDocument();
  });

  it("toggles batch mode and swaps SQL textarea label", async () => {
    render(RDSDataPage);
    const batchBtn = screen.getByRole("button", { name: "Toggle batch mode" });
    expect(batchBtn).toHaveTextContent("Batch OFF");
    await fireEvent.click(batchBtn);
    expect(batchBtn).toHaveTextContent("Batch ON");
    expect(screen.getByRole("textbox", { name: "Batch SQL Template" })).toBeInTheDocument();
    expect(screen.getByText("Run Batch")).toBeInTheDocument();
  });

  it("executes ExecuteStatement and renders real result rows from the response", async () => {
    mockSend.mockResolvedValueOnce({
      records: [[{ stringValue: "alice" }], [{ isNull: true }]],
      columnMetadata: [{ name: "name", typeName: "TEXT" }],
      numberOfRecordsUpdated: 0,
    });

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);

    // Regression check for the rendered-cell bug class: cell text comes
    // from real response data flowing through the render logic.
    await waitFor(() => {
      expect(screen.getByText("alice")).toBeInTheDocument();
    });
    expect(screen.getByText("NULL")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: /name/ })).toBeInTheDocument();

    const [request] = mockSend.mock.calls[0];
    expect(request.input.resourceArn).toBe("arn:aws:rds:us-east-1:123456789012:cluster:my-cluster");
    expect(request.input.sql).toBe("SELECT 1;");
  });

  it("surfaces an execution error with the AWS error name in the banner", async () => {
    const err = Object.assign(new Error("transaction txn-999 not found"), {
      name: "TransactionNotFoundException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(err);

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);

    await waitFor(() => {
      expect(screen.getByText(/TransactionNotFoundException/)).toBeInTheDocument();
    });
    expect(screen.getByText(/HTTP 400/)).toBeInTheDocument();
  });

  it("BatchExecuteStatement sends parameter sets built from the Parameters editor, not a raw SQL blob", async () => {
    mockSend.mockResolvedValueOnce({ updateResults: [{ generatedFields: [{ longValue: 7 }] }] });

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByRole("button", { name: "Toggle batch mode" }));
    await fireEvent.input(screen.getByRole("textbox", { name: "Batch SQL Template" }), {
      target: { value: "INSERT INTO t (id) VALUES (:id)" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Add Parameter" }));
    await fireEvent.input(screen.getByLabelText("Parameter name"), { target: { value: "id" } });
    await fireEvent.input(screen.getByLabelText("Parameter value"), { target: { value: "42" } });

    await fireEvent.click(screen.getByText("Run Batch").closest("button")!);

    await waitFor(() => {
      expect(screen.getByText(/generatedFields: 7/)).toBeInTheDocument();
    });

    const [request] = mockSend.mock.calls[0];
    expect(request.input.sql).toBe("INSERT INTO t (id) VALUES (:id)");
    expect(request.input.parameterSets).toEqual([
      [{ name: "id", value: { stringValue: "42" }, typeHint: undefined }],
    ]);
  });

  it("switches to Transactions tab and begins a transaction", async () => {
    mockSend.mockResolvedValueOnce({ transactionId: "txn-000001" });

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByRole("tab", { name: "Transactions" }));

    const beginBtn = await screen.findByRole("button", { name: /Begin Transaction/ });
    await fireEvent.click(beginBtn);

    await waitFor(() => {
      expect(screen.getByText("txn-000001")).toBeInTheDocument();
    });
  });

  it("Begin Transaction is disabled until the connection fields are filled", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Transactions" }));
    const btn = await screen.findByRole("button", { name: /Begin Transaction/ });
    expect(btn).toBeDisabled();
  });

  it("confirms before rolling back a transaction and calls RollbackTransaction", async () => {
    mockSend.mockResolvedValueOnce({ transactionId: "txn-000002" });
    mockSend.mockResolvedValueOnce({ transactionStatus: "RollbackComplete" });

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByRole("tab", { name: "Transactions" }));
    await fireEvent.click(await screen.findByRole("button", { name: /Begin Transaction/ }));
    await screen.findByText("txn-000002");

    await fireEvent.click(screen.getByRole("button", { name: "Rollback" }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.queryByText("txn-000002")).not.toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input.transactionId).toBe("txn-000002");
  });

  it("shows Statement History empty state, then records executed statements with op and status", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Statement History" }));
    await waitFor(() => {
      expect(screen.getByText("No statements executed yet this session")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({ numberOfRecordsUpdated: 1 });
    await fireEvent.click(screen.getByRole("tab", { name: "Query Console" }));
    await fillConnection();
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    await fireEvent.click(screen.getByRole("tab", { name: "Statement History" }));
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "ExecuteStatement" })).toBeInTheDocument();
    });
    expect(screen.getByText("OK")).toBeInTheDocument();
  });

  it("shows the ExecuteSql legacy tab with its deprecation/limitation notice", async () => {
    render(RDSDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "ExecuteSql (legacy)" }));
    expect(screen.getByText(/ExecuteSql is deprecated/)).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Legacy SQL Statements" })).toBeInTheDocument();
  });

  it("runs ExecuteSql and shows numberOfRecordsUpdated for a DML statement", async () => {
    mockSend.mockResolvedValueOnce({ sqlStatementResults: [{ numberOfRecordsUpdated: 3 }] });

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByRole("tab", { name: "ExecuteSql (legacy)" }));
    await fireEvent.click(screen.getByText("Run").closest("button")!);

    await waitFor(() => {
      expect(screen.getByText(/numberOfRecordsUpdated: 3/)).toBeInTheDocument();
    });

    const [request] = mockSend.mock.calls[0];
    expect(request.input.dbClusterOrInstanceArn).toBe(
      "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
    );
    expect(request.input.awsSecretStoreArn).toBe(
      "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret",
    );
  });

  it("runs ExecuteSql and shows resultFrame rows for a SELECT", async () => {
    mockSend.mockResolvedValueOnce({
      sqlStatementResults: [
        {
          numberOfRecordsUpdated: 0,
          resultFrame: {
            resultSetMetadata: { columnCount: 1, columnMetadata: [{ name: "id" }] },
            records: [{ values: [{ bigIntValue: 42 }] }],
          },
        },
      ],
    });

    render(RDSDataPage);
    await fillConnection();
    await fireEvent.click(screen.getByRole("tab", { name: "ExecuteSql (legacy)" }));
    await fireEvent.click(screen.getByText("Run").closest("button")!);

    await waitFor(() => {
      expect(screen.getByRole("columnheader", { name: "id" })).toBeInTheDocument();
      expect(screen.getByRole("cell", { name: "42" })).toBeInTheDocument();
    });
  });
});
