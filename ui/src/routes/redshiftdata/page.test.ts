import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import RedshiftDataPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getRedshiftDataClient: () => ({ send: mockSend }),
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

async function fillDatabase(value = "dev"): Promise<void> {
  const dbInput = screen.getByLabelText("Database *");
  await fireEvent.input(dbInput, { target: { value } });
}

describe("RedshiftData Page", () => {
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
    render(RedshiftDataPage);
    expect(screen.getByText("Redshift Data")).toBeInTheDocument();
  });

  it("shows all four tabs", () => {
    render(RedshiftDataPage);
    expect(screen.getByRole("tab", { name: "Query Console" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Statement History" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Sessions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Catalog" })).toBeInTheDocument();
  });

  it("shows Query Console tab by default with SQL textarea", () => {
    render(RedshiftDataPage);
    expect(screen.getByRole("textbox", { name: "SQL Query" })).toBeInTheDocument();
    expect(screen.getByText("Run Query")).toBeInTheDocument();
  });

  it("shows empty results placeholder when no query has run", () => {
    render(RedshiftDataPage);
    expect(screen.getByText(/Run a query to see results/)).toBeInTheDocument();
  });

  it("Run Query button is disabled when database is empty", async () => {
    render(RedshiftDataPage);
    await fillDatabase("");
    const btn = screen.getByText("Run Query").closest("button")!;
    expect(btn).toBeDisabled();
  });

  it("Run Query button is enabled once database is filled", async () => {
    render(RedshiftDataPage);
    await fillDatabase("dev");
    const btn = screen.getByText("Run Query").closest("button")!;
    expect(btn).not.toBeDisabled();
  });

  it("shows Keyboard shortcut hint", () => {
    render(RedshiftDataPage);
    expect(screen.getByText(/Ctrl\+Enter/)).toBeInTheDocument();
  });

  it("shows batch mode toggle and switches to Batch SQL textarea", async () => {
    render(RedshiftDataPage);
    const batchBtn = screen.getByRole("button", { name: "Toggle batch mode" });
    expect(batchBtn).toHaveTextContent("Batch OFF");
    await fireEvent.click(batchBtn);
    expect(batchBtn).toHaveTextContent("Batch ON");
    expect(screen.getByRole("textbox", { name: "Batch SQL" })).toBeInTheDocument();
  });

  it("executes a statement, polls to FINISHED, and renders result cells", async () => {
    // Call order: ExecuteStatement, then DescribeStatement (poll), then
    // GetStatementResult.
    mockSend
      .mockResolvedValueOnce({ Id: "stmt-xyz" })
      .mockResolvedValueOnce({ Status: "FINISHED", HasResultSet: true, Duration: 1_500_000 })
      .mockResolvedValueOnce({
        ColumnMetadata: [{ name: "col1", typeName: "varchar" }],
        Records: [[{ stringValue: "hello" }]],
        TotalNumRows: 1,
      });

    render(RedshiftDataPage);
    await fillDatabase("dev");
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);

    // Regression check for the rendered-cell bug class: the column header
    // and cell text come from real response data flowing through render
    // logic, not a static placeholder.
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "hello" })).toBeInTheDocument();
    });
    expect(screen.getByText(/col1/)).toBeInTheDocument();
    expect(screen.getByText("Export CSV")).toBeInTheDocument();
  });

  it("renders NULL result cells with italic styling", async () => {
    mockSend
      .mockResolvedValueOnce({ Id: "stmt-null" })
      .mockResolvedValueOnce({ Status: "FINISHED", HasResultSet: true })
      .mockResolvedValueOnce({
        ColumnMetadata: [{ name: "col1", typeName: "varchar" }],
        Records: [[{ isNull: true }]],
      });

    render(RedshiftDataPage);
    await fillDatabase("dev");
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);

    await waitFor(() => {
      const nullCell = screen.getByText("NULL");
      expect(nullCell.className).toContain("italic");
    });
  });

  it("surfaces a FAILED statement's error message", async () => {
    mockSend
      .mockResolvedValueOnce({ Id: "stmt-fail" })
      .mockResolvedValueOnce({ Status: "FAILED", Error: 'column "nope" does not exist' });

    render(RedshiftDataPage);
    await fillDatabase("dev");
    await fireEvent.click(screen.getByText("Run Query").closest("button")!);

    await waitFor(() => {
      expect(screen.getByText('column "nope" does not exist')).toBeInTheDocument();
    });
  });

  it("loads Statement History and renders a row, including a render-snippet cell", async () => {
    mockSend.mockResolvedValueOnce({
      Statements: [
        {
          Id: "stmt-001",
          QueryString: "SELECT 1",
          Status: "FINISHED",
          IsBatchStatement: false,
          CreatedAt: new Date("2024-01-01T00:00:00Z"),
        },
      ],
    });

    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Statement History" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "stmt-001" })).toBeInTheDocument();
    });
    // Status is rendered through defineColumns' `render` snippet
    // (histStatusCell), not the default String(cellValue) fallback -- this
    // is the assertion that would catch a blank cell if `render` were ever
    // passed a plain function instead of a real snippet. Scoped to the
    // badge (not getByText, which also matches the filter <option>).
    expect(screen.getByRole("cell", { name: "FINISHED" })).toBeInTheDocument();
    expect(screen.getByText("SELECT 1")).toBeInTheDocument();
  });

  it("shows the ListStatements default-filter hint and a status filter select", async () => {
    mockSend.mockResolvedValueOnce({ Statements: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Statement History" }));

    await waitFor(() => {
      expect(screen.getByRole("combobox", { name: "Filter by status" })).toBeInTheDocument();
    });
    expect(screen.getByText("No statements found")).toBeInTheDocument();
  });

  it("reuses a history statement in the Query Console", async () => {
    mockSend.mockResolvedValueOnce({
      Statements: [
        { Id: "stmt-002", QueryString: "SELECT 2", Status: "FINISHED", CreatedAt: new Date() },
      ],
    });

    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Statement History" }));
    await waitFor(() => screen.getByRole("cell", { name: "stmt-002" }));

    await fireEvent.click(screen.getByRole("button", { name: "Reuse statement stmt-002" }));

    expect(screen.getByRole("tab", { name: "Query Console" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByRole("textbox", { name: "SQL Query" })).toHaveValue("SELECT 2");
  });

  it("loads Sessions and renders a row with its status cell", async () => {
    mockSend.mockResolvedValueOnce({
      Sessions: [
        {
          SessionId: "sess-1",
          Status: "AVAILABLE",
          Database: "dev",
          DbUser: "admin",
          CreatedAt: new Date("2024-01-01T00:00:00Z"),
          UpdatedAt: new Date("2024-01-01T00:05:00Z"),
        },
      ],
    });

    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Sessions" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "sess-1" })).toBeInTheDocument();
    });
    // Scoped to the cell (not getByText, which also matches the filter <option>).
    expect(screen.getByRole("cell", { name: "AVAILABLE" })).toBeInTheDocument();
  });

  it("shows an empty Sessions hint explaining how sessions appear", async () => {
    mockSend.mockResolvedValueOnce({ Sessions: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Sessions" }));

    await waitFor(() => {
      expect(screen.getByText(/Sessions only appear here/)).toBeInTheDocument();
    });
  });

  it("shows a hint instead of calling the API when Catalog is opened with no database set", async () => {
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Catalog" }));

    await waitFor(() => {
      expect(screen.getByText("Enter a Database name above, then Refresh")).toBeInTheDocument();
    });
    expect(mockSend).not.toHaveBeenCalled();
  });

  it("loads the Catalog once a database is set: databases, schemas, and a table row", async () => {
    mockSend
      .mockResolvedValueOnce({ Databases: ["dev"] })
      .mockResolvedValueOnce({ Schemas: ["public"] })
      .mockResolvedValueOnce({ Tables: [{ name: "users", schema: "public", type: "TABLE" }] });

    render(RedshiftDataPage);
    await fillDatabase("dev");
    await fireEvent.click(screen.getByRole("tab", { name: "Catalog" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "users" })).toBeInTheDocument();
    });
    expect(screen.getByText("dev")).toBeInTheDocument();
    // "public" appears both as the schema-filter chip and as the table
    // row's Schema cell -- scope to the chip button specifically.
    expect(screen.getByRole("button", { name: "public" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "public" })).toBeInTheDocument();
  });

  it("shows an inline error banner with err.name, HTTP status, and message on a failed load", async () => {
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error("Rate exceeded."), {
        name: "ThrottlingException",
        $metadata: { httpStatusCode: 429 },
      }),
    );

    render(RedshiftDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Statement History" }));

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("shows connection settings fields", () => {
    render(RedshiftDataPage);
    expect(screen.getByLabelText("Database *")).toBeInTheDocument();
    expect(screen.getByLabelText("Workgroup Name")).toBeInTheDocument();
    expect(screen.getByLabelText("Cluster Identifier")).toBeInTheDocument();
    expect(screen.getByLabelText("DB User")).toBeInTheDocument();
    expect(screen.getByLabelText("Secret ARN")).toBeInTheDocument();
  });
});
