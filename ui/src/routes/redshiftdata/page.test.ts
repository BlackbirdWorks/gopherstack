import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import RedshiftDataPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getRedshiftDataClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
    info: vi.fn(),
  },
}));

describe("RedshiftData Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockSend.mockResolvedValue({ Statements: [] });
  });

  it("renders page title", () => {
    render(RedshiftDataPage);
    expect(screen.getByText("Redshift Data")).toBeInTheDocument();
  });

  it("shows SQL Editor tab by default", () => {
    render(RedshiftDataPage);
    expect(screen.getByText("SQL Editor")).toBeInTheDocument();
    expect(screen.getByText("Run Query")).toBeInTheDocument();
  });

  it("shows all five tabs", () => {
    render(RedshiftDataPage);
    expect(screen.getByText("SQL Editor")).toBeInTheDocument();
    expect(screen.getByText("Query History")).toBeInTheDocument();
    expect(screen.getByText("Schema Browser")).toBeInTheDocument();
    expect(screen.getByText("Docs")).toBeInTheDocument();
    expect(screen.getByText("Metrics")).toBeInTheDocument();
  });

  it("shows SQL textarea with aria-label", () => {
    render(RedshiftDataPage);
    expect(screen.getByRole("textbox", { name: "SQL Query" })).toBeInTheDocument();
  });

  it("Run Query button is disabled when database is empty", async () => {
    render(RedshiftDataPage);
    const dbInput = screen.getByLabelText("Database name");
    await fireEvent.input(dbInput, { target: { value: "" } });
    const btn = screen.getByText("Run Query").closest("button")!;
    expect(btn).toBeDisabled();
  });

  it("shows Keyboard shortcut hint", () => {
    render(RedshiftDataPage);
    expect(screen.getByText(/Ctrl\+Enter/)).toBeInTheDocument();
  });

  it("shows Connection Settings summary", () => {
    render(RedshiftDataPage);
    expect(screen.getByText("Connection Settings")).toBeInTheDocument();
  });

  it("shows empty history state", async () => {
    mockSend.mockResolvedValue({ Statements: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Query History"));
    await waitFor(
      () => {
        expect(screen.getByText("No recent statements")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows status filter dropdown in history tab", async () => {
    mockSend.mockResolvedValue({ Statements: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Query History"));
    await waitFor(
      () => {
        expect(screen.getByRole("combobox", { name: "Filter by status" })).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Refresh button in history tab", async () => {
    mockSend.mockResolvedValue({ Statements: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Query History"));
    await waitFor(
      () => {
        expect(screen.getByRole("button", { name: "Refresh history" })).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("renders history statements with Reuse and Copy SQL", async () => {
    mockSend.mockResolvedValue({
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
    await fireEvent.click(screen.getByText("Query History"));
    await waitFor(
      () => {
        expect(screen.getByText("SELECT 1")).toBeInTheDocument();
        expect(screen.getByText("Reuse")).toBeInTheDocument();
        expect(screen.getByText("Copy SQL")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads schema browser", async () => {
    mockSend
      .mockResolvedValueOnce({ Statements: [] })
      .mockResolvedValueOnce({ Databases: [] })
      .mockResolvedValueOnce({ Schemas: [] })
      .mockResolvedValueOnce({ Tables: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Schema Browser"));
    await waitFor(
      () => {
        expect(screen.getByText("No tables found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("schema browser shows Refresh button", async () => {
    mockSend
      .mockResolvedValueOnce({ Statements: [] })
      .mockResolvedValueOnce({ Databases: [] })
      .mockResolvedValueOnce({ Schemas: [] })
      .mockResolvedValueOnce({ Tables: [] });
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Schema Browser"));
    await waitFor(
      () => {
        expect(screen.getByRole("button", { name: "Refresh schema" })).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Docs tab with all 11 operations", async () => {
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Docs"));
    await waitFor(
      () => {
        expect(screen.getByText("ExecuteStatement")).toBeInTheDocument();
        expect(screen.getByText("BatchExecuteStatement")).toBeInTheDocument();
        expect(screen.getByText("DescribeStatement")).toBeInTheDocument();
        expect(screen.getByText("CancelStatement")).toBeInTheDocument();
        expect(screen.getByText("GetStatementResult")).toBeInTheDocument();
        expect(screen.getByText("GetStatementResultV2")).toBeInTheDocument();
        expect(screen.getByText("ListStatements")).toBeInTheDocument();
        expect(screen.getByText("ListDatabases")).toBeInTheDocument();
        expect(screen.getByText("ListSchemas")).toBeInTheDocument();
        expect(screen.getByText("ListTables")).toBeInTheDocument();
        expect(screen.getByText("DescribeTable")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Metrics tab with empty state", async () => {
    render(RedshiftDataPage);
    await fireEvent.click(screen.getByText("Metrics"));
    await waitFor(
      () => {
        expect(screen.getByText(/No metrics available/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows empty results placeholder when no query run", () => {
    render(RedshiftDataPage);
    expect(screen.getByText(/Run a query to see results/)).toBeInTheDocument();
  });

  it("shows Export CSV button after results loaded", async () => {
    mockSend
      .mockResolvedValueOnce({ Statements: [] })
      .mockResolvedValueOnce({ Id: "stmt-xyz" })
      .mockResolvedValueOnce({ Status: "FINISHED", HasResultSet: true })
      .mockResolvedValueOnce({
        ColumnMetadata: [{ name: "col1", typeName: "varchar" }],
        Records: [[{ stringValue: "hello" }]],
      });
    render(RedshiftDataPage);
    const btn = screen.getByText("Run Query").closest("button")!;
    await fireEvent.click(btn);
    await waitFor(
      () => {
        expect(screen.getByText("Export CSV")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
  });
});
