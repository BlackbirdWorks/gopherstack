import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import TimestreamPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getTimestreamWriteClient: () => ({ send: mockSend }),
  getTimestreamQueryClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleDb = {
  DatabaseName: "my-tsdb",
  Arn: "arn:aws:timestream:us-east-1:123:database/my-tsdb",
  TableCount: 2,
  KmsKeyId: "alias/my-key",
  CreationTime: new Date("2024-01-01T00:00:00Z"),
  LastUpdatedTime: new Date("2024-01-02T00:00:00Z"),
};

describe("Timestream Write Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Databases: [] });
    render(TimestreamPage);
    expect(screen.getByText("Amazon Timestream Write")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No databases found"));
  });

  it("shows empty state when no databases", async () => {
    mockSend.mockResolvedValueOnce({ Databases: [] });
    render(TimestreamPage);
    await waitFor(() => {
      expect(screen.getByText("No databases found")).toBeInTheDocument();
    });
  });

  it("lists loaded databases and renders the KMS key column via its render snippet", async () => {
    mockSend.mockResolvedValueOnce({ Databases: [exampleDb] });
    render(TimestreamPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-tsdb")).toBeInTheDocument();
    });
    expect(within(screen.getByRole("table")).getByText("alias/my-key")).toBeInTheDocument();
  });

  it("creates a database via the modal", async () => {
    mockSend.mockResolvedValueOnce({ Databases: [] });
    render(TimestreamPage);
    await waitFor(() => screen.getByText("No databases found"));

    await fireEvent.click(screen.getByText("Create database"));
    await fireEvent.input(within(openDialog()).getByLabelText("Database Name"), {
      target: { value: "new-db" },
    });

    mockSend.mockResolvedValueOnce({ Database: { DatabaseName: "new-db" } });
    mockSend.mockResolvedValueOnce({ Databases: [{ DatabaseName: "new-db", TableCount: 0 }] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("new-db")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { DatabaseName: "new-db", KmsKeyId: undefined } }),
    );
  });

  it("deletes a database after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Databases: [exampleDb] });
    render(TimestreamPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-tsdb"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Databases: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No databases found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { DatabaseName: "my-tsdb" } }),
    );
  });

  it("does not delete a database when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Databases: [exampleDb] });
    render(TimestreamPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-tsdb"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-tsdb")).toBeInTheDocument();
  });

  it("lists tables for a selected database", async () => {
    mockSend.mockResolvedValueOnce({ Databases: [exampleDb] });
    render(TimestreamPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-tsdb"));

    mockSend.mockResolvedValueOnce({
      Tables: [{ TableName: "my-table", DatabaseName: "my-tsdb", TableStatus: "ACTIVE" }],
    });
    await fireEvent.click(screen.getByTitle("View tables"));

    await waitFor(() => {
      expect(screen.getByText("my-table")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { DatabaseName: "my-tsdb" } }),
    );
    // Render snippet for table status renders the real status text.
    expect(screen.getByText("ACTIVE")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(TimestreamPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });
});
