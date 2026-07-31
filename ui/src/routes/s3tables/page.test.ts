import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import S3TablesPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getS3TablesClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleBucket = {
  arn: "arn:aws:s3tables:us-east-1:123456789012:bucket/example-bucket",
  name: "example-bucket",
  ownerAccountId: "123456789012",
  createdAt: new Date("2024-01-01T00:00:00Z"),
};

describe("S3 Tables Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [] });
    render(S3TablesPage);
    expect(screen.getByText("Amazon S3 Tables")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [] });
    render(S3TablesPage);
    expect(screen.getByText("Table Buckets")).toBeInTheDocument();
    expect(screen.getByText("Namespaces")).toBeInTheDocument();
    expect(screen.getByText("Tables")).toBeInTheDocument();
  });

  it("shows empty state when no table buckets", async () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [] });
    render(S3TablesPage);
    await waitFor(() => {
      expect(screen.getByText("No table buckets found")).toBeInTheDocument();
    });
  });

  it("lists table buckets", async () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [exampleBucket] });
    render(S3TablesPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-bucket" })).toBeInTheDocument();
    });
  });

  it("creates a table bucket via the modal", async () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [] });
    render(S3TablesPage);
    await waitFor(() => screen.getByText("No table buckets found"));

    await fireEvent.click(screen.getByText("Create table bucket"));
    expect(screen.getByText("Create Table Bucket")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Name"), { target: { value: "new-bucket" } });

    mockSend.mockResolvedValueOnce({ arn: exampleBucket.arn });
    mockSend.mockResolvedValueOnce({ tableBuckets: [exampleBucket] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-bucket" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a table bucket after confirming", async () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [exampleBucket] });
    render(S3TablesPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-bucket" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ tableBuckets: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No table buckets found")).toBeInTheDocument();
    });
  });

  it("does not delete a table bucket when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ tableBuckets: [exampleBucket] });
    render(S3TablesPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-bucket" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListTableBuckets call -- no DeleteTableBucket, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "example-bucket" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Table bucket not found."), {
      name: "NotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(S3TablesPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("NotFoundException (HTTP 404): Table bucket not found."),
      ).toBeInTheDocument();
    });
  });

  it("switches to the namespaces tab and loads namespaces for the selected bucket", async () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [exampleBucket] });
    render(S3TablesPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-bucket" }));

    mockSend.mockResolvedValueOnce({
      namespaces: [
        {
          namespace: ["analytics"],
          createdAt: new Date("2024-02-01T00:00:00Z"),
          createdBy: "123456789012",
          ownerAccountId: "123456789012",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Namespaces"));

    await waitFor(() => {
      expect(screen.getByText("analytics")).toBeInTheDocument();
    });
  });

  it("switches to the tables tab and loads tables scoped to the selected bucket", async () => {
    mockSend.mockResolvedValueOnce({ tableBuckets: [exampleBucket] });
    render(S3TablesPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-bucket" }));

    // Clicking the Tables tab triggers both the tables list load and the
    // namespaces-for-the-create-dialog prefetch (ensureNamespacesLoaded).
    mockSend.mockResolvedValueOnce({
      tables: [
        {
          namespace: ["analytics"],
          name: "events",
          type: "customer",
          tableARN: "arn:aws:s3tables:us-east-1:123456789012:bucket/example-bucket/table/events",
          createdAt: new Date("2024-03-01T00:00:00Z"),
          modifiedAt: new Date("2024-03-01T00:00:00Z"),
        },
      ],
    });
    mockSend.mockResolvedValueOnce({ namespaces: [] });
    await fireEvent.click(screen.getByText("Tables"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "events" })).toBeInTheDocument();
    });

    const call = mockSend.mock.calls.find((c) => c[0]?.constructor?.name === "ListTablesCommand");
    expect(call?.[0]?.input).toMatchObject({ tableBucketARN: exampleBucket.arn });
  });
});
