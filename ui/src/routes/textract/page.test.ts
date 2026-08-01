import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import TextractPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getTextractClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleAdapter = {
  AdapterId: "adapt-123",
  AdapterName: "my-adapter",
  CreationTime: new Date("2024-01-01T00:00:00Z"),
};

describe("Textract Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [] });
    render(TextractPage);
    expect(screen.getByText("Amazon Textract")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No adapters found"));
  });

  it("shows empty state when no adapters", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [] });
    render(TextractPage);
    await waitFor(() => {
      expect(screen.getByText("No adapters found")).toBeInTheDocument();
    });
  });

  it("lists loaded adapters", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [exampleAdapter] });
    render(TextractPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("adapt-123")).toBeInTheDocument();
    });
    expect(within(screen.getByRole("table")).getByText("my-adapter")).toBeInTheDocument();
  });

  it("creates an adapter via the modal", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [] });
    render(TextractPage);
    await waitFor(() => screen.getByText("No adapters found"));

    await fireEvent.click(screen.getByText("Create adapter"));
    await fireEvent.input(within(openDialog()).getByLabelText("Adapter Name"), {
      target: { value: "new-adapter" },
    });

    mockSend.mockResolvedValueOnce({ AdapterId: "adapt-456" });
    mockSend.mockResolvedValueOnce({
      Adapters: [{ AdapterId: "adapt-456", AdapterName: "new-adapter" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("new-adapter")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          AdapterName: "new-adapter",
          Description: undefined,
          FeatureTypes: ["QUERIES"],
          AutoUpdate: "DISABLED",
        },
      }),
    );
  });

  it("deletes an adapter after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [exampleAdapter] });
    render(TextractPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("adapt-123"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Adapters: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No adapters found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { AdapterId: "adapt-123" } }),
    );
  });

  it("does not delete an adapter when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Adapters: [exampleAdapter] });
    render(TextractPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("adapt-123"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("adapt-123")).toBeInTheDocument();
  });

  it("lists adapter versions for a selected adapter, rendering the status pill via its render snippet", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [exampleAdapter] });
    render(TextractPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("adapt-123"));

    mockSend.mockResolvedValueOnce({
      AdapterVersions: [{ AdapterId: "adapt-123", AdapterVersion: "1", Status: "ACTIVE" }],
    });
    await fireEvent.click(screen.getByTitle("View versions"));

    await waitFor(() => {
      expect(screen.getByText("ACTIVE")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { AdapterId: "adapt-123" } }),
    );
  });

  it("runs a synchronous DetectDocumentText analysis against an S3 object", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [] });
    render(TextractPage);
    await waitFor(() => screen.getByText("No adapters found"));

    await fireEvent.click(screen.getByRole("tab", { name: "Analyze Document" }));
    await fireEvent.click(screen.getByText("S3 Object"));
    await fireEvent.input(screen.getByLabelText("S3 Bucket"), { target: { value: "my-bucket" } });
    await fireEvent.input(screen.getByLabelText("S3 Key"), { target: { value: "doc.pdf" } });

    mockSend.mockResolvedValueOnce({ Blocks: [{ BlockType: "LINE", Text: "hello" }] });
    await fireEvent.click(screen.getByRole("button", { name: "Run" }));

    await waitFor(() => {
      expect(screen.getByText("hello")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { Document: { S3Object: { Bucket: "my-bucket", Name: "doc.pdf" } } },
      }),
    );
  });

  it("starts an async text detection job and refreshes its status", async () => {
    mockSend.mockResolvedValueOnce({ Adapters: [] });
    render(TextractPage);
    await waitFor(() => screen.getByText("No adapters found"));

    await fireEvent.click(screen.getByRole("tab", { name: "Async Jobs" }));
    await fireEvent.input(screen.getByLabelText("S3 Bucket"), { target: { value: "my-bucket" } });
    await fireEvent.input(screen.getByLabelText("S3 Key"), { target: { value: "doc.pdf" } });

    mockSend.mockResolvedValueOnce({ JobId: "job-1" });
    await fireEvent.click(screen.getByRole("button", { name: "Start Job" }));

    await waitFor(() => {
      expect(screen.getByText("job-1")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { DocumentLocation: { S3Object: { Bucket: "my-bucket", Name: "doc.pdf" } } },
      }),
    );

    mockSend.mockResolvedValueOnce({ JobStatus: "SUCCEEDED", Blocks: [{ BlockType: "LINE" }] });
    await fireEvent.click(screen.getByTitle("Refresh status"));

    await waitFor(() => {
      expect(screen.getByText("SUCCEEDED")).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(TextractPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });
});
