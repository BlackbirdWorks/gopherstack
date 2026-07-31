import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import S3Page from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getS3Client: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe("S3 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ Buckets: [] });

    render(S3Page);

    expect(screen.getByText("S3 Buckets")).toBeInTheDocument();
    expect(screen.getByText("+ Create Bucket")).toBeInTheDocument();
  });

  it("displays loaded buckets", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [
        { Name: "my-data", CreationDate: new Date("2024-01-01") },
        { Name: "backups", CreationDate: new Date("2024-06-15") },
      ],
    });

    render(S3Page);

    await waitFor(
      () => {
        expect(screen.getByText("my-data")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("backups")).toBeInTheDocument();
  });

  it("filters buckets via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [
        { Name: "alpha-bucket", CreationDate: new Date() },
        { Name: "beta-bucket", CreationDate: new Date() },
        { Name: "gamma-bucket", CreationDate: new Date() },
      ],
    });

    render(S3Page);

    await waitFor(
      () => {
        expect(screen.getByText("alpha-bucket")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search buckets...");
    await fireEvent.input(searchInput, { target: { value: "beta" } });

    await waitFor(() => {
      expect(screen.queryByText("alpha-bucket")).not.toBeInTheDocument();
    });
    expect(screen.getByText("beta-bucket")).toBeInTheDocument();
    expect(screen.queryByText("gamma-bucket")).not.toBeInTheDocument();
  });

  it("opens and closes create bucket modal", async () => {
    mockSend.mockResolvedValueOnce({ Buckets: [] });

    render(S3Page);

    const createBtn = screen.getByText("+ Create Bucket");
    await fireEvent.click(createBtn);

    expect(screen.getByText("Create Bucket")).toBeInTheDocument();
    expect(screen.getByLabelText("Bucket Name")).toBeInTheDocument();

    const cancelBtn = screen.getByText("Cancel");
    await fireEvent.click(cancelBtn);

    await waitFor(() => {
      expect(screen.queryByLabelText("Bucket Name")).not.toBeInTheDocument();
    });
  });

  it("creates a bucket via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ Buckets: [] });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "new-bucket", CreationDate: new Date() }],
    });
    // loadBucketSizes calls ListObjectsV2 for the newly created bucket
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);

    await fireEvent.click(screen.getByText("+ Create Bucket"));

    const nameInput = screen.getByLabelText("Bucket Name");
    await fireEvent.input(nameInput, { target: { value: "new-bucket" } });

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

  it("opens bucket detail view on card click", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "my-files", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({
      Contents: [],
      CommonPrefixes: [],
    });

    render(S3Page);

    await waitFor(
      () => {
        expect(screen.getByText("my-files")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const bucketBtn = screen.getByText("my-files");
    await fireEvent.click(bucketBtn);

    await waitFor(() => {
      expect(screen.getByText("Upload File")).toBeInTheDocument();
    });
    expect(screen.getByText("Back")).toBeInTheDocument();
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network failure"));

    render(S3Page);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
