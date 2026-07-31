import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import ObjectDetailPage from "./+page.svelte";

const mockSend = vi.fn();
const mockParams = { bucket: "my-bucket", objectKey: "folder/notes.txt" };

vi.mock("$app/state", () => ({
  get page() {
    return { params: mockParams };
  },
}));

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getS3Client: () => ({ send: mockSend }),
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

function bodyOf(text: string) {
  return { transformToByteArray: () => new TextEncoder().encode(text) };
}

describe("S3 Object Detail Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders object metadata after HeadObject resolves", async () => {
    mockSend.mockResolvedValueOnce({
      ContentType: "text/plain",
      ContentLength: 42,
      LastModified: new Date("2024-05-01T00:00:00Z"),
      ETag: '"abc123"',
      StorageClass: "GLACIER",
      VersionId: "v-1",
      Metadata: {},
    });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);

    await waitFor(() => {
      expect(screen.getByText("text/plain")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });
    expect(screen.getByText("42 B")).toBeInTheDocument();
    expect(screen.getByText("GLACIER")).toBeInTheDocument();
    expect(screen.getByText('"abc123"')).toBeInTheDocument();

    const headInput = mockSend.mock.calls[0][0].input;
    expect(headInput).toEqual({ Bucket: "my-bucket", Key: "folder/notes.txt" });
  });

  it("shows an inline banner with the AWS error code when HeadObject fails", async () => {
    const awsError = Object.assign(new Error("The specified key does not exist."), {
      name: "NoSuchKey",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(awsError);
    // loadAll() still runs loadObjectVersions()/loadObjectTags() even after
    // the metadata fetch fails -- queue real responses for determinism.
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);

    await waitFor(() => {
      expect(
        screen.getByText("NoSuchKey (HTTP 404): The specified key does not exist."),
      ).toBeInTheDocument();
    });
  });

  it("renders version rows with the exact formatted size text", async () => {
    mockSend.mockResolvedValueOnce({
      ContentType: "text/plain",
      ContentLength: 42,
    });
    mockSend.mockResolvedValueOnce({
      Versions: [
        {
          Key: "folder/notes.txt",
          VersionId: "version-one-abcdef",
          Size: 2048,
          LastModified: new Date("2024-01-01"),
        },
        { Key: "other-key.txt", VersionId: "should-be-filtered-out", Size: 99 },
      ],
    });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);

    await waitFor(() => expect(screen.getByText("2 KB")).toBeInTheDocument());
    // Versions for a different key must be filtered out client-side.
    expect(screen.queryByText("99 B")).not.toBeInTheDocument();

    const versionsInput = mockSend.mock.calls[1][0].input;
    expect(versionsInput).toEqual({ Bucket: "my-bucket", Prefix: "folder/notes.txt", MaxKeys: 20 });
  });

  it("saves object tags with the exact PutObjectTagging TagSet", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 1 });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [{ Key: "env", Value: "dev" }] });

    render(ObjectDetailPage);
    await waitFor(() => expect(screen.getByText("env")).toBeInTheDocument());

    await fireEvent.input(screen.getByLabelText("Key"), { target: { value: "team" } });
    await fireEvent.input(screen.getByLabelText("Value"), { target: { value: "platform" } });
    await fireEvent.click(screen.getByText("Add Tag"));

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Save Tags"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(4));
    expect(mockSend.mock.calls[3][0].input).toEqual({
      Bucket: "my-bucket",
      Key: "folder/notes.txt",
      Tagging: {
        TagSet: [
          { Key: "env", Value: "dev" },
          { Key: "team", Value: "platform" },
        ],
      },
    });
  });

  it("deletes the object after confirmation and navigates back to the bucket", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 1 });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("text/plain")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Delete"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(4));
    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend.mock.calls[3][0].input).toEqual({
      Bucket: "my-bucket",
      Key: "folder/notes.txt",
    });

    const { goto } = await import("$app/navigation");
    expect(vi.mocked(goto)).toHaveBeenCalledWith("/dashboard/s3");
  });

  it("deletes a specific version with the exact VersionId, distinct from a whole-object delete", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 1 });
    mockSend.mockResolvedValueOnce({
      Versions: [{ Key: "folder/notes.txt", VersionId: "old-version-1", Size: 5 }],
    });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("5 B")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    // DeleteObjectCommand for the version
    mockSend.mockResolvedValueOnce({});
    // reload after delete
    mockSend.mockResolvedValueOnce({ Versions: [] });

    // "Delete" matches both the header's whole-object delete (nested in a
    // <span>, so the header <button> itself is not a direct-text match) and
    // the version row's delete (plain text directly in the <button>). Pick
    // the actual <button> element, not the header's <span>, to avoid
    // triggering the wrong handler via event bubbling.
    const deleteVersionBtn = screen.getAllByText("Delete").find((el) => el.tagName === "BUTTON")!;
    await fireEvent.click(deleteVersionBtn);

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(5));
    expect(mockSend.mock.calls[3][0].input).toEqual({
      Bucket: "my-bucket",
      Key: "folder/notes.txt",
      VersionId: "old-version-1",
    });
  });

  it("downloads a specific version with VersionId set, and current object with it undefined", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 1 });
    mockSend.mockResolvedValueOnce({
      Versions: [{ Key: "folder/notes.txt", VersionId: "old-version-1", Size: 5 }],
    });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("5 B")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    mockSend.mockResolvedValueOnce({ Body: bodyOf("v1 content") });
    await fireEvent.click(
      screen.getByText("Download", { selector: "button span" }).closest("button")!,
    );
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(4));
    expect(mockSend.mock.calls[3][0].input).toEqual({
      Bucket: "my-bucket",
      Key: "folder/notes.txt",
      VersionId: undefined,
    });

    mockSend.mockResolvedValueOnce({ Body: bodyOf("v1 content") });
    const versionDownloadBtn = screen
      .getAllByText("Download")
      .find((el) => el.tagName === "BUTTON")!;
    await fireEvent.click(versionDownloadBtn);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(5));
    expect(mockSend.mock.calls[4][0].input).toEqual({
      Bucket: "my-bucket",
      Key: "folder/notes.txt",
      VersionId: "old-version-1",
    });
  });

  it("previews a small text object with a Range covering the whole object (regression: no longer drops the last byte)", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 11 });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("text/plain")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    mockSend.mockResolvedValueOnce({ Body: bodyOf("hello world") });
    await fireEvent.click(screen.getByText("Preview"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(4));
    const previewInput = mockSend.mock.calls[3][0].input;
    // 11-byte object: end-of-range index is inclusive, so bytes=0-10 fetches
    // all 11 bytes. The pre-fix formula computed bytes=0-9, silently
    // truncating the last byte of every preview that fit under the cap.
    expect(previewInput.Range).toBe("bytes=0-10");
    expect(screen.getByText("hello world")).toBeInTheDocument();
  });

  it("truncates a preview larger than the 10KB cap to exactly the cap", async () => {
    const bigSize = 20000;
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: bigSize });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("text/plain")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    mockSend.mockResolvedValueOnce({ Body: bodyOf("x".repeat(10240)) });
    await fireEvent.click(screen.getByText("Preview"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(4));
    expect(mockSend.mock.calls[3][0].input.Range).toBe("bytes=0-10239");
  });

  it("shows a toast with the AWS error code when preview fetch fails", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 11 });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("text/plain")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    const awsError = Object.assign(new Error("access denied"), {
      name: "AccessDenied",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(awsError);
    await fireEvent.click(screen.getByText("Preview"));

    const { toast } = await import("svelte-sonner");
    await waitFor(() => {
      expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
        "Preview failed: AccessDenied (HTTP 403): access denied",
      );
    });
  });

  it("generates a presigned URL with the correct structure and expiry", async () => {
    mockSend.mockResolvedValueOnce({ ContentType: "text/plain", ContentLength: 1 });
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockResolvedValueOnce({ TagSet: [] });

    render(ObjectDetailPage);
    await waitFor(() => {
      expect(screen.getByText("text/plain")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    await fireEvent.click(screen.getByText("Generate"));

    const urlInput = screen.getByDisplayValue(
      /X-Amz-Algorithm=AWS4-HMAC-SHA256/,
    ) as HTMLInputElement;
    const url = new URL(urlInput.value);
    expect(url.pathname).toBe("/my-bucket/folder/notes.txt");
    expect(url.searchParams.get("X-Amz-Expires")).toBe("3600");
    expect(url.searchParams.get("X-Amz-SignedHeaders")).toBe("host");
    expect(url.searchParams.get("X-Amz-Signature")).toBe("0".repeat(64));
  });
});
