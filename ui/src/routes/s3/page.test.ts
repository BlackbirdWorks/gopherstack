import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import S3Page from "./+page.svelte";
import { setMockPageUrl } from "$lib/mock-page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

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

describe("S3 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    setStoredRegion(DEFAULT_REGION);
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
    mockSend.mockResolvedValue({ Contents: [], IsTruncated: false });

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
    mockSend.mockResolvedValue({ Contents: [], IsTruncated: false });

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

  it("honors ?q= and ?sort= from the URL on load", async () => {
    setMockPageUrl(new URL("http://localhost/s3?q=beta&sort=newest"));
    mockSend.mockResolvedValueOnce({
      Buckets: [
        { Name: "alpha-bucket", CreationDate: new Date("2024-01-01") },
        { Name: "beta-bucket", CreationDate: new Date("2024-06-15") },
      ],
    });
    mockSend.mockResolvedValue({ Contents: [], IsTruncated: false });

    render(S3Page);

    await waitFor(() => {
      expect(screen.getByText("beta-bucket")).toBeInTheDocument();
    });
    expect(screen.queryByText("alpha-bucket")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Search Buckets")).toHaveValue("beta");
    expect(screen.getByLabelText("Sort by")).toHaveValue("newest");
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
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    mockSend.mockResolvedValueOnce({
      Contents: [],
      CommonPrefixes: [],
      IsTruncated: false,
    });

    render(S3Page);

    await waitFor(
      () => {
        expect(screen.getByText("my-files")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const bucketBtn = screen.getByRole("button", { name: "View" });
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

  // ---------------------------------------------------------------------
  // Error paths: inline banner shows the AWS error code (err.name +
  // $metadata.httpStatusCode), not just a generic message.
  // ---------------------------------------------------------------------

  it("shows an inline banner with the AWS error code when the bucket list fails to load", async () => {
    const awsError = Object.assign(new Error("Access Denied"), {
      name: "AccessDenied",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(awsError);

    render(S3Page);

    await waitFor(() => {
      expect(screen.getByText("Failed to load buckets")).toBeInTheDocument();
    });
    expect(screen.getByText("AccessDenied (HTTP 403): Access Denied")).toBeInTheDocument();
  });

  it("shows an inline banner with the AWS error code when the object list fails to load", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "broken-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    const awsError = Object.assign(new Error("no such bucket"), {
      name: "NoSuchBucket",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(awsError);

    render(S3Page);

    await waitFor(() => {
      expect(screen.getByText("broken-bucket")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByRole("button", { name: "View" }));

    await waitFor(() => {
      expect(screen.getByText("Failed to load objects")).toBeInTheDocument();
    });
    expect(screen.getByText("NoSuchBucket (HTTP 404): no such bucket")).toBeInTheDocument();
  });

  // ---------------------------------------------------------------------
  // Exact command-input assertions for the mutations this page performs.
  // ---------------------------------------------------------------------

  it("sends exact CreateBucket and PutBucketVersioning input when versioning is enabled", async () => {
    // initial mount
    mockSend.mockResolvedValueOnce({ Buckets: [] });
    // CreateBucketCommand
    mockSend.mockResolvedValueOnce({});
    // PutBucketVersioningCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "v-bucket", CreationDate: new Date() }],
      // loadBuckets refresh
    });
    // loadBucketSizes
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);

    await fireEvent.click(screen.getByText("+ Create Bucket"));
    await fireEvent.input(screen.getByLabelText("Bucket Name"), { target: { value: "v-bucket" } });
    await fireEvent.click(screen.getByLabelText("Enable Versioning"));

    const submitBtn = screen.getByRole("dialog").querySelector('button[type="submit"]')!;
    await fireEvent.click(submitBtn);

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(5));

    expect(mockSend.mock.calls[1][0].input).toEqual({ Bucket: "v-bucket" });
    expect(mockSend.mock.calls[2][0].input).toEqual({
      Bucket: "v-bucket",
      VersioningConfiguration: { Status: "Enabled" },
    });
  });

  it("deletes an empty bucket: checks for objects and versions, then deletes it", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "empty-bucket", CreationDate: new Date() }],
      // mount
    });
    // size calc
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // deleteAllObjectsInBucket: plain listing
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // deleteAllObjectsInBucket: versions
    mockSend.mockResolvedValueOnce({ Versions: [], DeleteMarkers: [], IsTruncated: false });
    // DeleteBucketCommand
    mockSend.mockResolvedValueOnce({});
    // loadBuckets refresh
    mockSend.mockResolvedValueOnce({ Buckets: [] });

    render(S3Page);

    await waitFor(() => expect(screen.getByText("empty-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByTitle("Delete bucket"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(6));
    expect(confirmDestructive).toHaveBeenCalled();

    expect(mockSend.mock.calls[2][0].input).toMatchObject({ Bucket: "empty-bucket" });
    expect(mockSend.mock.calls[3][0].input).toMatchObject({ Bucket: "empty-bucket" });
    expect(mockSend.mock.calls[4][0].input).toEqual({ Bucket: "empty-bucket" });
  });

  it("purging a versioned bucket deletes every version and delete marker by VersionId, not just current objects", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "versioned-bucket", CreationDate: new Date() }],
      // mount
    });
    // size calc
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "old.txt", Size: 10 }],
      IsTruncated: false,
      // plain listing (current object)
    });
    // DeleteObject(old.txt) -- current version
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Versions: [
        { Key: "old.txt", VersionId: "v1" },
        { Key: "old.txt", VersionId: "v2" },
      ],
      DeleteMarkers: [{ Key: "old.txt", VersionId: "dm1" }],
      IsTruncated: false,
      // ListObjectVersions
    });
    // DeleteObject v1
    mockSend.mockResolvedValueOnce({});
    // DeleteObject v2
    mockSend.mockResolvedValueOnce({});
    // DeleteObject dm1
    mockSend.mockResolvedValueOnce({});
    // DeleteBucketCommand
    mockSend.mockResolvedValueOnce({});
    // loadBuckets refresh
    mockSend.mockResolvedValueOnce({ Buckets: [] });

    render(S3Page);

    await waitFor(() => expect(screen.getByText("versioned-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByText("Purge All"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(10));

    const deleteCalls = mockSend.mock.calls
      .map((c) => c[0].input)
      .filter((input) => "Key" in input && input.Key === "old.txt");
    const versionIds = deleteCalls.map((c: { VersionId?: string }) => c.VersionId);
    expect(versionIds).toEqual([undefined, "v1", "v2", "dm1"]);
  });

  it("paginates object listing with ContinuationToken and appends (not replaces) results", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "paged-bucket", CreationDate: new Date() }],
    });
    // size calc
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "a.txt", Size: 1 }],
      IsTruncated: true,
      NextContinuationToken: "TOKEN-1",
    });
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "b.txt", Size: 2 }],
      IsTruncated: false,
    });

    render(S3Page);

    await waitFor(() => expect(screen.getByText("paged-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));

    await waitFor(() => {
      expect(screen.getByText("b.txt")).toBeInTheDocument();
    });
    expect(screen.getByText("a.txt")).toBeInTheDocument();

    expect(mockSend.mock.calls[2][0].input.ContinuationToken).toBeUndefined();
    expect(mockSend.mock.calls[3][0].input.ContinuationToken).toBe("TOKEN-1");
  });

  it("uploads a file with the exact PutObject wire shape (key, content-type, body bytes)", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "upload-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // openBucket -> loadObjects
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);

    await waitFor(() => expect(screen.getByText("upload-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    // Sync on the mocked fetch having resolved, not on empty-state text --
    // that text also renders before the mount fetch resolves and races it.
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));

    await fireEvent.click(screen.getByText("Upload File"));

    const file = new File(["hello world"], "notes.txt", { type: "text/plain" });
    const fileInput = screen.getByLabelText("File") as HTMLInputElement;
    await fireEvent.change(fileInput, { target: { files: [file] } });

    // PutObjectCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "notes.txt", Size: 11 }],
      IsTruncated: false,
    });

    // jsdom doesn't mark a `required` file input as satisfying constraint
    // validation when .files is set programmatically (via fireEvent.change),
    // so a click on the submit button gets silently blocked by native HTML5
    // validation before the click ever reaches the form's submit handler.
    // Dispatch the submit event directly to bypass that jsdom quirk.
    const form = screen.getByRole("dialog").querySelector("form")!;
    await fireEvent.submit(form);

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(5));

    const putInput = mockSend.mock.calls[3][0].input;
    expect(putInput.Bucket).toBe("upload-bucket");
    expect(putInput.Key).toBe("notes.txt");
    expect(putInput.ContentType).toBe("text/plain");
    expect(putInput.Body).toBeInstanceOf(Uint8Array);
    expect(new TextDecoder().decode(putInput.Body)).toBe("hello world");
  });

  it("object round trip: upload, list, inspect (fetch), then delete", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "rt-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // openBucket
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("rt-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));

    // Upload
    await fireEvent.click(screen.getByText("Upload File"));
    const file = new File(["round trip"], "rt.txt", { type: "text/plain" });
    await fireEvent.change(screen.getByLabelText("File"), { target: { files: [file] } });
    // PutObject
    mockSend.mockResolvedValueOnce({});
    // reload (list)
    mockSend.mockResolvedValueOnce({ Contents: [{ Key: "rt.txt", Size: 10 }], IsTruncated: false });
    // See the equivalent comment in the upload test: a required file input
    // set via fireEvent.change doesn't satisfy jsdom's constraint validation,
    // so submit the form directly rather than clicking the submit button.
    await fireEvent.submit(screen.getByRole("dialog").querySelector("form")!);

    // Listed
    await waitFor(() => expect(screen.getByText("rt.txt")).toBeInTheDocument());

    // Fetched (inspect)
    mockSend.mockResolvedValueOnce({
      ContentType: "text/plain",
      Body: { transformToByteArray: () => new TextEncoder().encode("round trip") },
    });
    mockSend.mockResolvedValueOnce({ TagSet: [] });
    await fireEvent.click(screen.getByText("rt.txt"));
    // Fetched content renders in a readonly <textarea>, whose value is not
    // part of its textContent -- assert via display value, not getByText.
    await waitFor(() => expect(screen.getByDisplayValue("round trip")).toBeInTheDocument());

    // Deleted (click the row's Delete button underneath the preview overlay --
    // fireEvent dispatches directly on the target node regardless of jsdom's
    // lack of hit-testing/z-order, so there's no need to close the modal first).
    // DeleteObject
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    await fireEvent.click(screen.getByText("Delete"));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(9));

    expect(confirmDestructive).toHaveBeenCalled();
    const deleteCallIdx = mockSend.mock.calls.findIndex(
      (c) =>
        c[0].input?.Key === "rt.txt" && !("Body" in c[0].input) && !("ContentType" in c[0].input),
    );
    expect(mockSend.mock.calls[deleteCallIdx][0].input).toEqual({
      Bucket: "rt-bucket",
      Key: "rt.txt",
    });
  });

  it("percent-encodes the CopySource key when copying an object with special characters", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "copy-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "my file.txt", Size: 5 }],
      IsTruncated: false,
    });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("copy-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    await waitFor(() => expect(screen.getByText("my file.txt")).toBeInTheDocument());

    await fireEvent.click(screen.getByText("Copy"));
    // CopyObjectCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "my file.txt", Size: 5 }],
      IsTruncated: false,
    });

    const submitBtn = screen.getByRole("dialog").querySelector('button[type="submit"]')!;
    await fireEvent.click(submitBtn);

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(5));
    const copyInput = mockSend.mock.calls[3][0].input;
    expect(copyInput.Bucket).toBe("copy-bucket");
    expect(copyInput.CopySource).toBe("copy-bucket/my%20file.txt");
    expect(copyInput.Key).toBe("my file.txt-copy");
  });

  it("renames an object via CopyObject (encoded source) followed by DeleteObject", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "rename-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "a+b.txt", Size: 5 }],
      IsTruncated: false,
    });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("rename-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    await waitFor(() => expect(screen.getByText("a+b.txt")).toBeInTheDocument());

    await fireEvent.click(screen.getByText("Rename"));
    const newKeyInput = screen.getByLabelText("New Key");
    await fireEvent.input(newKeyInput, { target: { value: "renamed.txt" } });

    // CopyObjectCommand
    mockSend.mockResolvedValueOnce({});
    // DeleteObjectCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({
      Contents: [{ Key: "renamed.txt", Size: 5 }],
      IsTruncated: false,
    });

    const submitBtn = screen.getByRole("dialog").querySelector('button[type="submit"]')!;
    await fireEvent.click(submitBtn);

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(6));
    const copyInput = mockSend.mock.calls[3][0].input;
    expect(copyInput.CopySource).toBe("rename-bucket/a%2Bb.txt");
    expect(copyInput.Key).toBe("renamed.txt");
    const deleteInput = mockSend.mock.calls[4][0].input;
    expect(deleteInput).toEqual({ Bucket: "rename-bucket", Key: "a+b.txt" });
  });

  it("toggles bucket versioning with the exact PutBucketVersioning status", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "vers-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // openBucket objects tab
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("vers-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));

    // GetBucketVersioning
    mockSend.mockResolvedValueOnce({ Status: "" });
    // GetBucketLocation
    mockSend.mockResolvedValueOnce({ LocationConstraint: "" });
    // GetBucketEncryption
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "ServerSideEncryptionConfigurationNotFoundError" }),
    );
    // GetObjectLockConfiguration
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "ObjectLockConfigurationNotFoundError" }),
    );
    // ListObjectVersions
    mockSend.mockResolvedValueOnce({ Versions: [] });
    // GetBucketWebsite
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "NoSuchWebsiteConfiguration" }),
    );

    await fireEvent.click(screen.getByText("Properties"));
    await waitFor(() => expect(screen.getByText("Enable Versioning")).toBeInTheDocument());

    // PutBucketVersioningCommand
    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Enable Versioning"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(10));
    const putVersioningInput = mockSend.mock.calls[9][0].input;
    expect(putVersioningInput).toEqual({
      Bucket: "vers-bucket",
      VersioningConfiguration: { Status: "Enabled" },
    });
  });

  // ---------------------------------------------------------------------
  // Regression for bd gopherstack-pejf: the Properties tab's Region field
  // must never silently keep showing a PREVIOUS successful fetch's value
  // when a later GetBucketLocation call fails. That's exactly how the
  // dashboard ended up displaying "ap-southeast-1" as every bucket's region
  // regardless of what the region selector said -- one successful fetch set
  // it once, and every failing fetch after that (e.g. because the Go-side
  // fix in services/s3/handler.go's enforceBucketRegion is missing, or any
  // other transient error) left the stale value on screen with no
  // indication anything had gone wrong.
  // ---------------------------------------------------------------------

  it("shows 'Unknown' (not a stale previous region) when GetBucketLocation fails on a later Properties tab load", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "stale-region-bucket", CreationDate: new Date() }],
    });
    // loadBucketSizes
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // openBucket -> loadObjects
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("stale-region-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));

    // First Properties load: everything succeeds, region is a real, non-default value.
    // GetBucketVersioning
    mockSend.mockResolvedValueOnce({ Status: "" });
    // GetBucketLocation
    mockSend.mockResolvedValueOnce({ LocationConstraint: "eu-west-1" });
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "ServerSideEncryptionConfigurationNotFoundError" }),
    );
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "ObjectLockConfigurationNotFoundError" }),
    );
    // ListObjectVersions
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "NoSuchWebsiteConfiguration" }),
    );

    await fireEvent.click(screen.getByText("Properties"));
    await waitFor(() => expect(screen.getByText("eu-west-1")).toBeInTheDocument());

    // Second Properties load (e.g. the user reopens the tab, or a region
    // switch re-triggers it): GetBucketLocation now fails. Region display
    // must reset to "Unknown", not keep showing "eu-west-1".
    // GetBucketVersioning
    mockSend.mockResolvedValueOnce({ Status: "" });
    const locationError = Object.assign(new Error("The bucket is in another region"), {
      name: "PermanentRedirect",
      $metadata: { httpStatusCode: 301 },
    });
    // GetBucketLocation
    mockSend.mockRejectedValueOnce(locationError);
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "ServerSideEncryptionConfigurationNotFoundError" }),
    );
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "ObjectLockConfigurationNotFoundError" }),
    );
    // ListObjectVersions
    mockSend.mockResolvedValueOnce({ Versions: [] });
    mockSend.mockRejectedValueOnce(
      Object.assign(new Error(), { name: "NoSuchWebsiteConfiguration" }),
    );

    await fireEvent.click(screen.getByText("Properties"));

    await waitFor(() => expect(screen.getByText("Unknown")).toBeInTheDocument());
    expect(screen.queryByText("eu-west-1")).not.toBeInTheDocument();

    const { toast } = await import("svelte-sonner");
    expect(vi.mocked(toast.error)).toHaveBeenCalledWith(
      expect.stringContaining("Failed to load bucket region"),
    );
  });

  it("saves bucket tags with the exact PutBucketTagging TagSet", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "tag-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    // openBucket
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("tag-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));

    // GetBucketTagging
    mockSend.mockResolvedValueOnce({ TagSet: [] });
    await fireEvent.click(screen.getByText("Tags"));
    await waitFor(() => expect(screen.getByText("No tags set")).toBeInTheDocument());

    await fireEvent.input(screen.getByLabelText("Key"), { target: { value: "env" } });
    await fireEvent.input(screen.getByLabelText("Value"), { target: { value: "prod" } });
    await fireEvent.click(screen.getByText("Add"));

    // PutBucketTaggingCommand
    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Save Tags"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(5));
    expect(mockSend.mock.calls[4][0].input).toEqual({
      Bucket: "tag-bucket",
      Tagging: { TagSet: [{ Key: "env", Value: "prod" }] },
    });
  });

  // ---------------------------------------------------------------------
  // Rendered-text assertion (not just "some element exists"): verify the
  // formatted size and storage-class badge text actually shown in a table
  // cell match the computed/derived value, the assertion class that caught
  // a blank-column bug elsewhere in this dashboard.
  // ---------------------------------------------------------------------

  it("renders the formatted size and storage-class badge text for an object row", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [{ Name: "render-bucket", CreationDate: new Date() }],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });
    mockSend.mockResolvedValueOnce({
      Contents: [
        {
          Key: "archive.zip",
          Size: 1536,
          StorageClass: "GLACIER",
          LastModified: new Date("2024-03-01"),
        },
      ],
      IsTruncated: false,
    });

    render(S3Page);
    await waitFor(() => expect(screen.getByText("render-bucket")).toBeInTheDocument());
    await fireEvent.click(screen.getByRole("button", { name: "View" }));

    await waitFor(() => expect(screen.getByText("archive.zip")).toBeInTheDocument());

    const row = screen.getByText("archive.zip").closest("tr")!;
    expect(within(row).getByText("GLACIER")).toBeInTheDocument();
    expect(within(row).getByText("1.5 KB")).toBeInTheDocument();
  });

  // ---------------------------------------------------------------------
  // ListBuckets is account-wide in real S3, so the bucket list correctly
  // shows buckets from every region regardless of the dashboard's currently
  // selected region -- but until now nothing on screen said a bucket lived
  // somewhere other than the selected region. These cover the fix: a Region
  // line on each bucket card, sourced from the real BucketRegion field on
  // the ListBuckets response (never inferred/defaulted client-side), plus a
  // subtle marker when it differs from the selected region.
  // ---------------------------------------------------------------------

  it("shows each bucket's real region from BucketRegion, without a cross-region marker when it matches the selected region", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [
        { Name: "same-region-bucket", CreationDate: new Date(), BucketRegion: "us-east-1" },
      ],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);

    const card = await waitFor(
      () => screen.getByText("same-region-bucket").closest('[id^="bucket-"]') as HTMLElement,
    );
    expect(within(card).getByTestId("region-chip").textContent).toBe("us-east-1");
    expect(within(card).queryByText("different region")).not.toBeInTheDocument();
  });

  it("marks a bucket whose real region differs from the selected dashboard region", async () => {
    mockSend.mockResolvedValueOnce({
      Buckets: [
        { Name: "cross-region-bucket", CreationDate: new Date(), BucketRegion: "ap-southeast-1" },
      ],
    });
    mockSend.mockResolvedValueOnce({ Contents: [], IsTruncated: false });

    render(S3Page);

    const card = await waitFor(
      () => screen.getByText("cross-region-bucket").closest('[id^="bucket-"]') as HTMLElement,
    );
    // The selector defaults to us-east-1 (no setStoredRegion call in this
    // test), so a bucket actually stored in ap-southeast-1 must show both
    // its real region and the cross-region marker.
    expect(within(card).getByTestId("region-chip").textContent).toBe("ap-southeast-1");
    const marker = within(card).getByText("different region");
    expect(marker).toBeInTheDocument();
    expect(marker.getAttribute("title")).toContain("ap-southeast-1");
  });

  describe("All regions mode", () => {
    it("issues exactly one ListBuckets call regardless of region mode -- the bucket namespace is account-wide, so there is nothing to fan out", async () => {
      setStoredRegion(ALL_REGIONS);
      mockSend.mockResolvedValueOnce({
        Buckets: [
          { Name: "bucket-a", CreationDate: new Date(), BucketRegion: "us-east-1" },
          { Name: "bucket-b", CreationDate: new Date(), BucketRegion: "eu-west-1" },
        ],
      });
      mockSend.mockResolvedValue({ Contents: [], IsTruncated: false });

      render(S3Page);

      await waitFor(() => expect(screen.getByText("bucket-a")).toBeInTheDocument());
      expect(screen.getByText("bucket-b")).toBeInTheDocument();
      // One ListBuckets call plus one loadBucketSizes ListObjectsV2 call per
      // bucket -- never a per-region ListBuckets call, since there is only
      // ever one account-wide bucket namespace to list.
      expect(mockSend).toHaveBeenCalledTimes(3);
    });

    it("shows the write-region hint in Create Bucket modal only when All is selected", async () => {
      setStoredRegion(ALL_REGIONS);
      mockSend.mockResolvedValueOnce({ Buckets: [] });

      render(S3Page);

      await waitFor(() => expect(screen.getByText("S3 Buckets")).toBeInTheDocument());
      const createBtn = screen.getByText("+ Create Bucket");
      await fireEvent.click(createBtn);

      await waitFor(() => expect(screen.getByTestId("write-region-hint")).toBeInTheDocument());
    });

    it("hides the write-region hint when a specific region is selected", async () => {
      setStoredRegion(DEFAULT_REGION);
      mockSend.mockResolvedValueOnce({ Buckets: [] });

      render(S3Page);

      await waitFor(() => expect(screen.getByText("S3 Buckets")).toBeInTheDocument());
      expect(screen.queryByTestId("write-region-hint")).not.toBeInTheDocument();
    });
  });
});
