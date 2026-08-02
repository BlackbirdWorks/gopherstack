import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MediaStoreDataPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMediaStoreDataClient: () => ({ send: mockSend }),
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

const folderItem = { Name: "clips", Type: "FOLDER" };
const objectItem = {
  Name: "video.mp4",
  Type: "OBJECT",
  ContentType: "video/mp4",
  ContentLength: 2048,
  ETag: '"abc123"',
  LastModified: new Date("2026-01-01T00:00:00Z"),
};

async function renderLoaded(items: unknown[] = [folderItem, objectItem]): Promise<void> {
  mockSend.mockResolvedValueOnce({ Items: items });
  render(MediaStoreDataPage);
  // Synchronize on a loaded cell's rendered text, not the empty-state
  // placeholder -- the empty state also renders before the initial
  // ListItems call resolves and would race it.
  await waitFor(() => {
    expect(screen.getByText("video.mp4")).toBeInTheDocument();
  });
}

describe("MediaStoreData Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    await renderLoaded();
    expect(screen.getByText("MediaStore Data")).toBeInTheDocument();
  });

  it("calls ListItems with no Path on initial load (root)", async () => {
    await renderLoaded();
    expect(mockSend).toHaveBeenCalledTimes(1);
    const input = mockSend.mock.calls[0][0].input;
    expect(input.Path).toBeUndefined();
  });

  it("renders both a folder row and an object row with real response data", async () => {
    await renderLoaded();
    expect(screen.getByText("clips")).toBeInTheDocument();
    expect(screen.getByText("video.mp4")).toBeInTheDocument();
    expect(screen.getByText("video/mp4")).toBeInTheDocument();
    expect(screen.getByText("2.0 KB")).toBeInTheDocument();
  });

  it("navigating into a folder issues ListItems with the nested Path", async () => {
    await renderLoaded();
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(screen.getByText("clips"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));
    const input = mockSend.mock.calls[1][0].input;
    expect(input.Path).toBe("clips/");
    // Breadcrumb should now show the folder segment.
    expect(screen.getByText("clips", { selector: "button" })).toBeInTheDocument();
  });

  it("filters items by name via search", async () => {
    await renderLoaded();
    const search = screen.getByPlaceholderText("Search by name or content type...");
    await fireEvent.input(search, { target: { value: "video" } });
    expect(screen.getByText("video.mp4")).toBeInTheDocument();
    expect(screen.queryByText("clips")).not.toBeInTheDocument();
  });

  it("uploads a file via PutObject with the expected wire shape", async () => {
    await renderLoaded();

    const uploadButtons = screen.getAllByRole("button", { name: "Upload" });
    await fireEvent.click(uploadButtons[0]);

    const file = new File(["hello world"], "clip.mp4", { type: "video/mp4" });
    const fileInput = screen.getByLabelText("File *") as HTMLInputElement;
    await fireEvent.change(fileInput, { target: { files: [file] } });

    const pathInput = screen.getByLabelText("Object Path *") as HTMLInputElement;
    expect(pathInput.value).toBe("clip.mp4");

    mockSend.mockResolvedValueOnce({
      ContentSHA256: "sha",
      ETag: '"etag"',
      StorageClass: "TEMPORAL",
    });
    mockSend.mockResolvedValueOnce({ Items: [] });

    const submitButtons = screen.getAllByRole("button", { name: "Upload" });
    await fireEvent.click(submitButtons.at(-1)!);

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));
    const putInput = mockSend.mock.calls[1][0].input;
    expect(putInput.Path).toBe("clip.mp4");
    expect(putInput.ContentType).toBe("video/mp4");
    expect(putInput.Body).toBeInstanceOf(Uint8Array);
    // StorageClass must never be sent -- TEMPORAL is the only legal value
    // and the backend already defaults to it when omitted.
    expect(putInput.StorageClass).toBeUndefined();
  });

  it("deletes an object via DeleteObject after confirmation", async () => {
    await renderLoaded();
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Items: [folderItem] });

    await fireEvent.click(screen.getByLabelText("Delete video.mp4"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3));
    expect(confirmDestructive).toHaveBeenCalled();
    const deleteInput = mockSend.mock.calls[1][0].input;
    expect(deleteInput.Path).toBe("video.mp4");
  });

  it("opens object details via DescribeObject and renders real response fields", async () => {
    await renderLoaded();
    mockSend.mockResolvedValueOnce({
      ETag: '"abc123"',
      ContentType: "video/mp4",
      ContentLength: 2048,
      CacheControl: "max-age=3600",
      LastModified: new Date("2026-01-01T00:00:00Z"),
    });

    await fireEvent.click(screen.getByLabelText("Details for video.mp4"));

    await waitFor(() => {
      expect(screen.getByText('"abc123"')).toBeInTheDocument();
    });
    const describeInput = mockSend.mock.calls[1][0].input;
    expect(describeInput.Path).toBe("video.mp4");
    expect(screen.getByText("max-age=3600")).toBeInTheDocument();
  });

  it("fetches a byte range via GetObject and renders the real status/content-range", async () => {
    await renderLoaded();
    mockSend.mockResolvedValueOnce({
      ETag: '"abc123"',
      ContentType: "video/mp4",
      ContentLength: 2048,
      LastModified: new Date("2026-01-01T00:00:00Z"),
    });
    await fireEvent.click(screen.getByLabelText("Details for video.mp4"));
    await waitFor(() => expect(screen.getByText('"abc123"')).toBeInTheDocument());

    mockSend.mockResolvedValueOnce({
      StatusCode: 206,
      ContentRange: "bytes 0-99/2048",
      ContentType: "video/mp4",
      Body: { transformToByteArray: vi.fn().mockResolvedValue(new Uint8Array(100)) },
    });

    const rangeField = screen.getByLabelText("Range header value");
    await fireEvent.input(rangeField, { target: { value: "bytes=0-99" } });
    await fireEvent.click(screen.getByRole("button", { name: "Fetch" }));

    await waitFor(() => {
      expect(screen.getByText("bytes 0-99/2048")).toBeInTheDocument();
    });
    const rangeCallInput = mockSend.mock.calls[2][0].input;
    expect(rangeCallInput.Path).toBe("video.mp4");
    expect(rangeCallInput.Range).toBe("bytes=0-99");
    const status = within(screen.getByText("Status:").parentElement as HTMLElement);
    expect(status.getByText("206")).toBeInTheDocument();
  });
});
