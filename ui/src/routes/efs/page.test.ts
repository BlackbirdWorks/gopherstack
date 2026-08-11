import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/svelte";
import EFSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEFSClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

describe("EFS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // DescribeFileSystems call against a single region, so pin single-region
    // mode here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    expect(screen.getByText("Amazon EFS")).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    expect(screen.getAllByText("File Systems").length).toBeGreaterThanOrEqual(1);
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no file systems", async () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    await waitFor(
      () => {
        expect(screen.getByText(/no file systems/i)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded file systems", async () => {
    mockSend.mockResolvedValue({
      FileSystems: [
        {
          FileSystemId: "fs-12345",
          LifeCycleState: "available",
          PerformanceMode: "generalPurpose",
          SizeInBytes: { Value: 1073741824 },
        },
      ],
    });
    render(EFSPage);
    await waitFor(
      () => {
        expect(screen.getByText("fs-12345")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows File Systems tab", () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    expect(screen.getAllByText("File Systems").length).toBeGreaterThanOrEqual(1);
  });

  it("shows Mount Targets tab", () => {
    mockSend.mockResolvedValue({ FileSystems: [] });
    render(EFSPage);
    expect(screen.getAllByText("Mount Targets").length).toBeGreaterThanOrEqual(1);
  });

  describe("All regions mode", () => {
    it("fans DescribeFileSystems out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ FileSystems: [{ FileSystemId: "fs-us" }] });
      mockSend.mockResolvedValueOnce({ FileSystems: [{ FileSystemId: "fs-eu" }] });

      render(EFSPage);

      await waitFor(() => expect(screen.getByText("fs-us")).toBeInTheDocument());
      expect(screen.getByText("fs-eu")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ FileSystems: [{ FileSystemId: "fs-solo" }] });

      render(EFSPage);

      await waitFor(() => expect(screen.getByText("fs-solo")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeFileSystems call in single-region mode", async () => {
      mockSend.mockResolvedValue({ FileSystems: [{ FileSystemId: "fs-solo" }] });
      render(EFSPage);
      await waitFor(() => expect(screen.getByText("fs-solo")).toBeInTheDocument());
      const describeFsCalls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "DescribeFileSystemsCommand",
      );
      expect(describeFsCalls).toHaveLength(1);
    });

    it("renders the same file system id from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ FileSystems: [{ FileSystemId: "fs-shared" }] });
      mockSend.mockResolvedValueOnce({ FileSystems: [{ FileSystemId: "fs-shared" }] });

      render(EFSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("fs-shared");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
