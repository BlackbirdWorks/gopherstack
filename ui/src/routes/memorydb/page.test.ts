import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import MemoryDBPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMemoryDBClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("MemoryDB Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Clusters: [], Snapshots: [] });
    render(MemoryDBPage);
    expect(screen.getByText("Amazon MemoryDB")).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ Clusters: [], Snapshots: [] });
    render(MemoryDBPage);
    expect(screen.getByText("Total Clusters")).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ Clusters: [], Snapshots: [] });
    render(MemoryDBPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no clusters", async () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(MemoryDBPage);
    await waitFor(
      () => {
        expect(screen.getByText(/no clusters/i)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded clusters", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          Name: "my-cluster",
          Status: "available",
          NodeType: "db.r6g.large",
          NumberOfShards: 1,
          EngineVersion: "6.2",
        },
      ],
    });
    render(MemoryDBPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-cluster")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(MemoryDBPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Clusters tab", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(MemoryDBPage);
    expect(screen.getAllByText("Clusters").length).toBeGreaterThanOrEqual(1);
  });

  it("shows Snapshots tab", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(MemoryDBPage);
    expect(screen.getAllByText("Snapshots").length).toBeGreaterThanOrEqual(1);
  });
});
