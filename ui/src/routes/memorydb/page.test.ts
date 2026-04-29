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

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(false),
}));

describe("MemoryDB Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockSend.mockResolvedValue({ Clusters: [], Snapshots: [], Users: [], ACLs: [] });
  });

  it("renders page title", () => {
    render(MemoryDBPage);
    expect(screen.getByText("Amazon MemoryDB")).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    render(MemoryDBPage);
    expect(screen.getByText("Total Clusters")).toBeInTheDocument();
  });

  it("shows search input", () => {
    render(MemoryDBPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no clusters", async () => {
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
      Snapshots: [],
      Users: [],
      ACLs: [],
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
    render(MemoryDBPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Clusters tab", () => {
    render(MemoryDBPage);
    expect(screen.getAllByText("Clusters").length).toBeGreaterThanOrEqual(1);
  });

  it("shows Snapshots tab", () => {
    render(MemoryDBPage);
    expect(screen.getAllByText("Snapshots").length).toBeGreaterThanOrEqual(1);
  });

  it("shows Users tab", () => {
    render(MemoryDBPage);
    expect(screen.getAllByText("Users").length).toBeGreaterThanOrEqual(1);
  });

  it("shows ACLs tab", () => {
    render(MemoryDBPage);
    expect(screen.getAllByText("ACLs").length).toBeGreaterThanOrEqual(1);
  });

  it("shows Create Cluster button", () => {
    render(MemoryDBPage);
    expect(screen.getAllByText(/create cluster/i).length).toBeGreaterThanOrEqual(1);
  });

  it("displays loaded users when Users tab is populated", async () => {
    mockSend.mockResolvedValue({
      Clusters: [],
      Snapshots: [],
      Users: [{ Name: "alice", Status: "active", AccessString: "on ~* +@all" }],
      ACLs: [],
    });
    render(MemoryDBPage);
    // Click Users tab
    await waitFor(() => {
      const usersTabs = screen.getAllByText("Users");
      expect(usersTabs.length).toBeGreaterThanOrEqual(1);
    });
    const usersTabs = screen.getAllByText("Users");
    usersTabs[0].click();
    await waitFor(
      () => {
        expect(screen.getByText("alice")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded ACLs when ACLs tab is populated", async () => {
    mockSend.mockResolvedValue({
      Clusters: [],
      Snapshots: [],
      Users: [],
      ACLs: [{ Name: "my-acl", Status: "active", UserNames: [] }],
    });
    render(MemoryDBPage);
    await waitFor(() => {
      const aclTabs = screen.getAllByText("ACLs");
      expect(aclTabs.length).toBeGreaterThanOrEqual(1);
    });
    const aclTabs = screen.getAllByText("ACLs");
    aclTabs[0].click();
    await waitFor(
      () => {
        expect(screen.getByText("my-acl")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
