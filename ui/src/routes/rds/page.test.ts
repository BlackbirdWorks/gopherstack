import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import RDSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();
vi.mock("$lib/aws-client", () => ({ getRDSClient: () => ({ send: mockSend }) }));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
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

const mockDB = {
  DBInstanceIdentifier: "my-postgres",
  DBInstanceClass: "db.t3.medium",
  Engine: "postgres",
  DBInstanceStatus: "available",
  Endpoint: { Address: "my-postgres.abc.us-east-1.rds.amazonaws.com", Port: 5432 },
  AllocatedStorage: 100,
  MultiAZ: false,
};

describe("RDS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    // Every test below predates "All" mode and assumes exactly one
    // Describe*Command call per action against a single region, so pin
    // single-region mode here; the "All regions mode" describe block below
    // opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ DBInstances: [] });
    render(RDSPage);
    expect(screen.getByText("RDS Instances")).toBeInTheDocument();
  });

  it("shows Create Instance button", () => {
    mockSend.mockResolvedValue({ DBInstances: [] });
    render(RDSPage);
    expect(screen.getByText("Create Instance")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({ DBInstances: [] });
    render(RDSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No RDS instances found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded db instances", async () => {
    mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
    render(RDSPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-postgres")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getAllByText("postgres").length).toBeGreaterThan(0);
  });

  it("shows status badge", async () => {
    mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
    render(RDSPage);
    await waitFor(
      () => {
        expect(screen.getByText("available")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows endpoint address", async () => {
    mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
    render(RDSPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-postgres.abc.us-east-1.rds.amazonaws.com")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("filters instances by search", async () => {
    const db2 = {
      ...mockDB,
      DBInstanceIdentifier: "my-mysql",
      Engine: "mysql",
      DBInstanceStatus: "available",
      Endpoint: { Address: "my-mysql.xyz.rds.amazonaws.com", Port: 3306 },
    };
    mockSend.mockResolvedValueOnce({ DBInstances: [mockDB, db2] });
    render(RDSPage);
    await waitFor(() => expect(screen.getByText("my-postgres")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const searchInput = screen.getByPlaceholderText(/search/i);
    await fireEvent.input(searchInput, { target: { value: "mysql" } });
    await waitFor(() => expect(screen.queryByText("my-postgres")).not.toBeInTheDocument());
    expect(screen.getByText("my-mysql")).toBeInTheDocument();
  });

  it("shows error toast on failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network error"));
    render(RDSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalled(), { timeout: 3000 });
  });

  it("shows instance class", async () => {
    mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
    render(RDSPage);
    await waitFor(
      () => {
        expect(screen.getByText("db.t3.medium")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans DescribeDBInstances out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
      mockSend.mockResolvedValueOnce({
        DBInstances: [{ ...mockDB, DBInstanceIdentifier: "eu-postgres" }],
      });

      render(RDSPage);

      await waitFor(() => expect(screen.getByText("my-postgres")).toBeInTheDocument());
      expect(screen.getByText("eu-postgres")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });

      render(RDSPage);

      await waitFor(() => expect(screen.getByText("my-postgres")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeDBInstances call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
      render(RDSPage);
      await waitFor(() => expect(screen.getByText("my-postgres")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same instance id from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });

      render(RDSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("my-postgres");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".rounded-lg") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("deletes the row's own region, not the picker's, when two regions share an instance id", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });

      render(RDSPage);
      await waitFor(() => expect(screen.getAllByText("my-postgres")).toHaveLength(2));

      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({ DBInstances: [mockDB] });
      mockSend.mockResolvedValueOnce({ DBInstances: [] });

      const rows = screen.getAllByText("my-postgres");
      const euRow = rows
        .map((r) => r.closest(".rounded-lg") as HTMLElement)
        .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
      await fireEvent.click(within(euRow).getByRole("button", { name: /delete/i }));

      await waitFor(() => {
        const remaining = screen.getAllByText("my-postgres");
        expect(remaining).toHaveLength(1);
        expect(
          within(remaining[0].closest(".rounded-lg") as HTMLElement).getByTestId("region-chip")
            .textContent,
        ).toBe("us-east-1");
      });

      vi.unstubAllGlobals();
    });
  });
});

// Additional tab tests (appended)

it("shows 3 tab buttons", () => {
  mockSend.mockResolvedValue({ DBInstances: [] });
  render(RDSPage);
  const btns = screen.getAllByRole("button");
  const texts = btns.map((b) => b.textContent?.trim());
  expect(texts.some((t) => t?.includes("Instances"))).toBe(true);
  expect(texts.some((t) => t?.includes("Snapshots"))).toBe(true);
  expect(texts.some((t) => t?.includes("Aurora Clusters"))).toBe(true);
});

it("loads and displays snapshots", async () => {
  mockSend.mockResolvedValueOnce({ DBInstances: [] });
  mockSend.mockResolvedValueOnce({
    DBSnapshots: [
      {
        DBSnapshotIdentifier: "snap-2024",
        DBInstanceIdentifier: "my-db",
        Engine: "postgres",
        Status: "available",
        SnapshotType: "manual",
        AllocatedStorage: 20,
      },
    ],
  });
  render(RDSPage);
  await waitFor(() => screen.getByText("No RDS instances found"), { timeout: 3000 });
  const snapBtns = screen.getAllByText(/Snapshots/i);
  const tabBtn = snapBtns.find((el) => el.closest("button"));
  if (tabBtn) await fireEvent.click(tabBtn.closest("button") ?? tabBtn);
  await waitFor(
    () => {
      expect(screen.getByText("snap-2024")).toBeInTheDocument();
    },
    { timeout: 3000 },
  );
});
