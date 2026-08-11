import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import BackupPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getBackupClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
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

describe("AWS Backup Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ BackupPlansList: [] });
    render(BackupPage);
    expect(screen.getByText("AWS Backup")).toBeInTheDocument();
  });

  it("shows three tabs", () => {
    mockSend.mockResolvedValueOnce({ BackupPlansList: [] });
    render(BackupPage);
    expect(screen.getByText("Backup Plans")).toBeInTheDocument();
    expect(screen.getByText("Backup Vaults")).toBeInTheDocument();
    expect(screen.getByText("Backup Jobs")).toBeInTheDocument();
  });

  it("displays backup plans", async () => {
    mockSend.mockResolvedValueOnce({
      BackupPlansList: [
        {
          BackupPlanId: "plan-1",
          BackupPlanName: "daily-backup",
          VersionId: "v1",
          CreationDate: new Date("2024-01-01"),
        },
      ],
    });

    render(BackupPage);

    await waitFor(() => {
      expect(screen.getByText("daily-backup")).toBeInTheDocument();
    });
  });

  it("shows create plan modal", async () => {
    mockSend.mockResolvedValueOnce({ BackupPlansList: [] });
    render(BackupPage);
    await fireEvent.click(screen.getByText("Create Plan"));
    expect(screen.getByText("Create Backup Plan")).toBeInTheDocument();
  });

  it("switches to vaults tab", async () => {
    mockSend
      .mockResolvedValueOnce({ BackupPlansList: [] })
      .mockResolvedValueOnce({ BackupVaultList: [] });
    render(BackupPage);
    await fireEvent.click(screen.getByText("Backup Vaults"));
    await waitFor(() => {
      expect(screen.getByText("No backup vaults found")).toBeInTheDocument();
    });
  });

  describe("All regions mode", () => {
    it("fans ListBackupPlans out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        BackupPlansList: [{ BackupPlanId: "p1", BackupPlanName: "daily-backup" }],
      });
      mockSend.mockResolvedValueOnce({
        BackupPlansList: [{ BackupPlanId: "p2", BackupPlanName: "eu-backup" }],
      });

      render(BackupPage);

      await waitFor(() => expect(screen.getByText("daily-backup")).toBeInTheDocument());
      expect(screen.getByText("eu-backup")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListBackupPlans call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({
        BackupPlansList: [{ BackupPlanId: "p1", BackupPlanName: "daily-backup" }],
      });
      render(BackupPage);
      await waitFor(() => expect(screen.getByText("daily-backup")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same plan name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        BackupPlansList: [{ BackupPlanId: "p1", BackupPlanName: "shared-plan" }],
      });
      mockSend.mockResolvedValueOnce({
        BackupPlansList: [{ BackupPlanId: "p2", BackupPlanName: "shared-plan" }],
      });

      render(BackupPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-plan");
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
