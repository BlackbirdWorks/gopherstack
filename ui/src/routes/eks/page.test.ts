import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import EKSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEKSClient: () => ({ send: mockSend }),
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

describe("EKS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ clusters: [] });
    render(EKSPage);
    expect(screen.getByText("EKS Clusters")).toBeInTheDocument();
  });

  it("shows Create Cluster button", () => {
    mockSend.mockResolvedValue({ clusters: [] });
    render(EKSPage);
    expect(screen.getByText("Create Cluster")).toBeInTheDocument();
  });

  it("shows empty state when no clusters", async () => {
    mockSend.mockResolvedValue({ clusters: [] });
    render(EKSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No clusters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded clusters", async () => {
    mockSend.mockResolvedValueOnce({ clusters: ["prod-cluster", "staging-cluster"] });
    render(EKSPage);
    await waitFor(
      () => {
        expect(screen.getByText("prod-cluster")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("staging-cluster")).toBeInTheDocument();
  });

  it("filters clusters via search", async () => {
    mockSend.mockResolvedValueOnce({
      clusters: ["prod-cluster", "dev-cluster", "staging-cluster"],
    });
    render(EKSPage);
    await waitFor(
      () => {
        expect(screen.getByText("prod-cluster")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search clusters...");
    await fireEvent.input(searchInput, { target: { value: "dev" } });
    await waitFor(() => {
      expect(screen.queryByText("prod-cluster")).not.toBeInTheDocument();
    });
    expect(screen.getByText("dev-cluster")).toBeInTheDocument();
  });

  it("opens create cluster modal", async () => {
    mockSend.mockResolvedValue({ clusters: [] });
    render(EKSPage);
    await fireEvent.click(screen.getByText("Create Cluster"));
    await waitFor(() => {
      expect(screen.getByText("Create EKS Cluster")).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. production-cluster")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ clusters: [] });
    render(EKSPage);
    await fireEvent.click(screen.getByText("Create Cluster"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. production-cluster")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. production-cluster")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(EKSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans ListClusters out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ clusters: ["prod-cluster"] });
      mockSend.mockResolvedValueOnce({ clusters: ["eu-cluster"] });

      render(EKSPage);

      await waitFor(() => expect(screen.getByText("prod-cluster")).toBeInTheDocument());
      expect(screen.getByText("eu-cluster")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListClusters call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ clusters: ["prod-cluster"] });
      render(EKSPage);
      await waitFor(() => expect(screen.getByText("prod-cluster")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same cluster name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ clusters: ["shared-cluster"] });
      mockSend.mockResolvedValueOnce({ clusters: ["shared-cluster"] });

      render(EKSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-cluster");
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
  });
});
