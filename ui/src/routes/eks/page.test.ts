import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import EKSPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEKSClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("EKS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
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
});
