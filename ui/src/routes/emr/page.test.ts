import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import EMRPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEMRClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("EMR Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(EMRPage);
    expect(screen.getByText("Amazon EMR")).toBeInTheDocument();
  });

  it("shows empty state when no clusters", async () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(EMRPage);
    await waitFor(
      () => {
        expect(screen.getByText("No active clusters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded clusters", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          Id: "j-abc123",
          Name: "analytics-cluster",
          Status: { State: "RUNNING" },
          NormalizedInstanceHours: 120,
        },
        {
          Id: "j-def456",
          Name: "ml-training",
          Status: { State: "WAITING" },
          NormalizedInstanceHours: 48,
        },
      ],
    });
    render(EMRPage);
    await waitFor(
      () => {
        expect(screen.getByText("analytics-cluster")).toBeInTheDocument();
        expect(screen.getByText("ml-training")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(EMRPage);
    expect(screen.getByPlaceholderText("Search clusters...")).toBeInTheDocument();
  });

  it("shows cluster status badges", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          Id: "j-abc",
          Name: "my-cluster",
          Status: { State: "RUNNING" },
          NormalizedInstanceHours: 10,
        },
      ],
    });
    render(EMRPage);
    await waitFor(
      () => {
        expect(screen.getByText("RUNNING")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("navigates to cluster detail", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          Id: "j-abc",
          Name: "my-cluster",
          Status: { State: "RUNNING" },
          NormalizedInstanceHours: 5,
        },
      ],
    });
    render(EMRPage);
    await waitFor(() => screen.getByText("my-cluster"), { timeout: 3000 });

    mockSend.mockResolvedValue({
      Cluster: {
        Id: "j-abc",
        Name: "my-cluster",
        Status: { State: "RUNNING", Timeline: { CreationDateTime: new Date() } },
        ReleaseLabel: "emr-7.0.0",
        AutoTerminate: false,
        NormalizedInstanceHours: 5,
        Applications: [
          { Name: "Spark", Version: "3.5.0" },
          { Name: "Hadoop", Version: "3.3.6" },
        ],
      },
    });

    await fireEvent.click(screen.getByText("my-cluster"));
    await waitFor(
      () => {
        expect(screen.getByText("Overview")).toBeInTheDocument();
        expect(screen.getByText("Steps")).toBeInTheDocument();
        expect(screen.getByText("Instances")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows applications as badges in overview", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          Id: "j-abc",
          Name: "my-cluster",
          Status: { State: "RUNNING" },
          NormalizedInstanceHours: 5,
        },
      ],
    });
    render(EMRPage);
    await waitFor(() => screen.getByText("my-cluster"), { timeout: 3000 });

    mockSend.mockResolvedValue({
      Cluster: {
        Id: "j-abc",
        Name: "my-cluster",
        Status: { State: "RUNNING", Timeline: {} },
        ReleaseLabel: "emr-7.0.0",
        Applications: [{ Name: "Spark", Version: "3.5" }],
      },
    });

    await fireEvent.click(screen.getByText("my-cluster"));
    await waitFor(
      () => {
        expect(screen.getByText("Spark 3.5")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
