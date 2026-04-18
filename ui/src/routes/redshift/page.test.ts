import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import RedshiftPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getRedshiftClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Redshift Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(RedshiftPage);
    expect(screen.getByText("Amazon Redshift")).toBeInTheDocument();
  });

  it("shows Create Cluster button", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(RedshiftPage);
    expect(screen.getByText("Create Cluster")).toBeInTheDocument();
  });

  it("shows empty state when no clusters", async () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(RedshiftPage);
    await waitFor(
      () => {
        expect(screen.getByText("No clusters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded clusters", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          ClusterIdentifier: "prod-warehouse",
          ClusterStatus: "available",
          NodeType: "ra3.4xlarge",
          NumberOfNodes: 4,
          DBName: "analytics",
          Endpoint: {
            Address: "prod-warehouse.abc123.us-east-1.redshift.amazonaws.com",
            Port: 5439,
          },
        },
        {
          ClusterIdentifier: "dev-warehouse",
          ClusterStatus: "creating",
          NodeType: "dc2.large",
          NumberOfNodes: 1,
          DBName: "dev",
          Endpoint: null,
        },
      ],
    });
    render(RedshiftPage);
    await waitFor(
      () => {
        expect(screen.getByText("prod-warehouse")).toBeInTheDocument();
        expect(screen.getByText("dev-warehouse")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create cluster modal", async () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(RedshiftPage);
    await fireEvent.click(screen.getByText("Create Cluster"));
    expect(screen.getByText("Cluster Identifier")).toBeInTheDocument();
    expect(screen.getByText("Master Username")).toBeInTheDocument();
  });

  it("cancels create cluster modal", async () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(RedshiftPage);
    await fireEvent.click(screen.getByText("Create Cluster"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Cluster Identifier")).not.toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ Clusters: [] });
    render(RedshiftPage);
    expect(screen.getByPlaceholderText("Search clusters...")).toBeInTheDocument();
  });

  it("shows cluster status badges", async () => {
    mockSend.mockResolvedValue({
      Clusters: [
        {
          ClusterIdentifier: "test-cluster",
          ClusterStatus: "available",
          NodeType: "dc2.large",
          NumberOfNodes: 1,
          DBName: "dev",
        },
      ],
    });
    render(RedshiftPage);
    await waitFor(
      () => {
        expect(screen.getByText("available")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
