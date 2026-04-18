import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import MSKPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMSKClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("MSK Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ClusterInfoList: [] });
    render(MSKPage);
    expect(screen.getByText("Amazon MSK")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend.mockResolvedValueOnce({ ClusterInfoList: [] });
    render(MSKPage);
    expect(screen.getByText("Clusters")).toBeInTheDocument();
    expect(screen.getByText("Kafka Versions")).toBeInTheDocument();
  });

  it("displays clusters", async () => {
    mockSend.mockResolvedValueOnce({
      ClusterInfoList: [
        {
          ClusterArn: "arn:kafka:cluster/my-cluster",
          ClusterName: "my-cluster",
          ClusterType: "PROVISIONED",
          State: "ACTIVE",
          CurrentVersion: "K1ABC123",
        },
      ],
    });

    render(MSKPage);

    await waitFor(() => {
      expect(screen.getByText("my-cluster")).toBeInTheDocument();
    });
  });

  it("shows create cluster modal", async () => {
    mockSend.mockResolvedValueOnce({ ClusterInfoList: [] });
    render(MSKPage);
    await fireEvent.click(screen.getByText("Create Cluster"));
    expect(screen.getByText("Create MSK Cluster")).toBeInTheDocument();
  });

  it("switches to versions tab", async () => {
    mockSend
      .mockResolvedValueOnce({ ClusterInfoList: [] })
      .mockResolvedValueOnce({ KafkaVersions: [{ Version: "3.5.1", Status: "ACTIVE" }] });
    render(MSKPage);
    await fireEvent.click(screen.getByText("Kafka Versions"));
    await waitFor(() => {
      expect(screen.getByText("3.5.1")).toBeInTheDocument();
    });
  });
});
