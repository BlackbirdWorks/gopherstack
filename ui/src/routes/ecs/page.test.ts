import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import ECSPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getECSClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

const clusterArn = "arn:aws:ecs:us-east-1:123456789:cluster/prod-cluster";
const serviceArn = "arn:aws:ecs:us-east-1:123456789:service/prod-cluster/api-service";
const taskArn = "arn:aws:ecs:us-east-1:123456789:task/prod-cluster/abc123defabc456";
const taskDefArn = "arn:aws:ecs:us-east-1:123456789:task-definition/my-app:5";

describe("ECS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ clusterArns: [], taskDefinitionArns: [] });
    render(ECSPage);
    expect(screen.getByText("ECS Clusters")).toBeInTheDocument();
  });

  it("shows Elastic Container Service subtitle", () => {
    mockSend.mockResolvedValue({ clusterArns: [], taskDefinitionArns: [] });
    render(ECSPage);
    expect(screen.getByText("Elastic Container Service")).toBeInTheDocument();
  });

  it("shows Create Cluster button", () => {
    mockSend.mockResolvedValue({ clusterArns: [], taskDefinitionArns: [] });
    render(ECSPage);
    expect(screen.getByText("Create Cluster")).toBeInTheDocument();
  });

  it("renders 4 stat cards", () => {
    mockSend.mockResolvedValue({ clusterArns: [], taskDefinitionArns: [] });
    render(ECSPage);
    expect(screen.getByText("Clusters")).toBeInTheDocument();
    expect(screen.getByText("Services")).toBeInTheDocument();
    expect(screen.getByText("Running Tasks")).toBeInTheDocument();
    expect(screen.getByText("Task Definitions")).toBeInTheDocument();
  });

  it("shows empty state when no clusters", async () => {
    mockSend.mockResolvedValue({ clusterArns: [], taskDefinitionArns: [] });
    render(ECSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No ECS clusters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded clusters", async () => {
    // ListClusters
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    // ListServices
    mockSend.mockResolvedValueOnce({ serviceArns: [] });
    // ListTasks
    mockSend.mockResolvedValueOnce({ taskArns: [] });
    // ListTaskDefinitions
    mockSend.mockResolvedValueOnce({ taskDefinitionArns: [] });
    render(ECSPage);
    await waitFor(
      () => {
        expect(screen.getByText("prod-cluster")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows services tab when cluster has services", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ serviceArns: [serviceArn] });
    mockSend.mockResolvedValueOnce({ taskArns: [] });
    mockSend.mockResolvedValueOnce({ taskDefinitionArns: [] });
    render(ECSPage);
    await waitFor(
      () => {
        expect(screen.getByText("api-service")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows task definitions tab", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ serviceArns: [] });
    mockSend.mockResolvedValueOnce({ taskArns: [] });
    mockSend.mockResolvedValueOnce({ taskDefinitionArns: [taskDefArn] });
    render(ECSPage);
    await waitFor(
      () => {
        expect(screen.getByText("Task Definitions")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows running tasks tab", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ serviceArns: [] });
    mockSend.mockResolvedValueOnce({ taskArns: [taskArn] });
    mockSend.mockResolvedValueOnce({ taskDefinitionArns: [] });
    render(ECSPage);
    await waitFor(
      () => {
        expect(screen.getByText("Tasks")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on cluster load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network error"));
    mockSend.mockResolvedValue({ taskDefinitionArns: [] });
    render(ECSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
