import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ECSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getECSClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
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

const clusterArn = "arn:aws:ecs:us-east-1:123456789:cluster/prod-cluster";
const serviceArn = "arn:aws:ecs:us-east-1:123456789:service/prod-cluster/api-service";

const exampleCluster = {
  clusterArn,
  clusterName: "prod-cluster",
  status: "ACTIVE",
  runningTasksCount: 2,
  pendingTasksCount: 0,
  activeServicesCount: 1,
  registeredContainerInstancesCount: 0,
};

const exampleService = {
  serviceArn,
  serviceName: "api-service",
  clusterArn,
  taskDefinition: "arn:aws:ecs:us-east-1:123456789:task-definition/api:3",
  status: "ACTIVE",
  desiredCount: 2,
  runningCount: 2,
  pendingCount: 0,
};

describe("ECS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [] });
    render(ECSPage);
    expect(screen.getByText("Amazon ECS")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [] });
    render(ECSPage);
    expect(screen.getByText("Clusters")).toBeInTheDocument();
    expect(screen.getByText("Services")).toBeInTheDocument();
    expect(screen.getByText("Tasks")).toBeInTheDocument();
    expect(screen.getByText("Task Definitions")).toBeInTheDocument();
    expect(screen.getByText("Container Instances")).toBeInTheDocument();
    expect(screen.getByText("Capacity Providers")).toBeInTheDocument();
    expect(screen.getByText("Task Sets")).toBeInTheDocument();
    expect(screen.getByText("Account Settings")).toBeInTheDocument();
  });

  it("lists clusters", async () => {
    // ListClusters, then DescribeClusters
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });
    render(ECSPage);
    // Synchronize on an actually-loaded cell, not the empty-state text --
    // the empty state renders before the in-flight fetch resolves and would
    // race the mocked responses out of order.
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "prod-cluster" })).toBeInTheDocument();
    });
  });

  it("creates a cluster via the modal", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [] });
    render(ECSPage);
    await waitFor(() => screen.getByText("Clusters"));

    await fireEvent.click(screen.getByText("Create cluster"));
    expect(screen.getByText("Create Cluster")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Cluster name"), {
      target: { value: "prod-cluster" },
    });

    mockSend.mockResolvedValueOnce({ cluster: exampleCluster });
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "prod-cluster" })).toBeInTheDocument();
    });
  });

  it("deletes a cluster after confirming", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });
    render(ECSPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-cluster" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ clusterArns: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No clusters found")).toBeInTheDocument();
    });
  });

  it("does not delete a cluster when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });
    render(ECSPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-cluster" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListClusters + DescribeClusters calls -- no
    // DeleteCluster, no reload.
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(screen.getByRole("cell", { name: "prod-cluster" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Throttled."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ECSPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(screen.getByText("ThrottlingException (HTTP 400): Throttled.")).toBeInTheDocument();
    });
  });

  it("opens a cluster's detail view", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });
    render(ECSPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-cluster" }));

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(clusterArn).length).toBeGreaterThan(0);
    });
  });

  it("switches to the Services tab, scoped to the auto-selected cluster", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
    mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });
    render(ECSPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-cluster" }));

    // ListServices, then DescribeServices
    mockSend.mockResolvedValueOnce({ serviceArns: [serviceArn] });
    mockSend.mockResolvedValueOnce({ services: [exampleService] });
    await fireEvent.click(screen.getByText("Services"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "api-service" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenLastCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({ cluster: clusterArn }),
      }),
    );
  });

  it("shows a cluster-selection prompt for cluster-scoped tabs with no cluster", async () => {
    mockSend.mockResolvedValueOnce({ clusterArns: [] });
    render(ECSPage);
    await waitFor(() => screen.getByText("Clusters"));

    await fireEvent.click(screen.getByText("Tasks"));

    await waitFor(() => {
      expect(screen.getByText("Select a cluster to see its tasks")).toBeInTheDocument();
    });
  });

  describe("All regions mode", () => {
    const usArn = "arn:aws:ecs:us-east-1:123456789:cluster/us-cluster";
    const euArn = "arn:aws:ecs:eu-west-1:123456789:cluster/eu-cluster";

    it("fans ListClusters+DescribeClusters out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend
        .mockResolvedValueOnce({ clusterArns: [usArn] })
        .mockResolvedValueOnce({ clusterArns: [euArn] })
        .mockResolvedValueOnce({ clusters: [{ clusterArn: usArn, clusterName: "us-cluster" }] })
        .mockResolvedValueOnce({ clusters: [{ clusterArn: euArn, clusterName: "eu-cluster" }] });

      render(ECSPage);

      await waitFor(() =>
        expect(screen.getByRole("cell", { name: "us-cluster" })).toBeInTheDocument(),
      );
      expect(screen.getByRole("cell", { name: "eu-cluster" })).toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListClusters call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ clusterArns: [clusterArn] });
      mockSend.mockResolvedValueOnce({ clusters: [exampleCluster] });
      render(ECSPage);
      await waitFor(() => screen.getByRole("cell", { name: "prod-cluster" }));
      const listCalls = mockSend.mock.calls.filter(
        ([cmd]) => cmd.constructor.name === "ListClustersCommand",
      );
      expect(listCalls).toHaveLength(1);
    });

    it("renders the same cluster name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend
        .mockResolvedValueOnce({ clusterArns: [usArn] })
        .mockResolvedValueOnce({ clusterArns: [euArn] })
        .mockResolvedValueOnce({ clusters: [{ clusterArn: usArn, clusterName: "shared-cluster" }] })
        .mockResolvedValueOnce({
          clusters: [{ clusterArn: euArn, clusterName: "shared-cluster" }],
        });

      render(ECSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByRole("cell", { name: "shared-cluster" });
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("scopes the Services tab to the selected cluster's own region, not the picker's region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend
        .mockResolvedValueOnce({ clusterArns: [] })
        .mockResolvedValueOnce({ clusterArns: [euArn] })
        .mockResolvedValueOnce({ clusters: [{ clusterArn: euArn, clusterName: "eu-cluster" }] });

      render(ECSPage);
      await waitFor(() =>
        expect(screen.getByRole("cell", { name: "eu-cluster" })).toBeInTheDocument(),
      );

      mockSend.mockResolvedValueOnce({ serviceArns: [serviceArn] });
      mockSend.mockResolvedValueOnce({ services: [exampleService] });
      await fireEvent.click(screen.getByText("Services"));

      await waitFor(() =>
        expect(screen.getByRole("cell", { name: "api-service" })).toBeInTheDocument(),
      );
      expect(mockSend).toHaveBeenLastCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({ cluster: euArn }),
        }),
      );

      vi.unstubAllGlobals();
    });
  });
});
