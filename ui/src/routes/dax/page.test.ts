import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DaxPage from "./+page.svelte";
import { DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDAXClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleCluster = {
  ClusterName: "my-cluster",
  NodeType: "dax.r4.large",
  TotalNodes: 3,
  Status: "available",
};

const exampleParamGroup = {
  ParameterGroupName: "my-params",
  Description: "custom params",
};

const exampleSubnetGroup = {
  SubnetGroupName: "my-subnets",
  VpcId: "vpc-12345",
};

describe("DAX Page", () => {
  let promptSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    promptSpy = vi.spyOn(window, "prompt").mockReturnValue(null);
    // Region defaults to "All" for a fresh user (see region.svelte.ts).
    // Every test below predates "All" mode and assumes exactly one
    // DescribeX call per action against a single region, so pin
    // single-region mode here.
    setStoredRegion(DEFAULT_REGION);
  });

  afterEach(() => {
    promptSpy.mockRestore();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    expect(screen.getByText("Amazon DynamoDB Accelerator (DAX)")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    expect(screen.getByText("Clusters")).toBeInTheDocument();
    expect(screen.getByText("Parameter Groups")).toBeInTheDocument();
    expect(screen.getByText("Subnet Groups")).toBeInTheDocument();
  });

  it("lists clusters, rendering node type and node count", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(DaxPage);
    await waitFor(() => {
      expect(screen.getByText("my-cluster")).toBeInTheDocument();
    });
    expect(screen.getByText("dax.r4.large · 3 nodes")).toBeInTheDocument();
  });

  it("shows empty state when there are no clusters", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => {
      expect(screen.getByText("No clusters found")).toBeInTheDocument();
    });
  });

  it("creates a cluster via the modal", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => screen.getByText("No clusters found"));

    mockSend.mockResolvedValueOnce({ Cluster: exampleCluster });
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });

    await fireEvent.click(screen.getByText("Create cluster"));
    const input = screen.getByLabelText("Cluster Name");
    await fireEvent.input(input, { target: { value: "new-cluster" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-cluster")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          ClusterName: "new-cluster",
          NodeType: "dax.r4.large",
          ReplicationFactor: 1,
          IamRoleArn: "arn:aws:iam::000000000000:role/dax-role",
        },
      }),
    );
  });

  it("does not create a cluster when the modal is cancelled", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => screen.getByText("No clusters found"));

    await fireEvent.click(screen.getByText("Create cluster"));
    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    // Only the initial DescribeClusters call -- no CreateCluster, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("deletes a cluster after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(DaxPage);
    await waitFor(() => screen.getByText("my-cluster"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Clusters: [] });

    const row = screen.getByText("my-cluster").closest("div.flex.flex-col") as HTMLElement;
    await fireEvent.click(within(row).getByRole("button", { name: "" }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { ClusterName: "my-cluster" } }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No clusters found")).toBeInTheDocument();
    });
  });

  it("does not delete a cluster when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(DaxPage);
    await waitFor(() => screen.getByText("my-cluster"));

    const row = screen.getByText("my-cluster").closest("div.flex.flex-col") as HTMLElement;
    await fireEvent.click(within(row).getByRole("button", { name: "" }));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial DescribeClusters call -- no DeleteCluster, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-cluster")).toBeInTheDocument();
  });

  it("increases the replication factor to TotalNodes + 1", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(DaxPage);
    await waitFor(() => screen.getByText("my-cluster"));

    mockSend.mockResolvedValueOnce({ Cluster: { ...exampleCluster, TotalNodes: 4 } });
    mockSend.mockResolvedValueOnce({ Clusters: [{ ...exampleCluster, TotalNodes: 4 }] });

    await fireEvent.click(screen.getByText("+ Rep"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: { ClusterName: "my-cluster", NewReplicationFactor: 4 },
        }),
      );
    });
  });

  it("decreases the replication factor to TotalNodes - 1", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(DaxPage);
    await waitFor(() => screen.getByText("my-cluster"));

    mockSend.mockResolvedValueOnce({ Cluster: { ...exampleCluster, TotalNodes: 2 } });
    mockSend.mockResolvedValueOnce({ Clusters: [{ ...exampleCluster, TotalNodes: 2 }] });

    await fireEvent.click(screen.getByText("- Rep"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: { ClusterName: "my-cluster", NewReplicationFactor: 2 },
        }),
      );
    });
  });

  it("refuses to decrease the replication factor below 1", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [{ ...exampleCluster, TotalNodes: 1 }] });
    render(DaxPage);
    await waitFor(() => screen.getByText("my-cluster"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("- Rep"));

    expect(toast.error).toHaveBeenCalledWith("Cannot decrease below 1");
    // Only the initial DescribeClusters call -- no DecreaseReplicationFactor.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("updates a cluster's description via the prompt", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(DaxPage);
    await waitFor(() => screen.getByText("my-cluster"));

    promptSpy.mockReturnValue("new description");
    mockSend.mockResolvedValueOnce({ Cluster: exampleCluster });
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });

    await fireEvent.click(screen.getByText("Edit"));

    expect(promptSpy).toHaveBeenCalledWith("New Description:");
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: { ClusterName: "my-cluster", Description: "new description" },
        }),
      );
    });
  });

  it("shows a toast error including the AWS error code when loading clusters fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(DaxPage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load DAX data: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("shows a toast error including the AWS error code when creating a cluster fails", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => screen.getByText("No clusters found"));

    const error = Object.assign(new Error("Limit exceeded."), {
      name: "LimitExceededFault",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Create cluster"));
    const input = screen.getByLabelText("Cluster Name");
    await fireEvent.input(input, { target: { value: "new-cluster" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith("LimitExceededFault: Limit exceeded.");
    });
  });

  it("switches to the Parameter Groups tab, lists, creates, and deletes a param group", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => screen.getByText("No clusters found"));

    mockSend.mockResolvedValueOnce({ ParameterGroups: [exampleParamGroup] });
    await fireEvent.click(screen.getByText("Parameter Groups"));

    await waitFor(() => {
      expect(screen.getByText("my-params")).toBeInTheDocument();
    });
    expect(screen.getByText("custom params")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ ParameterGroup: exampleParamGroup });
    mockSend.mockResolvedValueOnce({ ParameterGroups: [exampleParamGroup] });

    await fireEvent.click(screen.getByText("Create param group"));
    const input = screen.getByLabelText("Parameter Group Name");
    await fireEvent.input(input, { target: { value: "new-params" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({ input: { ParameterGroupName: "new-params" } }),
      );
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ParameterGroups: [] });

    const row = screen.getByText("my-params").closest("div.justify-between") as HTMLElement;
    await fireEvent.click(within(row).getByRole("button", { name: "" }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No parameter groups found")).toBeInTheDocument();
    });
  });

  it("updates a parameter group via the two prompts", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => screen.getByText("No clusters found"));

    mockSend.mockResolvedValueOnce({ ParameterGroups: [exampleParamGroup] });
    await fireEvent.click(screen.getByText("Parameter Groups"));
    await waitFor(() => screen.getByText("my-params"));

    promptSpy.mockReturnValueOnce("query-cache-ttl").mockReturnValueOnce("300");
    mockSend.mockResolvedValueOnce({ ParameterGroup: exampleParamGroup });
    mockSend.mockResolvedValueOnce({ ParameterGroups: [exampleParamGroup] });

    await fireEvent.click(screen.getByText("Edit Params"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: {
            ParameterGroupName: "my-params",
            ParameterNameValues: [{ ParameterName: "query-cache-ttl", ParameterValue: "300" }],
          },
        }),
      );
    });
  });

  it("switches to the Subnet Groups tab, lists, creates, and deletes a subnet group", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(DaxPage);
    await waitFor(() => screen.getByText("No clusters found"));

    mockSend.mockResolvedValueOnce({ SubnetGroups: [exampleSubnetGroup] });
    await fireEvent.click(screen.getByText("Subnet Groups"));

    await waitFor(() => {
      expect(screen.getByText("my-subnets")).toBeInTheDocument();
    });
    expect(screen.getByText("VPC: vpc-12345")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ SubnetGroup: exampleSubnetGroup });
    mockSend.mockResolvedValueOnce({ SubnetGroups: [exampleSubnetGroup] });

    await fireEvent.click(screen.getByText("Create subnet group"));
    const input = screen.getByLabelText("Subnet Group Name");
    await fireEvent.input(input, { target: { value: "new-subnets" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { SubnetGroupName: "new-subnets", SubnetIds: ["subnet-12345"] },
        }),
      );
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ SubnetGroups: [] });

    const row = screen.getByText("my-subnets").closest("div.justify-between") as HTMLElement;
    await fireEvent.click(within(row).getByRole("button", { name: "" }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No subnet groups found")).toBeInTheDocument();
    });
  });
});
