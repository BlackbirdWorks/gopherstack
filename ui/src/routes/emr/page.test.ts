import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import EMRPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEMRClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

const exampleCluster = {
  Id: "j-ABC123",
  Name: "my-cluster",
  Status: {
    State: "WAITING",
    Timeline: { CreationDateTime: new Date("2024-01-01T00:00:00Z") },
  },
  ClusterArn: "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-ABC123",
  NormalizedInstanceHours: 5,
};

describe("EMR Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(EMRPage);
    expect(screen.getByText("Amazon EMR")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(EMRPage);
    expect(screen.getByText("Clusters")).toBeInTheDocument();
    expect(screen.getByText("Steps")).toBeInTheDocument();
    expect(screen.getByText("Instance Groups")).toBeInTheDocument();
    expect(screen.getByText("Instance Fleets")).toBeInTheDocument();
    expect(screen.getByText("Instances")).toBeInTheDocument();
    expect(screen.getByText("Bootstrap Actions")).toBeInTheDocument();
    expect(screen.getByText("Security Configurations")).toBeInTheDocument();
    expect(screen.getByText("Studios")).toBeInTheDocument();
    expect(screen.getByText("Notebook Executions")).toBeInTheDocument();
  });

  it("lists clusters", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-cluster" })).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Throttled."), {
      name: "InternalServerException",
      $metadata: { httpStatusCode: 500 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(EMRPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("InternalServerException (HTTP 500): Throttled."),
      ).toBeInTheDocument();
    });
  });

  it("launches a cluster via RunJobFlow", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    await fireEvent.click(screen.getByText("Launch cluster"));
    expect(screen.getByText("Launch Cluster")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Cluster Name"), {
      target: { value: "new-cluster" },
    });

    mockSend.mockResolvedValueOnce({ JobFlowId: "j-NEW456", ClusterArn: "arn:new" });
    mockSend.mockResolvedValueOnce({
      Clusters: [exampleCluster, { ...exampleCluster, Id: "j-NEW456", Name: "new-cluster" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Launch" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "new-cluster" })).toBeInTheDocument();
    });

    const runJobFlowCall = mockSend.mock.calls[1][0];
    expect(runJobFlowCall.input.Name).toBe("new-cluster");
    expect(runJobFlowCall.input.Instances.InstanceGroups[0].InstanceRole).toBe("MASTER");
  });

  it("terminates a cluster after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Clusters: [] });

    await fireEvent.click(screen.getByTitle("Terminate"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.queryByRole("cell", { name: "my-cluster" })).not.toBeInTheDocument();
    });
    const terminateCall = mockSend.mock.calls[1][0];
    expect(terminateCall.input.JobFlowIds).toEqual(["j-ABC123"]);
  });

  it("does not terminate a cluster when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    await fireEvent.click(screen.getByTitle("Terminate"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListClusters call -- no TerminateJobFlows, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "my-cluster" })).toBeInTheDocument();
  });

  it("opens a cluster's detail view", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    mockSend.mockResolvedValueOnce({
      Cluster: { ...exampleCluster, ReleaseLabel: "emr-7.3.0" },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText("emr-7.3.0").length).toBeGreaterThan(0);
    });
  });

  it("switches to the steps tab, scoped to the selected cluster, and adds a step", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    mockSend.mockResolvedValueOnce({
      Steps: [{ Id: "s-1", Name: "load-data", Status: { State: "PENDING" } }],
    });
    await fireEvent.click(screen.getByText("Steps"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "load-data" })).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("Add step"));
    await fireEvent.input(screen.getByLabelText("Step Name"), {
      target: { value: "transform-data" },
    });

    mockSend.mockResolvedValueOnce({ StepIds: ["s-2"] });
    mockSend.mockResolvedValueOnce({
      Steps: [
        { Id: "s-1", Name: "load-data", Status: { State: "PENDING" } },
        { Id: "s-2", Name: "transform-data", Status: { State: "PENDING" } },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "transform-data" })).toBeInTheDocument();
    });

    const addStepsCall = mockSend.mock.calls[2][0];
    expect(addStepsCall.input.JobFlowId).toBe("j-ABC123");
    expect(addStepsCall.input.Steps[0].Name).toBe("transform-data");
  });

  it("cancels a pending step", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    mockSend.mockResolvedValueOnce({
      Steps: [{ Id: "s-1", Name: "load-data", Status: { State: "PENDING" } }],
    });
    await fireEvent.click(screen.getByText("Steps"));
    await waitFor(() => screen.getByRole("cell", { name: "load-data" }));

    mockSend.mockResolvedValueOnce({
      CancelStepsInfoList: [{ StepId: "s-1", Status: "SUBMITTED" }],
    });
    mockSend.mockResolvedValueOnce({
      Steps: [{ Id: "s-1", Name: "load-data", Status: { State: "PENDING" } }],
    });

    await fireEvent.click(screen.getByTitle("Cancel"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      const cancelCall = mockSend.mock.calls[2][0];
      expect(cancelCall.input.StepIds).toEqual(["s-1"]);
    });
  });

  it("creates a security configuration", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    mockSend.mockResolvedValueOnce({ SecurityConfigurations: [] });
    await fireEvent.click(screen.getByText("Security Configurations"));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));

    await fireEvent.click(screen.getByText("Create security configuration"));
    await fireEvent.input(screen.getByLabelText("Configuration Name"), {
      target: { value: "my-sec-config" },
    });

    mockSend.mockResolvedValueOnce({ Name: "my-sec-config", CreationDateTime: new Date() });
    mockSend.mockResolvedValueOnce({
      SecurityConfigurations: [{ Name: "my-sec-config", CreationDateTime: new Date() }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-sec-config" })).toBeInTheDocument();
    });
  });

  it("deletes a studio after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [exampleCluster] });
    render(EMRPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-cluster" }));

    mockSend.mockResolvedValueOnce({
      Studios: [{ StudioId: "es-1", Name: "my-studio", AuthMode: "SSO", VpcId: "vpc-1" }],
    });
    await fireEvent.click(screen.getByText("Studios"));
    await waitFor(() => screen.getByRole("cell", { name: "my-studio" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Studios: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.queryByRole("cell", { name: "my-studio" })).not.toBeInTheDocument();
    });
    const deleteStudioCall = mockSend.mock.calls[2][0];
    expect(deleteStudioCall.input.StudioId).toBe("es-1");
  });

  it("shows a prompt to select a cluster for scoped tabs when none is selected", async () => {
    mockSend.mockResolvedValueOnce({ Clusters: [] });
    render(EMRPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    // No cluster is selected (the cluster list is empty), so fetchSteps
    // never calls the API at all -- it short-circuits to an empty list.
    await fireEvent.click(screen.getByText("Steps"));

    await waitFor(() => {
      expect(screen.getByText("Select a cluster to see its steps")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(1);
  });
});
