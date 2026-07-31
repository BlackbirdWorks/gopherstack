import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import DmsPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDMSClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleInstance = {
  ReplicationInstanceIdentifier: "my-instance",
  ReplicationInstanceArn: "arn:aws:dms:us-east-1:123456789012:rep:instance-1",
  ReplicationInstanceClass: "dms.t3.micro",
  ReplicationInstanceStatus: "available",
  EngineVersion: "3.5.2",
};

const exampleEndpoint = {
  EndpointIdentifier: "my-endpoint",
  EndpointArn: "arn:aws:dms:us-east-1:123456789012:endpoint:endpoint-1",
  EndpointType: "source",
  EngineName: "mysql",
  ServerName: "db.example.com",
  Port: 3306,
  Status: "active",
};

const exampleTargetEndpoint = {
  EndpointIdentifier: "my-target-endpoint",
  EndpointArn: "arn:aws:dms:us-east-1:123456789012:endpoint:endpoint-2",
  EndpointType: "target",
  EngineName: "postgres",
  ServerName: "target.example.com",
  Port: 5432,
  Status: "active",
};

const exampleTask = {
  ReplicationTaskIdentifier: "my-task",
  ReplicationTaskArn: "arn:aws:dms:us-east-1:123456789012:task:task-1",
  Status: "stopped",
  MigrationType: "full-load",
};

describe("DMS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    expect(screen.getByText("AWS Database Migration Service")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    expect(screen.getByText("Replication Instances")).toBeInTheDocument();
    expect(screen.getByText("Endpoints")).toBeInTheDocument();
    expect(screen.getByText("Replication Tasks")).toBeInTheDocument();
    expect(screen.getByText("Subnet Groups")).toBeInTheDocument();
    expect(screen.getByText("Event Subscriptions")).toBeInTheDocument();
    expect(screen.getByText("Certificates")).toBeInTheDocument();
    expect(screen.getByText("Connections")).toBeInTheDocument();
  });

  it("lists replication instances using Marker pagination", async () => {
    mockSend.mockResolvedValueOnce({
      ReplicationInstances: [exampleInstance],
      Marker: "next-page",
    });
    render(DmsPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-instance" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: expect.objectContaining({ Marker: undefined }) }),
    );
    expect(screen.getByText("Load More")).toBeInTheDocument();
  });

  it("creates a replication instance via the modal", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    // Synchronize on the tab bar (not the empty-state text, which can race
    // the still-in-flight initial DescribeReplicationInstances call).
    await waitFor(() => screen.getByText("Replication Instances"));

    await fireEvent.click(screen.getByText("Create instance"));
    expect(screen.getByText("Create Replication Instance")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Instance Identifier"), {
      target: { value: "my-instance" },
    });

    mockSend.mockResolvedValueOnce({ ReplicationInstance: exampleInstance });
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-instance" })).toBeInTheDocument();
    });
    // DescribeReplicationInstances (initial) + CreateReplicationInstance + DescribeReplicationInstances (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: expect.objectContaining({
          ReplicationInstanceIdentifier: "my-instance",
          ReplicationInstanceClass: "dms.t3.micro",
        }),
      }),
    );
  });

  it("deletes a replication instance after confirming", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });
    render(DmsPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-instance" }));

    mockSend.mockResolvedValueOnce({ ReplicationInstance: exampleInstance });
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No replication instances found")).toBeInTheDocument();
    });
  });

  it("does not delete when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });
    render(DmsPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-instance" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial Describe call -- no Delete, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "my-instance" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DmsPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("opens a replication instance's detail view", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });
    render(DmsPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-instance" }));

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(exampleInstance.ReplicationInstanceArn).length).toBeGreaterThan(0);
    });
  });

  it("reboots a replication instance without a confirmation dialog", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });
    render(DmsPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-instance" }));

    mockSend.mockResolvedValueOnce({ ReplicationInstance: exampleInstance });
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });

    await fireEvent.click(screen.getByTitle("Reboot"));

    expect(confirmDestructive).not.toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(3);
    });
  });

  it("switches to the Endpoints tab and lists endpoints", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    await waitFor(() => screen.getByText("Replication Instances"));

    mockSend.mockResolvedValueOnce({ Endpoints: [exampleEndpoint] });
    await fireEvent.click(screen.getByText("Endpoints"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-endpoint" })).toBeInTheDocument();
    });
  });

  it("creates an endpoint via the modal", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    await waitFor(() => screen.getByText("Replication Instances"));

    mockSend.mockResolvedValueOnce({ Endpoints: [] });
    await fireEvent.click(screen.getByText("Endpoints"));
    await waitFor(() => screen.getByText("No endpoints found"));

    await fireEvent.click(screen.getByText("Create endpoint"));
    await fireEvent.input(screen.getByLabelText("Endpoint Identifier"), {
      target: { value: "my-endpoint" },
    });

    mockSend.mockResolvedValueOnce({ Endpoint: exampleEndpoint });
    mockSend.mockResolvedValueOnce({ Endpoints: [exampleEndpoint] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-endpoint" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: expect.objectContaining({
          EndpointIdentifier: "my-endpoint",
          EndpointType: "source",
          EngineName: "mysql",
        }),
      }),
    );
  });

  it("creates a replication task from the Tasks tab, loading endpoints first", async () => {
    // Instances is the default active tab, so it is already loaded (and
    // populated) by the time Create Task is opened -- only endpoints still
    // needs to be fetched by ensureReferenceDataLoaded.
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });
    render(DmsPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-instance" }));

    mockSend.mockResolvedValueOnce({ ReplicationTasks: [] });
    await fireEvent.click(screen.getByText("Replication Tasks"));
    await waitFor(() => screen.getByText("No replication tasks found"));

    // Both a source and a target endpoint are needed for the form to
    // auto-select valid defaults for both dropdowns.
    mockSend.mockResolvedValueOnce({ Endpoints: [exampleEndpoint, exampleTargetEndpoint] });

    await fireEvent.click(screen.getByText("Create task"));
    await waitFor(() => screen.getByText("Create Replication Task"));

    await fireEvent.input(screen.getByLabelText("Task Identifier"), {
      target: { value: "my-task" },
    });

    mockSend.mockResolvedValueOnce({ ReplicationTask: exampleTask });
    mockSend.mockResolvedValueOnce({ ReplicationTasks: [exampleTask] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-task" })).toBeInTheDocument();
    });
  });

  it("starts a stopped replication task", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    await waitFor(() => screen.getByText("Replication Instances"));

    mockSend.mockResolvedValueOnce({ ReplicationTasks: [exampleTask] });
    await fireEvent.click(screen.getByText("Replication Tasks"));
    await waitFor(() => screen.getByRole("cell", { name: "my-task" }));

    mockSend.mockResolvedValueOnce({ ReplicationTask: { ...exampleTask, Status: "running" } });
    mockSend.mockResolvedValueOnce({ ReplicationTasks: [{ ...exampleTask, Status: "running" }] });

    await fireEvent.click(screen.getByTitle("Start"));

    // StartReplicationTask is the 3rd call (initial Describe + tab-switch
    // Describe + Start); a 4th DescribeReplicationTasks refresh follows it,
    // so this must not be asserted as the *last* call.
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: expect.objectContaining({ ReplicationTaskArn: exampleTask.ReplicationTaskArn }),
        }),
      );
    });
  });

  it("shows a real event-subscription identifier read from the SubscriptionName wire key gopherstack actually sends", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    await waitFor(() => screen.getByText("Replication Instances"));

    // gopherstack's backend emits the identifier under "SubscriptionName",
    // not the real API's "CustSubscriptionId" -- see the wire-shape note in
    // +page.svelte. The UI must still show the right value.
    mockSend.mockResolvedValueOnce({
      EventSubscriptionsList: [
        {
          SubscriptionName: "my-subscription",
          SnsTopicArn: "arn:aws:sns:us-east-1:123456789012:topic",
          Status: "active",
          Enabled: true,
        },
      ],
    });
    await fireEvent.click(screen.getByText("Event Subscriptions"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-subscription" })).toBeInTheDocument();
    });
  });

  it("imports a certificate via the modal", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [] });
    render(DmsPage);
    await waitFor(() => screen.getByText("Replication Instances"));

    mockSend.mockResolvedValueOnce({ Certificates: [] });
    await fireEvent.click(screen.getByText("Certificates"));
    await waitFor(() => screen.getByText("No certificates found"));

    await fireEvent.click(screen.getByText("Import certificate"));
    await fireEvent.input(screen.getByLabelText("Certificate Identifier"), {
      target: { value: "my-cert" },
    });
    await fireEvent.input(screen.getByLabelText("Certificate PEM"), {
      target: { value: "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----" },
    });

    const cert = {
      CertificateIdentifier: "my-cert",
      CertificateArn: "arn:aws:dms:us-east-1:123456789012:cert:1",
    };
    mockSend.mockResolvedValueOnce({ Certificate: cert });
    mockSend.mockResolvedValueOnce({ Certificates: [cert] });

    await fireEvent.click(screen.getByRole("button", { name: "Import" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-cert" })).toBeInTheDocument();
    });
  });

  it("resolves connection endpoint/instance ARNs to friendly names using already-loaded lists", async () => {
    mockSend.mockResolvedValueOnce({ ReplicationInstances: [exampleInstance] });
    render(DmsPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-instance" }));

    mockSend.mockResolvedValueOnce({ Endpoints: [exampleEndpoint] });
    await fireEvent.click(screen.getByText("Endpoints"));
    await waitFor(() => screen.getByRole("cell", { name: "my-endpoint" }));

    mockSend.mockResolvedValueOnce({
      Connections: [
        {
          EndpointArn: exampleEndpoint.EndpointArn,
          ReplicationInstanceArn: exampleInstance.ReplicationInstanceArn,
          Status: "successful",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Connections"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-endpoint" })).toBeInTheDocument();
    });
    expect(screen.getByRole("cell", { name: "my-instance" })).toBeInTheDocument();
  });
});
