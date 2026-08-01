import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import KinesisAnalyticsV2Page from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getKinesisAnalyticsV2Client: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleApp = {
  ApplicationName: "my-flink-app",
  ApplicationStatus: "READY",
  RuntimeEnvironment: "FLINK-1_18",
  ApplicationVersionId: 1,
};

const createTimestamp = new Date("2024-01-01T00:00:00Z");

describe("Kinesis Analytics v2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    expect(screen.getByText("Kinesis Data Analytics v2 (Flink)")).toBeInTheDocument();
  });

  it("lists applications, rendering status, runtime, and version cells", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => {
      expect(screen.getByText("my-flink-app")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    expect(screen.getByRole("cell", { name: "READY" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "FLINK-1_18" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "v1" })).toBeInTheDocument();
  });

  it("shows empty state when there are no applications", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when loading applications fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), { name: "ThrottlingException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(KinesisAnalyticsV2Page);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load applications: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("creates an application via the form, defaulting the runtime and omitting a blank role", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create Application"));
    await fireEvent.input(screen.getByPlaceholderText("my-flink-app"), {
      target: { value: "new-app" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationName: "new-app" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("new-app")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          ApplicationName: "new-app",
          RuntimeEnvironment: "FLINK-1_18",
          ServiceExecutionRole: undefined,
        },
      }),
    );
  });

  it("refuses to create an application with a blank name", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("No applications found"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Create Application"));
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(toast.error).toHaveBeenCalledWith("Application name is required");
    // Only the initial ListApplications call -- no CreateApplication.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("shows a toast error including the AWS error code when creating an application fails", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create Application"));
    await fireEvent.input(screen.getByPlaceholderText("my-flink-app"), {
      target: { value: "new-app" },
    });

    const error = Object.assign(new Error("Limit exceeded."), { name: "LimitExceededException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to create application: LimitExceededException: Limit exceeded.",
      );
    });
  });

  it("deletes an application immediately, with no confirmation dialog", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: { ApplicationName: "my-flink-app", CreateTimestamp: createTimestamp },
    });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { ApplicationName: "my-flink-app", CreateTimestamp: createTimestamp },
        }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when deleting an application fails", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    const error = Object.assign(new Error("Application not found."), {
      name: "ResourceNotFoundException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to delete application: ResourceNotFoundException: Application not found.",
      );
    });
  });

  it("starts a READY application", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationStatus: "RUNNING" }],
    });

    await fireEvent.click(screen.getByTitle("Start"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { ApplicationName: "my-flink-app" } }),
      );
    });
  });

  it("stops a RUNNING application", async () => {
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationStatus: "RUNNING" }],
    });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ApplicationSummaries: [{ ...exampleApp, ApplicationStatus: "READY" }],
    });

    await fireEvent.click(screen.getByTitle("Stop"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { ApplicationName: "my-flink-app" } }),
      );
    });
  });

  it("rolls back an application to its current version", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({
      ApplicationDetail: { ApplicationName: "my-flink-app", ApplicationVersionId: 3 },
    });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });

    await fireEvent.click(screen.getByTitle("Rollback"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: { ApplicationName: "my-flink-app", CurrentApplicationVersionId: 3 },
        }),
      );
    });
  });

  it("expands an application's detail panel and loads its snapshots", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({
      SnapshotSummaries: [{ SnapshotName: "snap-1", SnapshotStatus: "READY" }],
    });
    await fireEvent.click(screen.getByText("my-flink-app"));

    await waitFor(() => {
      expect(screen.getByText("snap-1")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { ApplicationName: "my-flink-app" } }),
    );
    const snapRow = screen.getByText("snap-1").closest("div.flex") as HTMLElement;
    expect(within(snapRow).getByText("READY")).toBeInTheDocument();
  });

  it("creates a snapshot from the expanded detail panel", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({ SnapshotSummaries: [] });
    await fireEvent.click(screen.getByText("my-flink-app"));
    await waitFor(() => screen.getByText("No snapshots"));

    await fireEvent.input(screen.getByPlaceholderText("snapshot-name"), {
      target: { value: "my-snapshot" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      SnapshotSummaries: [{ SnapshotName: "my-snapshot", SnapshotStatus: "CREATING" }],
    });

    await fireEvent.click(screen.getByText("Create Snapshot"));

    await waitFor(() => {
      expect(screen.getByText("my-snapshot")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { ApplicationName: "my-flink-app", SnapshotName: "my-snapshot" },
      }),
    );
  });

  it("refuses to create a snapshot with a blank name", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({ SnapshotSummaries: [] });
    await fireEvent.click(screen.getByText("my-flink-app"));
    await waitFor(() => screen.getByText("No snapshots"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Create Snapshot"));

    expect(toast.error).toHaveBeenCalledWith("Snapshot name is required");
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("deletes a snapshot from the expanded detail panel", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    const snapTs = new Date("2024-02-01T00:00:00Z");
    mockSend.mockResolvedValueOnce({
      SnapshotSummaries: [
        { SnapshotName: "snap-1", SnapshotStatus: "READY", SnapshotCreationTimestamp: snapTs },
      ],
    });
    await fireEvent.click(screen.getByText("my-flink-app"));
    await waitFor(() => screen.getByText("snap-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ SnapshotSummaries: [] });

    const snapRow = screen.getByText("snap-1").closest("div.flex") as HTMLElement;
    await fireEvent.click(within(snapRow).getByRole("button"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: {
            ApplicationName: "my-flink-app",
            SnapshotName: "snap-1",
            SnapshotCreationTimestamp: snapTs,
          },
        }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No snapshots")).toBeInTheDocument();
    });
  });

  it("loads and renders version history", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({ SnapshotSummaries: [] });
    await fireEvent.click(screen.getByText("my-flink-app"));
    await waitFor(() => screen.getByText("No snapshots"));

    mockSend.mockResolvedValueOnce({
      ApplicationVersionSummaries: [{ ApplicationVersionId: 2, ApplicationStatus: "READY" }],
    });
    await fireEvent.click(screen.getByText("Version History"));

    await waitFor(() => {
      expect(screen.getByText("v2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { ApplicationName: "my-flink-app" } }),
    );
  });

  it("updates the maintenance window", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({ SnapshotSummaries: [] });
    await fireEvent.click(screen.getByText("my-flink-app"));
    await waitFor(() => screen.getByText("No snapshots"));

    await fireEvent.click(screen.getByText("Maintenance Window"));
    await fireEvent.input(screen.getByPlaceholderText("06:00 (UTC)"), {
      target: { value: "08:00" },
    });

    mockSend.mockResolvedValueOnce({});

    await fireEvent.click(screen.getByRole("button", { name: "Update" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: {
            ApplicationName: "my-flink-app",
            ApplicationMaintenanceConfigurationUpdate: {
              ApplicationMaintenanceWindowStartTimeUpdate: "08:00",
            },
          },
        }),
      );
    });
  });

  it("refuses to update the maintenance window with a blank start time", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [exampleApp] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("my-flink-app"));

    mockSend.mockResolvedValueOnce({ SnapshotSummaries: [] });
    await fireEvent.click(screen.getByText("my-flink-app"));
    await waitFor(() => screen.getByText("No snapshots"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Maintenance Window"));
    await fireEvent.click(screen.getByRole("button", { name: "Update" }));

    expect(toast.error).toHaveBeenCalledWith(
      "Maintenance window start time is required (e.g. 06:00)",
    );
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("discovers an input schema and renders the result", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Discover Schema"));
    await fireEvent.input(screen.getByPlaceholderText("Kinesis Stream ARN"), {
      target: { value: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream" },
    });

    mockSend.mockResolvedValueOnce({ InputSchema: { RecordFormat: { RecordFormatType: "JSON" } } });

    await fireEvent.click(screen.getByRole("button", { name: "Discover" }));

    await waitFor(() => {
      expect(screen.getByText(/"RecordFormatType": "JSON"/)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          ResourceARN: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
          ServiceExecutionRole: "",
          InputStartingPositionConfiguration: { InputStartingPosition: "NOW" },
        },
      }),
    );
  });

  it("refuses to discover a schema with a blank resource ARN", async () => {
    mockSend.mockResolvedValueOnce({ ApplicationSummaries: [] });
    render(KinesisAnalyticsV2Page);
    await waitFor(() => screen.getByText("No applications found"));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Discover Schema"));
    await fireEvent.click(screen.getByRole("button", { name: "Discover" }));

    expect(toast.error).toHaveBeenCalledWith("Resource ARN is required");
    expect(mockSend).toHaveBeenCalledTimes(1);
  });
});
