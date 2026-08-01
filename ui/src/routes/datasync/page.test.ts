import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DataSyncPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDataSyncClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleAgent = {
  AgentArn: "arn:aws:datasync:us-east-1:123456789012:agent/agent-0000000000000001",
  Name: "my-agent",
  Status: "ONLINE",
};

describe("DataSync Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [] });
    render(DataSyncPage);
    expect(screen.getByText("AWS DataSync")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No agents found"));
  });

  it("shows empty state when no agents", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [] });
    render(DataSyncPage);
    await waitFor(() => {
      expect(screen.getByText("No agents found")).toBeInTheDocument();
    });
  });

  it("lists agents", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [exampleAgent] });
    render(DataSyncPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-agent")).toBeInTheDocument();
    });
  });

  it("creates an agent via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [] });
    render(DataSyncPage);
    await waitFor(() => screen.getByText("No agents found"));

    await fireEvent.click(screen.getByText("Create agent"));
    expect(screen.getByText("Create Agent")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Activation key"), {
      target: { value: "ABCD-1234-EFGH-5678" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Name (optional)"), {
      target: { value: "my-agent" },
    });

    mockSend.mockResolvedValueOnce({ AgentArn: exampleAgent.AgentArn });
    mockSend.mockResolvedValueOnce({ Agents: [exampleAgent] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-agent")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      ActivationKey: "ABCD-1234-EFGH-5678",
      AgentName: "my-agent",
    });
  });

  it("deletes an agent after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [exampleAgent] });
    render(DataSyncPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-agent"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Agents: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No agents found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({ AgentArn: exampleAgent.AgentArn });
  });

  it("does not delete an agent when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Agents: [exampleAgent] });
    render(DataSyncPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-agent"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-agent")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Agent not found."), {
      name: "InvalidRequestException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DataSyncPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("InvalidRequestException (HTTP 400): Agent not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens an agent's detail view", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [exampleAgent] });
    render(DataSyncPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-agent"));

    mockSend.mockResolvedValueOnce({ ...exampleAgent });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText(exampleAgent.AgentArn)).toBeInTheDocument();
    });
  });

  it("switches to the Locations tab and creates an S3 location", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [] });
    render(DataSyncPage);
    await waitFor(() => screen.getByText("No agents found"));

    mockSend.mockResolvedValueOnce({ Locations: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Locations" }));
    await waitFor(() => screen.getByText("No locations found"));

    await fireEvent.click(screen.getByText("Create location"));
    await fireEvent.input(within(openDialog()).getByLabelText("S3 bucket ARN"), {
      target: { value: "arn:aws:s3:::my-bucket" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Bucket access role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/DataSyncRole" },
    });

    const exampleLocation = {
      LocationArn: "arn:aws:datasync:us-east-1:123456789012:location/loc-0000000000000001",
      LocationUri: "s3://my-bucket/",
    };
    mockSend.mockResolvedValueOnce({ LocationArn: exampleLocation.LocationArn });
    mockSend.mockResolvedValueOnce({ Locations: [exampleLocation] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("s3://my-bucket/")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[2][0].input).toEqual({
      S3BucketArn: "arn:aws:s3:::my-bucket",
      Subdirectory: "/",
      S3Config: { BucketAccessRoleArn: "arn:aws:iam::123456789012:role/DataSyncRole" },
    });
  });

  it("shows a note and no create button on the Task Executions tab, and cancels an execution", async () => {
    mockSend.mockResolvedValueOnce({ Agents: [] });
    render(DataSyncPage);
    await waitFor(() => screen.getByText("No agents found"));

    const exampleExecution = {
      TaskExecutionArn: "arn:aws:datasync:us-east-1:123456789012:task/task-1/execution/exec-1",
      Status: "SUCCESS",
    };
    mockSend.mockResolvedValueOnce({ TaskExecutions: [exampleExecution] });
    await fireEvent.click(screen.getByRole("tab", { name: "Task Executions" }));

    await waitFor(() => {
      expect(screen.getByText(/stopped via Cancel rather than deleted/)).toBeInTheDocument();
    });
    expect(screen.queryByText(/^Create task execution$/i)).not.toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ TaskExecutions: [] });

    await fireEvent.click(screen.getByTitle("Cancel"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No task executions found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[2][0].input).toEqual({
      TaskExecutionArn: exampleExecution.TaskExecutionArn,
    });
  });
});
