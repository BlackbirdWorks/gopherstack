import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import EMRServerlessPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEMRServerlessClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// Every modal on this page stays mounted in the DOM once rendered -- a
// closed <dialog> just loses the `open` attribute rather than unmounting --
// so getByLabelText across the whole document is ambiguous once more than
// one modal has been opened. Scope to the dialog that is actually open.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

// DataTable renders "Loading..." while a tab's fetch is in flight and swaps
// to the empty-state message only once it resolves. Waiting for "Loading..."
// to disappear (rather than waiting directly on the empty-state text) is the
// reliable sync point for empty-state assertions.
async function waitForLoadingToFinish(): Promise<void> {
  await waitFor(() => {
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
  });
}

const exampleApp = {
  id: "app-0123456789abcdef0",
  name: "my-spark-app",
  arn: "arn:aws:emr-serverless:us-east-1:123456789012:/applications/app-0123456789abcdef0",
  type: "SPARK",
  releaseLabel: "emr-7.1.0",
  state: "STOPPED",
  createdAt: new Date("2024-01-01T00:00:00Z"),
  updatedAt: new Date("2024-01-01T00:00:00Z"),
};

const exampleJobRun = {
  applicationId: exampleApp.id,
  id: "job-0123456789abcdef0",
  name: "daily-etl",
  arn: "arn:aws:emr-serverless:us-east-1:123456789012:/applications/app-0123456789abcdef0/jobruns/job-0123456789abcdef0",
  state: "RUNNING",
  createdAt: new Date("2024-01-01T00:00:00Z"),
  updatedAt: new Date("2024-01-01T00:00:00Z"),
};

describe("EMR Serverless Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ applications: [] });
    render(EMRServerlessPage);
    expect(screen.getByText("Amazon EMR Serverless")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ applications: [] });
    render(EMRServerlessPage);
    expect(screen.getByText("Applications")).toBeInTheDocument();
    expect(screen.getByText("Job Runs")).toBeInTheDocument();
    expect(screen.getByText("Sessions")).toBeInTheDocument();
  });

  it("lists applications and renders the state badge via its render snippet", async () => {
    mockSend.mockResolvedValueOnce({ applications: [exampleApp] });
    render(EMRServerlessPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-spark-app" })).toBeInTheDocument();
    });
    // Render-snippet assertion: the State column is defined with
    // `render: appStateCell`.
    expect(screen.getByText("STOPPED")).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { nextToken: undefined } }),
    );
  });

  it("shows empty state when no applications", async () => {
    mockSend.mockResolvedValueOnce({ applications: [] });
    render(EMRServerlessPage);
    await waitForLoadingToFinish();
    expect(screen.getByText("No applications found")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Internal error."), {
      name: "InternalServerException",
      $metadata: { httpStatusCode: 500 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(EMRServerlessPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("InternalServerException (HTTP 500): Internal error."),
      ).toBeInTheDocument();
    });
  });

  it("creates an application via CreateApplication with exact input", async () => {
    mockSend.mockResolvedValueOnce({ applications: [] });
    render(EMRServerlessPage);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create application"));
    expect(screen.getByText("Create Application")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name (optional)"), {
      target: { value: "new-app" },
    });

    mockSend.mockResolvedValueOnce({ applicationId: exampleApp.id });
    mockSend.mockResolvedValueOnce({ applications: [{ ...exampleApp, name: "new-app" }] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(3);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: { name: "new-app", releaseLabel: "emr-7.1.0", type: "SPARK" },
      }),
    );
  });

  it("deletes an application after confirming, with exact DeleteApplication input", async () => {
    mockSend.mockResolvedValueOnce({ applications: [exampleApp] });
    render(EMRServerlessPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-spark-app" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ applications: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { applicationId: exampleApp.id } }),
    );
  });

  it("does not delete an application when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ applications: [exampleApp] });
    render(EMRServerlessPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-spark-app" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListApplications call -- no DeleteApplication, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "my-spark-app" })).toBeInTheDocument();
  });

  it("starts a stopped application with exact StartApplication input", async () => {
    mockSend.mockResolvedValueOnce({ applications: [exampleApp] });
    render(EMRServerlessPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-spark-app" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ applications: [{ ...exampleApp, state: "STARTING" }] });

    await fireEvent.click(screen.getByTitle("Start"));

    await waitFor(() => {
      expect(screen.getByText("STARTING")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { applicationId: exampleApp.id } }),
    );
  });

  it("submits a job run via StartJobRun with exact input", async () => {
    mockSend.mockResolvedValueOnce({ applications: [exampleApp] });
    render(EMRServerlessPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-spark-app" }));

    mockSend.mockResolvedValueOnce({ jobRuns: [] });
    await fireEvent.click(screen.getByText("Job Runs"));
    await waitFor(() => screen.getByText("No job runs found for this application"));

    await fireEvent.click(screen.getByText("Submit job run"));
    expect(screen.getByText("Submit Job Run")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Execution role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Entry point (S3 URI)"), {
      target: { value: "s3://my-bucket/scripts/job.py" },
    });

    mockSend.mockResolvedValueOnce({ jobRunId: exampleJobRun.id });
    mockSend.mockResolvedValueOnce({ jobRuns: [exampleJobRun] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Submit" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          applicationId: exampleApp.id,
          name: undefined,
          executionRoleArn: "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole",
          jobDriver: {
            sparkSubmit: {
              entryPoint: "s3://my-bucket/scripts/job.py",
              entryPointArguments: undefined,
              sparkSubmitParameters: undefined,
            },
          },
        },
      }),
    );
  });

  it("cancels a job run (not delete) with exact CancelJobRun input", async () => {
    mockSend.mockResolvedValueOnce({ applications: [exampleApp] });
    render(EMRServerlessPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-spark-app" }));

    mockSend.mockResolvedValueOnce({ jobRuns: [exampleJobRun] });
    await fireEvent.click(screen.getByText("Job Runs"));
    await waitFor(() => screen.getByText("daily-etl"));

    // The action is Cancel, never Delete -- EMR Serverless job runs are
    // submitted and cancelled, not created and deleted, and there is no
    // DeleteJobRun operation in the real API.
    expect(screen.queryByTitle("Delete")).not.toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ applicationId: exampleApp.id, jobRunId: exampleJobRun.id });
    mockSend.mockResolvedValueOnce({ jobRuns: [{ ...exampleJobRun, state: "CANCELLING" }] });

    await fireEvent.click(screen.getByTitle("Cancel"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("CANCELLING")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { applicationId: exampleApp.id, jobRunId: exampleJobRun.id },
      }),
    );
  });
});
