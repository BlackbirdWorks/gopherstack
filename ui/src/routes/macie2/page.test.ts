import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import Macie2Page from "./+page.svelte";

// Multiple modals share label text (e.g. "Name"), and closed <dialog>
// elements stay in the DOM without the `open` attribute -- scope lookups to
// the currently open dialog, same pattern as dlm/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getMacie2Client: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleJob = {
  jobId: "job-0000000000001",
  name: "daily-scan",
  jobType: "ONE_TIME",
  jobStatus: "RUNNING",
};

const exampleIdentifier = {
  id: "id-0000000000001",
  name: "ssn-identifier",
  description: "Matches SSNs",
};

const exampleAllowList = {
  id: "al-0000000000001",
  name: "known-test-data",
  description: "Test fixtures",
};

const exampleFilter = {
  id: "filter-0000000000001",
  name: "suppress-low",
  action: "ARCHIVE",
};

describe("Macie2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    expect(screen.getByText("Amazon Macie")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No classification jobs found"));
  });

  it("lists classification jobs", async () => {
    mockSend.mockResolvedValueOnce({ items: [exampleJob] });
    render(Macie2Page);
    await waitFor(() => {
      expect(screen.getByText("daily-scan")).toBeInTheDocument();
    });
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
  });

  it("shows empty state when no jobs", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    await waitFor(() => {
      expect(screen.getByText("No classification jobs found")).toBeInTheDocument();
    });
  });

  it("creates a classification job via the modal with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("No classification jobs found"));

    await fireEvent.click(screen.getByText("Create job"));
    expect(screen.getByText("Create Classification Job")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "daily-scan" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Bucket owner account ID"), {
      target: { value: "123456789012" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Buckets (comma-separated)"), {
      target: { value: "my-bucket-1, my-bucket-2" },
    });

    mockSend.mockResolvedValueOnce({ jobId: exampleJob.jobId });
    mockSend.mockResolvedValueOnce({ items: [exampleJob] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("daily-scan")).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({
      name: "daily-scan",
      description: undefined,
      jobType: "ONE_TIME",
      s3JobDefinition: {
        bucketDefinitions: [{ accountId: "123456789012", buckets: ["my-bucket-1", "my-bucket-2"] }],
      },
    });
  });

  it("cancels a job after confirming, sending UpdateClassificationJobCommand with CANCELLED", async () => {
    mockSend.mockResolvedValueOnce({ items: [exampleJob] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("daily-scan"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ items: [{ ...exampleJob, jobStatus: "CANCELLED" }] });

    await fireEvent.click(screen.getByTitle("Cancel"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("CANCELLED")).toBeInTheDocument();
    });

    const updateCall = mockSend.mock.calls[1][0];
    expect(updateCall.input).toEqual({ jobId: exampleJob.jobId, jobStatus: "CANCELLED" });
  });

  it("does not cancel a job when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ items: [exampleJob] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("daily-scan"));

    await fireEvent.click(screen.getByTitle("Cancel"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListClassificationJobs call -- no UpdateClassificationJob, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(Macie2Page);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });

  it("lists and deletes a custom data identifier through the confirm dialog", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("No classification jobs found"));

    mockSend.mockResolvedValueOnce({ items: [exampleIdentifier] });
    await fireEvent.click(screen.getByText("Custom Data Identifiers"));
    await waitFor(() => screen.getByText("ssn-identifier"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ items: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No custom data identifiers found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[2][0];
    expect(deleteCall.input).toEqual({ id: exampleIdentifier.id });
  });

  it("creates, edits, and deletes an allow list", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("No classification jobs found"));

    mockSend.mockResolvedValueOnce({ allowLists: [] });
    await fireEvent.click(screen.getByText("Allow Lists"));
    await waitFor(() => screen.getByText("No allow lists found"));

    await fireEvent.click(screen.getByText("Create allow list"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "known-test-data" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Regex to ignore"), {
      target: { value: "^TEST-.*$" },
    });

    mockSend.mockResolvedValueOnce({ id: exampleAllowList.id });
    mockSend.mockResolvedValueOnce({ allowLists: [exampleAllowList] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("known-test-data"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      name: "known-test-data",
      description: undefined,
      criteria: { regex: "^TEST-.*$" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ allowLists: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No allow lists found")).toBeInTheDocument();
    });
  });

  it("creates and deletes a findings filter", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("No classification jobs found"));

    mockSend.mockResolvedValueOnce({ findingsFilterListItems: [] });
    await fireEvent.click(screen.getByText("Findings Filters"));
    await waitFor(() => screen.getByText("No findings filters found"));

    await fireEvent.click(screen.getByText("Create filter"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "suppress-low" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Equals (comma-separated)"), {
      target: { value: "SENSITIVE_DATA" },
    });

    mockSend.mockResolvedValueOnce({ id: exampleFilter.id });
    mockSend.mockResolvedValueOnce({ findingsFilterListItems: [exampleFilter] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("suppress-low"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      name: "suppress-low",
      description: undefined,
      action: "NOOP",
      findingCriteria: { criterion: { type: { eq: ["SENSITIVE_DATA"] } } },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ findingsFilterListItems: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No findings filters found")).toBeInTheDocument();
    });
  });

  it("lists findings and creates sample findings", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(Macie2Page);
    await waitFor(() => screen.getByText("No classification jobs found"));

    mockSend.mockResolvedValueOnce({ findingIds: ["f-1"] });
    mockSend.mockResolvedValueOnce({
      findings: [
        {
          id: "f-1",
          title: "S3 object contains PII",
          type: "SensitiveData:S3Object/Personal",
          category: "CLASSIFICATION",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Findings"));
    await waitFor(() => screen.getByText("S3 object contains PII"));

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.input).toEqual({});
    const getCall = mockSend.mock.calls[2][0];
    expect(getCall.input).toEqual({ findingIds: ["f-1"] });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ findingIds: [] });

    await fireEvent.click(screen.getByText("Create sample findings"));
    await waitFor(() => {
      expect(
        screen.getByText(
          "No findings found — Macie only produces findings from classification jobs or CreateSampleFindings",
        ),
      ).toBeInTheDocument();
    });
  });
});
