import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ResilienceHubPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getResiliencehubClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleApp = {
  appArn: "arn:aws:resiliencehub:us-east-1:123456789012:app/abc",
  name: "my-resilient-app",
  complianceStatus: "PolicyMet",
  resiliencyScore: 0.95,
  assessmentSchedule: "Disabled",
  creationTime: new Date("2024-01-01T00:00:00Z"),
};

describe("Resilience Hub Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getByText("AWS Resilience Hub")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No applications found"));
  });

  it("shows empty state when no applications", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("lists applications and renders the compliance badge snippet", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => {
      expect(screen.getByText(exampleApp.name)).toBeInTheDocument();
    });
    expect(screen.getByText("PolicyMet")).toBeInTheDocument();
  });

  it("creates an application via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByRole("button", { name: "Create application" }));
    expect(screen.getByText("Create Application")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-resilient-app" },
    });

    mockSend.mockResolvedValueOnce({ app: exampleApp });
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(exampleApp.name)).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.constructor.name).toBe("CreateAppCommand");
    expect(createCall.input).toEqual({
      name: "my-resilient-app",
      description: undefined,
      policyArn: undefined,
      assessmentSchedule: "Disabled",
    });
  });

  it("deletes an application after confirming", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText(exampleApp.name));

    mockSend.mockResolvedValueOnce({ appArn: exampleApp.appArn });
    mockSend.mockResolvedValueOnce({ appSummaries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.constructor.name).toBe("DeleteAppCommand");
    expect(deleteCall.input).toEqual({ appArn: exampleApp.appArn });
  });

  it("does not delete an application when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText(exampleApp.name));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText(exampleApp.name)).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ResilienceHubPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });

  it("opens an application's detail view", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText(exampleApp.name));

    mockSend.mockResolvedValueOnce({ app: exampleApp });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleApp.appArn)).toBeInTheDocument();
    });

    const getCall = mockSend.mock.calls[1][0];
    expect(getCall.constructor.name).toBe("DescribeAppCommand");
    expect(getCall.input).toEqual({ appArn: exampleApp.appArn });
  });

  it("switches to the Resiliency Policies tab and lists policies", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText("No applications found"));

    mockSend.mockResolvedValueOnce({
      resiliencyPolicies: [
        { policyArn: "arn:policy/1", policyName: "gold-tier", tier: "MissionCritical" },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Resiliency Policies" }));

    await waitFor(() => {
      // "gold-tier" also appears as an <option> in the (closed) app
      // create/edit modals' policy pickers, so assert at least one match.
      expect(screen.getAllByText("gold-tier").length).toBeGreaterThan(0);
    });
    expect(screen.getAllByText("MissionCritical").length).toBeGreaterThan(0);

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.constructor.name).toBe("ListResiliencyPoliciesCommand");
  });

  it("switches to the Assessments tab, lists assessments, and deletes one", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText("No applications found"));

    mockSend.mockResolvedValueOnce({
      assessmentSummaries: [
        {
          assessmentArn: "arn:assessment/1",
          assessmentName: "nightly-check",
          assessmentStatus: "Success",
          complianceStatus: "PolicyMet",
        },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Assessments" }));

    await waitFor(() => {
      expect(screen.getByText("nightly-check")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({
      assessmentArn: "arn:assessment/1",
      assessmentStatus: "Success",
    });
    mockSend.mockResolvedValueOnce({ assessmentSummaries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      expect(screen.getByText("No assessments found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[2][0];
    expect(deleteCall.constructor.name).toBe("DeleteAppAssessmentCommand");
    expect(deleteCall.input).toEqual({ assessmentArn: "arn:assessment/1" });
  });
});
