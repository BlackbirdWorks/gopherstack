import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ResilienceHubPage from "./+page.svelte";

// Every Modal on this page stays mounted in the DOM once rendered -- closed
// <dialog> elements just lose the `open` attribute rather than unmounting --
// so getByLabelText/getByRole across the whole document is ambiguous once
// more than one modal shares a label (e.g. "Name") or a footer button (e.g.
// "Create"). Scope to the dialog that is actually open, same pattern as
// accessanalyzer/page.test.ts, dlm/page.test.ts, and grafana/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
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
  appArn: "arn:aws:resiliencehub:us-east-1:000000000000:app/example-id",
  name: "example-app",
  status: "Active",
  complianceStatus: "MissingPolicy",
  resiliencyScore: 0,
  creationTime: new Date("2024-01-01T00:00:00Z"),
};

describe("Resilience Hub Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getByText("AWS Resilience Hub")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getByText("Apps")).toBeInTheDocument();
    expect(screen.getByText("App Components")).toBeInTheDocument();
    expect(screen.getByText("Resources")).toBeInTheDocument();
    expect(screen.getByText("Resource Mappings")).toBeInTheDocument();
    expect(screen.getByText("Input Sources")).toBeInTheDocument();
    expect(screen.getByText("Resiliency Policies")).toBeInTheDocument();
    expect(screen.getByText("Assessments")).toBeInTheDocument();
    expect(screen.getByText("Recommendation Templates")).toBeInTheDocument();
  });

  it("shows empty state when no apps", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("lists apps", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-app" })).toBeInTheDocument();
    });
  });

  it("shows the resiliency score as not computed by this emulator", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => {
      expect(screen.getByText("0 (not computed)")).toBeInTheDocument();
    });
  });

  it("creates an app via the modal", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create app"));
    expect(screen.getByText("Create Application")).toBeInTheDocument();

    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Name"), {
      target: { value: "example-app" },
    });

    mockSend.mockResolvedValueOnce({ app: exampleApp });
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-app" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ input: expect.objectContaining({}) }),
    );
  });

  it("deletes an app after confirming", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-app" }));

    mockSend.mockResolvedValueOnce({ appArn: exampleApp.appArn });
    mockSend.mockResolvedValueOnce({ appSummaries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No applications found")).toBeInTheDocument();
    });
  });

  it("does not delete an app when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-app" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListApps call -- no DeleteApp, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "example-app" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("App not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ResilienceHubPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): App not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens an app's detail view and honestly labels the score and summary as not computed", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-app" }));

    mockSend.mockResolvedValueOnce({ app: exampleApp });
    mockSend.mockResolvedValueOnce({ tags: {} });
    mockSend.mockResolvedValueOnce({ appVersions: [] });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText(/not computed by this emulator/)).toBeInTheDocument();
    });
  });

  it("switches to the assessments tab and starts an assessment for the selected app", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [exampleApp] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-app" }));

    mockSend.mockResolvedValueOnce({ assessmentSummaries: [] });
    await fireEvent.click(screen.getByText("Assessments"));
    await waitFor(() => screen.getByText("No assessments found"));

    await fireEvent.click(screen.getByText("Start assessment"));
    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Assessment name"), {
      target: { value: "example-assessment" },
    });

    mockSend.mockResolvedValueOnce({
      assessment: {
        assessmentArn: "arn:aws:resiliencehub:us-east-1:000000000000:app-assessment/abc",
        assessmentStatus: "Pending",
        invoker: "User",
      },
    });
    mockSend.mockResolvedValueOnce({
      assessmentSummaries: [
        {
          assessmentArn: "arn:aws:resiliencehub:us-east-1:000000000000:app-assessment/abc",
          assessmentName: "example-assessment",
          assessmentStatus: "Pending",
          invoker: "User",
        },
      ],
    });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Start" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-assessment" })).toBeInTheDocument();
    });
  });

  it("creates a resiliency policy with RTO/RPO for every disruption type", async () => {
    mockSend.mockResolvedValueOnce({ appSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(() => screen.getByText("No applications found"));

    mockSend.mockResolvedValueOnce({ resiliencyPolicies: [] });
    await fireEvent.click(screen.getByText("Resiliency Policies"));
    await waitFor(() => screen.getByText("No resiliency policies found"));

    await fireEvent.click(screen.getByText("Create policy"));
    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Name"), {
      target: { value: "example-policy" },
    });

    mockSend.mockResolvedValueOnce({
      policy: { policyArn: "arn:aws:resiliencehub:us-east-1:000000000000:resiliency-policy/p1" },
    });
    mockSend.mockResolvedValueOnce({
      resiliencyPolicies: [
        {
          policyArn: "arn:aws:resiliencehub:us-east-1:000000000000:resiliency-policy/p1",
          policyName: "example-policy",
          tier: "Important",
        },
      ],
    });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-policy" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: expect.objectContaining({
          policyName: "example-policy",
          policy: expect.objectContaining({
            Software: expect.any(Object),
            Hardware: expect.any(Object),
            AZ: expect.any(Object),
            Region: expect.any(Object),
          }),
        }),
      }),
    );
  });
});
