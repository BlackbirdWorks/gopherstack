import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import QuickSightPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getQuickSightClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// The page resolves its AwsAccountId from GET /dashboard/api/system/settings
// before making any QuickSight calls (see ensureAccountId in +page.svelte).
// Mock fetch so that resolves immediately and predictably in every test.
const fetchMock = vi.fn().mockResolvedValue({
  ok: true,
  json: () => Promise.resolve({ accountID: "123456789012" }),
});
vi.stubGlobal("fetch", fetchMock);

const exampleDashboard = {
  Arn: "arn:aws:quicksight:us-east-1:123456789012:dashboard/example",
  DashboardId: "example",
  Name: "Sales Overview",
  PublishedVersionNumber: 1,
  CreatedTime: new Date("2024-01-01T00:00:00Z"),
  LastUpdatedTime: new Date("2024-01-01T00:00:00Z"),
};

describe("QuickSight Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ accountID: "123456789012" }),
    });
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    expect(screen.getByText("Amazon QuickSight")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    expect(screen.getByText("Dashboards")).toBeInTheDocument();
    expect(screen.getByText("Analyses")).toBeInTheDocument();
    expect(screen.getByText("Data Sets")).toBeInTheDocument();
    expect(screen.getByText("Data Sources")).toBeInTheDocument();
    expect(screen.getByText("Folders")).toBeInTheDocument();
    expect(screen.getByText("VPC Connections")).toBeInTheDocument();
  });

  it("shows empty state when no dashboards", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => {
      expect(screen.getByText("No dashboards found")).toBeInTheDocument();
    });
  });

  it("lists dashboards", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Sales Overview" })).toBeInTheDocument();
    });
  });

  // Multi-step modal interaction (open, fill 2 fields, submit, refresh) runs
  // past the 5s default under the full 2000+ test suite's CPU contention
  // (gopherstack CI job 93845979320); default timeout is fine standalone.
  it("creates a dashboard via the modal", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => screen.getByText("No dashboards found"));

    await fireEvent.click(screen.getByText("Create dashboard"));
    expect(screen.getByText("Create Dashboard")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Dashboard ID"), { target: { value: "example" } });
    await fireEvent.input(screen.getByLabelText("Dashboard name"), {
      target: { value: "Sales Overview" },
    });

    mockSend.mockResolvedValueOnce({ Arn: exampleDashboard.Arn, DashboardId: "example" });
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Sales Overview" })).toBeInTheDocument();
    });
    // ListDashboards (initial) + CreateDashboard + ListDashboards (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
  }, 30000);

  // Same modal-under-load timing margin as "creates a dashboard via the modal" above.
  it("updates a dashboard via the edit modal", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    await fireEvent.click(screen.getByTitle("Edit"));
    expect(screen.getByText("Edit Dashboard")).toBeInTheDocument();

    const nameInput = screen.getByLabelText("New dashboard name") as HTMLInputElement;
    await fireEvent.input(nameInput, { target: { value: "Sales Overview v2" } });

    const updated = { ...exampleDashboard, Name: "Sales Overview v2" };
    mockSend.mockResolvedValueOnce({ Arn: exampleDashboard.Arn, DashboardId: "example" });
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [updated] });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Sales Overview v2" })).toBeInTheDocument();
    });
  }, 30000);

  it("deletes a dashboard after confirming", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No dashboards found")).toBeInTheDocument();
    });
  });

  it("does not delete a dashboard when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListDashboards call -- no DeleteDashboard, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "Sales Overview" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Dashboard not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(QuickSightPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Dashboard not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a dashboard's detail view", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    mockSend.mockResolvedValueOnce({ Dashboard: exampleDashboard });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(exampleDashboard.Arn).length).toBeGreaterThan(0);
    });
  });

  it("switches to the analyses tab and loads analyses", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    mockSend.mockResolvedValueOnce({
      AnalysisSummaryList: [
        {
          AnalysisId: "quarterly-trends",
          Name: "Quarterly Trends",
          Status: "CREATION_SUCCESSFUL",
          CreatedTime: new Date("2024-02-01T00:00:00Z"),
          LastUpdatedTime: new Date("2024-02-01T00:00:00Z"),
        },
      ],
    });
    await fireEvent.click(screen.getByText("Analyses"));

    await waitFor(() => {
      expect(screen.getByText("Quarterly Trends")).toBeInTheDocument();
    });
  });

  // gopherstack-ks2s.15: the six original tabs (Dashboards/Analyses/Data
  // Sets/Data Sources/Folders/VPC Connections) covered above; these confirm
  // the 13 remaining resource families the epic named are now real tabs.
  it("shows the 13 previously-unexposed resource family tabs", () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    for (const label of [
      "Templates",
      "Themes",
      "Topics",
      "Namespaces",
      "Groups",
      "Users",
      "IAM Policy Assignments",
      "Custom Permissions",
      "Brands",
      "Action Connectors",
      "Agents",
      "Knowledge Bases",
      "Spaces",
    ]) {
      expect(screen.getByRole("tab", { name: label })).toBeInTheDocument();
    }
  });

  // Same modal-under-load timing margin as the dashboard modal tests above
  // (tab switch + 3-field modal fill also observed timing out under load).
  it("lists templates and creates one via the modal", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    // "No dashboards found" renders before the mount fetch settles too (the
    // list starts empty), so it can't gate queuing the next mock -- wait for
    // the real ListDashboards call to actually land first.
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({ TemplateSummaryList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Templates" }));
    await waitFor(() => screen.getByText("No templates found"));

    await fireEvent.click(screen.getByText("Create template"));
    await fireEvent.input(screen.getByLabelText("Template ID"), { target: { value: "tpl-1" } });
    await fireEvent.input(screen.getByLabelText("Template name"), {
      target: { value: "My Template" },
    });
    await fireEvent.input(screen.getByLabelText("Definition (JSON)"), {
      target: { value: '{"DataSetConfigurations":[]}' },
    });

    mockSend.mockResolvedValueOnce({ TemplateId: "tpl-1", Arn: "arn:aws:quicksight:x" });
    mockSend.mockResolvedValueOnce({
      TemplateSummaryList: [{ TemplateId: "tpl-1", Name: "My Template" }],
    });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "My Template" })).toBeInTheDocument();
    });
  }, 30000);

  it("lists namespaces and deletes one after confirming (no update op)", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({
      Namespaces: [{ Name: "default", CapacityRegion: "us-east-1" }],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Namespaces" }));
    await waitFor(() => screen.getByRole("cell", { name: "default" }));

    // Namespace has no UpdateNamespace op in the real API, so no Edit action.
    expect(screen.queryByTitle("Edit")).not.toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Namespaces: [] });
    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No namespaces found")).toBeInTheDocument();
    });
  });

  it("lists spaces using the family's camelCase wire shape", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({
      SpaceSummaries: [{ spaceId: "space-1", name: "Marketing Space", resourcesCount: 3 }],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Spaces" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Marketing Space" })).toBeInTheDocument();
    });
  });

  it("scopes the Groups list call to the current namespace", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({ GroupList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Groups" }));
    await waitFor(() => screen.getByText("No groups found"));
    await waitFor(() => expect(mockSend.mock.calls.length).toBeGreaterThanOrEqual(2));

    // Call #1 is the mount's ListDashboards (confirmed above); call #2 is
    // this test's own ListGroups -- checked directly by index rather than
    // "last called with" so a same-tick trailing call from another test's
    // component can't make this assertion flaky.
    expect(mockSend.mock.calls[1][0].input).toEqual(
      expect.objectContaining({ Namespace: "default" }),
    );
  });
});
