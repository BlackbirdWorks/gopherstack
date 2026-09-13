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

const exampleDataSet = {
  Arn: "arn:aws:quicksight:us-east-1:123456789012:dataset/sales-ds",
  DataSetId: "sales-ds",
  Name: "Sales Dataset",
  ImportMode: "SPICE",
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

  // Modal interaction (open, fill fields, submit, refresh) still runs past
  // the 5s default under the full suite's CPU contention: ~3.7s standalone,
  // but 5.2s-8.1s across full-suite runs even after fixing Tabs.svelte's
  // non-reactive tabElements bind:this (gopherstack-naoq). The contention
  // margin is real, not eliminated; keep the wider timeout.
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

  // gopherstack-jc2j: the shared ResourcePermissions section, mounted in the
  // data set detail modal. DescribeDataSetPermissions -> table of principals.
  it("shows a data set's permissions in a table after describing them", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({ DataSetSummaries: [exampleDataSet] });
    await fireEvent.click(screen.getByRole("tab", { name: "Data Sets" }));
    await waitFor(() => screen.getByRole("cell", { name: "Sales Dataset" }));

    mockSend.mockResolvedValueOnce({ DataSet: exampleDataSet }); // DescribeDataSet
    mockSend.mockResolvedValueOnce({
      Permissions: [
        {
          Principal: "arn:aws:quicksight:us-east-1:123456789012:user/default/alice",
          Actions: ["quicksight:DescribeDataSet"],
        },
      ],
    }); // DescribeDataSetPermissions
    mockSend.mockResolvedValueOnce({ Ingestions: [] }); // ListIngestions
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:quicksight:us-east-1:123456789012:user/default/alice"),
      ).toBeInTheDocument();
      expect(screen.getByText("quicksight:DescribeDataSet")).toBeInTheDocument();
    });
  });

  // Same section: granting a new principal sends UpdateDataSetPermissions
  // with the preset's actions under GrantPermissions, then re-describes.
  it("grants a data set permission with the request shape UpdateDataSetPermissions expects", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({ DataSetSummaries: [exampleDataSet] });
    await fireEvent.click(screen.getByRole("tab", { name: "Data Sets" }));
    await waitFor(() => screen.getByRole("cell", { name: "Sales Dataset" }));

    mockSend.mockResolvedValueOnce({ DataSet: exampleDataSet });
    mockSend.mockResolvedValueOnce({ Permissions: [] });
    mockSend.mockResolvedValueOnce({ Ingestions: [] });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByText("No principals granted access"));

    await fireEvent.input(screen.getByLabelText("Permissions: new principal ARN"), {
      target: { value: "arn:aws:quicksight:us-east-1:123456789012:user/default/bob" },
    });

    mockSend.mockResolvedValueOnce({}); // UpdateDataSetPermissions (no Permissions field on the response)
    mockSend.mockResolvedValueOnce({
      Permissions: [
        {
          Principal: "arn:aws:quicksight:us-east-1:123456789012:user/default/bob",
          Actions: ["quicksight:DescribeDataSet"],
        },
      ],
    }); // re-describe
    await fireEvent.click(screen.getByRole("button", { name: "Grant" }));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:quicksight:us-east-1:123456789012:user/default/bob"),
      ).toBeInTheDocument();
    });

    // switchTab() re-fires the page's onRegionChange effect (it reads
    // `activeTab`), which force-refreshes every OTHER tab in the background
    // (gopherstack pre-existing behavior, not this feature) -- so find the
    // call by command type instead of assuming a fixed index.
    const updateCall = mockSend.mock.calls.find(
      (c) => c[0].constructor.name === "UpdateDataSetPermissionsCommand",
    );
    expect(updateCall).toBeDefined();
    expect(updateCall![0].input).toEqual(
      expect.objectContaining({
        DataSetId: "sales-ds",
        GrantPermissions: [
          {
            Principal: "arn:aws:quicksight:us-east-1:123456789012:user/default/bob",
            Actions: [
              "quicksight:DescribeDataSet",
              "quicksight:DescribeDataSetPermissions",
              "quicksight:PassDataSet",
              "quicksight:DescribeIngestion",
              "quicksight:ListIngestions",
            ],
          },
        ],
      }),
    );
  }, 30000);

  // gopherstack-jc2j: CreateIngestion/ListIngestions/DescribeIngestion, also
  // in the data set detail modal.
  it("starts an ingestion and refreshes its status", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [] });
    render(QuickSightPage);
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));

    mockSend.mockResolvedValueOnce({ DataSetSummaries: [exampleDataSet] });
    await fireEvent.click(screen.getByRole("tab", { name: "Data Sets" }));
    await waitFor(() => screen.getByRole("cell", { name: "Sales Dataset" }));

    mockSend.mockResolvedValueOnce({ DataSet: exampleDataSet });
    mockSend.mockResolvedValueOnce({ Permissions: [] });
    mockSend.mockResolvedValueOnce({ Ingestions: [] });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByText("No ingestions"));

    await fireEvent.input(screen.getByLabelText("New ingestion ID"), {
      target: { value: "ing-1" },
    });

    mockSend.mockResolvedValueOnce({
      Arn: "arn:aws:quicksight:us-east-1:123456789012:dataset/sales-ds/ingestion/ing-1",
      IngestionId: "ing-1",
      IngestionStatus: "INITIALIZED",
    }); // CreateIngestion
    mockSend.mockResolvedValueOnce({
      Ingestions: [
        {
          Arn: "arn:aws:quicksight:us-east-1:123456789012:dataset/sales-ds/ingestion/ing-1",
          IngestionId: "ing-1",
          IngestionStatus: "RUNNING",
          CreatedTime: new Date("2024-03-01T00:00:00Z"),
        },
      ],
    }); // ListIngestions (refresh)
    await fireEvent.click(screen.getByRole("button", { name: "Start ingestion" }));

    await waitFor(() => {
      expect(screen.getByText("ing-1")).toBeInTheDocument();
      expect(screen.getByText("RUNNING")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({
      Ingestion: {
        Arn: "arn:aws:quicksight:us-east-1:123456789012:dataset/sales-ds/ingestion/ing-1",
        IngestionId: "ing-1",
        IngestionStatus: "COMPLETED",
        CreatedTime: new Date("2024-03-01T00:00:00Z"),
        RowInfo: { RowsIngested: 100, RowsDropped: 0, TotalRowsInDataset: 100 },
      },
    }); // DescribeIngestion
    await fireEvent.click(screen.getByRole("button", { name: "Refresh status" }));

    await waitFor(() => {
      expect(screen.getByText("COMPLETED")).toBeInTheDocument();
      expect(screen.getByText("100/100 (0 dropped)")).toBeInTheDocument();
    });
  }, 30000);

  // gopherstack-jc2j: UpdateDashboardPublishedVersion, driven by a version
  // selector populated from ListDashboardVersions.
  it("publishes a dashboard version", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    mockSend.mockResolvedValueOnce({ Dashboard: exampleDashboard }); // DescribeDashboard
    mockSend.mockResolvedValueOnce({ Permissions: [] }); // DescribeDashboardPermissions
    mockSend.mockResolvedValueOnce({
      DashboardVersionSummaryList: [
        { VersionNumber: 1, Status: "CREATION_SUCCESSFUL" },
        { VersionNumber: 2, Status: "CREATION_SUCCESSFUL" },
      ],
    }); // ListDashboardVersions
    await fireEvent.click(screen.getByTitle("View"));

    const versionSelect = (await screen.findByLabelText("Version to publish")) as HTMLSelectElement;
    await fireEvent.change(versionSelect, { target: { value: "2" } });

    mockSend.mockResolvedValueOnce({ DashboardId: "example", DashboardArn: exampleDashboard.Arn }); // UpdateDashboardPublishedVersion
    mockSend.mockResolvedValueOnce({ Dashboard: { ...exampleDashboard, Version: { VersionNumber: 2 } } }); // re-describe
    await fireEvent.click(screen.getByRole("button", { name: "Publish" }));

    await waitFor(() => {
      const publishCall = mockSend.mock.calls.find(
        (c) => c[0].constructor.name === "UpdateDashboardPublishedVersionCommand",
      );
      expect(publishCall).toBeDefined();
      expect(publishCall![0].input).toEqual(
        expect.objectContaining({ DashboardId: "example", VersionNumber: 2 }),
      );
    });
  }, 30000);

  // gopherstack-291eg: onRegionChange's effect used to read `activeTab`
  // synchronously, making activeTab a tracked dependency -- every
  // switchTab() call then re-ran the whole region-change handler and
  // force-refreshed every OTHER tab's list command in the background. Fixed
  // by untrack()-ing that read. This proves switching through three tabs
  // issues exactly one list call per tab, in order, with no extra calls for
  // tabs never visited (or re-visited).
  it("does not re-fire other tabs' list commands when switching tabs", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    mockSend.mockResolvedValueOnce({ AnalysisSummaryList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Analyses" }));
    await waitFor(() => screen.getByText("No analyses found"));

    mockSend.mockResolvedValueOnce({ DataSetSummaries: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Data Sets" }));
    await waitFor(() => screen.getByText("No data sets found"));

    // Switching back to an already-loaded tab must not refetch it either.
    await fireEvent.click(screen.getByRole("tab", { name: "Dashboards" }));
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    await waitFor(() => {
      const commandNames = mockSend.mock.calls.map((c) => c[0].constructor.name);
      expect(commandNames).toEqual([
        "ListDashboardsCommand",
        "ListAnalysesCommand",
        "ListDataSetsCommand",
      ]);
    });
  });

  // gopherstack-xs5xo: DescribeDashboardPermissions/UpdateDashboardPermissions
  // now carry LinkSharingConfiguration alongside the regular Permissions
  // list. The dashboard modal's second ResourcePermissions instance
  // ("Link sharing") must populate from Describe's LinkSharingConfiguration
  // on open and update from Update's LinkSharingConfiguration on grant --
  // and must send Grant/RevokeLinkPermissions, not Grant/RevokePermissions.
  it("rounds trip link sharing permissions through Describe/UpdateDashboardPermissions", async () => {
    mockSend.mockResolvedValueOnce({ DashboardSummaryList: [exampleDashboard] });
    render(QuickSightPage);
    await waitFor(() => screen.getByRole("cell", { name: "Sales Overview" }));

    const namespacePrincipal = "arn:aws:quicksight:us-east-1:123456789012:namespace/default";

    mockSend.mockResolvedValueOnce({ Dashboard: exampleDashboard }); // DescribeDashboard
    mockSend.mockResolvedValueOnce({
      Permissions: [],
      LinkSharingConfiguration: {
        Permissions: [{ Principal: namespacePrincipal, Actions: ["quicksight:DescribeDashboard"] }],
      },
    }); // DescribeDashboardPermissions
    mockSend.mockResolvedValueOnce({ DashboardVersionSummaryList: [] }); // ListDashboardVersions
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(namespacePrincipal)).toBeInTheDocument();
    });
    // The regular Permissions table (queued empty above) must stay empty --
    // link sharing rows must not leak into it.
    expect(screen.getByText("No principals granted access")).toBeInTheDocument();

    const linkViewer = "arn:aws:quicksight:us-east-1:123456789012:user/default/carol";
    await fireEvent.input(screen.getByLabelText("Link sharing: new principal ARN"), {
      target: { value: linkViewer },
    });

    mockSend.mockResolvedValueOnce({
      LinkSharingConfiguration: {
        Permissions: [
          { Principal: namespacePrincipal, Actions: ["quicksight:DescribeDashboard"] },
          {
            Principal: linkViewer,
            Actions: ["quicksight:DescribeDashboard", "quicksight:ListDashboardVersions", "quicksight:QueryDashboard"],
          },
        ],
      },
    }); // UpdateDashboardPermissions
    const grantButtons = screen.getAllByRole("button", { name: "Grant" });
    await fireEvent.click(grantButtons[1]); // second ResourcePermissions instance is "Link sharing"

    await waitFor(() => {
      expect(screen.getByText(linkViewer)).toBeInTheDocument();
    });

    const updateCall = mockSend.mock.calls.find(
      (c) => c[0].constructor.name === "UpdateDashboardPermissionsCommand",
    );
    expect(updateCall).toBeDefined();
    expect(updateCall![0].input).toEqual(
      expect.objectContaining({
        DashboardId: "example",
        GrantLinkPermissions: [
          {
            Principal: linkViewer,
            Actions: [
              "quicksight:DescribeDashboard",
              "quicksight:ListDashboardVersions",
              "quicksight:QueryDashboard",
            ],
          },
        ],
      }),
    );
    expect(updateCall![0].input.GrantPermissions).toBeUndefined();
  }, 30000);
});
