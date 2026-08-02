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
  });

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
  });

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
});
