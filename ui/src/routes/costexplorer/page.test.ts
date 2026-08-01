import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CostExplorerPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCostExplorerClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleCategory = {
  Name: "my-category",
  EffectiveStart: "2024-01-01T00:00:00Z",
  CostCategoryArn: "arn:aws:ce::123456789012:costcategory/abc123",
};

const exampleMonitor = {
  MonitorName: "my-monitor",
  MonitorType: "DIMENSIONAL",
  MonitorArn: "arn:aws:ce::123456789012:anomalymonitor/mon-1",
};

const exampleSubscription = {
  SubscriptionName: "my-subscription",
  Frequency: "DAILY",
  Threshold: 100,
  SubscriptionArn: "arn:aws:ce::123456789012:anomalysubscription/sub-1",
};

// Fields under the Forecasting/Reservations tabs use a bare <label> that
// isn't associated with its control (no for/id, and the label doesn't wrap
// it) -- getByLabelText doesn't work here. Fall back to walking to the
// label's sibling control instead of touching the page's markup.
function fieldNear(labelText: string): HTMLElement {
  const label = screen.getByText(labelText);
  return label.parentElement!.querySelector("input, select") as HTMLElement;
}

describe("Cost Explorer Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and all tabs", () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    expect(screen.getByText("Cost Explorer")).toBeInTheDocument();
    expect(screen.getByText("Cost Categories")).toBeInTheDocument();
    expect(screen.getByText("Anomaly Monitors")).toBeInTheDocument();
    expect(screen.getByText("Subscriptions")).toBeInTheDocument();
    expect(screen.getByText("Anomalies")).toBeInTheDocument();
    expect(screen.getByText("Forecasting")).toBeInTheDocument();
    expect(screen.getByText("Reservations")).toBeInTheDocument();
  });

  it("lists cost categories, rendering name, effective start, and ARN cells", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [exampleCategory] });
    render(CostExplorerPage);
    await waitFor(() => {
      expect(screen.getByText("my-category")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    expect(screen.getByText("2024-01-01T00:00:00Z")).toBeInTheDocument();
    expect(screen.getByText(exampleCategory.CostCategoryArn)).toBeInTheDocument();
  });

  it("shows empty state when there are no cost categories", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => {
      expect(screen.getByText("No cost categories defined.")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when loading categories fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), { name: "ThrottlingException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(CostExplorerPage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load cost categories: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("disables the Create button until a category name is entered", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    expect(screen.getByRole("button", { name: "Create" })).toBeDisabled();

    await fireEvent.input(screen.getByPlaceholderText("Category name"), {
      target: { value: "my-category" },
    });

    expect(screen.getByRole("button", { name: "Create" })).toBeEnabled();
  });

  it("creates a cost category with an empty rule set", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    await fireEvent.input(screen.getByPlaceholderText("Category name"), {
      target: { value: "my-category" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [exampleCategory] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-category")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: { Name: "my-category", RuleVersion: "CostCategoryExpression.v1", Rules: [] },
      }),
    );
  });

  it("deletes a cost category immediately, with no confirmation dialog", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [exampleCategory] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("my-category"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { CostCategoryArn: exampleCategory.CostCategoryArn } }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No cost categories defined.")).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when deleting a category fails", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [exampleCategory] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("my-category"));

    const error = Object.assign(new Error("Category not found."), {
      name: "ResourceNotFoundException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to delete category: ResourceNotFoundException: Category not found.",
      );
    });
  });

  it("switches to the Anomaly Monitors tab, lists, creates, and deletes a monitor", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({ AnomalyMonitors: [exampleMonitor] });
    await fireEvent.click(screen.getByText("Anomaly Monitors"));

    await waitFor(() => {
      expect(screen.getByText("my-monitor")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(2, expect.objectContaining({ input: {} }));
    expect(screen.getByRole("cell", { name: "DIMENSIONAL" })).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Monitor name"), {
      target: { value: "new-monitor" },
    });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      AnomalyMonitors: [exampleMonitor, { ...exampleMonitor, MonitorName: "new-monitor" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("new-monitor")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { AnomalyMonitor: { MonitorName: "new-monitor", MonitorType: "DIMENSIONAL" } },
      }),
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ AnomalyMonitors: [] });
    const deleteButtons = screen.getAllByTitle("Delete");
    await fireEvent.click(deleteButtons[0]);

    await waitFor(() => {
      expect(screen.getByText("No anomaly monitors configured.")).toBeInTheDocument();
    });
  });

  it("switches to the Subscriptions tab, lists with a formatted threshold, creates, and deletes", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({ AnomalySubscriptions: [exampleSubscription] });
    await fireEvent.click(screen.getByText("Subscriptions"));

    await waitFor(() => {
      expect(screen.getByText("my-subscription")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(2, expect.objectContaining({ input: {} }));
    expect(screen.getByText("$100")).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Subscription name"), {
      target: { value: "new-sub" },
    });
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ AnomalySubscriptions: [exampleSubscription] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: {
            AnomalySubscription: {
              SubscriptionName: "new-sub",
              Frequency: "DAILY",
              MonitorArnList: [],
              Subscribers: [],
              ThresholdExpression: undefined,
            },
          },
        }),
      );
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ AnomalySubscriptions: [] });
    await fireEvent.click(screen.getByTitle("Delete"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        5,
        expect.objectContaining({
          input: { SubscriptionArn: exampleSubscription.SubscriptionArn },
        }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("No anomaly subscriptions configured.")).toBeInTheDocument();
    });
  });

  it("switches to the Anomalies tab and renders a high-severity badge and formatted impact", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({
      Anomalies: [
        {
          AnomalyId: "anomaly-1",
          AnomalyStartDate: "2024-01-01",
          AnomalyEndDate: "2024-01-02",
          AnomalyScore: { MaxScore: 0.95 },
          Impact: { TotalActualSpend: 123.456 },
          Feedback: "YES",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Anomalies"));

    await waitFor(() => {
      expect(screen.getByText("anomaly-1")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: { DateInterval: { StartDate: "2020-01-01", EndDate: "2099-12-31" } },
      }),
    );
    expect(screen.getByText("High")).toBeInTheDocument();
    expect(screen.getByText("$123.46")).toBeInTheDocument();
  });

  it("filters anomalies by monitor ARN on the client, without an extra request", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({
      Anomalies: [
        { AnomalyId: "anomaly-1", MonitorArn: "arn:aws:ce::123456789012:anomalymonitor/mon-1" },
        { AnomalyId: "anomaly-2", MonitorArn: "arn:aws:ce::123456789012:anomalymonitor/mon-2" },
      ],
    });
    await fireEvent.click(screen.getByText("Anomalies"));
    await waitFor(() => screen.getByText("anomaly-1"));
    expect(screen.getByText("anomaly-2")).toBeInTheDocument();

    const callsBeforeFilter = mockSend.mock.calls.length;
    await fireEvent.input(screen.getByPlaceholderText("Filter by monitor ARN"), {
      target: { value: "mon-1" },
    });

    expect(screen.getByText("anomaly-1")).toBeInTheDocument();
    expect(screen.queryByText("anomaly-2")).not.toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledTimes(callsBeforeFilter);
  });

  it("re-fetches anomalies with a Feedback filter when the feedback select changes", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({ Anomalies: [] });
    await fireEvent.click(screen.getByText("Anomalies"));
    await waitFor(() => screen.getByText("No anomalies found."));

    mockSend.mockResolvedValueOnce({ Anomalies: [] });
    await fireEvent.change(screen.getByDisplayValue("All feedback"), {
      target: { value: "YES" },
    });

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: {
            DateInterval: { StartDate: "2020-01-01", EndDate: "2099-12-31" },
            Feedback: "YES",
          },
        }),
      );
    });
  });

  it("gets a cost forecast and renders the total amount and unit", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    // Entering the Forecasting tab fetches immediately with whatever
    // parameters are currently in state (switchTab calls loadForecast()
    // unconditionally), before the user has touched the date fields.
    mockSend.mockResolvedValueOnce({ Total: { Amount: "0.00", Unit: "USD" } });
    await fireEvent.click(screen.getByText("Forecasting"));
    await waitFor(() => screen.getByText("Get Forecast"));

    await fireEvent.input(fieldNear("Start Date"), { target: { value: "2024-06-01" } });
    await fireEvent.input(fieldNear("End Date"), { target: { value: "2024-06-30" } });

    mockSend.mockResolvedValueOnce({ Total: { Amount: "42.50", Unit: "USD" } });

    await fireEvent.click(screen.getByRole("button", { name: "Get Forecast" }));

    await waitFor(() => {
      expect(screen.getByText(/USD\s*42\.50/)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          TimePeriod: { Start: "2024-06-01", End: "2024-06-30" },
          Metric: "BLENDED_COST",
          Granularity: "MONTHLY",
        },
      }),
    );
  });

  it("falls back to a zero-amount forecast result when the response has no Total", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    // The tab-entry fetch itself is enough to exercise the fallback.
    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Forecasting"));

    await waitFor(() => {
      expect(screen.getByText(/0\.00/)).toBeInTheDocument();
    });
  });

  it("shows a toast error including the AWS error code when the forecast fails", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    const error = Object.assign(new Error("Date range too large."), {
      name: "LimitExceededException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Forecasting"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load forecast: LimitExceededException: Date range too large.",
      );
    });
  });

  it("shows empty state when entering Reservations returns no recommendations", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    // Entering the tab fetches immediately with the default parameters
    // (switchTab calls loadReservations() unconditionally on tab entry).
    mockSend.mockResolvedValueOnce({ Recommendations: [] });
    await fireEvent.click(screen.getByText("Reservations"));

    await waitFor(() => {
      expect(
        screen.getByText(
          'No reservation recommendations found. Click "Get Recommendations" to fetch.',
        ),
      ).toBeInTheDocument();
    });
  });

  it("gets reservation purchase recommendations with the default parameters and renders a row", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({
      Recommendations: [
        {
          AccountScope: "PAYER",
          TermInYears: "ONE_YEAR",
          PaymentOption: "NO_UPFRONT",
          RecommendationDetails: [{ InstanceDetails: {} }, { InstanceDetails: {} }],
        },
      ],
    });

    await fireEvent.click(screen.getByText("Reservations"));

    await waitFor(() => {
      expect(screen.getByText("PAYER")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          Service: "Amazon EC2",
          LookbackPeriodInDays: "THIRTY_DAYS",
          TermInYears: "ONE_YEAR",
          PaymentOption: "NO_UPFRONT",
        },
      }),
    );
    expect(screen.getByText("2 items")).toBeInTheDocument();
  });

  it("re-fetches reservation recommendations when Get Recommendations is clicked again", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    mockSend.mockResolvedValueOnce({ Recommendations: [] });
    await fireEvent.click(screen.getByText("Reservations"));
    await waitFor(() => screen.getByText(/No reservation recommendations found/));

    mockSend.mockResolvedValueOnce({
      Recommendations: [
        {
          AccountScope: "LINKED",
          TermInYears: "THREE_YEARS",
          PaymentOption: "ALL_UPFRONT",
          RecommendationDetails: [{ InstanceDetails: {} }],
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Get Recommendations" }));

    await waitFor(() => {
      expect(screen.getByText("LINKED")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Service: "Amazon EC2",
          LookbackPeriodInDays: "THIRTY_DAYS",
          TermInYears: "ONE_YEAR",
          PaymentOption: "NO_UPFRONT",
        },
      }),
    );
  });

  it("shows a toast error including the AWS error code when loading reservation recommendations fails", async () => {
    mockSend.mockResolvedValueOnce({ CostCategoryReferences: [] });
    render(CostExplorerPage);
    await waitFor(() => screen.getByText("No cost categories defined."));

    const error = Object.assign(new Error("Access denied."), { name: "AccessDeniedException" });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("Reservations"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load recommendations: AccessDeniedException: Access denied.",
      );
    });
  });
});
