import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ForecastPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getForecastClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleGroup = {
  DatasetGroupArn: "arn:aws:forecast:us-east-1:123456789012:dataset-group/my-group",
  DatasetGroupName: "my-group",
  Domain: "RETAIL",
};

describe("Forecast Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [] });
    render(ForecastPage);
    expect(screen.getByText("Amazon Forecast")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No dataset groups found"));
  });

  it("shows empty state when no dataset groups", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [] });
    render(ForecastPage);
    await waitFor(() => {
      expect(screen.getByText("No dataset groups found")).toBeInTheDocument();
    });
  });

  it("lists dataset groups", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [exampleGroup] });
    render(ForecastPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-group")).toBeInTheDocument();
    });
  });

  it("creates a dataset group via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [] });
    render(ForecastPage);
    await waitFor(() => screen.getByText("No dataset groups found"));

    await fireEvent.click(screen.getByText("Create dataset group"));
    expect(screen.getByText("Create Dataset Group")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-group" },
    });

    mockSend.mockResolvedValueOnce({ DatasetGroupArn: exampleGroup.DatasetGroupArn });
    mockSend.mockResolvedValueOnce({ DatasetGroups: [exampleGroup] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-group")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      DatasetGroupName: "my-group",
      Domain: "RETAIL",
    });
  });

  it("deletes a dataset group after confirming", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [exampleGroup] });
    render(ForecastPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-group"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DatasetGroups: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No dataset groups found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({
      DatasetGroupArn: exampleGroup.DatasetGroupArn,
    });
  });

  it("does not delete a dataset group when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ DatasetGroups: [exampleGroup] });
    render(ForecastPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-group"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-group")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Dataset group not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ForecastPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Dataset group not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a dataset group's detail view showing its status", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [exampleGroup] });
    render(ForecastPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-group"));

    mockSend.mockResolvedValueOnce({ ...exampleGroup, Status: "ACTIVE", DatasetArns: [] });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("ACTIVE")).toBeInTheDocument();
    });
    expect(within(openDialog()).getByText(exampleGroup.DatasetGroupArn)).toBeInTheDocument();
  });

  it("switches to the Datasets tab (present per the spec even though the prior page lacked it) and creates a dataset", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [] });
    render(ForecastPage);
    await waitFor(() => screen.getByText("No dataset groups found"));

    mockSend.mockResolvedValueOnce({ Datasets: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Datasets" }));
    await waitFor(() => screen.getByText("No datasets found"));

    await fireEvent.click(screen.getByText("Create dataset"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-dataset" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Attribute name"), {
      target: { value: "item_id" },
    });

    const exampleDataset = {
      DatasetArn: "arn:aws:forecast:us-east-1:123456789012:dataset/my-dataset",
      DatasetName: "my-dataset",
      DatasetType: "TARGET_TIME_SERIES",
      Domain: "RETAIL",
    };
    mockSend.mockResolvedValueOnce({ DatasetArn: exampleDataset.DatasetArn });
    mockSend.mockResolvedValueOnce({ Datasets: [exampleDataset] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-dataset")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[2][0].input).toEqual({
      DatasetName: "my-dataset",
      Domain: "RETAIL",
      DatasetType: "TARGET_TIME_SERIES",
      DataFrequency: undefined,
      Schema: { Attributes: [{ AttributeName: "item_id", AttributeType: "string" }] },
    });
  });

  it("shows predictor status with a create-then-poll status badge", async () => {
    mockSend.mockResolvedValueOnce({ DatasetGroups: [] });
    render(ForecastPage);
    await waitFor(() => screen.getByText("No dataset groups found"));

    mockSend.mockResolvedValueOnce({
      Predictors: [
        { PredictorArn: "arn:.../p1", PredictorName: "my-predictor", Status: "CREATE_PENDING" },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Predictors" }));

    await waitFor(() => {
      expect(screen.getByText("CREATE_PENDING")).toBeInTheDocument();
    });
  });
});
