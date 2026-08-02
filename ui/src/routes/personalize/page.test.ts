import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import PersonalizePage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();
const mockRuntimeSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getPersonalizeClient: () => ({ send: mockSend }),
  getPersonalizeRuntimeClient: () => ({ send: mockRuntimeSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleGroup = {
  datasetGroupArn: "arn:aws:personalize:us-east-1:123456789012:dataset-group/my-group",
  name: "my-group",
  status: "ACTIVE",
};

describe("Personalize Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockRuntimeSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [] });
    render(PersonalizePage);
    expect(screen.getByText("Amazon Personalize")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No dataset groups found"));
  });

  it("shows empty state when no dataset groups", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [] });
    render(PersonalizePage);
    await waitFor(() => {
      expect(screen.getByText("No dataset groups found")).toBeInTheDocument();
    });
  });

  it("lists dataset groups with a status badge from a render snippet", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [exampleGroup] });
    render(PersonalizePage);
    await waitFor(() => {
      const table = screen.getByRole("table");
      expect(within(table).getByText("my-group")).toBeInTheDocument();
      expect(within(table).getByText("ACTIVE")).toBeInTheDocument();
    });
  });

  it("creates a dataset group via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [] });
    render(PersonalizePage);
    await waitFor(() => screen.getByText("No dataset groups found"));

    await fireEvent.click(screen.getByText("Create dataset group"));
    expect(screen.getByText("Create Dataset Group")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-group" },
    });

    mockSend.mockResolvedValueOnce({ datasetGroupArn: exampleGroup.datasetGroupArn });
    mockSend.mockResolvedValueOnce({ datasetGroups: [exampleGroup] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-group")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      name: "my-group",
      domain: undefined,
    });
  });

  it("deletes a dataset group after confirming", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [exampleGroup] });
    render(PersonalizePage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-group"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ datasetGroups: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No dataset groups found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({
      datasetGroupArn: exampleGroup.datasetGroupArn,
    });
  });

  it("does not delete a dataset group when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ datasetGroups: [exampleGroup] });
    render(PersonalizePage);
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

    render(PersonalizePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Dataset group not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a dataset group's detail view showing its status (unwraps the datasetGroup response envelope)", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [exampleGroup] });
    render(PersonalizePage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-group"));

    mockSend.mockResolvedValueOnce({ datasetGroup: { ...exampleGroup, domain: "ECOMMERCE" } });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("ECOMMERCE")).toBeInTheDocument();
    });
    expect(within(openDialog()).getByText(exampleGroup.datasetGroupArn)).toBeInTheDocument();
  });

  it("switches to the Datasets tab and creates a dataset", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [exampleGroup] });
    render(PersonalizePage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-group"));

    mockSend.mockResolvedValueOnce({ datasets: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Datasets" }));
    await waitFor(() => screen.getByText("No datasets found"));

    await fireEvent.click(screen.getByText("Create dataset"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-dataset" },
    });
    await fireEvent.change(within(openDialog()).getByLabelText("Dataset group"), {
      target: { value: exampleGroup.datasetGroupArn },
    });
    await fireEvent.input(within(openDialog()).getByLabelText(/Schema ARN/), {
      target: { value: "arn:aws:personalize:::schema/my-schema" },
    });

    const exampleDataset = {
      datasetArn: "arn:aws:personalize:us-east-1:123456789012:dataset/my-dataset",
      name: "my-dataset",
      datasetType: "INTERACTIONS",
      status: "ACTIVE",
    };
    mockSend.mockResolvedValueOnce({ datasetArn: exampleDataset.datasetArn });
    mockSend.mockResolvedValueOnce({ datasets: [exampleDataset] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-dataset")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[2][0].input).toEqual({
      name: "my-dataset",
      datasetGroupArn: exampleGroup.datasetGroupArn,
      datasetType: "INTERACTIONS",
      schemaArn: "arn:aws:personalize:::schema/my-schema",
    });
  });

  it("shows solution version status with a create-then-poll status badge and can stop training", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [] });
    render(PersonalizePage);
    await waitFor(() => screen.getByText("No dataset groups found"));

    mockSend.mockResolvedValueOnce({
      solutionVersions: [
        {
          solutionVersionArn: "arn:.../sv1",
          status: "CREATE IN_PROGRESS",
          trainingMode: "FULL",
        },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Solution Versions" }));

    await waitFor(() => {
      expect(screen.getByText("CREATE IN_PROGRESS")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      solutionVersions: [{ solutionVersionArn: "arn:.../sv1", status: "CREATE STOPPING" }],
    });

    await fireEvent.click(screen.getByTitle("Stop"));

    await waitFor(() => {
      expect(mockSend.mock.calls.at(-2)?.[0].input).toEqual({
        solutionVersionArn: "arn:.../sv1",
      });
    });
  });

  it("runs Get Recommendations against the personalizeruntime client", async () => {
    mockSend.mockResolvedValueOnce({ datasetGroups: [] });
    render(PersonalizePage);
    await waitFor(() => screen.getByText("No dataset groups found"));

    await fireEvent.click(screen.getByRole("tab", { name: "Get Recommendations" }));
    await fireEvent.input(screen.getByLabelText("Campaign ARN"), {
      target: { value: "arn:aws:personalize:us-east-1:123456789012:campaign/my-campaign" },
    });

    mockRuntimeSend.mockResolvedValueOnce({
      itemList: [{ itemId: "item-1", score: 0.9321 }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Get Recommendations" }));

    await waitFor(() => {
      expect(screen.getByText("item-1")).toBeInTheDocument();
      expect(screen.getByText("0.9321")).toBeInTheDocument();
    });

    expect(mockRuntimeSend.mock.calls[0][0].input).toEqual({
      campaignArn: "arn:aws:personalize:us-east-1:123456789012:campaign/my-campaign",
      userId: undefined,
      numResults: undefined,
    });
    // GetRecommendations goes through the runtime client, not the
    // control-plane client -- only the initial ListDatasetGroups call (from
    // page mount) should have hit mockSend.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });
});
