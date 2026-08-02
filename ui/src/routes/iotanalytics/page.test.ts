import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import IotAnalyticsPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getIoTAnalyticsClient: () => ({ send: mockSend }),
}));

// iotanalytics fires deletes immediately -- it never imports confirmDestructive
// (grep confirms no such import in +page.svelte), so there is nothing to mock
// for a confirm dialog here.

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

const exampleChannel = { channelName: "my-channel", status: "ACTIVE" };
const exampleDatastore = { datastoreName: "my-datastore", status: "ACTIVE" };
const exampleDataset = { datasetName: "my-dataset", status: "ACTIVE" };
const examplePipeline = {
  pipelineName: "my-pipeline",
  creationTime: new Date("2024-01-01T00:00:00Z"),
  reprocessingSummaries: [{ id: "reproc-1", status: "RUNNING" }],
};

// The Datastores/Datasets/Pipelines toolbar "Create" button and the modal's
// submit "Create" button both render the exact text "Create" and coexist in
// the DOM once the modal is open -- scope to the modal by its heading to
// disambiguate.
function withinModal(headingText: string) {
  const heading = screen.getByText(headingText);
  return within(heading.closest("div.rounded-2xl") as HTMLElement);
}

// The Batch Ingest and Sample Channel Data cards both label their channel-name
// input "Channel name *", so getByLabelText is ambiguous between them --
// disambiguate by the input's id.
function channelInputById(id: "batch-channel" | "sample-channel"): HTMLInputElement {
  const el = screen.getAllByLabelText("Channel name *").find((e) => e.id === id) as
    | HTMLInputElement
    | undefined;
  if (!el) throw new Error(`No channel-name input found with id ${id}`);
  return el;
}

/**
 * The page's `onRegionChange` effect fires immediately on mount (see
 * region-effect.svelte.ts) and unconditionally kicks off ALL FIVE list loads
 * -- Channels, Datastores, Datasets, Pipelines, Logging -- regardless of
 * which tab is active. Tab clicks themselves never issue a fetch (there is
 * no per-tab load-on-select code), so every test must supply exactly these
 * five responses, in this order, before any user-triggered action's mocks.
 */
function queueInitialLoads(
  opts: {
    channels?: unknown[];
    datastores?: unknown[];
    datasets?: unknown[];
    pipelines?: unknown[];
    logging?: { roleArn?: string; level?: string; enabled?: boolean };
  } = {},
) {
  mockSend.mockResolvedValueOnce({ channelSummaries: opts.channels ?? [] });
  mockSend.mockResolvedValueOnce({ datastoreSummaries: opts.datastores ?? [] });
  mockSend.mockResolvedValueOnce({ datasetSummaries: opts.datasets ?? [] });
  mockSend.mockResolvedValueOnce({ pipelineSummaries: opts.pipelines ?? [] });
  mockSend.mockResolvedValueOnce({ loggingOptions: opts.logging });
}

describe("IoT Analytics Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and all tabs", () => {
    queueInitialLoads();
    render(IotAnalyticsPage);
    expect(screen.getByText("IoT Analytics")).toBeInTheDocument();
    // "Channels" also appears as the card heading for the (default-active)
    // Channels tab's own content -- scope to the tab button by role.
    expect(screen.getByRole("button", { name: "Channels" })).toBeInTheDocument();
    expect(screen.getByText("Datastores")).toBeInTheDocument();
    expect(screen.getByText("Datasets")).toBeInTheDocument();
    expect(screen.getByText("Pipelines")).toBeInTheDocument();
    expect(screen.getByText("Logging")).toBeInTheDocument();
  });

  it("lists channels on mount with no pagination params sent", async () => {
    queueInitialLoads({ channels: [exampleChannel] });
    render(IotAnalyticsPage);
    await waitFor(() => {
      expect(screen.getByText("my-channel")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(1, expect.objectContaining({ input: {} }));
  });

  it("creates a channel via the modal using the placeholder-only input (no label association)", async () => {
    queueInitialLoads();
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("+ Create Channel"));
    expect(screen.getByText("Create Channel")).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Channel name"), {
      target: { value: "my-channel" },
    });

    mockSend.mockResolvedValueOnce({ channel: exampleChannel });
    mockSend.mockResolvedValueOnce({ channelSummaries: [exampleChannel] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-channel")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      6,
      expect.objectContaining({ input: { channelName: "my-channel" } }),
    );
  });

  it("deletes a channel immediately with no confirmation dialog", async () => {
    queueInitialLoads({ channels: [exampleChannel] });
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("my-channel"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ channelSummaries: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      6,
      expect.objectContaining({ input: { channelName: "my-channel" } }),
    );
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });
  });

  it("shows the AWS error code in the toast when loading channels fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);
    mockSend.mockResolvedValueOnce({ datastoreSummaries: [] });
    mockSend.mockResolvedValueOnce({ datasetSummaries: [] });
    mockSend.mockResolvedValueOnce({ pipelineSummaries: [] });
    mockSend.mockResolvedValueOnce({ loggingOptions: undefined });
    const { toast } = await import("svelte-sonner");

    render(IotAnalyticsPage);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ThrottlingException: Rate exceeded."),
      );
    });
  });

  it("shows the AWS error code in the toast when deleting a channel fails", async () => {
    queueInitialLoads({ channels: [exampleChannel] });
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("my-channel"));

    const error = Object.assign(new Error("Channel is in use."), {
      name: "ResourceInUseException",
      $metadata: { httpStatusCode: 409 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ResourceInUseException: Channel is in use."),
      );
    });
  });

  it("batch-ingests a message with the correct channel name, message id, and encoded payload", async () => {
    queueInitialLoads({ channels: [exampleChannel] });
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("my-channel"));

    // Clicking "Ingest" on the row pre-fills the batch-ingest form's channel
    // field. Exact name match -- the form's own submit button is "Ingest
    // Message", a regex like /Ingest/ would match both and throw on
    // ambiguity.
    await fireEvent.click(screen.getByRole("button", { name: "Ingest" }));
    expect(channelInputById("batch-channel")).toHaveValue("my-channel");

    await fireEvent.input(screen.getByLabelText("Message ID *"), {
      target: { value: "msg-42" },
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Ingest Message" }));

    // Assert the encoded payload's decoded content directly rather than via
    // `expect.any(Uint8Array)` -- the TextEncoder in the component and the
    // one available to this test file can be different realms of the same
    // global, which makes an `instanceof` check on the typed array flaky.
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(6));
    const call = mockSend.mock.calls[5][0] as {
      input: { channelName: string; messages: Array<{ messageId: string; payload: Uint8Array }> };
    };
    expect(call.input.channelName).toBe("my-channel");
    expect(call.input.messages).toHaveLength(1);
    expect(call.input.messages[0].messageId).toBe("msg-42");
    expect(new TextDecoder().decode(call.input.messages[0].payload)).toBe(
      '{"temperature": 22.5, "ts": 0}',
    );
  });

  it("samples channel data and renders the decoded payload", async () => {
    queueInitialLoads({ channels: [exampleChannel] });
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("my-channel"));

    // Both the row action and the form's own submit button render the exact
    // text "Sample" -- scope to the row to avoid an ambiguous match.
    const row = screen.getByText("my-channel").closest("tr")!;
    await fireEvent.click(within(row).getByRole("button", { name: "Sample" }));
    expect(channelInputById("sample-channel")).toHaveValue("my-channel");

    // The row action and the card's own submit button both render the exact
    // text "Sample" too -- scope the submit click to the "Sample Channel
    // Data" card.
    const sampleCard = screen
      .getByText("Sample Channel Data")
      .closest("div.rounded-lg") as HTMLElement;
    const encoder = new TextEncoder();
    mockSend.mockResolvedValueOnce({
      payloads: [encoder.encode('{"temp":1}')],
    });
    await fireEvent.click(within(sampleCard).getByRole("button", { name: "Sample" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        6,
        expect.objectContaining({
          input: expect.objectContaining({ channelName: "my-channel", maxMessages: 10 }),
        }),
      );
    });
    await fireEvent.click(screen.getByText("Message 1"));
    expect(screen.getByText(/"temp": 1/)).toBeInTheDocument();
  });

  it("switches to Datastores, creates one, and deletes it with no confirmation dialog", async () => {
    queueInitialLoads();
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Datastores"));
    await waitFor(() => screen.getByText("No datastores"));

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await fireEvent.input(screen.getByPlaceholderText("Datastore name"), {
      target: { value: "my-datastore" },
    });

    mockSend.mockResolvedValueOnce({ datastore: exampleDatastore });
    mockSend.mockResolvedValueOnce({ datastoreSummaries: [exampleDatastore] });
    await fireEvent.click(withinModal("Create Datastore").getByRole("button", { name: "Create" }));

    await waitFor(() => screen.getByText("my-datastore"));
    expect(mockSend).toHaveBeenNthCalledWith(
      6,
      expect.objectContaining({ input: { datastoreName: "my-datastore" } }),
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ datastoreSummaries: [] });
    await fireEvent.click(screen.getByTitle("Delete"));

    expect(mockSend).toHaveBeenNthCalledWith(
      8,
      expect.objectContaining({ input: { datastoreName: "my-datastore" } }),
    );
    await waitFor(() => screen.getByText("No datastores"));
  });

  it("switches to Datasets, creates one with a hardcoded default action, and triggers content generation", async () => {
    queueInitialLoads();
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Datasets"));
    await waitFor(() => screen.getByText("No datasets"));

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await fireEvent.input(screen.getByPlaceholderText("Dataset name"), {
      target: { value: "my-dataset" },
    });

    mockSend.mockResolvedValueOnce({ dataset: exampleDataset });
    mockSend.mockResolvedValueOnce({ datasetSummaries: [exampleDataset] });
    await fireEvent.click(withinModal("Create Dataset").getByRole("button", { name: "Create" }));

    await waitFor(() => screen.getByText("my-dataset"));
    expect(mockSend).toHaveBeenNthCalledWith(
      6,
      expect.objectContaining({
        input: {
          datasetName: "my-dataset",
          actions: [{ actionName: "default" }],
        },
      }),
    );

    // Trigger content generation ("Run"): fires CreateDatasetContentCommand,
    // then (since no dataset is selected yet) falls through to
    // selectDataset(), which fetches the content-versions list.
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      datasetContentSummaries: [
        {
          version: "v1",
          status: { state: "SUCCEEDED" },
          creationTime: new Date("2024-01-01T00:00:00Z"),
          completionTime: new Date("2024-01-01T00:01:00Z"),
        },
      ],
    });
    await fireEvent.click(screen.getByRole("button", { name: "Run" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      8,
      expect.objectContaining({ input: { datasetName: "my-dataset" } }),
    );
    await waitFor(() => screen.getByText("v1"));
    expect(screen.getByText("SUCCEEDED")).toBeInTheDocument();
  });

  it("switches to Pipelines, creates one with the hardcoded default channel activity, and expands reprocessing jobs", async () => {
    queueInitialLoads();
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Pipelines"));
    await waitFor(() => screen.getByText("No pipelines"));

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await fireEvent.input(screen.getByPlaceholderText("Pipeline name"), {
      target: { value: "my-pipeline" },
    });

    mockSend.mockResolvedValueOnce({ pipeline: examplePipeline });
    mockSend.mockResolvedValueOnce({ pipelineSummaries: [examplePipeline] });
    await fireEvent.click(withinModal("Create Pipeline").getByRole("button", { name: "Create" }));

    await waitFor(() => screen.getByText("my-pipeline"));
    expect(mockSend).toHaveBeenNthCalledWith(
      6,
      expect.objectContaining({
        input: {
          pipelineName: "my-pipeline",
          pipelineActivities: [{ channel: { name: "channel", channelName: "default", next: "" } }],
        },
      }),
    );

    // Expand the reprocessing jobs list for the pipeline (purely local state,
    // derived from the already-loaded summaries -- no extra request).
    await fireEvent.click(screen.getByText("1 job(s) ↓"));
    expect(screen.getByText("reproc-1")).toBeInTheDocument();
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
  });

  it("starts and cancels pipeline reprocessing with the right pipeline/reprocessing identifiers", async () => {
    queueInitialLoads({ pipelines: [examplePipeline] });
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Pipelines"));
    await waitFor(() => screen.getByText("my-pipeline"));

    mockSend.mockResolvedValueOnce({ reprocessingId: "reproc-2" });
    mockSend.mockResolvedValueOnce({ pipelineSummaries: [examplePipeline] });
    await fireEvent.click(screen.getByRole("button", { name: "Reprocess" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        6,
        expect.objectContaining({ input: { pipelineName: "my-pipeline" } }),
      );
    });

    await fireEvent.click(screen.getByText("1 job(s) ↓"));
    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        8,
        expect.objectContaining({
          input: { pipelineName: "my-pipeline", reprocessingId: "reproc-1" },
        }),
      );
    });
  });

  it("loads and saves logging options with the exact PutLoggingOptions shape", async () => {
    queueInitialLoads({
      logging: { roleArn: "arn:aws:iam::000000000000:role/Existing", level: "INFO", enabled: true },
    });
    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Logging"));

    await waitFor(() => {
      expect(screen.getByLabelText("Role ARN")).toHaveValue(
        "arn:aws:iam::000000000000:role/Existing",
      );
    });
    expect(screen.getByLabelText("Enabled")).toBeChecked();

    await fireEvent.input(screen.getByLabelText("Role ARN"), {
      target: { value: "arn:aws:iam::000000000000:role/NewRole" },
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Save Logging Options" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        6,
        expect.objectContaining({
          input: {
            loggingOptions: {
              roleArn: "arn:aws:iam::000000000000:role/NewRole",
              level: "INFO",
              enabled: true,
            },
          },
        }),
      );
    });
  });

  it("suppresses the toast when logging options are not yet configured (404/not found)", async () => {
    mockSend.mockResolvedValueOnce({ channelSummaries: [] });
    mockSend.mockResolvedValueOnce({ datastoreSummaries: [] });
    mockSend.mockResolvedValueOnce({ datasetSummaries: [] });
    mockSend.mockResolvedValueOnce({ pipelineSummaries: [] });
    const error = Object.assign(new Error("Logging options not found for this account."), {
      name: "ResourceNotFoundException",
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(IotAnalyticsPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Logging"));

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Save Logging Options" })).toBeInTheDocument();
    });
    expect(toast.error).not.toHaveBeenCalled();
  });
});
