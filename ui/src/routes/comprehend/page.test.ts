import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ComprehendPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getComprehendClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("Comprehend Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title and defaults to the Detection tab", () => {
    render(ComprehendPage);
    expect(screen.getByText("Amazon Comprehend")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Analyze/ })).toBeInTheDocument();
  });

  it("runs a real-time sentiment detection with the correct input", async () => {
    render(ComprehendPage);
    await fireEvent.input(screen.getByPlaceholderText(/Enter sample text/), {
      target: { value: "I love this product" },
    });

    mockSend.mockResolvedValueOnce({
      Sentiment: "POSITIVE",
      SentimentScore: { Positive: 0.9, Negative: 0.02, Neutral: 0.05, Mixed: 0.03 },
    });

    await fireEvent.click(screen.getByRole("button", { name: /Analyze/ }));

    await waitFor(() => {
      expect(screen.getByText("POSITIVE")).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[0][0];
    expect(call.input).toEqual({ Text: "I love this product", LanguageCode: "en" });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "TooManyRequestsException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ComprehendPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Classifiers" }));

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("TooManyRequestsException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("lists classifiers, shows empty state, creates and deletes one", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({ DocumentClassifierPropertiesList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Classifiers" }));
    await waitFor(() => screen.getByText("No document classifiers found"));

    await fireEvent.click(screen.getByText("Create classifier"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-classifier" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Data access role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/ComprehendRole" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Input data config (JSON)"), {
      target: { value: '{"DataFormat": "COMPREHEND_CSV", "S3Uri": "s3://bucket/training.csv"}' },
    });

    mockSend.mockResolvedValueOnce({
      DocumentClassifierArn:
        "arn:aws:comprehend:us-east-1:123456789012:document-classifier/my-classifier",
    });
    mockSend.mockResolvedValueOnce({
      DocumentClassifierPropertiesList: [
        {
          DocumentClassifierArn:
            "arn:aws:comprehend:us-east-1:123456789012:document-classifier/my-classifier",
          Status: "SUBMITTED",
        },
      ],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));
    await waitFor(() =>
      screen.getByText(
        "arn:aws:comprehend:us-east-1:123456789012:document-classifier/my-classifier",
      ),
    );

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({
      DocumentClassifierName: "my-classifier",
      DataAccessRoleArn: "arn:aws:iam::123456789012:role/ComprehendRole",
      LanguageCode: "en",
      InputDataConfig: { DataFormat: "COMPREHEND_CSV", S3Uri: "s3://bucket/training.csv" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DocumentClassifierPropertiesList: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      expect(screen.getByText("No document classifiers found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[3][0];
    expect(deleteCall.input).toEqual({
      DocumentClassifierArn:
        "arn:aws:comprehend:us-east-1:123456789012:document-classifier/my-classifier",
    });
  });

  it("does not delete a classifier when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({
      DocumentClassifierPropertiesList: [
        {
          DocumentClassifierArn: "arn:aws:comprehend:us-east-1:123456789012:document-classifier/x",
          Status: "TRAINED",
        },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Classifiers" }));
    await waitFor(() =>
      screen.getByText("arn:aws:comprehend:us-east-1:123456789012:document-classifier/x"),
    );

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(
      screen.getByText("arn:aws:comprehend:us-east-1:123456789012:document-classifier/x"),
    ).toBeInTheDocument();
  });

  it("creates an entity recognizer with the correct input", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({ EntityRecognizerPropertiesList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Entity Recognizers" }));
    await waitFor(() => screen.getByText("No entity recognizers found"));

    await fireEvent.click(screen.getByText("Create recognizer"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-recognizer" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Data access role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/ComprehendRole" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Input data config (JSON)"), {
      target: { value: '{"EntityTypes": [{"Type": "PRODUCT"}]}' },
    });

    mockSend.mockResolvedValueOnce({
      EntityRecognizerArn:
        "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/my-recognizer",
    });
    mockSend.mockResolvedValueOnce({
      EntityRecognizerPropertiesList: [
        {
          EntityRecognizerArn:
            "arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/my-recognizer",
          Status: "SUBMITTED",
        },
      ],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));
    await waitFor(() =>
      screen.getByText("arn:aws:comprehend:us-east-1:123456789012:entity-recognizer/my-recognizer"),
    );

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({
      RecognizerName: "my-recognizer",
      DataAccessRoleArn: "arn:aws:iam::123456789012:role/ComprehendRole",
      LanguageCode: "en",
      InputDataConfig: { EntityTypes: [{ Type: "PRODUCT" }] },
    });
  });

  it("lists endpoints, views detail and deletes one", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({
      EndpointPropertiesList: [
        {
          EndpointArn: "arn:aws:comprehend:us-east-1:123456789012:endpoint/ep-1",
          Status: "IN_SERVICE",
          DesiredInferenceUnits: 2,
        },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Endpoints" }));
    await waitFor(() =>
      screen.getByText("arn:aws:comprehend:us-east-1:123456789012:endpoint/ep-1"),
    );

    mockSend.mockResolvedValueOnce({
      EndpointProperties: {
        EndpointArn: "arn:aws:comprehend:us-east-1:123456789012:endpoint/ep-1",
        Status: "IN_SERVICE",
        DesiredInferenceUnits: 2,
      },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("IN_SERVICE")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ EndpointPropertiesList: [] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Delete" }));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      expect(screen.getByText("No endpoints found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[2][0];
    expect(deleteCall.input).toEqual({
      EndpointArn: "arn:aws:comprehend:us-east-1:123456789012:endpoint/ep-1",
    });
  });

  it("shows a dataset's detail with no update or delete affordance", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({
      DatasetPropertiesList: [
        {
          DatasetArn: "arn:aws:comprehend:us-east-1:123456789012:dataset/ds-1",
          DatasetName: "ds-1",
          DatasetType: "TRAIN",
          Status: "COMPLETED",
        },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Datasets" }));
    await waitFor(() => screen.getByText("ds-1"));

    mockSend.mockResolvedValueOnce({
      DatasetProperties: {
        DatasetArn: "arn:aws:comprehend:us-east-1:123456789012:dataset/ds-1",
        DatasetName: "ds-1",
        DatasetType: "TRAIN",
        Status: "COMPLETED",
      },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText(/Datasets are immutable/)).toBeInTheDocument();
    });
    expect(within(openDialog()).queryByRole("button", { name: "Delete" })).not.toBeInTheDocument();
  });

  it("lists jobs for the selected job family with the status shown via a render snippet", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({
      DocumentClassificationJobPropertiesList: [
        { JobId: "job-1", JobName: "my-job", JobStatus: "IN_PROGRESS" },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Jobs" }));

    await waitFor(() => {
      expect(screen.getByText("my-job")).toBeInTheDocument();
      expect(screen.getByText("IN_PROGRESS")).toBeInTheDocument();
    });

    const listCall = mockSend.mock.calls[0][0];
    expect(listCall.input).toEqual({});
  });

  it("starts a job with the correct input for the selected family", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({ DocumentClassificationJobPropertiesList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Jobs" }));
    await waitFor(() => screen.getByText(/No Document Classification jobs found/));

    await fireEvent.click(screen.getByText("Start job"));
    await fireEvent.input(within(openDialog()).getByLabelText("Data access role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/ComprehendRole" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Input S3 URI"), {
      target: { value: "s3://bucket/input/" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Output S3 URI"), {
      target: { value: "s3://bucket/output/" },
    });

    mockSend.mockResolvedValueOnce({ JobId: "job-2", JobArn: "arn:job-2", JobStatus: "SUBMITTED" });
    mockSend.mockResolvedValueOnce({
      DocumentClassificationJobPropertiesList: [{ JobId: "job-2", JobStatus: "SUBMITTED" }],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Start" }));
    await waitFor(() => screen.getByText("job-2"));

    const startCall = mockSend.mock.calls[1][0];
    expect(startCall.input).toEqual({
      JobName: undefined,
      DataAccessRoleArn: "arn:aws:iam::123456789012:role/ComprehendRole",
      InputDataConfig: { S3Uri: "s3://bucket/input/" },
      OutputDataConfig: { S3Uri: "s3://bucket/output/" },
    });
  });

  it("does not offer Stop for a job family with no real Stop operation", async () => {
    render(ComprehendPage);

    mockSend.mockResolvedValueOnce({
      DocumentClassificationJobPropertiesList: [
        { JobId: "job-3", JobName: "no-stop-job", JobStatus: "IN_PROGRESS" },
      ],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Jobs" }));
    await waitFor(() => screen.getByText("no-stop-job"));

    mockSend.mockResolvedValueOnce({
      DocumentClassificationJobProperties: {
        JobId: "job-3",
        JobName: "no-stop-job",
        JobStatus: "IN_PROGRESS",
      },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("no-stop-job")).toBeInTheDocument();
    });
    expect(
      within(openDialog()).queryByRole("button", { name: /Stop job/ }),
    ).not.toBeInTheDocument();
  });
});
