import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import TranslatePage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getTranslateClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleTerminology = {
  Name: "my-glossary",
  SourceLanguageCode: "en",
  TargetLanguageCodes: ["es"],
};

describe("Translate Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    expect(screen.getByText("Amazon Translate")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No terminologies found"));
  });

  it("shows empty state when no terminologies", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    await waitFor(() => {
      expect(screen.getByText("No terminologies found")).toBeInTheDocument();
    });
  });

  it("lists terminologies with a languages cell from a render snippet", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [exampleTerminology] });
    render(TranslatePage);
    await waitFor(() => {
      const table = screen.getByRole("table");
      expect(within(table).getByText("my-glossary")).toBeInTheDocument();
      expect(within(table).getByText("en → es")).toBeInTheDocument();
    });
  });

  it("imports a terminology via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    await waitFor(() => screen.getByText("No terminologies found"));

    await fireEvent.click(screen.getByText("Import terminology"));
    expect(screen.getByText("Import Terminology")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-glossary" },
    });
    const dataField = within(openDialog()).getByLabelText(
      /Terminology data/,
    ) as HTMLTextAreaElement;
    await fireEvent.input(dataField, { target: { value: "en,es\nhello,hola\n" } });

    mockSend.mockResolvedValueOnce({ TerminologyProperties: exampleTerminology });
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [exampleTerminology] });

    await fireEvent.click(screen.getByRole("button", { name: "Import" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-glossary")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      Name: "my-glossary",
      MergeStrategy: "OVERWRITE",
      Description: undefined,
      TerminologyData: {
        File: new TextEncoder().encode("en,es\nhello,hola\n"),
        Format: "CSV",
      },
    });
  });

  it("deletes a terminology after confirming", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [exampleTerminology] });
    render(TranslatePage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-glossary"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No terminologies found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({ Name: "my-glossary" });
  });

  it("does not delete a terminology when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [exampleTerminology] });
    render(TranslatePage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-glossary"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-glossary")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Terminology not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(TranslatePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Terminology not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a terminology's detail view", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [exampleTerminology] });
    render(TranslatePage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-glossary"));

    mockSend.mockResolvedValueOnce({
      TerminologyProperties: { ...exampleTerminology, TermCount: 3, SizeBytes: 42 },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("3")).toBeInTheDocument();
    });
  });

  it("switches to the Translation Jobs tab and starts a job with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    await waitFor(() => screen.getByText("No terminologies found"));

    mockSend.mockResolvedValueOnce({ TextTranslationJobPropertiesList: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Translation Jobs" }));
    await waitFor(() => screen.getByText("No translation jobs found"));

    await fireEvent.click(screen.getByText("Start translation job"));
    await fireEvent.input(within(openDialog()).getByLabelText("Data access role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/translate-role" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Input S3 URI"), {
      target: { value: "s3://in-bucket/data" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Output S3 URI"), {
      target: { value: "s3://out-bucket/results" },
    });

    mockSend.mockResolvedValueOnce({ JobId: "job-1", JobStatus: "SUBMITTED" });
    mockSend.mockResolvedValueOnce({
      TextTranslationJobPropertiesList: [{ JobId: "job-1", JobStatus: "SUBMITTED" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Start" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("job-1")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[2][0].input).toEqual({
      JobName: undefined,
      DataAccessRoleArn: "arn:aws:iam::123456789012:role/translate-role",
      InputDataConfig: { S3Uri: "s3://in-bucket/data", ContentType: "text/plain" },
      OutputDataConfig: { S3Uri: "s3://out-bucket/results" },
      SourceLanguageCode: "en",
      TargetLanguageCodes: ["es"],
    });
  });

  it("runs the Run Translation tester with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ TerminologyPropertiesList: [] });
    render(TranslatePage);
    await waitFor(() => screen.getByText("No terminologies found"));

    await fireEvent.click(screen.getByRole("tab", { name: "Run Translation" }));
    await fireEvent.input(screen.getByPlaceholderText("Enter text to translate..."), {
      target: { value: "hello" },
    });

    mockSend.mockResolvedValueOnce({ TranslatedText: "hola", SourceLanguageCode: "en" });

    await fireEvent.click(screen.getByRole("button", { name: "Translate" }));

    await waitFor(() => {
      expect(screen.getByText("hola")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      Text: "hello",
      SourceLanguageCode: "auto",
      TargetLanguageCode: "es",
    });
  });
});
