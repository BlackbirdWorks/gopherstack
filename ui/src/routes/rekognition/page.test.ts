import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import RekognitionPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getRekognitionClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("Rekognition Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: [] });
    render(RekognitionPage);
    expect(screen.getByText("Amazon Rekognition")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No face collections found"));
  });

  it("shows empty state when no collections", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: [] });
    render(RekognitionPage);
    await waitFor(() => {
      expect(screen.getByText("No face collections found")).toBeInTheDocument();
    });
  });

  it("lists collections with the collection ID from a render snippet", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: ["my-collection"] });
    render(RekognitionPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-collection")).toBeInTheDocument();
    });
  });

  it("creates a collection via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: [] });
    render(RekognitionPage);
    await waitFor(() => screen.getByText("No face collections found"));

    await fireEvent.click(screen.getByText("Create collection"));
    expect(screen.getByText("Create Collection")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Collection ID"), {
      target: { value: "my-collection" },
    });

    mockSend.mockResolvedValueOnce({
      CollectionArn: "arn:aws:rekognition:us-east-1:123456789012:collection/my-collection",
    });
    mockSend.mockResolvedValueOnce({ CollectionIds: ["my-collection"] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-collection")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      CollectionId: "my-collection",
    });
  });

  it("deletes a collection after confirming", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: ["my-collection"] });
    render(RekognitionPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-collection"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ CollectionIds: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No face collections found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({
      CollectionId: "my-collection",
    });
  });

  it("does not delete a collection when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ CollectionIds: ["my-collection"] });
    render(RekognitionPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-collection"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-collection")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(RekognitionPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 400): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("opens a collection's detail view and lists its faces", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: ["my-collection"] });
    render(RekognitionPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-collection"));

    mockSend.mockResolvedValueOnce({
      CollectionARN: "arn:aws:rekognition:us-east-1:123456789012:collection/my-collection",
      FaceCount: 1,
    });
    mockSend.mockResolvedValueOnce({ Faces: [{ FaceId: "face-1" }] });

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("face-1")).toBeInTheDocument();
    });
  });

  it("switches to the Stream Processors tab and creates a processor", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: [] });
    render(RekognitionPage);
    await waitFor(() => screen.getByText("No face collections found"));

    mockSend.mockResolvedValueOnce({ StreamProcessors: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Stream Processors" }));
    await waitFor(() => screen.getByText("No stream processors found"));

    await fireEvent.click(screen.getByText("Create stream processor"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-processor" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Input Kinesis Video Stream ARN"), {
      target: { value: "arn:aws:kinesisvideo:us-east-1:123456789012:stream/in" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Output Kinesis Data Stream ARN"), {
      target: { value: "arn:aws:kinesis:us-east-1:123456789012:stream/out" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/rekognition-role" },
    });
    // Connected Home mode has no collection dependency (unlike Face Search,
    // which needs a pre-loaded collection to pick from).
    await fireEvent.change(within(openDialog()).getByLabelText("Analysis mode"), {
      target: { value: "ConnectedHome" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText(/Labels/), {
      target: { value: "PERSON, ALL" },
    });

    mockSend.mockResolvedValueOnce({ StreamProcessorArn: "arn:.../my-processor" });
    mockSend.mockResolvedValueOnce({
      StreamProcessors: [{ Name: "my-processor", Status: "STOPPED" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-processor")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[2][0].input).toEqual({
      Name: "my-processor",
      Input: {
        KinesisVideoStream: { Arn: "arn:aws:kinesisvideo:us-east-1:123456789012:stream/in" },
      },
      Output: { KinesisDataStream: { Arn: "arn:aws:kinesis:us-east-1:123456789012:stream/out" } },
      RoleArn: "arn:aws:iam::123456789012:role/rekognition-role",
      Settings: { ConnectedHome: { Labels: ["PERSON", "ALL"] } },
    });
  });

  it("runs Detect Faces against an S3 image", async () => {
    mockSend.mockResolvedValueOnce({ CollectionIds: [] });
    render(RekognitionPage);
    await waitFor(() => screen.getByText("No face collections found"));

    await fireEvent.click(screen.getByRole("tab", { name: "Detect Faces" }));
    await fireEvent.input(screen.getByLabelText("S3 Bucket"), { target: { value: "my-bucket" } });
    await fireEvent.input(screen.getByLabelText("S3 Key (image)"), {
      target: { value: "photo.jpg" },
    });

    mockSend.mockResolvedValueOnce({ FaceDetails: [{ Confidence: 99.5 }] });

    await fireEvent.click(screen.getByRole("button", { name: "Detect Faces" }));

    await waitFor(() => {
      expect(screen.getByText("Confidence 99.5%")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      Image: { S3Object: { Bucket: "my-bucket", Name: "photo.jpg" } },
      Attributes: ["ALL"],
    });
  });
});
