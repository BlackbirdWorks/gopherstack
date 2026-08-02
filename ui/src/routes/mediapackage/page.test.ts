import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MediaPackagePage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getMediaPackageClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleChannel = {
  Id: "live-channel-1",
  Arn: "arn:aws:mediapackage:us-east-1:123456789012:channels/live-channel-1",
  Description: "Primary live channel",
};

const exampleEndpoint = {
  Id: "endpoint-1",
  ChannelId: "live-channel-1",
  ManifestName: "index",
  Url: "https://example.mediapackage.amazonaws.com/out/v1/endpoint-1/index.m3u8",
};

const exampleHarvestJob = {
  Id: "harvest-1",
  ChannelId: "live-channel-1",
  OriginEndpointId: "endpoint-1",
  Status: "SUCCEEDED",
};

describe("MediaPackage Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaPackagePage);
    expect(screen.getByText("AWS Elemental MediaPackage")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No channels found"));
  });

  it("lists channels", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaPackagePage);
    await waitFor(() => {
      expect(screen.getByText("live-channel-1")).toBeInTheDocument();
    });
    expect(screen.getByText("Primary live channel")).toBeInTheDocument();
  });

  it("shows empty state when no channels", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaPackagePage);
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });
  });

  it("creates a channel via the modal with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaPackagePage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Create channel"));
    expect(screen.getByText("Create Channel")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("ID"), {
      target: { value: "live-channel-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Description"), {
      target: { value: "Primary live channel" },
    });

    mockSend.mockResolvedValueOnce({ Id: exampleChannel.Id });
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("live-channel-1")).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({ Id: "live-channel-1", Description: "Primary live channel" });
  });

  it("deletes a channel after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaPackagePage);
    await waitFor(() => screen.getByText("live-channel-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Channels: [] });
    mockSend.mockResolvedValueOnce({ OriginEndpoints: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.input).toEqual({ Id: exampleChannel.Id });
  });

  it("does not delete a channel when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaPackagePage);
    await waitFor(() => screen.getByText("live-channel-1"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("live-channel-1")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Channel not found."), {
      name: "NotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(MediaPackagePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("NotFoundException (HTTP 404): Channel not found."),
      ).toBeInTheDocument();
    });
  });

  it("lists origin endpoints and creates one with an HLS package", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaPackagePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ OriginEndpoints: [] });
    await fireEvent.click(screen.getByText("Origin Endpoints"));
    await waitFor(() => screen.getByText("No origin endpoints found"));

    await fireEvent.click(screen.getByText("Create origin endpoint"));
    await fireEvent.input(within(openDialog()).getByLabelText("ID"), {
      target: { value: "endpoint-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Channel ID"), {
      target: { value: "live-channel-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Manifest name"), {
      target: { value: "index" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("HLS package (JSON, optional)"), {
      target: { value: '{"SegmentDurationSeconds": 6}' },
    });

    mockSend.mockResolvedValueOnce({ Id: exampleEndpoint.Id });
    mockSend.mockResolvedValueOnce({ OriginEndpoints: [exampleEndpoint] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("endpoint-1"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      Id: "endpoint-1",
      ChannelId: "live-channel-1",
      Description: undefined,
      ManifestName: "index",
      HlsPackage: { SegmentDurationSeconds: 6 },
    });
  });

  it("deletes an origin endpoint after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaPackagePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ OriginEndpoints: [exampleEndpoint] });
    await fireEvent.click(screen.getByText("Origin Endpoints"));
    await waitFor(() => screen.getByText("endpoint-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ OriginEndpoints: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No origin endpoints found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[2][0];
    expect(deleteCall.input).toEqual({ Id: exampleEndpoint.Id });
  });

  it("lists harvest jobs and creates one with S3 destination fields", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaPackagePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ HarvestJobs: [] });
    await fireEvent.click(screen.getByText("Harvest Jobs"));
    await waitFor(() => screen.getByText("No harvest jobs found"));

    await fireEvent.click(screen.getByText("Create harvest job"));
    await fireEvent.input(within(openDialog()).getByLabelText("ID"), {
      target: { value: "harvest-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Origin endpoint ID"), {
      target: { value: "endpoint-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Start time (ISO8601)"), {
      target: { value: "2026-01-01T00:00:00Z" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("End time (ISO8601)"), {
      target: { value: "2026-01-01T01:00:00Z" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("S3 bucket name"), {
      target: { value: "my-harvest-bucket" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("S3 manifest key"), {
      target: { value: "harvest-1/index.m3u8" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("S3 role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/HarvestRole" },
    });

    mockSend.mockResolvedValueOnce({ Id: exampleHarvestJob.Id });
    mockSend.mockResolvedValueOnce({ HarvestJobs: [exampleHarvestJob] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("harvest-1"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      Id: "harvest-1",
      OriginEndpointId: "endpoint-1",
      StartTime: "2026-01-01T00:00:00Z",
      EndTime: "2026-01-01T01:00:00Z",
      S3Destination: {
        BucketName: "my-harvest-bucket",
        ManifestKey: "harvest-1/index.m3u8",
        RoleArn: "arn:aws:iam::123456789012:role/HarvestRole",
      },
    });

    // Harvest jobs have no delete/update op -- the row's only action is View.
    expect(screen.queryByTitle("Delete")).not.toBeInTheDocument();
  });
});
