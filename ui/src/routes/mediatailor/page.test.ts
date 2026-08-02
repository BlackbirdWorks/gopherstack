import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MediaTailorPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getMediaTailorClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleChannel = {
  ChannelName: "linear-channel",
  PlaybackMode: "LINEAR",
  Tier: "BASIC",
  ChannelState: "STOPPED",
};

const exampleSourceLocation = {
  SourceLocationName: "primary-source",
  Arn: "arn:aws:mediatailor:us-east-1:123456789012:sourceLocation/primary-source",
};

const exampleConfig = {
  Name: "config-1",
  PlaybackConfigurationArn:
    "arn:aws:mediatailor:us-east-1:123456789012:playbackConfiguration/config-1",
};

const exampleVodSource = {
  VodSourceName: "vod-1",
  SourceLocationName: "primary-source",
};

describe("MediaTailor Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    expect(screen.getByText("AWS Elemental MediaTailor")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No channels found"));
  });

  it("lists channels", async () => {
    mockSend.mockResolvedValueOnce({ Items: [exampleChannel] });
    render(MediaTailorPage);
    await waitFor(() => {
      expect(screen.getByText("linear-channel")).toBeInTheDocument();
    });
    expect(screen.getByText("STOPPED")).toBeInTheDocument();
  });

  it("shows empty state when no channels", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });
  });

  it("creates a channel via the modal with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Create channel"));
    expect(screen.getByText("Create Channel")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "linear-channel" },
    });

    mockSend.mockResolvedValueOnce({ ...exampleChannel });
    mockSend.mockResolvedValueOnce({ Items: [exampleChannel] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("linear-channel")).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({
      ChannelName: "linear-channel",
      PlaybackMode: "LINEAR",
      Tier: "BASIC",
      Outputs: [{ ManifestName: "index", SourceGroup: "default" }],
    });
  });

  it("deletes a channel after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Items: [exampleChannel] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("linear-channel"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Items: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.input).toEqual({ ChannelName: exampleChannel.ChannelName });
  });

  it("does not delete a channel when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Items: [exampleChannel] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("linear-channel"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("linear-channel")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Channel not found."), {
      name: "NotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(MediaTailorPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("NotFoundException (HTTP 404): Channel not found."),
      ).toBeInTheDocument();
    });
  });

  it("starts a stopped channel from the detail view", async () => {
    mockSend.mockResolvedValueOnce({ Items: [exampleChannel] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("linear-channel"));

    mockSend.mockResolvedValueOnce({ ...exampleChannel });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByRole("button", { name: "Start" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ...exampleChannel, ChannelState: "RUNNING" });
    mockSend.mockResolvedValueOnce({ Items: [{ ...exampleChannel, ChannelState: "RUNNING" }] });

    await fireEvent.click(screen.getByRole("button", { name: "Start" }));

    const startCall = mockSend.mock.calls[2][0];
    expect(startCall.input).toEqual({ ChannelName: exampleChannel.ChannelName });
    await waitFor(() => {
      expect(screen.getByText("RUNNING")).toBeInTheDocument();
    });
  });

  it("lists source locations and creates one", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ Items: [] });
    await fireEvent.click(screen.getByText("Source Locations"));
    await waitFor(() => screen.getByText("No source locations found"));

    await fireEvent.click(screen.getByText("Create source location"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "primary-source" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Base URL"), {
      target: { value: "https://example.com/media/" },
    });

    mockSend.mockResolvedValueOnce({ ...exampleSourceLocation });
    mockSend.mockResolvedValueOnce({ Items: [exampleSourceLocation] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("primary-source"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      SourceLocationName: "primary-source",
      HttpConfiguration: { BaseUrl: "https://example.com/media/" },
    });
  });

  it("lists playback configs and creates one via PutPlaybackConfiguration", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ Items: [] });
    await fireEvent.click(screen.getByText("Playback Configs"));
    await waitFor(() => screen.getByText("No playback configs found"));

    await fireEvent.click(screen.getByText("Create playback config"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "config-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Video content source URL"), {
      target: { value: "https://example.com/vod/" },
    });

    mockSend.mockResolvedValueOnce({ ...exampleConfig });
    mockSend.mockResolvedValueOnce({ Items: [exampleConfig] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("config-1"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      Name: "config-1",
      VideoContentSourceUrl: "https://example.com/vod/",
      AdDecisionServerUrl: undefined,
    });
  });

  it("shows a prompt instead of fetching VOD sources until a source location is entered", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("VOD Sources"));
    await waitFor(() => {
      expect(
        screen.getByText("Enter a source location above to list its VOD sources"),
      ).toBeInTheDocument();
    });
    // No ListVodSources call was made -- only the initial ListChannels call happened.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("lists VOD sources for an entered source location and creates one", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    render(MediaTailorPage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("VOD Sources"));
    await waitFor(() => screen.getByText("Enter a source location above to list its VOD sources"));

    mockSend.mockResolvedValueOnce({ Items: [] });
    const sourceLocationFilterInput = document.querySelector(
      "#source-location-filter",
    ) as HTMLInputElement;
    await fireEvent.input(sourceLocationFilterInput, {
      target: { value: "primary-source" },
    });
    await fireEvent.change(sourceLocationFilterInput);
    await waitFor(() => screen.getByText("No VOD sources found"));

    const listCall = mockSend.mock.calls[1][0];
    expect(listCall.input).toEqual({ SourceLocationName: "primary-source" });

    await fireEvent.click(screen.getByText("Create VOD source"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "vod-1" },
    });
    await fireEvent.input(
      within(openDialog()).getByLabelText("Path (relative to source location base URL)"),
      {
        target: { value: "/media/asset.m3u8" },
      },
    );

    mockSend.mockResolvedValueOnce({ ...exampleVodSource });
    mockSend.mockResolvedValueOnce({ Items: [exampleVodSource] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("vod-1"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      VodSourceName: "vod-1",
      SourceLocationName: "primary-source",
      HttpPackageConfigurations: [
        { Path: "/media/asset.m3u8", SourceGroup: "default", Type: "HLS" },
      ],
    });
  });
});
