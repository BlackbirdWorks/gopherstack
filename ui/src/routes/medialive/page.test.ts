import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MediaLivePage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getMediaLiveClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleChannel = {
  Id: "1234567",
  Name: "primary-channel",
  ChannelClass: "STANDARD",
  State: "IDLE",
};

const exampleInput = {
  Id: "in-0001",
  Name: "camera-1",
  Type: "URL_PULL",
  State: "DETACHED",
};

const exampleInputSg = {
  Id: "sg-0001",
  Arn: "arn:aws:medialive:us-east-1:123456789012:inputSecurityGroup:sg-0001",
  State: "IDLE",
  WhitelistRules: [{ Cidr: "10.0.0.0/24" }],
};

const exampleMultiplex = {
  Id: "mux-0001",
  Name: "primary-mux",
  State: "IDLE",
};

describe("MediaLive Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    expect(screen.getByText("AWS Elemental MediaLive")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No channels found"));
  });

  it("lists channels", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaLivePage);
    await waitFor(() => {
      expect(screen.getByText("primary-channel")).toBeInTheDocument();
    });
    expect(screen.getByText("IDLE")).toBeInTheDocument();
  });

  it("shows empty state when no channels", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });
  });

  it("creates a channel via the modal with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("No channels found"));

    await fireEvent.click(screen.getByText("Create channel"));
    expect(screen.getByText("Create Channel")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "primary-channel" },
    });

    mockSend.mockResolvedValueOnce({ Channel: exampleChannel });
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("primary-channel")).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.input).toEqual({
      Name: "primary-channel",
      ChannelClass: "STANDARD",
      RoleArn: undefined,
      EncoderSettings: undefined,
    });
  });

  it("deletes a channel after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("primary-channel"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Channels: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No channels found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.input).toEqual({ ChannelId: exampleChannel.Id });
  });

  it("does not delete a channel when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("primary-channel"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("primary-channel")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Channel not found."), {
      name: "NotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(MediaLivePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("NotFoundException (HTTP 404): Channel not found."),
      ).toBeInTheDocument();
    });
  });

  it("starts a channel from the detail view when it is IDLE", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [exampleChannel] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("primary-channel"));

    mockSend.mockResolvedValueOnce({ ...exampleChannel });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByRole("button", { name: "Start" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ...exampleChannel, State: "STARTING" });
    mockSend.mockResolvedValueOnce({ Channels: [{ ...exampleChannel, State: "STARTING" }] });

    await fireEvent.click(screen.getByRole("button", { name: "Start" }));

    const startCall = mockSend.mock.calls[2][0];
    expect(startCall.input).toEqual({ ChannelId: exampleChannel.Id });
    await waitFor(() => {
      expect(screen.getByText("STARTING")).toBeInTheDocument();
    });
  });

  it("lists inputs and creates one", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ Inputs: [] });
    await fireEvent.click(screen.getByText("Inputs"));
    await waitFor(() => screen.getByText("No inputs found"));

    await fireEvent.click(screen.getByText("Create input"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "camera-1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Source URL"), {
      target: { value: "https://example.com/stream.m3u8" },
    });

    mockSend.mockResolvedValueOnce({ Input: exampleInput });
    mockSend.mockResolvedValueOnce({ Inputs: [exampleInput] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("camera-1"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      Name: "camera-1",
      Type: "URL_PULL",
      InputSecurityGroups: undefined,
      Sources: [{ Url: "https://example.com/stream.m3u8" }],
      Destinations: undefined,
    });
  });

  it("lists input security groups and creates one with CIDR rules", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ InputSecurityGroups: [] });
    await fireEvent.click(screen.getByText("Input Security Groups"));
    await waitFor(() => screen.getByText("No input security groups found"));

    await fireEvent.click(screen.getByText("Create security group"));
    await fireEvent.input(
      within(openDialog()).getByLabelText("Whitelisted CIDRs (comma-separated)"),
      {
        target: { value: "10.0.0.0/24, 192.168.1.0/24" },
      },
    );

    mockSend.mockResolvedValueOnce({ SecurityGroup: exampleInputSg });
    mockSend.mockResolvedValueOnce({ InputSecurityGroups: [exampleInputSg] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("sg-0001"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      WhitelistRules: [{ Cidr: "10.0.0.0/24" }, { Cidr: "192.168.1.0/24" }],
    });
  });

  it("deletes an input security group after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ InputSecurityGroups: [exampleInputSg] });
    await fireEvent.click(screen.getByText("Input Security Groups"));
    await waitFor(() => screen.getByText("sg-0001"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ InputSecurityGroups: [] });

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No input security groups found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[2][0];
    expect(deleteCall.input).toEqual({ InputSecurityGroupId: exampleInputSg.Id });
  });

  it("lists multiplexes and creates one with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ Channels: [] });
    render(MediaLivePage);
    await waitFor(() => screen.getByText("No channels found"));

    mockSend.mockResolvedValueOnce({ Multiplexes: [] });
    await fireEvent.click(screen.getByText("Multiplexes"));
    await waitFor(() => screen.getByText("No multiplexes found"));

    await fireEvent.click(screen.getByText("Create multiplex"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "primary-mux" },
    });
    await fireEvent.input(
      within(openDialog()).getByLabelText("Availability zones (exactly two, comma-separated)"),
      { target: { value: "us-east-1a, us-east-1b" } },
    );
    await fireEvent.input(within(openDialog()).getByLabelText("Transport stream bitrate"), {
      target: { value: "10000000" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Transport stream ID"), {
      target: { value: "1" },
    });

    mockSend.mockResolvedValueOnce({ Multiplex: exampleMultiplex });
    mockSend.mockResolvedValueOnce({ Multiplexes: [exampleMultiplex] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => screen.getByText("primary-mux"));

    const createCall = mockSend.mock.calls[2][0];
    expect(createCall.input).toEqual({
      Name: "primary-mux",
      AvailabilityZones: ["us-east-1a", "us-east-1b"],
      MultiplexSettings: { TransportStreamBitrate: 10000000, TransportStreamId: 1 },
    });
  });
});
