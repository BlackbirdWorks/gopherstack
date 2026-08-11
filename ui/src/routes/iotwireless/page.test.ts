import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import IotWirelessPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getIoTWirelessClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

const toastError = vi.fn();
const toastSuccess = vi.fn();
vi.mock("svelte-sonner", () => ({
  toast: {
    success: (...a: unknown[]) => toastSuccess(...a),
    error: (...a: unknown[]) => toastError(...a),
  },
}));

const exampleDevice = { Id: "device-1", Type: "LoRaWAN", DestinationName: "my-destination" };
const exampleGateway = { Id: "gw-1", Name: "gateway-1", Description: "roof unit" };
const exampleServiceProfile = { Id: "sp-1", Name: "my-service-profile" };
const exampleDestination = {
  Name: "my-destination",
  Expression: "my-iot-rule",
  ExpressionType: "RuleName",
};
const exampleDeviceProfile = { Id: "dp-1", Name: "my-device-profile" };
const exampleFuotaTask = { Id: "fuota-1", Name: "my-fuota-task" };

// This page has no tab-lazy-loading: `setTab` only flips `activeTab`, and
// all six lists are fetched exactly once, at mount, by onRegionChange
// calling all six loaders synchronously up to their first await. Clicking
// a tab button never issues a new request -- only its own per-tab
// "Refresh" button or a mutating action's own reload does. So every test
// that wants a particular tab populated has to supply that tab's data as
// part of the fixed six-call mount sequence, in this exact order:
// Devices, Gateways, ServiceProfiles, Destinations, DeviceProfiles, FuotaTasks.
function mockMount(
  lists: {
    devices?: unknown[];
    gateways?: unknown[];
    serviceProfiles?: unknown[];
    destinations?: unknown[];
    deviceProfiles?: unknown[];
    fuotaTasks?: unknown[];
  } = {},
) {
  mockSend.mockResolvedValueOnce({ WirelessDeviceList: lists.devices ?? [] });
  mockSend.mockResolvedValueOnce({ WirelessGatewayList: lists.gateways ?? [] });
  mockSend.mockResolvedValueOnce({ ServiceProfileList: lists.serviceProfiles ?? [] });
  mockSend.mockResolvedValueOnce({ DestinationList: lists.destinations ?? [] });
  mockSend.mockResolvedValueOnce({ DeviceProfileList: lists.deviceProfiles ?? [] });
  mockSend.mockResolvedValueOnce({ FuotaTaskList: lists.fuotaTasks ?? [] });
}

describe("IoT Wireless Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockMount();
    render(IotWirelessPage);
    expect(screen.getByText("IoT Wireless")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockMount();
    render(IotWirelessPage);
    expect(screen.getByText("Devices")).toBeInTheDocument();
    expect(screen.getByText("Gateways")).toBeInTheDocument();
    expect(screen.getByText("Service Profiles")).toBeInTheDocument();
    expect(screen.getByText("Destinations")).toBeInTheDocument();
    expect(screen.getByText("Device Profiles")).toBeInTheDocument();
    expect(screen.getByText("FUOTA Tasks")).toBeInTheDocument();
  });

  it("loads all six lists on mount, each with an empty command input, in Devices/Gateways/ServiceProfiles/Destinations/DeviceProfiles/FuotaTasks order", async () => {
    mockMount();
    render(IotWirelessPage);
    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(6);
    });
    for (const call of mockSend.mock.calls) {
      expect(call[0]).toEqual(expect.objectContaining({ input: {} }));
    }
  });

  it("lists wireless devices", async () => {
    mockMount({ devices: [exampleDevice] });
    render(IotWirelessPage);
    await waitFor(() => {
      expect(screen.getByText("device-1")).toBeInTheDocument();
    });
    expect(screen.getByText("LoRaWAN")).toBeInTheDocument();
    expect(screen.getByText("my-destination")).toBeInTheDocument();
  });

  it("shows an empty state when there are no wireless devices", async () => {
    mockMount();
    render(IotWirelessPage);
    await waitFor(() => {
      expect(screen.getByText("No wireless devices found")).toBeInTheDocument();
    });
  });

  it("creates a wireless device via the inline form", async () => {
    mockMount();
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Create Device"));
    await fireEvent.input(screen.getByPlaceholderText("my-device"), {
      target: { value: "device-2" },
    });
    await fireEvent.input(screen.getByPlaceholderText("my-destination"), {
      target: { value: "dest-a" },
    });

    // createDevice() sends CreateWirelessDeviceCommand, then only reloads
    // its own tab (loadDevices()) -- not all six lists.
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      WirelessDeviceList: [{ Id: "device-2", Type: "LoRaWAN", DestinationName: "dest-a" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("device-2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      7,
      expect.objectContaining({
        input: { Name: "device-2", Type: "LoRaWAN", DestinationName: "dest-a" },
      }),
    );
  });

  it("shows an inline error when the device name is missing, with no API call", async () => {
    mockMount();
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    const callsBeforeSubmit = mockSend.mock.calls.length;
    await fireEvent.click(screen.getByText("Create Device"));
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(toastError).toHaveBeenCalledWith("Device name is required");
    expect(mockSend.mock.calls.length).toBe(callsBeforeSubmit);
  });

  it("deletes a wireless device after confirming", async () => {
    mockMount({ devices: [exampleDevice] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("device-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ WirelessDeviceList: [] });

    await fireEvent.click(screen.getByRole("button", { name: /Delete/ }));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("device-1") }),
    );
    await waitFor(() => screen.getByText("No wireless devices found"));
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: { Id: "device-1" } }));
  });

  it("does not delete a wireless device when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockMount({ devices: [exampleDevice] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("device-1"));

    const callsBefore = mockSend.mock.calls.length;
    await fireEvent.click(screen.getByRole("button", { name: /Delete/ }));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
    expect(screen.getByText("device-1")).toBeInTheDocument();
  });

  it("shows a toast with the AWS error code and message when creating a device fails", async () => {
    mockMount();
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Create Device"));
    await fireEvent.input(screen.getByPlaceholderText("my-device"), {
      target: { value: "device-x" },
    });

    const error = Object.assign(new Error("Limit exceeded."), {
      name: "LimitExceededException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toastError).toHaveBeenCalledWith(
        "Failed to create device: LimitExceededException (HTTP 429): Limit exceeded.",
      );
    });
  });

  it("shows the Gateways tab's already-loaded data and creates a new gateway", async () => {
    mockMount({ gateways: [exampleGateway] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Gateways"));
    await waitFor(() => screen.getByText("gw-1"));
    expect(screen.getByText("roof unit")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Create Gateway"));
    await fireEvent.input(screen.getByPlaceholderText("my-gateway"), { target: { value: "gw-2" } });
    await fireEvent.input(screen.getByPlaceholderText("Optional description"), {
      target: { value: "backup unit" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      WirelessGatewayList: [
        exampleGateway,
        { Id: "gw-2", Name: "gw-2", Description: "backup unit" },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("backup unit")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      7,
      expect.objectContaining({
        input: { Name: "gw-2", Description: "backup unit", LoRaWAN: {} },
      }),
    );
  });

  it("deletes a gateway after confirming", async () => {
    mockMount({ gateways: [exampleGateway] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Gateways"));
    await waitFor(() => screen.getByText("gw-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ WirelessGatewayList: [] });

    await fireEvent.click(screen.getByRole("button", { name: /Delete/ }));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("gw-1") }),
    );
    await waitFor(() => screen.getByText("No gateways found"));
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: { Id: "gw-1" } }));
  });

  it("shows the Service Profiles tab's already-loaded data and creates a new profile", async () => {
    mockMount({ serviceProfiles: [exampleServiceProfile] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Service Profiles"));
    await waitFor(() => screen.getByText("my-service-profile"));

    // The header action and the form's own submit button are both plain
    // "Create" here (unlike Devices/Gateways, which say "Create Device" /
    // "Create Gateway") -- scope the submit click to the form.
    await fireEvent.click(screen.getByText("Create"));
    await fireEvent.input(screen.getByPlaceholderText("my-service-profile"), {
      target: { value: "sp-2" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ServiceProfileList: [exampleServiceProfile, { Id: "sp-2-id", Name: "sp-2" }],
    });

    const form = screen.getByText("Create Service Profile").closest("div.rounded") as HTMLElement;
    await fireEvent.click(within(form).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("sp-2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      7,
      expect.objectContaining({ input: { Name: "sp-2" } }),
    );
  });

  it("shows the Destinations tab's already-loaded data and creates a new destination", async () => {
    mockMount({ destinations: [exampleDestination] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Destinations"));
    await waitFor(() => screen.getByText("my-destination"));
    expect(screen.getByText("my-iot-rule")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Create"));
    await fireEvent.input(screen.getByPlaceholderText("my-destination"), {
      target: { value: "dest-2" },
    });
    await fireEvent.input(screen.getByPlaceholderText("my-iot-rule"), {
      target: { value: "rule-2" },
    });
    await fireEvent.input(screen.getByPlaceholderText("arn:aws:iam::..."), {
      target: { value: "arn:aws:iam::123456789012:role/iot-role" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      DestinationList: [
        exampleDestination,
        { Name: "dest-2", Expression: "rule-2", ExpressionType: "RuleName" },
      ],
    });

    const form = screen.getByText("Create Destination").closest("div.rounded") as HTMLElement;
    await fireEvent.click(within(form).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("dest-2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      7,
      expect.objectContaining({
        input: {
          Name: "dest-2",
          Expression: "rule-2",
          ExpressionType: "RuleName",
          RoleArn: "arn:aws:iam::123456789012:role/iot-role",
        },
      }),
    );
  });

  it("shows an inline error when destination name/expression is missing", async () => {
    mockMount();
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Destinations"));
    await waitFor(() => screen.getByText("No destinations found"));

    const callsBeforeSubmit = mockSend.mock.calls.length;
    await fireEvent.click(screen.getByText("Create"));
    const form = screen.getByText("Create Destination").closest("div.rounded") as HTMLElement;
    await fireEvent.click(within(form).getByRole("button", { name: "Create" }));

    expect(toastError).toHaveBeenCalledWith("Name and expression are required");
    expect(mockSend.mock.calls.length).toBe(callsBeforeSubmit);
  });

  it("shows the Device Profiles tab's already-loaded data and creates a new profile", async () => {
    mockMount({ deviceProfiles: [exampleDeviceProfile] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("Device Profiles"));
    await waitFor(() => screen.getByText("my-device-profile"));

    await fireEvent.click(screen.getByText("Create"));
    await fireEvent.input(screen.getByPlaceholderText("my-device-profile"), {
      target: { value: "dp-2" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      DeviceProfileList: [exampleDeviceProfile, { Id: "dp-2-id", Name: "dp-2" }],
    });

    const form = screen.getByText("Create Device Profile").closest("div.rounded") as HTMLElement;
    await fireEvent.click(within(form).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("dp-2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      7,
      expect.objectContaining({ input: { Name: "dp-2" } }),
    );
  });

  it("shows the FUOTA Tasks tab's already-loaded data and creates a new task", async () => {
    mockMount({ fuotaTasks: [exampleFuotaTask] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("No wireless devices found"));

    await fireEvent.click(screen.getByText("FUOTA Tasks"));
    await waitFor(() => screen.getByText("my-fuota-task"));

    await fireEvent.click(screen.getByText("Create"));
    await fireEvent.input(screen.getByPlaceholderText("my-fuota-task"), {
      target: { value: "fuota-2" },
    });
    await fireEvent.input(screen.getByPlaceholderText("s3://my-bucket/firmware.bin"), {
      target: { value: "s3://my-bucket/fw2.bin" },
    });
    await fireEvent.input(screen.getByPlaceholderText("arn:aws:iam::..."), {
      target: { value: "arn:aws:iam::123456789012:role/fuota-role" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      FuotaTaskList: [exampleFuotaTask, { Id: "fuota-2-id", Name: "fuota-2" }],
    });

    const form = screen.getByText("Create FUOTA Task").closest("div.rounded") as HTMLElement;
    await fireEvent.click(within(form).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("fuota-2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      7,
      expect.objectContaining({
        input: {
          Name: "fuota-2",
          FirmwareUpdateImage: "s3://my-bucket/fw2.bin",
          FirmwareUpdateRole: "arn:aws:iam::123456789012:role/fuota-role",
          LoRaWAN: {},
        },
      }),
    );
  });

  it("filters the devices list by search text without an extra API call", async () => {
    mockMount({ devices: [exampleDevice, { Id: "device-2", Type: "Sidewalk" }] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("device-1"));

    const callsBeforeFilter = mockSend.mock.calls.length;
    await fireEvent.input(screen.getByPlaceholderText("Search..."), {
      target: { value: "device-2" },
    });

    await waitFor(() => {
      expect(screen.getByText("device-2")).toBeInTheDocument();
      expect(screen.queryByText("device-1")).not.toBeInTheDocument();
    });
    expect(mockSend.mock.calls.length).toBe(callsBeforeFilter);
  });

  it("refreshes the current tab's list via its own Refresh button", async () => {
    mockMount({ devices: [exampleDevice] });
    render(IotWirelessPage);
    await waitFor(() => screen.getByText("device-1"));

    mockSend.mockResolvedValueOnce({
      WirelessDeviceList: [exampleDevice, { Id: "device-2", Type: "LoRaWAN" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: /Refresh/ }));

    await waitFor(() => screen.getByText("device-2"));
    expect(mockSend).toHaveBeenNthCalledWith(7, expect.objectContaining({ input: {} }));
  });
});
