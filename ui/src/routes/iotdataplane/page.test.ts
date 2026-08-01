import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import IotDataPlanePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getIoTDataPlaneClient: () => ({ send: mockSend }),
}));

// iotdataplane never imports confirmDestructive -- deleteShadow, clearRetained,
// and deleteConnection all fire immediately with no confirmation dialog.

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

function jsonResponse(body: unknown, ok = true) {
  return Promise.resolve({ ok, json: () => Promise.resolve(body) });
}

/**
 * `onRegionChange` fires immediately on mount and calls both `listRetained()`
 * (AWS SDK -- ListRetainedMessagesCommand) and `listConnections()` (plain
 * `fetch("/connections")`). Every test needs a `fetch` mock installed before
 * render, even ones that only care about the Publish tab, or the
 * mount-time GET hits the real network.
 */
function installFetch(impl: (url: string, opts?: RequestInit) => Promise<unknown>) {
  vi.stubGlobal("fetch", vi.fn(impl));
}

// The Publish tab button and its own submit button both render the exact
// text "Publish" -- scope to the Publish Message card to disambiguate.
function publishCard(): HTMLElement {
  return screen.getByText("Publish Message").closest("div.space-y-4") as HTMLElement;
}

describe("IoT Data Plane Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    installFetch(() => jsonResponse({ connections: [] }));
  });

  it("renders page title and all tabs", () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    expect(screen.getByText("IoT Data Plane")).toBeInTheDocument();
    // "Publish" also names the (default-active) Publish tab's own submit
    // button -- scope to the tab bar to avoid an ambiguous match.
    const tabBar = screen.getByText("Thing Shadows").closest("div.flex") as HTMLElement;
    expect(within(tabBar).getByText("Publish")).toBeInTheDocument();
    expect(screen.getByText("Thing Shadows")).toBeInTheDocument();
    expect(screen.getByText("Retained Messages")).toBeInTheDocument();
    expect(screen.getByText("Connections")).toBeInTheDocument();
  });

  it("lists retained messages on mount with no pagination token, and fetches connections", async () => {
    mockSend.mockResolvedValueOnce({
      retainedTopics: [{ topic: "device/1/status", qos: 1, payloadSize: 42 }],
    });
    render(IotDataPlanePage);

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    });
    expect(fetch).toHaveBeenCalledWith("/connections");

    await fireEvent.click(screen.getByText("Retained Messages"));
    await waitFor(() => screen.getByText("device/1/status"));
    expect(screen.getByText("QoS 1")).toBeInTheDocument();
    expect(screen.getByText("42B")).toBeInTheDocument();
  });

  it("publishes a message with the exact topic/payload/qos/retain shape", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));

    await fireEvent.input(screen.getByLabelText("Topic *"), {
      target: { value: "my/device/telemetry" },
    });
    await fireEvent.input(screen.getByLabelText("Payload"), {
      target: { value: '{"temp":42}' },
    });
    await fireEvent.click(screen.getByLabelText("Retain message"));

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(within(publishCard()).getByRole("button", { name: "Publish" }));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));
    const call = mockSend.mock.calls[1][0] as {
      input: { topic: string; payload: Uint8Array; qos: number; retain: boolean };
    };
    expect(call.input.topic).toBe("my/device/telemetry");
    expect(call.input.qos).toBe(0);
    expect(call.input.retain).toBe(true);
    expect(new TextDecoder().decode(call.input.payload)).toBe('{"temp":42}');
    await waitFor(() => screen.getByText(/Published at/));
  });

  it("shows the AWS error code in both the toast and the inline result when publishing fails", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));

    await fireEvent.input(screen.getByLabelText("Topic *"), {
      target: { value: "my/device/telemetry" },
    });

    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(within(publishCard()).getByRole("button", { name: "Publish" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ThrottlingException: Rate exceeded."),
      );
    });
    // The same error text also appears in the always-visible Operation
    // History log below -- match the pubResult-specific "Error: " prefix.
    expect(screen.getByText("Error: ThrottlingException: Rate exceeded.")).toBeInTheDocument();
  });

  it("applies a publish template to the payload textarea", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));

    await fireEvent.click(screen.getByText("Alert"));

    expect(screen.getByLabelText("Payload")).toHaveValue(
      '{"level": "WARN", "code": "TEMP_HIGH", "value": 85}',
    );
  });

  it("gets a thing shadow and renders the desired/reported state breakdown", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Thing Shadows"));

    await fireEvent.input(screen.getByLabelText("Thing Name *"), {
      target: { value: "my-device" },
    });

    const shadowDoc = JSON.stringify({
      state: { desired: { power: "on" }, reported: { power: "off" } },
    });
    mockSend.mockResolvedValueOnce({ payload: new TextEncoder().encode(shadowDoc) });
    await fireEvent.click(screen.getByRole("button", { name: "Get" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: { thingName: "my-device", shadowName: undefined },
        }),
      );
    });
    expect(screen.getByText("Desired")).toBeInTheDocument();
    expect(screen.getByText("Reported")).toBeInTheDocument();
  });

  it("updates a thing shadow with the encoded payload and a named shadow", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Thing Shadows"));

    await fireEvent.input(screen.getByLabelText("Thing Name *"), {
      target: { value: "my-device" },
    });
    await fireEvent.input(screen.getByLabelText("Shadow Name (blank = classic)"), {
      target: { value: "config" },
    });
    await fireEvent.input(screen.getByLabelText("Payload (for Update)"), {
      target: { value: '{"state":{"desired":{"power":"on"}}}' },
    });

    mockSend.mockResolvedValueOnce({ payload: new TextEncoder().encode("{}") });
    await fireEvent.click(screen.getByText("Update"));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));
    const call = mockSend.mock.calls[1][0] as {
      input: { thingName: string; shadowName: string; payload: Uint8Array };
    };
    expect(call.input.thingName).toBe("my-device");
    expect(call.input.shadowName).toBe("config");
    expect(new TextDecoder().decode(call.input.payload)).toBe(
      '{"state":{"desired":{"power":"on"}}}',
    );
    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalledWith("Shadow updated");
  });

  it("deletes a thing shadow immediately with no confirmation dialog", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Thing Shadows"));

    await fireEvent.input(screen.getByLabelText("Thing Name *"), {
      target: { value: "my-device" },
    });

    mockSend.mockResolvedValueOnce({ payload: new TextEncoder().encode("{}") });
    await fireEvent.click(screen.getByText("Delete"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: { thingName: "my-device", shadowName: undefined },
        }),
      );
    });
    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalledWith("Shadow deleted");
  });

  it("lists named shadows and offers quick-select buttons", async () => {
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Thing Shadows"));

    await fireEvent.input(screen.getByLabelText("Thing Name *"), {
      target: { value: "my-device" },
    });

    mockSend.mockResolvedValueOnce({ results: ["config", "status"] });
    await fireEvent.click(screen.getByText("List Named"));

    await waitFor(() => {
      expect(screen.getByText("Named shadows: config, status")).toBeInTheDocument();
    });
    expect(screen.getByRole("button", { name: "config" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "status" })).toBeInTheDocument();
  });

  it("browses things with shadows via the admin endpoint and selects one", async () => {
    installFetch((url) => {
      if (url === "/api/things/shadow/ListThingsWithShadows") {
        return jsonResponse({ things: ["thing-a", "thing-b"] });
      }
      return jsonResponse({ connections: [] });
    });
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Thing Shadows"));

    await fireEvent.click(screen.getByText("Browse Things"));

    await waitFor(() => screen.getByText("thing-a"));
    await fireEvent.click(screen.getByText("thing-a"));

    expect(screen.getByLabelText("Thing Name *")).toHaveValue("thing-a");
    expect(screen.queryByText("thing-b")).not.toBeInTheDocument();
  });

  it("views a retained message's payload and clears it immediately with no confirmation dialog", async () => {
    mockSend.mockResolvedValueOnce({
      retainedTopics: [{ topic: "device/1/status", qos: 0, payloadSize: 10 }],
    });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Retained Messages"));
    await waitFor(() => screen.getByText("device/1/status"));

    mockSend.mockResolvedValueOnce({
      topic: "device/1/status",
      qos: 0,
      payload: new TextEncoder().encode('{"on":true}'),
    });
    await fireEvent.click(screen.getByTitle("View payload"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { topic: "device/1/status" } }),
      );
    });
    expect(screen.getByText(/"on": true/)).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    await fireEvent.click(screen.getByTitle("Clear retained message"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        3,
        expect.objectContaining({
          input: expect.objectContaining({ topic: "device/1/status", retain: true }),
        }),
      );
    });
    const clearCall = mockSend.mock.calls[2][0] as { input: { payload: Uint8Array } };
    expect(clearCall.input.payload).toHaveLength(0);
    await waitFor(() => screen.getByText("No retained messages"));
  });

  it("filters retained messages by topic", async () => {
    mockSend.mockResolvedValueOnce({
      retainedTopics: [
        { topic: "device/1/status", qos: 0 },
        { topic: "device/2/status", qos: 0 },
      ],
    });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Retained Messages"));
    await waitFor(() => screen.getByText("device/1/status"));

    await fireEvent.input(screen.getByPlaceholderText("Filter by topic..."), {
      target: { value: "device/2" },
    });

    expect(screen.getByText("device/2/status")).toBeInTheDocument();
    expect(screen.queryByText("device/1/status")).not.toBeInTheDocument();
  });

  it("lists connections via the admin endpoint and registers a new one", async () => {
    const registerFetch = vi.fn((url: string, opts?: RequestInit) => {
      if (url === "/connections" && (!opts || opts.method === undefined)) {
        return jsonResponse({
          connections: [{ clientId: "client-1", sourceIp: "10.0.0.1", connectedAt: 1700000000 }],
        });
      }
      if (url === "/connections/client-2" && opts?.method === "POST") {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
      }
      return jsonResponse({ connections: [] });
    });
    installFetch(registerFetch);
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Connections"));
    await waitFor(() => screen.getByText("client-1"));

    await fireEvent.input(screen.getByPlaceholderText("my-device-client-id"), {
      target: { value: "client-2" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Register" }));

    await waitFor(() => {
      expect(registerFetch).toHaveBeenCalledWith(
        "/connections/client-2",
        expect.objectContaining({ method: "POST" }),
      );
    });
  });

  it("deletes a connection immediately with no confirmation dialog", async () => {
    installFetch(() =>
      jsonResponse({
        connections: [{ clientId: "client-1", sourceIp: "10.0.0.1", connectedAt: 1700000000 }],
      }),
    );
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Connections"));
    await waitFor(() => screen.getByText("client-1"));

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByTitle("Delete connection"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ input: { clientId: "client-1" } }),
      );
    });
  });

  it("shows the AWS error code in the toast when deleting a connection fails", async () => {
    installFetch(() =>
      jsonResponse({
        connections: [{ clientId: "client-1", sourceIp: "10.0.0.1", connectedAt: 1700000000 }],
      }),
    );
    mockSend.mockResolvedValueOnce({ retainedTopics: [] });
    render(IotDataPlanePage);
    await waitFor(() => screen.getByText("Publish Message"));
    await fireEvent.click(screen.getByText("Connections"));
    await waitFor(() => screen.getByText("client-1"));

    const error = Object.assign(new Error("Connection not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByTitle("Delete connection"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ResourceNotFoundException: Connection not found."),
      );
    });
  });
});
