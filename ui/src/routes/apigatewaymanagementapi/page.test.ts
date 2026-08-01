import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ApiGwMgmtPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAPIGatewayManagementAPIClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

const defaultStats = {
  activeConnections: 0,
  bufferedMessages: 0,
  totalConnections: 0,
  totalDisconnections: 0,
  totalMessages: 0,
  totalBroadcasts: 0,
  totalBytesSent: 0,
  totalRejected: 0,
};

const exampleConnection = {
  connectionId: "conn-1",
  sourceIp: "203.0.113.5",
  userAgent: "TestAgent/1.0",
  connectedAt: new Date(Date.now() - 120_000).toISOString(),
  lastActiveAt: new Date(Date.now() - 5_000).toISOString(),
  postedMessages: 3,
  bytesSent: 1536,
};

/**
 * The admin endpoints (everything under `/_gopherstack/apigwmgmt`) are
 * plain `fetch`, not the AWS SDK client -- this page mixes both. Routes are
 * matched by HTTP method + URL suffix (each endpoint has a distinct final
 * path segment: /connections, /stats, /messages, /timeline, /ping,
 * /broadcast, /prune), so GET and DELETE on the same "/messages" suffix
 * don't collide.
 */
function installFetch(routes: Array<[string, string, unknown]>, fallback: unknown = {}) {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string, opts?: RequestInit) => {
      const method = (opts?.method ?? "GET").toUpperCase();
      const match = routes.find(([m, suffix]) => m === method && url.endsWith(suffix));
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve(match ? match[2] : fallback),
      });
    }),
  );
}

async function renderWithConnection() {
  installFetch([
    ["GET", "/connections", { connections: [exampleConnection] }],
    ["GET", "/stats", defaultStats],
  ]);
  render(ApiGwMgmtPage);
  await waitFor(() => screen.getByText("conn-1"));
}

// The Broadcast/Prune idle header buttons (which open their modal) and the
// modal's own submit button render the exact same text and coexist once the
// modal is open -- scope to the modal by its heading to disambiguate.
function withinModal(headingText: string) {
  const heading = screen.getByText(headingText);
  return within(heading.closest("div.rounded-2xl") as HTMLElement);
}

async function selectConnection() {
  installFetch([
    ["GET", "/connections", { connections: [exampleConnection] }],
    ["GET", "/stats", defaultStats],
    ["GET", "/connections/conn-1/messages", { messages: [] }],
    ["GET", "/connections/conn-1/timeline", { events: [] }],
  ]);
  await fireEvent.click(screen.getByText("conn-1"));
  await waitFor(() => screen.getByText("IP 203.0.113.5"));
}

describe("API Gateway Management API Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    installFetch([
      ["GET", "/connections", { connections: [] }],
      ["GET", "/stats", defaultStats],
    ]);
  });

  it("renders page title and fetches connections/stats on mount with no pagination params", async () => {
    render(ApiGwMgmtPage);
    expect(screen.getByText("API Gateway Management API")).toBeInTheDocument();

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith("/_gopherstack/apigwmgmt/connections");
    });
    expect(fetch).toHaveBeenCalledWith("/_gopherstack/apigwmgmt/stats");
  });

  it("lists a connection with formatted byte count and message count cells", async () => {
    await renderWithConnection();
    expect(screen.getByText("3 msgs")).toBeInTheDocument();
    expect(screen.getByText("1.5 KiB")).toBeInTheDocument();
    expect(screen.getByText("203.0.113.5")).toBeInTheDocument();
  });

  it("selects a connection and loads its messages and timeline", async () => {
    await renderWithConnection();
    await selectConnection();
    expect(screen.getByText("UA TestAgent/1.0")).toBeInTheDocument();
  });

  it("sends a message with the exact ConnectionId/encoded-Data shape", async () => {
    await renderWithConnection();
    await selectConnection();

    await fireEvent.input(screen.getByLabelText("Send message (PostToConnection)"), {
      target: { value: "hello there" },
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: /Send/ }));

    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
    const call = mockSend.mock.calls[0][0] as {
      input: { ConnectionId: string; Data: Uint8Array };
    };
    expect(call.input.ConnectionId).toBe("conn-1");
    expect(new TextDecoder().decode(call.input.Data)).toBe("hello there");
    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalledWith("Message posted");
  });

  it("shows the AWS error code in the toast when sending a message fails", async () => {
    await renderWithConnection();
    await selectConnection();

    await fireEvent.input(screen.getByLabelText("Send message (PostToConnection)"), {
      target: { value: "hello there" },
    });

    const error = Object.assign(new Error("Connection is gone."), {
      name: "GoneException",
      $metadata: { httpStatusCode: 410 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: /Send/ }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("GoneException: Connection is gone."),
      );
    });
  });

  it("applies a message template to the send textarea", async () => {
    await renderWithConnection();
    await selectConnection();

    await fireEvent.click(screen.getByText("Heartbeat"));

    expect(screen.getByLabelText("Send message (PostToConnection)")).toHaveValue(
      '{"type":"heartbeat","seq":1}',
    );
  });

  it("pings the selected connection with no confirmation dialog", async () => {
    await renderWithConnection();
    await selectConnection();

    installFetch([
      ["GET", "/connections", { connections: [exampleConnection] }],
      ["GET", "/stats", defaultStats],
      ["GET", "/connections/conn-1/messages", { messages: [] }],
      ["GET", "/connections/conn-1/timeline", { events: [] }],
      ["POST", "/connections/conn-1/ping", {}],
    ]);
    // Exact match -- the message-template button "Ping JSON" also matches a
    // /Ping/ regex.
    await fireEvent.click(screen.getByRole("button", { name: "Ping" }));

    expect(confirmDestructive).not.toHaveBeenCalled();
    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        "/_gopherstack/apigwmgmt/connections/conn-1/ping",
        expect.objectContaining({ method: "POST" }),
      );
    });
  });

  it("clears messages after confirming, hitting the DELETE messages endpoint", async () => {
    await renderWithConnection();
    await selectConnection();

    installFetch([
      ["GET", "/connections", { connections: [exampleConnection] }],
      ["GET", "/stats", defaultStats],
      ["DELETE", "/connections/conn-1/messages", {}],
    ]);
    await fireEvent.click(screen.getByRole("button", { name: /Clear msgs/ }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        "/_gopherstack/apigwmgmt/connections/conn-1/messages",
        expect.objectContaining({ method: "DELETE" }),
      );
    });
  });

  it("does not clear messages when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderWithConnection();
    await selectConnection();

    const before = (fetch as ReturnType<typeof vi.fn>).mock.calls.length;
    await fireEvent.click(screen.getByRole("button", { name: /Clear msgs/ }));

    expect(confirmDestructive).toHaveBeenCalled();
    expect((fetch as ReturnType<typeof vi.fn>).mock.calls.length).toBe(before);
  });

  it("disconnects the selected connection with DeleteConnectionCommand after confirming", async () => {
    await renderWithConnection();
    await selectConnection();

    installFetch([
      ["GET", "/connections", { connections: [] }],
      ["GET", "/stats", defaultStats],
    ]);
    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: /Disconnect/ }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ input: { ConnectionId: "conn-1" } }),
      );
    });
  });

  it("does not disconnect when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderWithConnection();
    await selectConnection();

    await fireEvent.click(screen.getByRole("button", { name: /Disconnect/ }));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).not.toHaveBeenCalled();
  });

  it("shows the AWS error code in the toast when disconnecting fails", async () => {
    await renderWithConnection();
    await selectConnection();

    const error = Object.assign(new Error("Connection not found."), {
      name: "GoneException",
      $metadata: { httpStatusCode: 410 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: /Disconnect/ }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("GoneException: Connection not found."),
      );
    });
  });

  it("simulates a new connection via the modal with the exact request body", async () => {
    render(ApiGwMgmtPage);
    await waitFor(() => screen.getByText(/simulate one to get started/));

    await fireEvent.click(screen.getByRole("button", { name: "Simulate" }));
    await fireEvent.input(screen.getByPlaceholderText("Connection ID"), {
      target: { value: "conn-new" },
    });

    let capturedBody: unknown;
    installFetch([]);
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, opts?: RequestInit) => {
        if (url.endsWith("/connections") && opts?.method === "POST") {
          capturedBody = JSON.parse(opts.body as string);
          return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
        }
        if (url.endsWith("/connections")) {
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                connections: [{ ...exampleConnection, connectionId: "conn-new" }],
              }),
          });
        }
        return Promise.resolve({ ok: true, json: () => Promise.resolve(defaultStats) });
      }),
    );

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(capturedBody).toEqual({
        connectionId: "conn-new",
        sourceIp: "127.0.0.1",
        userAgent: "Gopherstack UI",
      });
    });
  });

  it("shows the error message from the response body when simulating a connection fails", async () => {
    render(ApiGwMgmtPage);
    await waitFor(() => screen.getByText(/simulate one to get started/));

    await fireEvent.click(screen.getByRole("button", { name: "Simulate" }));
    await fireEvent.input(screen.getByPlaceholderText("Connection ID"), {
      target: { value: "conn-new" },
    });

    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, opts?: RequestInit) => {
        if (url.endsWith("/connections") && opts?.method === "POST") {
          return Promise.resolve({
            ok: false,
            json: () => Promise.resolve({ message: "Connection ID already in use." }),
          });
        }
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ connections: [] }) });
      }),
    );
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("Connection ID already in use."),
      );
    });
  });

  it("broadcasts a message via the modal and reports the delivered count", async () => {
    render(ApiGwMgmtPage);
    await waitFor(() => screen.getByText(/simulate one to get started/));

    await fireEvent.click(screen.getByRole("button", { name: "Broadcast" }));
    await fireEvent.input(screen.getByPlaceholderText("Payload"), {
      target: { value: "hello everyone" },
    });

    let capturedBody: unknown;
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, opts?: RequestInit) => {
        if (url.endsWith("/broadcast")) {
          capturedBody = JSON.parse(opts?.body as string);
          return Promise.resolve({ ok: true, json: () => Promise.resolve({ delivered: 2 }) });
        }
        if (url.endsWith("/connections")) {
          return Promise.resolve({ ok: true, json: () => Promise.resolve({ connections: [] }) });
        }
        return Promise.resolve({ ok: true, json: () => Promise.resolve(defaultStats) });
      }),
    );

    await fireEvent.click(
      withinModal("Broadcast to all connections").getByRole("button", { name: "Broadcast" }),
    );

    await waitFor(() => {
      expect(capturedBody).toEqual({ data: "hello everyone" });
    });
    const { toast } = await import("svelte-sonner");
    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith("Broadcast delivered to 2 connection(s)");
    });
  });

  it("prunes idle connections via the modal with the exact idleSeconds body", async () => {
    render(ApiGwMgmtPage);
    await waitFor(() => screen.getByText(/simulate one to get started/));

    await fireEvent.click(screen.getByRole("button", { name: "Prune idle" }));
    const secondsInput = screen.getByDisplayValue("60");
    await fireEvent.input(secondsInput, { target: { value: "120" } });

    let capturedBody: unknown;
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, opts?: RequestInit) => {
        if (url.endsWith("/prune")) {
          capturedBody = JSON.parse(opts?.body as string);
          return Promise.resolve({ ok: true, json: () => Promise.resolve({ pruned: ["conn-1"] }) });
        }
        if (url.endsWith("/connections")) {
          return Promise.resolve({ ok: true, json: () => Promise.resolve({ connections: [] }) });
        }
        return Promise.resolve({ ok: true, json: () => Promise.resolve(defaultStats) });
      }),
    );

    await fireEvent.click(
      withinModal("Prune idle connections").getByRole("button", { name: "Prune" }),
    );

    await waitFor(() => {
      expect(capturedBody).toEqual({ idleSeconds: 120 });
    });
    const { toast } = await import("svelte-sonner");
    await waitFor(() => {
      expect(toast.success).toHaveBeenCalledWith("Pruned 1 idle connection(s)");
    });
  });

  it("filters connections by id, IP, or user agent via the search box", async () => {
    const other = { ...exampleConnection, connectionId: "conn-2", sourceIp: "198.51.100.9" };
    installFetch([
      ["GET", "/connections", { connections: [exampleConnection, other] }],
      ["GET", "/stats", defaultStats],
    ]);
    render(ApiGwMgmtPage);
    await waitFor(() => screen.getByText("conn-1"));
    expect(screen.getByText("conn-2")).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Search id, IP, or user-agent"), {
      target: { value: "198.51.100.9" },
    });

    expect(screen.getByText("conn-2")).toBeInTheDocument();
    expect(screen.queryByText("conn-1")).not.toBeInTheDocument();
  });

  it("copies the connection id to the clipboard", async () => {
    Object.assign(navigator, { clipboard: { writeText: vi.fn().mockResolvedValue(null) } });
    await renderWithConnection();
    await selectConnection();

    await fireEvent.click(screen.getByTitle("Copy connection ID"));

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith("conn-1");
    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalledWith("Connection ID copied");
  });
});
