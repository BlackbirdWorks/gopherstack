import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import DirectConnectPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDirectConnectClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Direct Connect Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({
      connections: [],
      virtualInterfaces: [],
      directConnectGateways: [],
    });
    render(DirectConnectPage);
    expect(screen.getAllByText("AWS Direct Connect")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getAllByText("Connections")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({
      connections: [],
      virtualInterfaces: [],
      directConnectGateways: [],
    });
    render(DirectConnectPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no connections/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded connections", async () => {
    mockSend.mockResolvedValue({
      connections: [
        {
          connectionName: "my-dx",
          bandwidth: "1Gbps",
          location: "EqDC2",
          connectionState: "available",
        },
      ],
      virtualInterfaces: [],
      directConnectGateways: [],
    });
    render(DirectConnectPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-dx")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Virtual Interfaces tab", () => {
    mockSend.mockResolvedValue({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getAllByText("Virtual Interfaces")[0]).toBeInTheDocument();
  });

  it("shows Gateways tab", () => {
    mockSend.mockResolvedValue({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getAllByText("Gateways")[0]).toBeInTheDocument();
  });
});
