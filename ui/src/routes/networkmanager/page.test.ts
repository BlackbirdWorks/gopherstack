import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import NetworkManagerPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getNetworkManagerClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Network Manager Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getAllByText("AWS Network Manager")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getAllByText("Global Networks")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no networks", async () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no global networks/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded networks", async () => {
    mockSend.mockResolvedValue({
      GlobalNetworks: [
        { GlobalNetworkId: "global-net-123", State: "AVAILABLE", Description: "My network" },
      ],
    });
    render(NetworkManagerPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("global-net-123")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Devices tab", () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getAllByText("Devices")[0]).toBeInTheDocument();
  });

  it("shows Links tab", () => {
    mockSend.mockResolvedValue({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getAllByText("Links")[0]).toBeInTheDocument();
  });
});
