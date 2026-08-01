import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import NetworkManagerPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getNetworkManagerClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleNetwork = {
  GlobalNetworkId: "global-net-123",
  State: "AVAILABLE",
  Description: "My network",
};

describe("Network Manager Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getByText("AWS Network Manager")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No global networks found"));
  });

  it("shows empty state when no networks", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => {
      expect(screen.getByText("No global networks found")).toBeInTheDocument();
    });
  });

  it("lists loaded networks", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [exampleNetwork] });
    render(NetworkManagerPage);
    await waitFor(() => {
      expect(screen.getByText("global-net-123")).toBeInTheDocument();
    });
    expect(screen.getByText("AVAILABLE")).toBeInTheDocument();
  });

  it("does not show Sites/Devices/Links tabs until a network is opened", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [exampleNetwork] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("global-net-123"));

    expect(screen.queryByRole("tab", { name: "Sites" })).not.toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ Sites: [] });
    mockSend.mockResolvedValueOnce({ Devices: [] });
    mockSend.mockResolvedValueOnce({ Links: [] });

    await fireEvent.click(screen.getByTitle("Open"));

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "Sites" })).toBeInTheDocument();
    });
    expect(screen.getByRole("tab", { name: "Devices" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Links" })).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { GlobalNetworkId: "global-net-123" } }),
    );
  });

  it("creates a global network via the modal", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("No global networks found"));

    await fireEvent.click(screen.getByText("Create global network"));
    await fireEvent.input(within(openDialog()).getByLabelText("Description"), {
      target: { value: "My network" },
    });

    mockSend.mockResolvedValueOnce({ GlobalNetwork: exampleNetwork });
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [exampleNetwork] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("global-net-123")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { Description: "My network" } }),
    );
  });

  it("deletes a global network after confirming", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [exampleNetwork] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("global-net-123"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No global networks found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { GlobalNetworkId: "global-net-123" } }),
    );
  });

  it("does not delete a global network when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [exampleNetwork] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("global-net-123"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("global-net-123")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(NetworkManagerPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });
});
