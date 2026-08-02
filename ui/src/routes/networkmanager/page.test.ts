import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import NetworkManagerPage from "./+page.svelte";

// Only the ACTIVE tab's panel is ever mounted (+page.svelte's `{#if}` chain,
// not CSS visibility -- see its own doc comment), so at most one dialog
// exists at a time. Still scope to the open one, matching every other
// page.test.ts in this codebase, since a closed <dialog> just loses its
// `open` attribute rather than unmounting.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getNetworkManagerClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), message: vi.fn() },
}));

describe("Network Manager page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getByText("AWS Network Manager")).toBeInTheDocument();
  });

  it("shows both tab groups and defaults to the Global Networks tab", () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    expect(screen.getByText("Cloud WAN")).toBeInTheDocument();
    expect(screen.getAllByText("Global Networks").length).toBeGreaterThan(0);
    expect(screen.getByText("Sites")).toBeInTheDocument();
    expect(screen.getByText("Associations")).toBeInTheDocument();
  });

  it("shows empty state when there are no global networks", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => {
      expect(screen.getByText("No global networks found")).toBeInTheDocument();
    });
  });

  it("lists global networks", async () => {
    mockSend.mockResolvedValueOnce({
      GlobalNetworks: [{ GlobalNetworkId: "global-network-1234567890abcdef0", State: "AVAILABLE" }],
    });
    render(NetworkManagerPage);
    await waitFor(() => {
      expect(
        screen.getByRole("cell", { name: "global-network-1234567890abcdef0" }),
      ).toBeInTheDocument();
    });
  });

  it("creates a global network via the modal", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("No global networks found"));

    await fireEvent.click(screen.getByRole("button", { name: "Create global network" }));
    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Description"), {
      target: { value: "example global network" },
    });

    mockSend.mockResolvedValueOnce({ GlobalNetwork: { GlobalNetworkId: "gn-1" } });
    mockSend.mockResolvedValueOnce({
      GlobalNetworks: [
        { GlobalNetworkId: "gn-1", Description: "example global network", State: "PENDING" },
      ],
    });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "gn-1" })).toBeInTheDocument();
    });
  });

  it("deletes a global network after confirming", async () => {
    mockSend.mockResolvedValueOnce({
      GlobalNetworks: [{ GlobalNetworkId: "gn-1", State: "AVAILABLE" }],
    });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByRole("cell", { name: "gn-1" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });

    await fireEvent.click(screen.getByText("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No global networks found")).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error name and message when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(NetworkManagerPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("switches to the Sites tab and lists sites scoped to the picked global network", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("No global networks found"));

    // Sites tab mounts GlobalNetworkSelect (its own DescribeGlobalNetworks
    // call) plus SitesPanel's own GetSites call, once a global network is
    // picked.
    mockSend.mockResolvedValueOnce({
      GlobalNetworks: [{ GlobalNetworkId: "gn-1", Description: "primary" }],
    });
    mockSend.mockResolvedValueOnce({
      Sites: [{ SiteId: "site-1", Description: "hq" }],
    });
    await fireEvent.click(screen.getByText("Sites"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "site-1" })).toBeInTheDocument();
    });
  });

  it("switches to the Cloud WAN group and shows its tabs", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("No global networks found"));

    mockSend.mockResolvedValueOnce({ CoreNetworks: [] });
    await fireEvent.click(screen.getByText("Cloud WAN"));

    await waitFor(() => {
      expect(screen.getByText("Core Networks")).toBeInTheDocument();
      expect(screen.getByText("Route Analysis")).toBeInTheDocument();
      expect(screen.getByText("Network Insights")).toBeInTheDocument();
    });
  });

  it("shows the honest route-analysis gap disclosure, never implying a real path was computed", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("No global networks found"));

    mockSend.mockResolvedValueOnce({ CoreNetworks: [] });
    await fireEvent.click(screen.getByText("Cloud WAN"));
    await waitFor(() => screen.getByText("Core Networks"));

    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    await fireEvent.click(screen.getByText("Route Analysis"));

    await waitFor(() => {
      expect(screen.getByText(/no cross-service walk into EC2's/)).toBeInTheDocument();
      expect(screen.getByText(/gopherstack-700y/)).toBeInTheDocument();
    });
  });

  it("labels the Network Insights routes tab as honestly empty, not a real propagation engine", async () => {
    mockSend.mockResolvedValueOnce({ GlobalNetworks: [] });
    render(NetworkManagerPage);
    await waitFor(() => screen.getByText("No global networks found"));

    mockSend.mockResolvedValueOnce({ CoreNetworks: [] });
    await fireEvent.click(screen.getByText("Cloud WAN"));
    await waitFor(() => screen.getByText("Core Networks"));

    mockSend.mockResolvedValueOnce({ GlobalNetworks: [{ GlobalNetworkId: "gn-1" }] });
    mockSend.mockResolvedValueOnce({ NetworkResources: [] });
    await fireEvent.click(screen.getByText("Network Insights"));
    await waitFor(() => screen.getByText("No network resources found"));

    await fireEvent.click(screen.getByRole("tab", { name: "Routes" }));

    await waitFor(() => {
      expect(screen.getByText(/no route-propagation engine/)).toBeInTheDocument();
    });
  });
});
