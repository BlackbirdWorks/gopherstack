import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import LightsailPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getLightsailClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe("Lightsail Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    expect(screen.getByText("Lightsail Compute Deck")).toBeInTheDocument();
    expect(screen.getByText("Launch Flare")).toBeInTheDocument();
  });

  it("displays loaded instances", async () => {
    mockSend.mockResolvedValueOnce({
      instances: [
        { name: "my-vps", state: { name: "running" }, blueprintId: "amazon_linux_2023" },
        { name: "staging-server", state: { name: "running" }, blueprintId: "ubuntu_22_04" },
      ],
    });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-vps")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("staging-server")).toBeInTheDocument();
  });

  it("filters instances via search input", async () => {
    mockSend.mockResolvedValueOnce({
      instances: [
        { name: "prod-server", state: { name: "running" }, blueprintId: "amazon_linux_2023" },
        { name: "dev-instance", state: { name: "running" }, blueprintId: "ubuntu_22_04" },
        { name: "test-box", state: { name: "stopped" }, blueprintId: "ubuntu_22_04" },
      ],
    });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    await waitFor(
      () => {
        expect(screen.getByText("prod-server")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search compute flares...");
    await fireEvent.input(searchInput, { target: { value: "dev" } });

    await waitFor(() => {
      expect(screen.queryByText("prod-server")).not.toBeInTheDocument();
    });
    expect(screen.getByText("dev-instance")).toBeInTheDocument();
    expect(screen.queryByText("test-box")).not.toBeInTheDocument();
  });

  it("opens create modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    // There are two "Launch Flare" elements (button + modal title), click the button
    const createButtons = screen.getAllByText("Launch Flare");
    await fireEvent.click(createButtons[0]);

    expect(screen.getByPlaceholderText("e.g. bishop-test-vps")).toBeInTheDocument();
  });

  it("closes create modal on abort click", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    const createButtons = screen.getAllByText("Launch Flare");
    await fireEvent.click(createButtons[0]);

    expect(screen.getByPlaceholderText("e.g. bishop-test-vps")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Abort"));

    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. bishop-test-vps")).not.toBeInTheDocument();
    });
  });

  it("creates an instance via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });
    mockSend.mockResolvedValueOnce({ operations: [] });
    mockSend.mockResolvedValueOnce({
      instances: [
        { name: "new-vps", state: { name: "running" }, blueprintId: "amazon_linux_2023" },
      ],
    });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    const createButtons = screen.getAllByText("Launch Flare");
    await fireEvent.click(createButtons[0]);

    const nameInput = screen.getByPlaceholderText("e.g. bishop-test-vps");
    await fireEvent.input(nameInput, { target: { value: "new-vps" } });

    const form = nameInput.closest("form")!;
    await fireEvent.submit(form);

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(7);
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalled();
  });

  it("shows empty state when no instances exist", async () => {
    mockSend.mockResolvedValueOnce({ instances: [] });
    mockSend.mockResolvedValueOnce({ relationalDatabases: [] });
    mockSend.mockResolvedValueOnce({ staticIps: [] });

    render(LightsailPage);

    await waitFor(
      () => {
        expect(screen.getByText("No active compute flares detected.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));

    render(LightsailPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
