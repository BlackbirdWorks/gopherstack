import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DirectConnectPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDirectConnectClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleConnection = {
  connectionId: "dxcon-1",
  connectionName: "my-dx",
  bandwidth: "1Gbps",
  location: "EqDC2",
  connectionState: "available",
};

describe("Direct Connect Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getByText("AWS Direct Connect")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No connections found"));
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => {
      expect(screen.getByText("No connections found")).toBeInTheDocument();
    });
  });

  it("lists connections", async () => {
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    render(DirectConnectPage);
    await waitFor(() => {
      expect(screen.getByText("my-dx")).toBeInTheDocument();
    });
    expect(screen.getByText("available")).toBeInTheDocument();
  });

  it("shows Virtual Interfaces, Gateways and LAGs tabs", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));
    expect(screen.getByRole("tab", { name: "Virtual Interfaces" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Gateways" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "LAGs" })).toBeInTheDocument();
  });

  it("creates a connection via the modal", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    await fireEvent.click(screen.getByText("Create connection"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-dx" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Location"), {
      target: { value: "EqDC2" },
    });

    mockSend.mockResolvedValueOnce({ connection: { connectionId: "dxcon-1" } });
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-dx")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { connectionName: "my-dx", location: "EqDC2", bandwidth: "1Gbps" },
      }),
    );
  });

  it("deletes a connection after confirming", async () => {
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("my-dx"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ connections: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No connections found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { connectionId: "dxcon-1" } }),
    );
  });

  it("does not delete a connection when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("my-dx"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-dx")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Connection not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DirectConnectPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Connection not found."),
      ).toBeInTheDocument();
    });
  });
});
