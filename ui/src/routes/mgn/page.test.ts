import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MGNPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getMGNClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleServer = {
  sourceServerID: "s-abc123",
  dataReplicationInfo: { dataReplicationState: "CONTINUOUS" },
  lifeCycle: { state: "READY_FOR_TEST" },
  applicationID: "app-001",
};

const exampleApp = {
  applicationID: "app-001",
  name: "my-app",
  description: "An app",
  isArchived: false,
};

const exampleWave = {
  waveID: "wave-001",
  name: "my-wave",
  description: "A wave",
  isArchived: false,
};

describe("MGN Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MGNPage);
    expect(screen.getByText("AWS Application Migration Service")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No source servers found"));
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MGNPage);
    await waitFor(() => {
      expect(screen.getByText("No source servers found")).toBeInTheDocument();
    });
  });

  it("lists source servers with lifecycle state", async () => {
    mockSend.mockResolvedValueOnce({ items: [exampleServer] });
    render(MGNPage);
    await waitFor(() => {
      expect(screen.getByText("s-abc123")).toBeInTheDocument();
    });
    expect(screen.getByText("READY_FOR_TEST")).toBeInTheDocument();
  });

  it("shows Applications and Waves tabs", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MGNPage);
    await waitFor(() => screen.getByText("No source servers found"));
    expect(screen.getByRole("tab", { name: "Applications" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Waves" })).toBeInTheDocument();
  });

  it("creates an application via the modal", async () => {
    // initial servers load
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MGNPage);
    await waitFor(() => screen.getByText("No source servers found"));

    // applications load
    mockSend.mockResolvedValueOnce({ items: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Applications" }));
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create application"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-app" },
    });

    mockSend.mockResolvedValueOnce({ applicationID: "app-001" });
    mockSend.mockResolvedValueOnce({ items: [exampleApp] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-app")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { name: "my-app", description: undefined } }),
    );
  });

  it("deletes a wave after confirming", async () => {
    // servers
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MGNPage);
    await waitFor(() => screen.getByText("No source servers found"));

    // waves
    mockSend.mockResolvedValueOnce({ items: [exampleWave] });
    await fireEvent.click(screen.getByRole("tab", { name: "Waves" }));
    await waitFor(() => screen.getByText("my-wave"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ items: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No waves found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { waveID: "wave-001" } }),
    );
  });

  it("does not delete a wave when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    // servers
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MGNPage);
    await waitFor(() => screen.getByText("No source servers found"));

    mockSend.mockResolvedValueOnce({ items: [exampleWave] });
    await fireEvent.click(screen.getByRole("tab", { name: "Waves" }));
    await waitFor(() => screen.getByText("my-wave"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the two loads (servers, waves) -- no DeleteWave call.
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(screen.getByText("my-wave")).toBeInTheDocument();
  });

  it("opens a source server's detail view and starts a test", async () => {
    mockSend.mockResolvedValueOnce({ items: [exampleServer] });
    render(MGNPage);
    await waitFor(() => screen.getByText("s-abc123"));

    await fireEvent.click(screen.getByTitle("View"));
    expect(screen.getByText("Start test")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ job: { jobID: "job-1" } });
    mockSend.mockResolvedValueOnce({ items: [exampleServer] });

    await fireEvent.click(screen.getByText("Start test"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({ input: { sourceServerIDs: ["s-abc123"] } }),
      );
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(MGNPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });
});
