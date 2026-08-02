import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import MgnPage from "./+page.svelte";

// Only the ACTIVE tab's panel is ever mounted (+page.svelte's `{#if}` chain,
// not CSS visibility -- see its own doc comment for why), so at most one
// dialog exists at a time. Still scope to the open one, matching every
// other page.test.ts in this codebase (accessanalyzer/dlm/resiliencehub),
// since a closed <dialog> just loses its `open` attribute rather than
// unmounting.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMGNClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), message: vi.fn() },
}));

const exampleServer = {
  sourceServerID: "s-1234567890abcdef0",
  lifeCycle: { state: "READY_FOR_TEST" },
  dataReplicationInfo: { dataReplicationState: "CONTINUOUS" },
  sourceProperties: { identificationHints: { hostname: "web-01" } },
};

describe("MGN page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    expect(screen.getByText("AWS Application Migration Service")).toBeInTheDocument();
  });

  it("shows both tab groups and defaults to the Source Servers tab", () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    expect(screen.getByText("Replication")).toBeInTheDocument();
    expect(screen.getByText("Network Migration")).toBeInTheDocument();
    expect(screen.getByText("Source Servers")).toBeInTheDocument();
    expect(screen.getByText("Jobs")).toBeInTheDocument();
    expect(screen.getByText("Import / Export")).toBeInTheDocument();
  });

  it("explains that no operation creates a source server directly", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => {
      expect(screen.getByText(/No AWS operation registers a source server/)).toBeInTheDocument();
    });
  });

  it("shows empty state when there are no source servers", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => {
      expect(screen.getByText("No source servers found")).toBeInTheDocument();
    });
  });

  it("lists source servers", async () => {
    mockSend.mockResolvedValueOnce({ items: [exampleServer] });
    render(MgnPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "s-1234567890abcdef0" })).toBeInTheDocument();
    });
  });

  it("deletes a source server after confirming", async () => {
    mockSend.mockResolvedValueOnce({ items: [exampleServer] });
    render(MgnPage);
    await waitFor(() => screen.getByRole("cell", { name: "s-1234567890abcdef0" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ items: [] });

    await fireEvent.click(screen.getByText("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No source servers found")).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error name and message when a load fails", async () => {
    const error = Object.assign(new Error("Account not initialized."), {
      name: "UninitializedAccountException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(MgnPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("UninitializedAccountException (HTTP 400): Account not initialized."),
      ).toBeInTheDocument();
    });
  });

  it("switches to the Applications tab and creates an application via the modal", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => screen.getByText("No source servers found"));

    mockSend.mockResolvedValueOnce({ items: [] });
    await fireEvent.click(screen.getByText("Applications"));
    await waitFor(() => screen.getByText("No applications found"));

    await fireEvent.click(screen.getByText("Create application"));
    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Name"), {
      target: { value: "example-app" },
    });

    mockSend.mockResolvedValueOnce({ applicationID: "app-1", name: "example-app" });
    mockSend.mockResolvedValueOnce({ items: [{ applicationID: "app-1", name: "example-app" }] });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-app" })).toBeInTheDocument();
    });
  });

  it("switches to the Network Migration group and shows its tabs", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => screen.getByText("No source servers found"));

    mockSend.mockResolvedValueOnce({ items: [] });
    await fireEvent.click(screen.getByText("Network Migration"));

    await waitFor(() => {
      expect(screen.getByText("Definitions")).toBeInTheDocument();
      expect(screen.getByText("Mapper Segments")).toBeInTheDocument();
      expect(screen.getByText("Deployment")).toBeInTheDocument();
    });
  });

  it("shows an honest empty/gap explanation on the Mapper Segments tab, not a blank panel", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => screen.getByText("No source servers found"));

    await fireEvent.click(screen.getByText("Network Migration"));
    mockSend.mockResolvedValueOnce({ items: [] });
    await waitFor(() => screen.getByText("Definitions"));

    await fireEvent.click(screen.getByText("Mapper Segments"));

    await waitFor(() => {
      expect(screen.getByText(/deliberately empty in this emulator/)).toBeInTheDocument();
    });
  });

  it("surfaces the documented CSV import schema and the cli.go wiring caveat on Import/Export", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => screen.getByText("No source servers found"));

    mockSend.mockResolvedValueOnce({ items: [] });
    mockSend.mockResolvedValueOnce({ items: [] });
    mockSend.mockResolvedValueOnce({ items: [] });
    await fireEvent.click(screen.getByText("Import / Export"));

    await waitFor(() => {
      expect(screen.getByText("hostname")).toBeInTheDocument();
      expect(screen.getByText(/header row required/)).toBeInTheDocument();
      expect(screen.getByText(/not yet wired into the running server/)).toBeInTheDocument();
    });
  });

  it("calls InitializeService from the Service Init tab", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(MgnPage);
    await waitFor(() => screen.getByText("No source servers found"));

    mockSend.mockResolvedValueOnce({ items: [] });
    await fireEvent.click(screen.getByText("Service Init"));
    await waitFor(() => screen.getByText("Initialize service"));

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByText("Initialize service"));

    await waitFor(() => {
      expect(screen.getByText("Initialized.")).toBeInTheDocument();
    });
  });
});
