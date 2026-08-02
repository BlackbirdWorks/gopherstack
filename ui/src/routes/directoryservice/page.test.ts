import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import DirectoryServicePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDirectoryServiceClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleDirectory = {
  DirectoryId: "d-1234567890",
  Name: "corp.example.com",
  ShortName: "CORP",
  Type: "SimpleAD",
  Stage: "Active",
  Size: "Small",
};

const exampleSnapshot = {
  SnapshotId: "s-1234567890",
  DirectoryId: "d-1234567890",
  Name: "example-snapshot",
  Type: "Manual",
  Status: "Completed",
};

describe("Directory Service Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [] });
    render(DirectoryServicePage);
    expect(screen.getByText("AWS Directory Service")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [] });
    render(DirectoryServicePage);
    expect(screen.getByText("Directories")).toBeInTheDocument();
    expect(screen.getByText("Snapshots")).toBeInTheDocument();
    expect(screen.getByText("Trusts")).toBeInTheDocument();
    expect(screen.getByText("Conditional Forwarders")).toBeInTheDocument();
    expect(screen.getByText("Log Subscriptions")).toBeInTheDocument();
    expect(screen.getByText("IP Routes")).toBeInTheDocument();
    expect(screen.getByText("Schema Extensions")).toBeInTheDocument();
    expect(screen.getByText("Certificates")).toBeInTheDocument();
    expect(screen.getByText("Event Topics")).toBeInTheDocument();
    expect(screen.getByText("Domain Controllers")).toBeInTheDocument();
    expect(screen.getByText("Regions")).toBeInTheDocument();
    expect(screen.getByText("Shared Directories")).toBeInTheDocument();
    expect(screen.getByText("AD Assessments")).toBeInTheDocument();
    expect(screen.getByText("Settings")).toBeInTheDocument();
  });

  it("lists directories", async () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [exampleDirectory] });
    render(DirectoryServicePage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "corp.example.com" })).toBeInTheDocument();
    });
  });

  it("creates a Simple AD directory via the modal", async () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [] });
    render(DirectoryServicePage);
    // Synchronize on the tab bar (not the empty-state text, which can race
    // the still-in-flight initial DescribeDirectories call).
    await waitFor(() => screen.getByText("Directories"));

    await fireEvent.click(screen.getByText("Create directory"));
    expect(screen.getByText("Create Directory")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Fully qualified name"), {
      target: { value: "corp.example.com" },
    });
    await fireEvent.input(screen.getByLabelText("Administrator password"), {
      target: { value: "SuperSecretPassw0rd!" },
    });

    mockSend.mockResolvedValueOnce({ DirectoryId: exampleDirectory.DirectoryId });
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [exampleDirectory] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "corp.example.com" })).toBeInTheDocument();
    });
    // DescribeDirectories (initial) + CreateDirectory + DescribeDirectories (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a directory after confirming", async () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [exampleDirectory] });
    render(DirectoryServicePage);
    await waitFor(() => screen.getByRole("cell", { name: "corp.example.com" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No directories found")).toBeInTheDocument();
    });
  });

  it("does not delete a directory when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [exampleDirectory] });
    render(DirectoryServicePage);
    await waitFor(() => screen.getByRole("cell", { name: "corp.example.com" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial DescribeDirectories call -- no DeleteDirectory, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "corp.example.com" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Directory not found."), {
      name: "EntityDoesNotExistException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DirectoryServicePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("EntityDoesNotExistException (HTTP 400): Directory not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a directory's detail view", async () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [exampleDirectory] });
    render(DirectoryServicePage);
    await waitFor(() => screen.getByRole("cell", { name: "corp.example.com" }));

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(exampleDirectory.DirectoryId).length).toBeGreaterThan(0);
    });
  });

  it("switches to the Snapshots tab, scoped to the auto-selected directory", async () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [exampleDirectory] });
    render(DirectoryServicePage);
    await waitFor(() => screen.getByRole("cell", { name: "corp.example.com" }));

    mockSend.mockResolvedValueOnce({ Snapshots: [exampleSnapshot] });
    await fireEvent.click(screen.getByText("Snapshots"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-snapshot" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenLastCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({ DirectoryId: exampleDirectory.DirectoryId }),
      }),
    );
  });

  it("shows a directory-selection prompt for strictly-scoped tabs with no directory", async () => {
    mockSend.mockResolvedValueOnce({ DirectoryDescriptions: [] });
    render(DirectoryServicePage);
    await waitFor(() => screen.getByText("Directories"));

    mockSend.mockResolvedValueOnce({ IpRoutesInfo: [] });
    await fireEvent.click(screen.getByText("IP Routes"));

    await waitFor(() => {
      expect(screen.getByText("Select a directory to see its IP routes")).toBeInTheDocument();
    });
  });
});
