import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import GrafanaPage from "./+page.svelte";

// Every Modal on this page stays mounted in the DOM once rendered -- closed
// <dialog> elements just lose the `open` attribute rather than unmounting --
// so getByLabelText/getByRole across the whole document is ambiguous once
// more than one modal shares a label (e.g. "Name") or a footer button (e.g.
// "Create"). Scope to the dialog that is actually open, same pattern as
// accessanalyzer/page.test.ts and dlm/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getGrafanaClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleWorkspace = {
  id: "g-abc12345",
  name: "example",
  status: "ACTIVE",
  grafanaVersion: "10.4",
  endpoint: "https://g-abc12345.grafana-workspace.us-east-1.amazonaws.com",
  created: new Date("2024-01-01T00:00:00Z"),
  modified: new Date("2024-01-01T00:00:00Z"),
  authentication: { providers: ["AWS_SSO"] },
};

describe("Grafana Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    expect(screen.getByText("Amazon Managed Grafana")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    expect(screen.getByText("Workspaces")).toBeInTheDocument();
    expect(screen.getByText("Service Accounts")).toBeInTheDocument();
    expect(screen.getByText("Tokens")).toBeInTheDocument();
    expect(screen.getByText("Permissions")).toBeInTheDocument();
    expect(screen.getByText("Versions")).toBeInTheDocument();
  });

  it("shows empty state when no workspaces", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    await waitFor(() => {
      expect(screen.getByText("No workspaces found")).toBeInTheDocument();
    });
  });

  it("lists workspaces", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
    });
  });

  it("creates a workspace via the modal", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    await waitFor(() => screen.getByText("No workspaces found"));

    await fireEvent.click(screen.getByText("Create workspace"));
    expect(screen.getByText("Create Workspace")).toBeInTheDocument();

    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Name"), {
      target: { value: "example" },
    });

    mockSend.mockResolvedValueOnce({ workspace: exampleWorkspace });
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a workspace after confirming", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ workspaces: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No workspaces found")).toBeInTheDocument();
    });
  });

  it("does not delete a workspace when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListWorkspaces call -- no DeleteWorkspace, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Workspace not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(GrafanaPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Workspace not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a workspace's detail view", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({ workspace: exampleWorkspace });
    mockSend.mockResolvedValueOnce({ tags: {} });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText("g-abc12345").length).toBeGreaterThan(0);
    });
  });

  it("switches to the service accounts tab and loads accounts for the selected workspace", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({
      serviceAccounts: [
        { id: "sa-1", name: "my-service-account", isDisabled: "false", grafanaRole: "VIEWER" },
      ],
      workspaceId: exampleWorkspace.id,
    });
    await fireEvent.click(screen.getByText("Service Accounts"));

    await waitFor(() => {
      expect(screen.getByText("my-service-account")).toBeInTheDocument();
    });
  });
});
