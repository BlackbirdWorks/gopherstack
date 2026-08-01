import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import GrafanaPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
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
  id: "g-0000000001",
  name: "prod-observability",
  status: "ACTIVE",
  grafanaVersion: "10.4",
  endpoint: "https://g-0000000001.grafana-workspace.us-east-1.amazonaws.com",
  created: new Date("2024-01-01T00:00:00Z"),
  modified: new Date("2024-01-02T00:00:00Z"),
  authentication: { providers: ["AWS_SSO"] },
};

describe("Grafana Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    expect(screen.getByText("Amazon Managed Grafana")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No workspaces found"));
  });

  it("shows empty state when no workspaces", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    await waitFor(() => {
      expect(screen.getByText("No workspaces found")).toBeInTheDocument();
    });
  });

  it("lists workspaces and renders the status badge snippet", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => {
      expect(screen.getByText(exampleWorkspace.name)).toBeInTheDocument();
    });
    // Status/Auth columns use {#snippet} render functions -- assert their
    // actual rendered text, not just that the row exists, per the class of
    // bug that shipped a blank-rendering column elsewhere in this app.
    expect(screen.getByText("ACTIVE")).toBeInTheDocument();
    expect(screen.getByText("AWS_SSO")).toBeInTheDocument();
  });

  it("creates a workspace via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [] });
    render(GrafanaPage);
    await waitFor(() => screen.getByText("No workspaces found"));

    await fireEvent.click(screen.getByText("Create workspace"));
    expect(screen.getByText("Create Workspace")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "prod-observability" },
    });

    mockSend.mockResolvedValueOnce({ workspace: exampleWorkspace });
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(exampleWorkspace.name)).toBeInTheDocument();
    });

    const createCall = mockSend.mock.calls[1][0];
    expect(createCall.constructor.name).toBe("CreateWorkspaceCommand");
    expect(createCall.input).toEqual({
      workspaceName: "prod-observability",
      workspaceDescription: undefined,
      accountAccessType: "CURRENT_ACCOUNT",
      permissionType: "CUSTOMER_MANAGED",
      workspaceRoleArn: undefined,
      authenticationProviders: ["AWS_SSO"],
    });
  });

  it("deletes a workspace after confirming", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByText(exampleWorkspace.name));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ workspaces: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No workspaces found")).toBeInTheDocument();
    });

    const deleteCall = mockSend.mock.calls[1][0];
    expect(deleteCall.constructor.name).toBe("DeleteWorkspaceCommand");
    expect(deleteCall.input).toEqual({ workspaceId: exampleWorkspace.id });
  });

  it("does not delete a workspace when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByText(exampleWorkspace.name));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListWorkspaces call -- no DeleteWorkspace, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText(exampleWorkspace.name)).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(GrafanaPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("opens a workspace's detail view and its service accounts", async () => {
    mockSend.mockResolvedValueOnce({ workspaces: [exampleWorkspace] });
    render(GrafanaPage);
    await waitFor(() => screen.getByText(exampleWorkspace.name));

    mockSend.mockResolvedValueOnce({ workspace: exampleWorkspace });
    mockSend.mockResolvedValueOnce({
      serviceAccounts: [{ id: "sa-1", name: "ci-bot", grafanaRole: "EDITOR", isDisabled: "false" }],
      workspaceId: exampleWorkspace.id,
    });

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleWorkspace.endpoint)).toBeInTheDocument();
    });
    expect(screen.getByText("ci-bot · EDITOR")).toBeInTheDocument();

    const describeCall = mockSend.mock.calls[1][0];
    expect(describeCall.constructor.name).toBe("DescribeWorkspaceCommand");
    expect(describeCall.input).toEqual({ workspaceId: exampleWorkspace.id });

    const listSaCall = mockSend.mock.calls[2][0];
    expect(listSaCall.constructor.name).toBe("ListWorkspaceServiceAccountsCommand");
    expect(listSaCall.input).toEqual({ workspaceId: exampleWorkspace.id });
  });
});
