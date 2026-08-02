import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import WorkMailPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getWorkMailClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleOrg = {
  OrganizationId: "m-org123",
  Alias: "my-org",
  DefaultMailDomain: "example.com",
  State: "Active",
};

describe("WorkMail Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [] });
    render(WorkMailPage);
    expect(screen.getByText("Amazon WorkMail")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No organizations found"));
  });

  it("shows empty state when no organizations", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [] });
    render(WorkMailPage);
    await waitFor(() => {
      expect(screen.getByText("No organizations found")).toBeInTheDocument();
    });
  });

  it("lists loaded organizations and renders the State column via its render snippet", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [exampleOrg] });
    render(WorkMailPage);
    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-org")).toBeInTheDocument();
    });
    expect(within(screen.getByRole("table")).getByText("Active")).toBeInTheDocument();
  });

  it("creates an organization via the modal", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [] });
    render(WorkMailPage);
    await waitFor(() => screen.getByText("No organizations found"));

    await fireEvent.click(screen.getByText("Create organization"));
    await fireEvent.input(within(openDialog()).getByLabelText("Alias"), {
      target: { value: "new-org" },
    });

    mockSend.mockResolvedValueOnce({ OrganizationId: "m-org456" });
    mockSend.mockResolvedValueOnce({
      OrganizationSummaries: [{ OrganizationId: "m-org456", Alias: "new-org" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("new-org")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { Alias: "new-org", Domains: undefined } }),
    );
  });

  it("deletes an organization after confirming", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [exampleOrg] });
    render(WorkMailPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-org"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No organizations found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { OrganizationId: "m-org123", DeleteDirectory: false } }),
    );
  });

  it("does not delete an organization when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [exampleOrg] });
    render(WorkMailPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-org"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-org")).toBeInTheDocument();
  });

  it("lists users for a selected organization", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [exampleOrg] });
    render(WorkMailPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-org"));

    mockSend.mockResolvedValueOnce({
      Users: [
        {
          Id: "u-1",
          Name: "alice",
          Email: "alice@example.com",
          UserRole: "USER",
          State: "ENABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByTitle("Manage users"));

    await waitFor(() => {
      expect(screen.getByText("alice")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { OrganizationId: "m-org123" } }),
    );
  });

  it("creates a user within the selected organization", async () => {
    mockSend.mockResolvedValueOnce({ OrganizationSummaries: [exampleOrg] });
    render(WorkMailPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-org"));

    mockSend.mockResolvedValueOnce({ Users: [] });
    await fireEvent.click(screen.getByTitle("Manage users"));
    await waitFor(() => screen.getByText("No users found in this organization"));

    await fireEvent.click(screen.getByText("Create user"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "bob" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Display Name"), {
      target: { value: "Bob Smith" },
    });

    mockSend.mockResolvedValueOnce({ UserId: "u-2" });
    mockSend.mockResolvedValueOnce({
      Users: [{ Id: "u-2", Name: "bob", DisplayName: "Bob Smith", State: "DISABLED" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("bob")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { OrganizationId: "m-org123", Name: "bob", DisplayName: "Bob Smith", Role: "USER" },
      }),
    );
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(WorkMailPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });
});
