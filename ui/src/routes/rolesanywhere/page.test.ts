import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import RolesAnywherePage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getRolesAnywhereClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleAnchor = {
  trustAnchorId: "anchor-0000000000000001",
  trustAnchorArn:
    "arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/anchor-0000000000000001",
  name: "my-anchor",
  enabled: true,
  source: { sourceType: "CERTIFICATE_BUNDLE" },
  createdAt: new Date("2024-01-01T00:00:00Z"),
  updatedAt: new Date("2024-01-02T00:00:00Z"),
};

describe("Roles Anywhere Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [] });
    render(RolesAnywherePage);
    expect(screen.getByText("IAM Roles Anywhere")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No trust anchors found"));
  });

  it("shows empty state when no trust anchors", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [] });
    render(RolesAnywherePage);
    await waitFor(() => {
      expect(screen.getByText("No trust anchors found")).toBeInTheDocument();
    });
  });

  it("lists trust anchors", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [exampleAnchor] });
    render(RolesAnywherePage);
    await waitFor(() => {
      expect(screen.getByText("my-anchor")).toBeInTheDocument();
    });
    expect(screen.getByText("anchor-0000000000000001")).toBeInTheDocument();
  });

  it("creates a trust anchor via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("No trust anchors found"));

    await fireEvent.click(screen.getByText("Create trust anchor"));
    expect(screen.getByText("Create Trust Anchor")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-anchor" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Certificate data (PEM)"), {
      target: { value: "-----BEGIN CERTIFICATE-----abc-----END CERTIFICATE-----" },
    });

    mockSend.mockResolvedValueOnce({ trustAnchor: exampleAnchor });
    mockSend.mockResolvedValueOnce({ trustAnchors: [exampleAnchor] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-anchor")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      name: "my-anchor",
      enabled: true,
      source: {
        sourceType: "CERTIFICATE_BUNDLE",
        sourceData: {
          x509CertificateData: "-----BEGIN CERTIFICATE-----abc-----END CERTIFICATE-----",
        },
      },
    });
  });

  it("deletes a trust anchor after confirming", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [exampleAnchor] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("my-anchor"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ trustAnchors: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No trust anchors found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({ trustAnchorId: "anchor-0000000000000001" });
  });

  it("does not delete a trust anchor when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ trustAnchors: [exampleAnchor] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("my-anchor"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-anchor")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Trust anchor not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(RolesAnywherePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Trust anchor not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a trust anchor's detail view", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [exampleAnchor] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("my-anchor"));

    mockSend.mockResolvedValueOnce({ trustAnchor: exampleAnchor });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleAnchor.trustAnchorArn)).toBeInTheDocument();
    });
    expect(within(openDialog()).getByText("CERTIFICATE_BUNDLE")).toBeInTheDocument();
  });

  it("disables an enabled trust anchor from the list", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [exampleAnchor] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("my-anchor"));

    mockSend.mockResolvedValueOnce({ trustAnchor: { ...exampleAnchor, enabled: false } });
    mockSend.mockResolvedValueOnce({ trustAnchors: [{ ...exampleAnchor, enabled: false }] });

    await fireEvent.click(screen.getByTitle("Disable"));

    await waitFor(() => {
      expect(mockSend.mock.calls[1][0].input).toEqual({ trustAnchorId: "anchor-0000000000000001" });
    });
    expect(screen.getByText("Disabled")).toBeInTheDocument();
  });

  it("switches to the Profiles tab and creates a profile", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("No trust anchors found"));

    mockSend.mockResolvedValueOnce({ profiles: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Profiles" }));
    await waitFor(() => screen.getByText("No profiles found"));

    await fireEvent.click(screen.getByText("Create profile"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-profile" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Role ARNs (comma-separated)"), {
      target: { value: "arn:aws:iam::123456789012:role/MyRole" },
    });

    const exampleProfile = {
      profileId: "profile-0000000000000001",
      profileArn: "arn:aws:rolesanywhere:us-east-1:123456789012:profile/profile-0000000000000001",
      name: "my-profile",
      roleArns: ["arn:aws:iam::123456789012:role/MyRole"],
      enabled: true,
    };
    mockSend.mockResolvedValueOnce({ profile: exampleProfile });
    mockSend.mockResolvedValueOnce({ profiles: [exampleProfile] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-profile")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[2][0].input).toEqual({
      name: "my-profile",
      roleArns: ["arn:aws:iam::123456789012:role/MyRole"],
      durationSeconds: 3600,
      enabled: true,
    });
  });

  it("shows the read-only note and no create button on the Subjects tab", async () => {
    mockSend.mockResolvedValueOnce({ trustAnchors: [] });
    render(RolesAnywherePage);
    await waitFor(() => screen.getByText("No trust anchors found"));

    mockSend.mockResolvedValueOnce({ subjects: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Subjects" }));

    await waitFor(() => {
      expect(
        screen.getByText(/populated automatically when a certificate authenticates/),
      ).toBeInTheDocument();
    });
    expect(screen.queryByText(/Create subject/i)).not.toBeInTheDocument();
  });
});
