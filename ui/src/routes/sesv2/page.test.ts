import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SesV2Page from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSESv2Client: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

const toastError = vi.fn();
const toastSuccess = vi.fn();
vi.mock("svelte-sonner", () => ({
  toast: {
    success: (...a: unknown[]) => toastSuccess(...a),
    error: (...a: unknown[]) => toastError(...a),
  },
}));

const exampleIdentity = {
  IdentityName: "identity@example.com",
  IdentityType: "EMAIL_ADDRESS",
  SendingEnabled: true,
};

const exampleContactList = {
  ContactListName: "my-newsletter",
};

const exampleContact = {
  EmailAddress: "subscriber@example.com",
  UnsubscribeAll: false,
};

const exampleSuppressed = {
  EmailAddress: "bounced@example.com",
  Reason: "BOUNCE",
};

const exampleTemplate = {
  TemplateName: "welcome-email",
};

describe("SES v2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    expect(screen.getByText("Simple Email Service v2")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    expect(screen.getByText("Identities")).toBeInTheDocument();
    expect(screen.getByText("Contact Lists")).toBeInTheDocument();
    expect(screen.getByText("Suppression")).toBeInTheDocument();
    expect(screen.getByText("Templates")).toBeInTheDocument();
    expect(screen.getByText("Send Email")).toBeInTheDocument();
    expect(screen.getByText("Account")).toBeInTheDocument();
  });

  it("loads identities on mount with an empty ListEmailIdentitiesCommand input", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [exampleIdentity] });
    render(SesV2Page);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "identity@example.com" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    // Rendered by a plain template expression (this page has no snippet-column
    // architecture), but still confirms the Type/Sending cells actually paint
    // real values rather than staying blank.
    expect(screen.getByRole("cell", { name: "EMAIL_ADDRESS" })).toBeInTheDocument();
    expect(screen.getByText("Enabled")).toBeInTheDocument();
  });

  it("creates an identity via the modal", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    await fireEvent.click(screen.getByText("Add Identity"));
    expect(screen.getByText("Add Email Identity")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Email or Domain"), {
      target: { value: "new@example.com" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      EmailIdentities: [
        { IdentityName: "new@example.com", IdentityType: "EMAIL_ADDRESS", SendingEnabled: false },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create Identity" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "new@example.com" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { EmailIdentity: "new@example.com" } }),
    );
  });

  it("deletes an identity after confirming", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [exampleIdentity] });
    render(SesV2Page);
    await waitFor(() => screen.getByRole("cell", { name: exampleIdentity.IdentityName }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });

    await fireEvent.click(screen.getByTitle("Delete identity"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => screen.getByText("No identities found"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { EmailIdentity: exampleIdentity.IdentityName } }),
    );
  });

  it("does not delete an identity when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ EmailIdentities: [exampleIdentity] });
    render(SesV2Page);
    await waitFor(() => screen.getByRole("cell", { name: exampleIdentity.IdentityName }));

    await fireEvent.click(screen.getByTitle("Delete identity"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListEmailIdentities call -- no Delete, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: exampleIdentity.IdentityName })).toBeInTheDocument();
  });

  it("shows the AWS error name via toast when a load fails", async () => {
    // sesv2's catch blocks interpolate the raw caught value (`${e}`), not
    // `err.message` -- Error.prototype.toString() naturally combines
    // err.name and err.message ("ThrottlingException: Rate exceeded."),
    // so the toast still surfaces the AWS error code even without explicit
    // formatting code.
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(SesV2Page);

    await waitFor(() => {
      expect(toastError).toHaveBeenCalledWith(
        "Failed to load identities: ThrottlingException: Rate exceeded.",
      );
    });
  });

  it("switches to the Contact Lists tab and lists contact lists", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ ContactLists: [exampleContactList] });
    await fireEvent.click(screen.getByText("Contact Lists"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-newsletter" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(2, expect.objectContaining({ input: {} }));
  });

  it("creates a contact list via the modal", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ ContactLists: [] });
    await fireEvent.click(screen.getByText("Contact Lists"));
    await waitFor(() => screen.getByText("No contact lists found"));

    await fireEvent.click(screen.getByText("New List"));
    await fireEvent.input(screen.getByLabelText("List Name *"), {
      target: { value: "my-newsletter" },
    });
    await fireEvent.input(screen.getByLabelText("Description"), {
      target: { value: "Weekly digest" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ContactLists: [exampleContactList] });

    await fireEvent.click(screen.getByRole("button", { name: "Create List" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-newsletter" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { ContactListName: "my-newsletter", Description: "Weekly digest" },
      }),
    );
  });

  it("deletes a contact list after confirming", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ ContactLists: [exampleContactList] });
    await fireEvent.click(screen.getByText("Contact Lists"));
    await waitFor(() => screen.getByRole("cell", { name: "my-newsletter" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ContactLists: [] });

    await fireEvent.click(screen.getByTitle("Delete contact list"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => screen.getByText("No contact lists found"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { ContactListName: "my-newsletter" } }),
    );
  });

  it("opens a contact list's members and adds a contact", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ ContactLists: [exampleContactList] });
    await fireEvent.click(screen.getByText("Contact Lists"));
    await waitFor(() => screen.getByRole("cell", { name: "my-newsletter" }));

    mockSend.mockResolvedValueOnce({ Contacts: [] });
    await fireEvent.click(screen.getByTitle("Manage members"));
    await waitFor(() => screen.getByText("No contacts in this list."));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { ContactListName: "my-newsletter" } }),
    );

    await fireEvent.click(screen.getByText("Add Contact"));
    await fireEvent.input(screen.getByLabelText("Email Address *"), {
      target: { value: "subscriber@example.com" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Contacts: [exampleContact] });

    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "subscriber@example.com" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: {
          ContactListName: "my-newsletter",
          EmailAddress: "subscriber@example.com",
          UnsubscribeAll: false,
        },
      }),
    );
  });

  it("edits an existing contact via UpdateContactCommand", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ ContactLists: [exampleContactList] });
    await fireEvent.click(screen.getByText("Contact Lists"));
    await waitFor(() => screen.getByRole("cell", { name: "my-newsletter" }));

    mockSend.mockResolvedValueOnce({ Contacts: [exampleContact] });
    await fireEvent.click(screen.getByTitle("Manage members"));
    await waitFor(() => screen.getByRole("cell", { name: "subscriber@example.com" }));

    await fireEvent.click(screen.getByTitle("Edit contact"));
    expect(screen.getByText("Edit Contact")).toBeInTheDocument();
    await fireEvent.click(screen.getByLabelText("Unsubscribe from all topics"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Contacts: [{ ...exampleContact, UnsubscribeAll: true }] });

    await fireEvent.click(screen.getByRole("button", { name: "Update" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            ContactListName: "my-newsletter",
            EmailAddress: "subscriber@example.com",
            UnsubscribeAll: true,
          },
        }),
      );
    });
  });

  it("removes a contact after confirming", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ ContactLists: [exampleContactList] });
    await fireEvent.click(screen.getByText("Contact Lists"));
    await waitFor(() => screen.getByRole("cell", { name: "my-newsletter" }));

    mockSend.mockResolvedValueOnce({ Contacts: [exampleContact] });
    await fireEvent.click(screen.getByTitle("Manage members"));
    await waitFor(() => screen.getByRole("cell", { name: "subscriber@example.com" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Contacts: [] });

    await fireEvent.click(screen.getByTitle("Remove contact"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => screen.getByText("No contacts in this list."));
  });

  it("switches to the Suppression tab, lists addresses, and adds one with a chosen reason", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ SuppressedDestinationSummaries: [exampleSuppressed] });
    await fireEvent.click(screen.getByText("Suppression"));
    await waitFor(() => screen.getByRole("cell", { name: "bounced@example.com" }));
    expect(screen.getByText("BOUNCE")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Add Address"));
    await fireEvent.input(screen.getByLabelText("Email Address *"), {
      target: { value: "spammy@example.com" },
    });
    await fireEvent.change(screen.getByLabelText("Reason"), { target: { value: "COMPLAINT" } });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      SuppressedDestinationSummaries: [
        exampleSuppressed,
        { EmailAddress: "spammy@example.com", Reason: "COMPLAINT" },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Add to Suppression List" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "spammy@example.com" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { EmailAddress: "spammy@example.com", Reason: "COMPLAINT" },
      }),
    );
  });

  it("removes a suppressed address after confirming", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ SuppressedDestinationSummaries: [exampleSuppressed] });
    await fireEvent.click(screen.getByText("Suppression"));
    await waitFor(() => screen.getByRole("cell", { name: "bounced@example.com" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ SuppressedDestinationSummaries: [] });

    await fireEvent.click(screen.getByTitle("Remove from suppression list"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => screen.getByText("No suppressed addresses"));
  });

  it("creates an email template with subject and body content", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ TemplatesMetadata: [] });
    await fireEvent.click(screen.getByText("Templates"));
    await waitFor(() => screen.getByText("No email templates found"));

    await fireEvent.click(screen.getByText("Create Template"));
    await fireEvent.input(screen.getByLabelText("Template Name *"), {
      target: { value: "welcome-email" },
    });
    await fireEvent.input(screen.getByLabelText("Subject *"), {
      target: { value: "Welcome!" },
    });
    await fireEvent.input(screen.getByLabelText("HTML Body"), {
      target: { value: "<h1>Hi</h1>" },
    });
    await fireEvent.input(screen.getByLabelText("Text Body"), {
      target: { value: "Hi" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ TemplatesMetadata: [exampleTemplate] });

    // "Create Template" also names the tab-bar button that opened this
    // modal, so scope the click to the modal itself.
    const modal = screen
      .getByText("Create Email Template")
      .closest("div.rounded-lg") as HTMLElement;
    await fireEvent.click(within(modal).getByRole("button", { name: "Create Template" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "welcome-email" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          TemplateName: "welcome-email",
          TemplateContent: { Subject: "Welcome!", Html: "<h1>Hi</h1>", Text: "Hi" },
        },
      }),
    );
  });

  it("deletes a template after confirming", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({ TemplatesMetadata: [exampleTemplate] });
    await fireEvent.click(screen.getByText("Templates"));
    await waitFor(() => screen.getByRole("cell", { name: "welcome-email" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ TemplatesMetadata: [] });

    await fireEvent.click(screen.getByTitle("Delete template"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => screen.getByText("No email templates found"));
  });

  it("sends an email with a comma-separated recipient list", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    await fireEvent.click(screen.getByText("Send Email"));
    await fireEvent.input(screen.getByLabelText("From (verified identity)"), {
      target: { value: "noreply@example.com" },
    });
    await fireEvent.input(screen.getByLabelText("To (comma-separated)"), {
      target: { value: "a@example.com, b@example.com" },
    });

    mockSend.mockResolvedValueOnce({ MessageId: "msg-1" });

    // "Send Email" also names the tab button, so scope the click to the
    // form container.
    const form = screen
      .getByLabelText("From (verified identity)")
      .closest("div.max-w-2xl") as HTMLElement;
    await fireEvent.click(within(form).getByRole("button", { name: "Send Email" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: expect.objectContaining({
            FromEmailAddress: "noreply@example.com",
            Destination: { ToAddresses: ["a@example.com", "b@example.com"] },
            Content: {
              Simple: {
                Subject: { Data: "Test from GopherStack SES v2" },
                Body: { Text: { Data: "This is a test email sent via Amazon SES v2." } },
              },
            },
          }),
        }),
      );
    });
  });

  it("loads and renders account info on the Account tab", async () => {
    mockSend.mockResolvedValueOnce({ EmailIdentities: [] });
    render(SesV2Page);
    await waitFor(() => screen.getByText("No identities found"));

    mockSend.mockResolvedValueOnce({
      SendingEnabled: true,
      SendQuota: { Max24HourSend: 50000, MaxSendRate: 14, SentLast24Hours: 120 },
      EnforcementStatus: "HEALTHY",
      ProductionAccessEnabled: true,
    });
    await fireEvent.click(screen.getByText("Account"));

    await waitFor(() => {
      expect(screen.getByText("50000")).toBeInTheDocument();
    });
    expect(screen.getByText("14 msg/sec")).toBeInTheDocument();
    expect(screen.getByText("HEALTHY")).toBeInTheDocument();
    expect(mockSend).toHaveBeenNthCalledWith(2, expect.objectContaining({ input: {} }));
  });
});
