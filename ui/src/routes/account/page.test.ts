import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import AccountPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getAccountClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const notFound = Object.assign(new Error("not found"), { name: "ResourceNotFoundException" });

describe("Account Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockRejectedValueOnce(notFound);
    render(AccountPage);
    expect(screen.getByText("AWS Account")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No contact information set"));
  });

  it("shows empty state when no contact information is set", async () => {
    mockSend.mockRejectedValueOnce(notFound);
    render(AccountPage);
    await waitFor(() => {
      expect(screen.getByText("No contact information set")).toBeInTheDocument();
    });
  });

  it("displays loaded contact information", async () => {
    mockSend.mockResolvedValueOnce({
      ContactInformation: {
        FullName: "Jane Doe",
        AddressLine1: "123 Main St",
        City: "Seattle",
        CountryCode: "US",
        PhoneNumber: "+12065551234",
        PostalCode: "98101",
      },
    });
    render(AccountPage);
    await waitFor(() => {
      expect(screen.getByText("Jane Doe")).toBeInTheDocument();
    });
    expect(screen.getByText("Seattle")).toBeInTheDocument();
  });

  it("saves contact information via the modal with exact command input", async () => {
    mockSend.mockRejectedValueOnce(notFound);
    render(AccountPage);
    await waitFor(() => screen.getByText("No contact information set"));

    await fireEvent.click(screen.getByRole("button", { name: "Set contact info" }));
    expect(screen.getByText("Set Contact Information")).toBeInTheDocument();

    const dialog = openDialog();
    await fireEvent.input(within(dialog).getByLabelText("Full name"), {
      target: { value: "Jane Doe" },
    });
    await fireEvent.input(within(dialog).getByLabelText("Address line 1"), {
      target: { value: "123 Main St" },
    });
    await fireEvent.input(within(dialog).getByLabelText("City"), { target: { value: "Seattle" } });
    await fireEvent.input(within(dialog).getByLabelText("Postal code"), {
      target: { value: "98101" },
    });
    await fireEvent.input(within(dialog).getByLabelText("Country code"), {
      target: { value: "US" },
    });
    await fireEvent.input(within(dialog).getByLabelText("Phone number"), {
      target: { value: "+12065551234" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ContactInformation: {
        FullName: "Jane Doe",
        AddressLine1: "123 Main St",
        City: "Seattle",
        CountryCode: "US",
        PhoneNumber: "+12065551234",
        PostalCode: "98101",
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(screen.getByText("Jane Doe")).toBeInTheDocument();
    });

    const putCall = mockSend.mock.calls[1][0];
    expect(putCall.constructor.name).toBe("PutContactInformationCommand");
    expect(putCall.input).toEqual({
      ContactInformation: {
        FullName: "Jane Doe",
        AddressLine1: "123 Main St",
        City: "Seattle",
        CountryCode: "US",
        PhoneNumber: "+12065551234",
        PostalCode: "98101",
        CompanyName: undefined,
        StateOrRegion: undefined,
      },
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "TooManyRequestsException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(AccountPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("TooManyRequestsException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("switches to Alternate Contacts, deletes an existing contact after confirming", async () => {
    mockSend.mockRejectedValueOnce(notFound);
    render(AccountPage);
    await waitFor(() => screen.getByText("No contact information set"));

    mockSend.mockResolvedValueOnce({
      AlternateContact: {
        Name: "Billing Bob",
        EmailAddress: "billing@example.com",
        Title: "Manager",
        PhoneNumber: "+1",
        AlternateContactType: "BILLING",
      },
    });
    mockSend.mockRejectedValueOnce(notFound);
    mockSend.mockRejectedValueOnce(notFound);

    await fireEvent.click(screen.getByRole("tab", { name: "Alternate Contacts" }));

    await waitFor(() => {
      expect(screen.getByText("Billing Bob (billing@example.com)")).toBeInTheDocument();
    });
    // OPERATIONS and SECURITY are both unset, so "(not set)" appears twice.
    expect(screen.getAllByText("(not set)").length).toBe(2);

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      AlternateContact: {
        Name: "Billing Bob",
        EmailAddress: "billing@example.com",
        Title: "Manager",
        PhoneNumber: "+1",
        AlternateContactType: "BILLING",
      },
    });
    mockSend.mockRejectedValueOnce(notFound);
    mockSend.mockRejectedValueOnce(notFound);

    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();

    await waitFor(() => {
      const deleteCall = mockSend.mock.calls.find(
        (c) => c[0].constructor.name === "DeleteAlternateContactCommand",
      );
      expect(deleteCall?.[0].input).toEqual({ AlternateContactType: "BILLING" });
    });
  });

  it("does not delete an alternate contact when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockRejectedValueOnce(notFound);
    render(AccountPage);
    await waitFor(() => screen.getByText("No contact information set"));

    mockSend.mockResolvedValueOnce({
      AlternateContact: {
        Name: "Billing Bob",
        EmailAddress: "billing@example.com",
        Title: "Manager",
        PhoneNumber: "+1",
        AlternateContactType: "BILLING",
      },
    });
    mockSend.mockRejectedValueOnce(notFound);
    mockSend.mockRejectedValueOnce(notFound);

    await fireEvent.click(screen.getByRole("tab", { name: "Alternate Contacts" }));
    await waitFor(() => screen.getByText("Billing Bob (billing@example.com)"));

    const callsBefore = mockSend.mock.calls.length;
    await fireEvent.click(screen.getByTitle("Delete"));
    expect(confirmDestructive).toHaveBeenCalled();
    // No DeleteAlternateContactCommand, no reload -- call count unchanged.
    expect(mockSend.mock.calls.length).toBe(callsBefore);
  });

  it("switches to the Regions tab and enables a disabled region", async () => {
    mockSend.mockRejectedValueOnce(notFound);
    render(AccountPage);
    await waitFor(() => screen.getByText("No contact information set"));

    mockSend.mockResolvedValueOnce({
      Regions: [{ RegionName: "ap-south-2", RegionOptStatus: "DISABLED" }],
    });
    await fireEvent.click(screen.getByRole("tab", { name: "Regions" }));

    await waitFor(() => {
      expect(screen.getByText("ap-south-2")).toBeInTheDocument();
    });
    expect(screen.getByText("DISABLED")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Regions: [{ RegionName: "ap-south-2", RegionOptStatus: "ENABLED" }],
    });

    await fireEvent.click(screen.getByTitle("Enable"));

    await waitFor(() => {
      expect(screen.getByText("ENABLED")).toBeInTheDocument();
    });

    const enableCall = mockSend.mock.calls[2][0];
    expect(enableCall.constructor.name).toBe("EnableRegionCommand");
    expect(enableCall.input).toEqual({ RegionName: "ap-south-2" });
  });
});
