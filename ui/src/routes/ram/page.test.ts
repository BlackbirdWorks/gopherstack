import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import RAMPage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getRAMClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleShare = {
  resourceShareArn: "arn:aws:ram:us-east-1:123456789012:resource-share/my-share",
  name: "my-share",
  status: "ACTIVE",
  allowExternalPrincipals: false,
};

describe("RAM Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [] });
    render(RAMPage);
    expect(screen.getByText("AWS Resource Access Manager")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No resource shares found"));
  });

  it("shows empty state when no resource shares", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [] });
    render(RAMPage);
    await waitFor(() => {
      expect(screen.getByText("No resource shares found")).toBeInTheDocument();
    });
  });

  it("lists resource shares with a status badge from a render snippet", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [exampleShare] });
    render(RAMPage);
    await waitFor(() => {
      const table = screen.getByRole("table");
      expect(within(table).getByText("my-share")).toBeInTheDocument();
      expect(within(table).getByText("ACTIVE")).toBeInTheDocument();
    });
  });

  it("creates a resource share via the modal with exact command input", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [] });
    render(RAMPage);
    await waitFor(() => screen.getByText("No resource shares found"));

    await fireEvent.click(screen.getByText("Create resource share"));
    expect(screen.getByText("Create Resource Share")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-share" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText(/Resource ARNs/), {
      target: { value: "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1" },
    });

    mockSend.mockResolvedValueOnce({ resourceShare: exampleShare });
    mockSend.mockResolvedValueOnce({ resourceShares: [exampleShare] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(within(screen.getByRole("table")).getByText("my-share")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      name: "my-share",
      resourceArns: ["arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"],
      principals: undefined,
      permissionArns: undefined,
      allowExternalPrincipals: false,
    });
  });

  it("deletes a resource share after confirming", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [exampleShare] });
    render(RAMPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-share"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ resourceShares: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No resource shares found")).toBeInTheDocument();
    });
    expect(mockSend.mock.calls[1][0].input).toEqual({
      resourceShareArn: exampleShare.resourceShareArn,
    });
  });

  it("does not delete a resource share when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ resourceShares: [exampleShare] });
    render(RAMPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-share"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(within(screen.getByRole("table")).getByText("my-share")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Resource share not found."), {
      name: "UnknownResourceException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(RAMPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("UnknownResourceException (HTTP 404): Resource share not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a resource share's detail view and lists its associations", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [exampleShare] });
    render(RAMPage);
    await waitFor(() => within(screen.getByRole("table")).getByText("my-share"));

    mockSend.mockResolvedValueOnce({
      resourceShareAssociations: [
        { associatedEntity: "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1" },
      ],
    });
    mockSend.mockResolvedValueOnce({
      resourceShareAssociations: [{ associatedEntity: "123456789012" }],
    });

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(
        within(openDialog()).getByText("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"),
      ).toBeInTheDocument();
      expect(within(openDialog()).getByText("123456789012")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[1][0].input).toEqual({
      associationType: "RESOURCE",
      resourceShareArns: [exampleShare.resourceShareArn],
    });
    expect(mockSend.mock.calls[2][0].input).toEqual({
      associationType: "PRINCIPAL",
      resourceShareArns: [exampleShare.resourceShareArn],
    });
  });

  it("switches to the Permissions tab and creates a permission with exact command input (never calls the fabricated ListTagsForResource op)", async () => {
    mockSend.mockResolvedValueOnce({ resourceShares: [] });
    render(RAMPage);
    await waitFor(() => screen.getByText("No resource shares found"));

    mockSend.mockResolvedValueOnce({ permissions: [] });
    await fireEvent.click(screen.getByRole("tab", { name: "Permissions" }));
    await waitFor(() => screen.getByText("No permissions found"));

    await fireEvent.click(screen.getByText("Create permission"));
    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "my-permission" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText(/Resource type/), {
      target: { value: "ec2:Subnet" },
    });

    const examplePermission = {
      arn: "arn:aws:ram::123456789012:permission/my-permission",
      name: "my-permission",
      resourceType: "ec2:Subnet",
      version: "1",
    };
    mockSend.mockResolvedValueOnce({ permission: examplePermission });
    mockSend.mockResolvedValueOnce({ permissions: [examplePermission] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("my-permission")).toBeInTheDocument();
    });

    expect(mockSend.mock.calls[2][0].input).toEqual({
      name: "my-permission",
      resourceType: "ec2:Subnet",
      policyTemplate: JSON.stringify({ Effect: "Allow", Action: [] }),
    });

    for (const call of mockSend.mock.calls) {
      expect(call[0].constructor.name).not.toBe("ListTagsForResourceCommand");
    }
  });
});
