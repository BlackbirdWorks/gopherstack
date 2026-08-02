import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ResourceGroupsPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getResourceGroupsClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// Every modal on this page stays mounted in the DOM once rendered -- a
// closed <dialog> just loses the `open` attribute rather than unmounting --
// so getByLabelText across the whole document is ambiguous once more than
// one modal has been opened (e.g. both Create Group and Edit Group have a
// "Description" field). Scope to the dialog that is actually open.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

// DataTable renders "Loading..." while a tab's fetch is in flight and swaps
// to the empty-state message only once it resolves. Waiting for "Loading..."
// to disappear (rather than waiting directly on the empty-state text) is the
// reliable sync point for empty-state assertions.
async function waitForLoadingToFinish(): Promise<void> {
  await waitFor(() => {
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
  });
}

const exampleGroup = {
  GroupName: "prod-web-tier",
  GroupArn: "arn:aws:resource-groups:us-east-1:123456789012:group/prod-web-tier",
  Description: "Production web tier resources",
  Owner: "platform-team",
  Criticality: 2,
};

describe("Resource Groups Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [] });
    render(ResourceGroupsPage);
    expect(screen.getByText("Resource Groups")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [] });
    render(ResourceGroupsPage);
    expect(screen.getByText("Groups")).toBeInTheDocument();
    expect(screen.getByText("Resources")).toBeInTheDocument();
    expect(screen.getByText("Tags")).toBeInTheDocument();
    expect(screen.getByText("Grouping Statuses")).toBeInTheDocument();
    expect(screen.getByText("Tag Sync Tasks")).toBeInTheDocument();
    expect(screen.getByText("Account Settings")).toBeInTheDocument();
  });

  it("lists groups and renders the Owner column via its render snippet", async () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [exampleGroup] });
    render(ResourceGroupsPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "prod-web-tier" })).toBeInTheDocument();
    });
    // Render-snippet assertion: the Owner column is defined with
    // `render: groupOwnerCell`.
    expect(screen.getByText("platform-team")).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { NextToken: undefined } }),
    );
  });

  it("shows empty state when no groups", async () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [] });
    render(ResourceGroupsPage);
    await waitForLoadingToFinish();
    expect(screen.getByText("No groups found")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Access denied."), {
      name: "AccessDeniedException",
      $metadata: { httpStatusCode: 403 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ResourceGroupsPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("AccessDeniedException (HTTP 403): Access denied."),
      ).toBeInTheDocument();
    });
  });

  it("creates a group via CreateGroup with exact input", async () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [] });
    render(ResourceGroupsPage);
    await waitFor(() => screen.getByText("No groups found"));

    await fireEvent.click(screen.getByText("Create group"));
    expect(screen.getByText("Create Group")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "new-group" },
    });

    mockSend.mockResolvedValueOnce({ Group: { ...exampleGroup, GroupName: "new-group" } });
    mockSend.mockResolvedValueOnce({
      GroupIdentifiers: [{ ...exampleGroup, GroupName: "new-group" }],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(3);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          Name: "new-group",
          Description: undefined,
          ResourceQuery: {
            Type: "TAG_FILTERS_1_0",
            Query:
              '{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"Stage","Values":["Test"]}]}',
          },
        },
      }),
    );
  });

  it("deletes a group after confirming, with exact DeleteGroup input", async () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [exampleGroup] });
    render(ResourceGroupsPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-web-tier" }));

    mockSend.mockResolvedValueOnce({ Group: exampleGroup });
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No groups found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { Group: "prod-web-tier" } }),
    );
  });

  it("does not delete a group when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [exampleGroup] });
    render(ResourceGroupsPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-web-tier" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListGroups call -- no DeleteGroup, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "prod-web-tier" })).toBeInTheDocument();
  });

  it("groups a resource into the selected group with exact GroupResources input", async () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [exampleGroup] });
    render(ResourceGroupsPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-web-tier" }));

    mockSend.mockResolvedValueOnce({ Resources: [] });
    await fireEvent.click(screen.getByText("Resources"));
    await waitFor(() => screen.getByText("No resources in this group"));

    await fireEvent.input(screen.getByPlaceholderText("Resource ARN to add..."), {
      target: { value: "arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Resources: [
        {
          Identifier: {
            ResourceArn: "arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0",
            ResourceType: "AWS::EC2::Instance",
          },
        },
      ],
    });

    await fireEvent.click(screen.getByText("Group resource"));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0"),
      ).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          Group: "prod-web-tier",
          ResourceArns: ["arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0"],
        },
      }),
    );
  });

  it("adds a tag to the selected group with exact Tag input", async () => {
    mockSend.mockResolvedValueOnce({ GroupIdentifiers: [exampleGroup] });
    render(ResourceGroupsPage);
    await waitFor(() => screen.getByRole("cell", { name: "prod-web-tier" }));

    mockSend.mockResolvedValueOnce({ Tags: {} });
    await fireEvent.click(screen.getByText("Tags"));
    await waitFor(() => screen.getByText("No tags on this group"));

    await fireEvent.input(screen.getByPlaceholderText("Tag key"), { target: { value: "Stage" } });
    await fireEvent.input(screen.getByPlaceholderText("Tag value"), { target: { value: "Prod" } });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Tags: { Stage: "Prod" } });

    await fireEvent.click(screen.getByText("Add tag"));

    await waitFor(() => {
      expect(screen.getByText("Prod")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { Arn: exampleGroup.GroupArn, Tags: { Stage: "Prod" } },
      }),
    );
  });
});
