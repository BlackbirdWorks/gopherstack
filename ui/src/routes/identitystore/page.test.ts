import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import IdentityStorePage from "./+page.svelte";
import {
  ListUsersCommand,
  ListGroupsCommand,
  ListGroupMembershipsCommand,
  ListGroupMembershipsForMemberCommand,
} from "@aws-sdk/client-identitystore";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getIdentityStoreClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const defaultStoreID = "d-0000000000";

// identitystore does not use the shared confirmDestructive dialog -- delete
// confirmations are a hand-rolled <dialog role="dialog"> modal
// (showDeleteUserConfirm / showDeleteGroupConfirm) toggled entirely by local
// component state, so there is nothing to mock for it.

const exampleUser = {
  UserId: "u-1111",
  UserName: "alice.johnson",
  DisplayName: "Alice Johnson",
  Name: { GivenName: "Alice", FamilyName: "Johnson" },
  Emails: [{ Value: "alice@example.com", Type: "work", Primary: true }],
  PhoneNumbers: [{ Value: "+1-555-0100", Type: "work", Primary: true }],
  Title: "Engineer",
};

const exampleGroup = {
  GroupId: "g-2222",
  DisplayName: "Engineering",
  Description: "Software engineering team",
};

// Requests this page issues that a given test doesn't care about (background
// member-count fan-out, modal-open list calls, etc.) fall through to this
// handler so they resolve to something harmless instead of `undefined`
// (which would blow up on `.then`). Tests that DO care about a specific call
// layer a `mockResolvedValueOnce` in front of it -- vitest drains the "once"
// queue before falling back to `mockImplementation`.
function installDefaultHandler() {
  mockSend.mockImplementation((cmd: unknown) => {
    if (cmd instanceof ListUsersCommand) return Promise.resolve({ Users: [] });
    if (cmd instanceof ListGroupsCommand) return Promise.resolve({ Groups: [] });
    if (cmd instanceof ListGroupMembershipsCommand)
      return Promise.resolve({ GroupMemberships: [] });
    if (cmd instanceof ListGroupMembershipsForMemberCommand)
      return Promise.resolve({ GroupMemberships: [] });
    return Promise.resolve({});
  });
}

/**
 * Renders the page with one fixture user and one fixture group already
 * loaded. The Users tab is active by default, so only the user row is
 * visible/asserted here -- the Groups tab's own content only mounts once
 * selected (see switchToGroupsTab).
 */
async function renderLoaded() {
  // ListUsersCommand
  mockSend.mockResolvedValueOnce({ Users: [exampleUser] });
  // ListGroupsCommand
  mockSend.mockResolvedValueOnce({ Groups: [exampleGroup] });
  render(IdentityStorePage);
  await waitFor(() => screen.getByText(exampleUser.UserName));
}

async function switchToGroupsTab() {
  await fireEvent.click(screen.getByText("Groups"));
  await waitFor(() => screen.getByText(exampleGroup.DisplayName));
}

describe("IdentityStore Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    installDefaultHandler();
  });

  it("renders the page title, tabs, and IdentityStoreId input", () => {
    render(IdentityStorePage);
    expect(screen.getByText("Identity Store")).toBeInTheDocument();
    expect(screen.getByText("Users")).toBeInTheDocument();
    expect(screen.getByText("Groups")).toBeInTheDocument();
    expect(screen.getByLabelText("IdentityStoreId")).toHaveValue(defaultStoreID);
  });

  it("loads users and groups on mount using the default IdentityStoreId, rendering username/display-name/email cells", async () => {
    await renderLoaded();

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { IdentityStoreId: defaultStoreID },
      }),
    );
    expect(screen.getByText(exampleUser.UserName)).toBeInTheDocument();
    // Appears twice: the DisplayName cell, and the given+family name
    // sub-line underneath it (both happen to read "Alice Johnson" for this
    // fixture) -- assert presence, not uniqueness.
    expect(screen.getAllByText(exampleUser.DisplayName).length).toBeGreaterThan(0);
    expect(screen.getByText(exampleUser.Emails[0].Value)).toBeInTheDocument();

    await switchToGroupsTab();
    expect(screen.getByText(exampleGroup.Description)).toBeInTheDocument();
  });

  it("shows an inline error banner with the failure message when the initial load fails, and retries on click", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockReset();
    // ListUsersCommand rejects -> Promise.all rejects
    mockSend.mockRejectedValueOnce(error);

    render(IdentityStorePage);

    await waitFor(() => {
      expect(screen.getByText("Rate exceeded.")).toBeInTheDocument();
    });
    expect(screen.getByText("Error:")).toBeInTheDocument();

    installDefaultHandler();
    mockSend.mockResolvedValueOnce({ Users: [exampleUser] });
    mockSend.mockResolvedValueOnce({ Groups: [] });

    await fireEvent.click(screen.getByText("Retry"));

    await waitFor(() => {
      expect(screen.getByText(exampleUser.UserName)).toBeInTheDocument();
    });
    expect(screen.queryByText("Rate exceeded.")).not.toBeInTheDocument();
  });

  it("creates a user via the modal, deriving DisplayName from given+family name when left blank", async () => {
    await renderLoaded();

    await fireEvent.click(screen.getByRole("button", { name: "Create new user" }));
    await fireEvent.input(screen.getByLabelText("Username *"), {
      target: { value: "john.doe" },
    });
    await fireEvent.input(screen.getByLabelText("First Name"), { target: { value: "John" } });
    await fireEvent.input(screen.getByLabelText("Last Name"), { target: { value: "Doe" } });
    await fireEvent.input(screen.getByLabelText("Email"), {
      target: { value: "john@example.com" },
    });
    await fireEvent.input(screen.getByLabelText("Phone"), {
      target: { value: "+1-555-0199" },
    });

    const newUser = {
      UserId: "u-3333",
      UserName: "john.doe",
      DisplayName: "John Doe",
      Emails: [{ Value: "john@example.com" }],
    };
    // CreateUserCommand
    mockSend.mockResolvedValueOnce({ UserId: newUser.UserId });
    // reload
    mockSend.mockResolvedValueOnce({ Users: [exampleUser, newUser] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("john.doe")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          IdentityStoreId: defaultStoreID,
          UserName: "john.doe",
          DisplayName: "John Doe",
          Name: { GivenName: "John", FamilyName: "Doe" },
          Emails: [{ Value: "john@example.com", Type: "work", Primary: true }],
          PhoneNumbers: [{ Value: "+1-555-0199", Type: "work", Primary: true }],
        }),
      }),
    );
  });

  it("shows a toast error with the AWS error code when creating a user fails", async () => {
    await renderLoaded();
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create new user" }));
    await fireEvent.input(screen.getByLabelText("Username *"), {
      target: { value: "john.doe" },
    });

    const error = Object.assign(new Error("A user with this UserName already exists."), {
      name: "ConflictException",
      $metadata: { httpStatusCode: 409 },
    });
    mockSend.mockRejectedValueOnce(error);

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to create user: A user with this UserName already exists.",
      );
    });
    // The modal stays open on failure -- the user hasn't lost their input.
    expect(screen.getByText("Create User")).toBeInTheDocument();
  });

  it("creates a group via the modal", async () => {
    await renderLoaded();
    await switchToGroupsTab();

    await fireEvent.click(screen.getByRole("button", { name: "Create new group" }));
    await fireEvent.input(screen.getByLabelText("Group Name *"), {
      target: { value: "Platform" },
    });
    await fireEvent.input(screen.getByLabelText("Description"), {
      target: { value: "Platform team" },
    });

    // CreateGroupCommand
    mockSend.mockResolvedValueOnce({ GroupId: "g-4444" });
    mockSend.mockResolvedValueOnce({
      Groups: [exampleGroup, { GroupId: "g-4444", DisplayName: "Platform" }],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("Platform")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          IdentityStoreId: defaultStoreID,
          DisplayName: "Platform",
          Description: "Platform team",
        },
      }),
    );
  });

  it("deletes a user after confirming in the delete-confirmation dialog", async () => {
    await renderLoaded();

    await fireEvent.click(
      screen.getByRole("button", { name: `Delete user ${exampleUser.UserName}` }),
    );
    const dialog = screen.getByRole("dialog", { name: "Delete User" });
    expect(within(dialog).getByText(exampleUser.UserName)).toBeInTheDocument();

    // DeleteUserCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ Users: [] });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Delete" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { IdentityStoreId: defaultStoreID, UserId: exampleUser.UserId },
      }),
    );
    await waitFor(() => {
      expect(screen.queryByText(exampleUser.UserName)).not.toBeInTheDocument();
    });
  });

  it("does not delete a user when the delete-confirmation dialog is cancelled", async () => {
    await renderLoaded();
    const callsBefore = mockSend.mock.calls.length;

    await fireEvent.click(
      screen.getByRole("button", { name: `Delete user ${exampleUser.UserName}` }),
    );
    const dialog = screen.getByRole("dialog", { name: "Delete User" });
    await fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));

    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByText(exampleUser.UserName)).toBeInTheDocument();
  });

  it("edits a user's profile via the modal, sending an Operations attribute-patch array", async () => {
    await renderLoaded();

    await fireEvent.click(
      screen.getByRole("button", { name: `Edit user ${exampleUser.UserName}` }),
    );
    const dialog = screen.getByRole("dialog", { name: `Edit Profile — ${exampleUser.UserName}` });

    const emailField = within(dialog).getByLabelText("Primary Email");
    await fireEvent.input(emailField, { target: { value: "alice.j@example.com" } });

    // UpdateUserCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Users: [{ ...exampleUser, Emails: [{ Value: "alice.j@example.com" }] }],
    });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Save" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          IdentityStoreId: defaultStoreID,
          UserId: exampleUser.UserId,
          Operations: [
            { AttributePath: "displayName", AttributeValue: exampleUser.DisplayName },
            {
              AttributePath: "emails",
              AttributeValue: [{ Value: "alice.j@example.com", Type: "work", Primary: true }],
            },
            {
              AttributePath: "phoneNumbers",
              AttributeValue: [
                { Value: exampleUser.PhoneNumbers[0].Value, Type: "work", Primary: true },
              ],
            },
            { AttributePath: "addresses", AttributeValue: [] },
            { AttributePath: "name.givenName", AttributeValue: exampleUser.Name.GivenName },
            { AttributePath: "name.familyName", AttributeValue: exampleUser.Name.FamilyName },
            { AttributePath: "title", AttributeValue: exampleUser.Title },
          ],
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("alice.j@example.com")).toBeInTheDocument();
    });
  });

  it("edits a group's name and description via the modal", async () => {
    await renderLoaded();
    await switchToGroupsTab();

    await fireEvent.click(
      screen.getByRole("button", { name: `Edit group ${exampleGroup.DisplayName}` }),
    );
    const dialog = screen.getByRole("dialog", { name: "Edit Group" });
    await fireEvent.input(within(dialog).getByLabelText("Description"), {
      target: { value: "Updated description" },
    });

    // UpdateGroupCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Groups: [{ ...exampleGroup, Description: "Updated description" }],
    });

    await fireEvent.click(within(dialog).getByRole("button", { name: "Save" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          IdentityStoreId: defaultStoreID,
          GroupId: exampleGroup.GroupId,
          Operations: [
            { AttributePath: "displayName", AttributeValue: exampleGroup.DisplayName },
            { AttributePath: "description", AttributeValue: "Updated description" },
          ],
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Updated description")).toBeInTheDocument();
    });
  });

  it("adds and removes a group membership from the Memberships modal", async () => {
    await renderLoaded();

    // ListGroupMembershipsForMemberCommand
    mockSend.mockResolvedValueOnce({ GroupMemberships: [] });
    await fireEvent.click(
      screen.getByRole("button", { name: `Manage memberships for ${exampleUser.UserName}` }),
    );
    const dialog = screen.getByRole("dialog", {
      name: `Memberships — ${exampleUser.UserName}`,
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          IdentityStoreId: defaultStoreID,
          MemberId: { UserId: exampleUser.UserId },
        },
      }),
    );
    await fireEvent.change(within(dialog).getByLabelText("Add to Group"), {
      target: { value: exampleGroup.GroupId },
    });

    const newMembership = {
      MembershipId: "m-5555",
      GroupId: exampleGroup.GroupId,
      MemberId: { UserId: exampleUser.UserId },
    };
    // CreateGroupMembershipCommand
    mockSend.mockResolvedValueOnce({ MembershipId: newMembership.MembershipId });
    // reload
    mockSend.mockResolvedValueOnce({ GroupMemberships: [newMembership] });

    await fireEvent.click(
      within(dialog).getByRole("button", { name: "Add user to selected group" }),
    );

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          IdentityStoreId: defaultStoreID,
          GroupId: exampleGroup.GroupId,
          MemberId: { UserId: exampleUser.UserId },
        },
      }),
    );
    await waitFor(() => {
      expect(
        within(dialog).getByRole("button", { name: `Remove from ${exampleGroup.DisplayName}` }),
      ).toBeInTheDocument();
    });
    expect(within(dialog).getByText("Member")).toBeInTheDocument();

    // DeleteGroupMembershipCommand
    mockSend.mockResolvedValueOnce({});
    // reload
    mockSend.mockResolvedValueOnce({ GroupMemberships: [] });

    await fireEvent.click(
      within(dialog).getByRole("button", { name: `Remove from ${exampleGroup.DisplayName}` }),
    );

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { IdentityStoreId: defaultStoreID, MembershipId: newMembership.MembershipId },
      }),
    );
    await waitFor(() => {
      expect(
        within(dialog).queryByRole("button", { name: `Remove from ${exampleGroup.DisplayName}` }),
      ).not.toBeInTheDocument();
    });
  });

  it("views and removes a member from a group via the View Members modal", async () => {
    await renderLoaded();
    await switchToGroupsTab();

    const membership = {
      MembershipId: "m-6666",
      GroupId: exampleGroup.GroupId,
      MemberId: { UserId: exampleUser.UserId },
    };
    // ListGroupMembershipsCommand
    mockSend.mockResolvedValueOnce({ GroupMemberships: [membership] });

    await fireEvent.click(
      screen.getByRole("button", { name: `View members of ${exampleGroup.DisplayName}` }),
    );

    const dialog = screen.getByRole("dialog", {
      name: `Members of ${exampleGroup.DisplayName}`,
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { IdentityStoreId: defaultStoreID, GroupId: exampleGroup.GroupId },
      }),
    );
    await waitFor(() => {
      expect(within(dialog).getByText(exampleUser.UserName)).toBeInTheDocument();
    });
    expect(within(dialog).getByText("1 member")).toBeInTheDocument();

    // DeleteGroupMembershipCommand
    mockSend.mockResolvedValueOnce({});
    // re-fetch inside openViewMembersModal
    mockSend.mockResolvedValueOnce({ GroupMemberships: [] });

    await fireEvent.click(
      within(dialog).getByRole("button", { name: `Remove ${exampleUser.UserName} from group` }),
    );

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { IdentityStoreId: defaultStoreID, MembershipId: membership.MembershipId },
      }),
    );
    await waitFor(() => {
      expect(within(dialog).getByText("No members in this group")).toBeInTheDocument();
    });
  });

  it("sorts users by username, toggling ascending/descending on repeated clicks", async () => {
    const bob = { UserId: "u-b", UserName: "bob.smith", DisplayName: "Bob Smith" };
    // alice then bob
    mockSend.mockResolvedValueOnce({ Users: [exampleUser, bob] });
    mockSend.mockResolvedValueOnce({ Groups: [] });
    render(IdentityStorePage);
    await waitFor(() => screen.getByText("alice.johnson"));

    const cellsAsc = screen
      .getAllByRole("row")
      .slice(1)
      .map((r) => r.textContent);
    expect(cellsAsc[0]).toContain("alice.johnson");
    expect(cellsAsc[1]).toContain("bob.smith");

    await fireEvent.click(screen.getByRole("button", { name: "Sort by username" }));

    const cellsDesc = screen
      .getAllByRole("row")
      .slice(1)
      .map((r) => r.textContent);
    expect(cellsDesc[0]).toContain("bob.smith");
    expect(cellsDesc[1]).toContain("alice.johnson");
  });

  it("filters the users table via the search box", async () => {
    const bob = { UserId: "u-b", UserName: "bob.smith", DisplayName: "Bob Smith" };
    mockSend.mockResolvedValueOnce({ Users: [exampleUser, bob] });
    mockSend.mockResolvedValueOnce({ Groups: [] });
    render(IdentityStorePage);
    await waitFor(() => screen.getByText("alice.johnson"));
    expect(screen.getByText("bob.smith")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Search users by username, name, or email"), {
      target: { value: "bob" },
    });

    expect(screen.queryByText("alice.johnson")).not.toBeInTheDocument();
    expect(screen.getByText("bob.smith")).toBeInTheDocument();
    expect(screen.getByText("1 of 2")).toBeInTheDocument();
  });

  it("shows computed totals on the Metrics tab", async () => {
    await renderLoaded();

    // The initial mount's background member-count fan-out already ran (and,
    // via the default handler, found 0 members). Click Refresh to trigger a
    // fresh loadGroupMemberCounts with the 3-membership fixture queued
    // ahead of it -- queuing it before render would be too late, since the
    // fan-out fires immediately after mount, before a test gets a chance to
    // enqueue a response for it.
    mockSend.mockResolvedValueOnce({ Users: [exampleUser] });
    mockSend.mockResolvedValueOnce({ Groups: [exampleGroup] });
    mockSend.mockResolvedValueOnce({
      GroupMemberships: [{ MembershipId: "m1" }, { MembershipId: "m2" }, { MembershipId: "m3" }],
    });
    await fireEvent.click(screen.getByRole("button", { name: "Refresh identity store data" }));
    await waitFor(() =>
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({ GroupId: exampleGroup.GroupId }),
        }),
      ),
    );

    await fireEvent.click(screen.getByText("Metrics"));

    await waitFor(() => {
      expect(screen.getByText("Store Metrics")).toBeInTheDocument();
    });
    expect(screen.getByText("Total Users")).toBeInTheDocument();
    // Each metric tile is <div.rounded-xl><div>label</div><div>value</div></div>
    // -- the label's parentElement is the tile, not the label div itself.
    const totalUsersCard = screen.getByText("Total Users").parentElement;
    expect(within(totalUsersCard!).getByText("1")).toBeInTheDocument();
    const totalGroupsCard = screen.getByText("Total Groups").parentElement;
    expect(within(totalGroupsCard!).getByText("1")).toBeInTheDocument();
    await waitFor(() => {
      const totalMembershipsCard = screen.getByText("Total Memberships").parentElement;
      expect(within(totalMembershipsCard!).getByText("3")).toBeInTheDocument();
    });
  });

  it("lists all supported operations on the Docs tab", async () => {
    render(IdentityStorePage);
    await fireEvent.click(screen.getByText("Docs"));
    expect(screen.getByText("Supported Operations")).toBeInTheDocument();
    expect(screen.getByText("CreateUser")).toBeInTheDocument();
    expect(screen.getByText("ListGroupMembershipsForMember")).toBeInTheDocument();
  });
});
