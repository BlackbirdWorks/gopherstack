import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CognitoIdpPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCognitoIDPClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const examplePool = {
  Id: "us-east-1_ABC123",
  Name: "my-pool",
  Status: "Enabled",
  CreationDate: new Date("2026-01-01T00:00:00Z"),
};

const exampleUser = {
  Username: "jdoe",
  UserStatus: "CONFIRMED",
  Enabled: true,
  UserCreateDate: new Date("2026-01-02T00:00:00Z"),
  Attributes: [{ Name: "email", Value: "jdoe@example.com" }],
};

describe("Cognito User Pools Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ UserPools: [] });
    render(CognitoIdpPage);
    expect(screen.getByText("Cognito User Pools")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ UserPools: [] });
    render(CognitoIdpPage);
    expect(screen.getByText("User Pools")).toBeInTheDocument();
    expect(screen.getByText("Users")).toBeInTheDocument();
    expect(screen.getByText("Groups")).toBeInTheDocument();
    expect(screen.getByText("App Clients")).toBeInTheDocument();
    expect(screen.getByText("Identity Providers")).toBeInTheDocument();
    expect(screen.getByText("Resource Servers")).toBeInTheDocument();
  });

  it("lists user pools and sends the required MaxResults", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });
    render(CognitoIdpPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-pool" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: expect.objectContaining({ MaxResults: 60 }) }),
    );
  });

  it("creates a user pool via the modal", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [] });
    render(CognitoIdpPage);
    // Synchronize on the tab bar (not the empty-state text, which can race
    // the still-in-flight initial ListUserPools call).
    await waitFor(() => screen.getByText("User Pools"));

    await fireEvent.click(screen.getByText("Create user pool"));
    expect(screen.getByText("Create User Pool")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Pool name"), {
      target: { value: "my-pool" },
    });

    mockSend.mockResolvedValueOnce({ UserPool: examplePool });
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "my-pool" })).toBeInTheDocument();
    });
    // ListUserPools (initial) + CreateUserPool + ListUserPools (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a user pool after confirming", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });
    render(CognitoIdpPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-pool" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ UserPools: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No user pools found")).toBeInTheDocument();
    });
  });

  it("does not delete a user pool when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });
    render(CognitoIdpPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-pool" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListUserPools call -- no DeleteUserPool, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "my-pool" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "TooManyRequestsException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(CognitoIdpPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("TooManyRequestsException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("opens a user pool's detail view via DescribeUserPool", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });
    render(CognitoIdpPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-pool" }));

    mockSend.mockResolvedValueOnce({
      UserPool: {
        ...examplePool,
        Arn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_ABC123",
      },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_ABC123"),
      ).toBeInTheDocument();
    });
  });

  it("switches to the Users tab, scoped to the auto-selected pool, using PaginationToken", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });
    render(CognitoIdpPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-pool" }));

    mockSend.mockResolvedValueOnce({ Users: [exampleUser], PaginationToken: "next-page" });
    await fireEvent.click(screen.getByText("Users"));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "jdoe" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenLastCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({ UserPoolId: examplePool.Id }),
      }),
    );
    // A LoadMore button should be present since PaginationToken was returned.
    expect(screen.getByText("Load More")).toBeInTheDocument();
  });

  it("shows a pool-selection prompt for scoped tabs with no pool", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [] });
    render(CognitoIdpPage);
    await waitFor(() => screen.getByText("User Pools"));

    mockSend.mockResolvedValueOnce({ Groups: [] });
    await fireEvent.click(screen.getByText("Groups"));

    await waitFor(() => {
      expect(screen.getByText("Select a user pool to see its groups")).toBeInTheDocument();
    });
  });

  it("deletes a user after confirming, using AdminDeleteUser", async () => {
    mockSend.mockResolvedValueOnce({ UserPools: [examplePool] });
    render(CognitoIdpPage);
    await waitFor(() => screen.getByRole("cell", { name: "my-pool" }));

    mockSend.mockResolvedValueOnce({ Users: [exampleUser] });
    await fireEvent.click(screen.getByText("Users"));
    await waitFor(() => screen.getByRole("cell", { name: "jdoe" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Users: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No users found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({ UserPoolId: examplePool.Id, Username: "jdoe" }),
      }),
    );
  });
});
