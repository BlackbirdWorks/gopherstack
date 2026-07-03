import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import IAMPage from "./+page.svelte";
import {
  ListUsersCommand,
  ListRolesCommand,
  ListGroupsCommand,
  ListPoliciesCommand,
  ListAccountAliasesCommand,
  GetAccountSummaryCommand,
  CreateUserCommand,
  SimulateCustomPolicyCommand,
  GenerateCredentialReportCommand,
  GetCredentialReportCommand,
} from "@aws-sdk/client-iam";

const mockSend = vi.fn();
vi.mock("$lib/aws-client", () => ({ getIAMClient: () => ({ send: mockSend }) }));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

const mockUser = {
  UserName: "alice",
  UserId: "AIDA123",
  Arn: "arn:aws:iam::123:user/alice",
  Path: "/",
  CreateDate: new Date("2024-01-01"),
};
const mockRole = {
  RoleName: "AdminRole",
  RoleId: "AROA456",
  Arn: "arn:aws:iam::123:role/AdminRole",
  Path: "/",
  AssumeRolePolicyDocument: encodeURIComponent('{"Version":"2012-10-17","Statement":[]}'),
  CreateDate: new Date("2024-01-01"),
};
const mockGroup = {
  GroupName: "Developers",
  GroupId: "AGPA789",
  Arn: "arn:aws:iam::123:group/Developers",
  Path: "/",
  CreateDate: new Date("2024-01-01"),
};
const mockPolicy = {
  PolicyName: "MyPolicy",
  Arn: "arn:aws:iam::123:policy/MyPolicy",
  AttachmentCount: 2,
  Path: "/",
};

function defaultMockImpl(cmd: unknown) {
  if (cmd instanceof ListUsersCommand) return Promise.resolve({ Users: [] });
  if (cmd instanceof ListRolesCommand) return Promise.resolve({ Roles: [] });
  if (cmd instanceof ListGroupsCommand) return Promise.resolve({ Groups: [] });
  if (cmd instanceof ListPoliciesCommand) return Promise.resolve({ Policies: [] });
  if (cmd instanceof ListAccountAliasesCommand) return Promise.resolve({ AccountAliases: [] });
  if (cmd instanceof GetAccountSummaryCommand) return Promise.resolve({ SummaryMap: {} });
  return Promise.resolve({});
}

describe("IAM Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockImplementation(defaultMockImpl);
  });

  it("renders page title", () => {
    render(IAMPage);
    expect(screen.getByText("IAM")).toBeInTheDocument();
  });

  it("shows tabs for Users/Roles/Groups/Policies/Metrics/Docs", () => {
    render(IAMPage);
    const buttons = screen.getAllByRole("button");
    const labels = buttons.map((b) => b.textContent?.trim());
    expect(labels.some((n) => n?.includes("Users"))).toBe(true);
    expect(labels.some((n) => n?.includes("Roles"))).toBe(true);
    expect(labels.some((n) => n?.includes("Groups"))).toBe(true);
    expect(labels.some((n) => n?.includes("Policies"))).toBe(true);
    expect(labels.some((n) => n?.includes("Metrics"))).toBe(true);
    expect(labels.some((n) => n?.includes("Docs"))).toBe(true);
  });

  it("shows loaded users", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListUsersCommand) return Promise.resolve({ Users: [mockUser] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("alice")).toBeInTheDocument(), { timeout: 3000 });
  });

  it("shows empty state for users", async () => {
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("shows user ARN in list", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListUsersCommand) return Promise.resolve({ Users: [mockUser] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(
      () => expect(screen.getByText("arn:aws:iam::123:user/alice")).toBeInTheDocument(),
      { timeout: 3000 },
    );
  });

  it("switches to Roles tab and shows roles", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListRolesCommand) return Promise.resolve({ Roles: [mockRole] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const rolesTab = document.querySelector("#roles-tab");
    await fireEvent.click(rolesTab!);
    await waitFor(() => expect(screen.getByText("AdminRole")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("switches to Groups tab and shows groups", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListGroupsCommand) return Promise.resolve({ Groups: [mockGroup] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const groupsTab = document.querySelector("#groups-tab");
    await fireEvent.click(groupsTab!);
    await waitFor(() => expect(screen.getByText("Developers")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("switches to Policies tab and shows policies", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListPoliciesCommand) return Promise.resolve({ Policies: [mockPolicy] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const policiesTab = document.querySelector("#policies-tab");
    await fireEvent.click(policiesTab!);
    await waitFor(() => expect(screen.getByText("MyPolicy")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("searches users by name", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListUsersCommand)
        return Promise.resolve({
          Users: [mockUser, { UserName: "bob", UserId: "B1", Arn: "arn:aws:iam::123:user/bob" }],
        });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("alice")).toBeInTheDocument(), { timeout: 3000 });
    const searchInput = screen.getByPlaceholderText(/search users/i);
    await fireEvent.input(searchInput, { target: { value: "alice" } });
    await waitFor(() => expect(screen.queryByText("bob")).not.toBeInTheDocument());
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListUsersCommand) return Promise.reject(new Error("forbidden"));
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalledWith("forbidden"), {
      timeout: 3000,
    });
  });

  it("shows Create User button", () => {
    render(IAMPage);
    const buttons = screen.getAllByRole("button");
    expect(buttons.some((b) => b.textContent?.includes("Create User"))).toBe(true);
  });

  it("opens Create User modal on button click", async () => {
    render(IAMPage);
    const createBtn = screen
      .getAllByRole("button")
      .find((b) => b.textContent?.includes("Create User"));
    await fireEvent.click(createBtn!);
    await waitFor(() => expect(screen.getByText("Create IAM User")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("creates a user via the modal", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof CreateUserCommand)
        return Promise.resolve({
          User: { UserName: "carol", UserId: "C1", Arn: "arn:aws:iam::123:user/carol" },
        });
      if (cmd instanceof ListUsersCommand) return Promise.resolve({ Users: [] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    const createBtn = screen
      .getAllByRole("button")
      .find((b) => b.textContent?.includes("Create User"));
    await fireEvent.click(createBtn!);
    await waitFor(() => expect(screen.getByText("Create IAM User")).toBeInTheDocument());
    const nameInput = screen.getByPlaceholderText(/e.g. alice/i);
    await fireEvent.input(nameInput, { target: { value: "carol" } });
    const submitBtns = screen
      .getAllByRole("button")
      .filter((b) => b.textContent?.trim() === "Create User");
    await fireEvent.click(submitBtns.at(-1)!);
    const { toast } = await import("svelte-sonner");
    await waitFor(() => expect(vi.mocked(toast.success)).toHaveBeenCalled(), { timeout: 3000 });
  });

  it("shows Metrics tab with summary data", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof GetAccountSummaryCommand)
        return Promise.resolve({ SummaryMap: { Users: 3, Roles: 5, Groups: 2 } });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const metricsTab = document.querySelector("#metrics-tab");
    await fireEvent.click(metricsTab!);
    await waitFor(() => expect(screen.getByText("Access Keys (total)")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("shows Docs tab with operation list", async () => {
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const docsTab = document.querySelector("#docs-tab");
    await fireEvent.click(docsTab!);
    await waitFor(() => expect(screen.getByText("Supported IAM Operations")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("shows user detail panel on user click", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListUsersCommand) return Promise.resolve({ Users: [mockUser] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("alice")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("alice"));
    await waitFor(() => expect(screen.getByText("Attached Policies")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("shows role detail panel with trust policy", async () => {
    const simpleMockRole = {
      ...mockRole,
      AssumeRolePolicyDocument: '{"Version":"2012-10-17","Statement":[]}',
    };
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListRolesCommand) return Promise.resolve({ Roles: [simpleMockRole] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    const rolesTab = document.querySelector("#roles-tab");
    await fireEvent.click(rolesTab!);
    await waitFor(() => expect(screen.getByText("AdminRole")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("AdminRole"));
    await waitFor(() => expect(screen.getByText("Trust Policy")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("shows account alias in header when set", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListAccountAliasesCommand)
        return Promise.resolve({ AccountAliases: ["mycompany"] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText(/mycompany/)).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("shows policy count badge in Policies tab", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof ListPoliciesCommand) return Promise.resolve({ Policies: [mockPolicy] });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    const policiesTab = document.querySelector("#policies-tab");
    await fireEvent.click(policiesTab!);
    await waitFor(() => expect(screen.getByText("MyPolicy")).toBeInTheDocument(), {
      timeout: 3000,
    });
    expect(screen.getByText("1 of 1 policies")).toBeInTheDocument();
  });

  it("shows Simulator and Credential Report tabs", () => {
    render(IAMPage);
    const labels = screen.getAllByRole("button").map((b) => b.textContent?.trim());
    expect(labels.some((n) => n?.includes("Simulator"))).toBe(true);
    expect(labels.some((n) => n?.includes("Credential Report"))).toBe(true);
  });

  it("runs a policy simulation and shows the decision", async () => {
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof SimulateCustomPolicyCommand)
        return Promise.resolve({
          EvaluationResults: [
            {
              EvalActionName: "s3:GetObject",
              EvalResourceName: "arn:aws:s3:::example-bucket/*",
              EvalDecision: "allowed",
            },
          ],
        });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(document.querySelector("#simulator-tab")!);
    await fireEvent.click(document.querySelector("#run-simulation")!);
    await waitFor(() => expect(screen.getByText("allowed")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const simCmd = mockSend.mock.calls
      .map((c) => c[0])
      .find((c) => c instanceof SimulateCustomPolicyCommand);
    expect(simCmd).toBeTruthy();
    expect((simCmd as SimulateCustomPolicyCommand).input.ActionNames).toContain("s3:GetObject");
  });

  it("loads and renders the credential report", async () => {
    const csv =
      "user,arn,user_creation_time,password_enabled,password_last_used," +
      "password_last_changed,password_next_rotation,mfa_active," +
      "access_key_1_active,access_key_2_active\n" +
      "alice,arn:aws:iam::123:user/alice,2024-01-01T00:00:00Z,true,N/A,N/A,N/A,false,true,false";
    mockSend.mockImplementation((cmd) => {
      if (cmd instanceof GenerateCredentialReportCommand)
        return Promise.resolve({ State: "COMPLETE" });
      if (cmd instanceof GetCredentialReportCommand)
        return Promise.resolve({
          Content: new TextEncoder().encode(csv),
          GeneratedTime: new Date("2024-01-02T00:00:00Z"),
        });
      return defaultMockImpl(cmd);
    });
    render(IAMPage);
    await waitFor(() => expect(screen.getByText("No users found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(document.querySelector("#report-tab")!);
    await waitFor(() => expect(screen.getByText("alice")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const genCmd = mockSend.mock.calls
      .map((c) => c[0])
      .find((c) => c instanceof GenerateCredentialReportCommand);
    expect(genCmd).toBeTruthy();
  });
});
