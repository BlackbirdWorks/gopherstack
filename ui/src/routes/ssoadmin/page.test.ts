import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SsoAdminPage from "./+page.svelte";
import {
  ListInstancesCommand,
  DescribeInstanceCommand,
  ListPermissionSetsCommand,
  DescribePermissionSetCommand,
  GetInlinePolicyForPermissionSetCommand,
  ListAccountAssignmentsCommand,
  ListApplicationsCommand,
  ListApplicationAssignmentsCommand,
  ListTrustedTokenIssuersCommand,
  ListRegionsCommand,
  ListAccountAssignmentCreationStatusCommand,
  ListAccountAssignmentDeletionStatusCommand,
  ListPermissionSetProvisioningStatusCommand,
} from "@aws-sdk/client-sso-admin";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSSOAdminClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const defaultAccountID = "123456789012";

const instanceArn = "arn:aws:sso:::instance/ssoins-instance1";
const exampleInstance = {
  InstanceArn: instanceArn,
  Name: "main",
  IdentityStoreId: "d-1111111111",
};

// This page re-fetches per-instance data (permission sets, applications,
// token issuers, regions, the instance description) both explicitly from
// refreshAll() AND reactively from a `$effect` that watches
// selectedInstanceArn -- so on mount these commands can legitimately fire
// more than once. Rather than hand-count calls, the mock is a stateful
// fake: reads always reflect the current `server` object, so any number of
// re-fetches converges on the same answer and mutations (create/delete)
// just need to update `server` before the next read.
type ServerState = {
  instances: (typeof exampleInstance)[];
  permissionSetArns: string[];
  permissionSets: Record<string, { Name?: string; Description?: string; SessionDuration?: string }>;
  inlinePolicies: Record<string, string>;
  assignments: Array<{
    PrincipalId?: string;
    PrincipalType?: string;
    AccountId?: string;
    PermissionSetArn?: string;
  }>;
  applications: Array<{
    ApplicationArn?: string;
    Name?: string;
    Status?: string;
    Description?: string;
  }>;
  applicationAssignments: Record<string, number>;
  tokenIssuers: Array<{
    TrustedTokenIssuerArn?: string;
    Name?: string;
    TrustedTokenIssuerType?: string;
    TrustedTokenIssuerConfiguration?: unknown;
  }>;
  regions: Array<{ Region?: string; RegionScopeType?: string }>;
  creationStatuses: unknown[];
  deletionStatuses: unknown[];
  provisioningStatuses: unknown[];
};

let server: ServerState;

function resetServer() {
  server = {
    instances: [exampleInstance],
    permissionSetArns: [],
    permissionSets: {},
    inlinePolicies: {},
    assignments: [],
    applications: [],
    applicationAssignments: {},
    tokenIssuers: [],
    regions: [],
    creationStatuses: [],
    deletionStatuses: [],
    provisioningStatuses: [],
  };
}

// Table of "which command is this, and how does the fake server answer it"
// pairs. Kept as data rather than an if/else-if chain so installDefaultHandler
// itself stays a single lookup -- each row is independent and new commands are
// added by appending a row, not by growing one function's branching.
type CommandDispatcher = {
  matches: (cmd: unknown) => boolean;
  respond: (cmd: unknown) => unknown;
};

const commandDispatchers: CommandDispatcher[] = [
  {
    matches: (cmd) => cmd instanceof ListInstancesCommand,
    respond: () => ({ Instances: server.instances }),
  },
  {
    matches: (cmd) => cmd instanceof DescribeInstanceCommand,
    respond: (cmd) => {
      const inst = server.instances.find(
        (i) => i.InstanceArn === (cmd as { input: { InstanceArn?: string } }).input.InstanceArn,
      );
      return inst ? { ...inst } : {};
    },
  },
  {
    matches: (cmd) => cmd instanceof ListPermissionSetsCommand,
    respond: () => ({ PermissionSets: server.permissionSetArns }),
  },
  {
    matches: (cmd) => cmd instanceof DescribePermissionSetCommand,
    respond: (cmd) => {
      const arn = (cmd as { input: { PermissionSetArn: string } }).input.PermissionSetArn;
      return { PermissionSet: server.permissionSets[arn] ?? {} };
    },
  },
  {
    matches: (cmd) => cmd instanceof GetInlinePolicyForPermissionSetCommand,
    respond: (cmd) => {
      const arn = (cmd as { input: { PermissionSetArn: string } }).input.PermissionSetArn;
      return { InlinePolicy: server.inlinePolicies[arn] ?? "" };
    },
  },
  {
    matches: (cmd) => cmd instanceof ListAccountAssignmentsCommand,
    respond: (cmd) => {
      const input = (cmd as { input: { PermissionSetArn?: string } }).input;
      return {
        AccountAssignments: server.assignments.filter(
          (a) => a.PermissionSetArn === input.PermissionSetArn,
        ),
      };
    },
  },
  {
    matches: (cmd) => cmd instanceof ListApplicationsCommand,
    respond: () => ({ Applications: server.applications }),
  },
  {
    matches: (cmd) => cmd instanceof ListApplicationAssignmentsCommand,
    respond: (cmd) => {
      const arn = (cmd as { input: { ApplicationArn: string } }).input.ApplicationArn;
      return {
        ApplicationAssignments: Array.from(
          { length: server.applicationAssignments[arn] ?? 0 },
          () => ({}),
        ),
      };
    },
  },
  {
    matches: (cmd) => cmd instanceof ListTrustedTokenIssuersCommand,
    respond: () => ({ TrustedTokenIssuers: server.tokenIssuers }),
  },
  {
    matches: (cmd) => cmd instanceof ListRegionsCommand,
    respond: () => ({ Regions: server.regions }),
  },
  {
    matches: (cmd) => cmd instanceof ListAccountAssignmentCreationStatusCommand,
    respond: () => ({ AccountAssignmentsCreationStatus: server.creationStatuses }),
  },
  {
    matches: (cmd) => cmd instanceof ListAccountAssignmentDeletionStatusCommand,
    respond: () => ({ AccountAssignmentsDeletionStatus: server.deletionStatuses }),
  },
  {
    matches: (cmd) => cmd instanceof ListPermissionSetProvisioningStatusCommand,
    respond: () => ({ PermissionSetsProvisioningStatus: server.provisioningStatuses }),
  },
];

function installDefaultHandler() {
  mockSend.mockImplementation((cmd: unknown) => {
    const dispatcher = commandDispatchers.find((d) => d.matches(cmd));
    return Promise.resolve(dispatcher ? dispatcher.respond(cmd) : {});
  });
}

async function renderLoaded() {
  render(SsoAdminPage);
  await waitFor(() => screen.getByText(exampleInstance.Name));
}

async function switchTab(label: string) {
  await fireEvent.click(screen.getByRole("button", { name: label }));
}

/** The Assignments tab's "Permission Set" <select>. */
function permissionSetSelect(): HTMLSelectElement {
  return screen.getByLabelText("Permission Set") as HTMLSelectElement;
}

describe("SsoAdmin Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    resetServer();
    installDefaultHandler();
  });

  it("renders the page title and all tabs", () => {
    render(SsoAdminPage);
    expect(screen.getByText("SSO Admin")).toBeInTheDocument();
    for (const label of [
      "Instances",
      "Permission Sets",
      "Assignments",
      "Applications",
      "Token Issuers",
      "Provisioning",
      "Metrics",
      "Docs",
    ]) {
      expect(screen.getByRole("button", { name: label })).toBeInTheDocument();
    }
  });

  it("loads and auto-selects the first instance, rendering its ARN and identity store", async () => {
    await renderLoaded();
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
    expect(screen.getByText(exampleInstance.InstanceArn)).toBeInTheDocument();
    await waitFor(() => {
      expect(
        screen.getByText(
          (_, el) => el?.textContent === `Identity Store: ${exampleInstance.IdentityStoreId}`,
        ),
      ).toBeInTheDocument();
    });
  });

  it("creates an instance via the Instances tab", async () => {
    await renderLoaded();

    await fireEvent.input(screen.getByPlaceholderText("Instance name"), {
      target: { value: "new-instance" },
    });

    server.instances = [
      exampleInstance,
      {
        InstanceArn: "arn:aws:sso:::instance/ssoins-2",
        Name: "new-instance",
        IdentityStoreId: "d-2222222222",
      },
    ];
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { Name: "new-instance" } }),
    );
    await waitFor(() => {
      expect(screen.getByText("new-instance")).toBeInTheDocument();
    });
  });

  it("deletes an instance after confirming, passing a plain string to confirmDestructive", async () => {
    await renderLoaded();
    server.instances = [];

    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(confirmDestructive).toHaveBeenCalledWith(expect.stringContaining(exampleInstance.Name));
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { InstanceArn: instanceArn } }),
    );
    await waitFor(() => {
      expect(screen.queryByText(exampleInstance.InstanceArn)).not.toBeInTheDocument();
    });
  });

  it("does not delete an instance when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderLoaded();
    const callsBefore = mockSend.mock.calls.length;

    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
    expect(screen.getByText(exampleInstance.InstanceArn)).toBeInTheDocument();
  });

  it("adds and removes a region for the selected instance", async () => {
    await renderLoaded();
    await waitFor(() => screen.getByText("No regions configured for this instance."));

    await fireEvent.input(screen.getByPlaceholderText("e.g. us-west-2"), {
      target: { value: "us-west-2" },
    });
    server.regions = [{ Region: "us-west-2", RegionScopeType: "ALL_REGIONS" }];
    await fireEvent.click(screen.getByRole("button", { name: "Add Region" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { InstanceArn: instanceArn, RegionName: "us-west-2" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("us-west-2")).toBeInTheDocument();
    });

    server.regions = [];
    await fireEvent.click(screen.getByRole("button", { name: "Remove" }));

    expect(confirmDestructive).toHaveBeenCalledWith(expect.stringContaining("us-west-2"));
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { InstanceArn: instanceArn, RegionName: "us-west-2" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("No regions configured for this instance.")).toBeInTheDocument();
    });
  });

  it("creates a permission set and shows its assignment-count and inline-policy badges once loaded", async () => {
    await renderLoaded();
    await switchTab("Permission Sets");
    await waitFor(() => screen.getByPlaceholderText("Permission set name"));

    await fireEvent.input(screen.getByPlaceholderText("Permission set name"), {
      target: { value: "AdminAccess" },
    });
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess", Description: "Full access" };
    server.inlinePolicies[psArn] = '{"Version":"2012-10-17","Statement":[]}';
    server.assignments = [
      {
        PrincipalId: "user-1",
        PrincipalType: "USER",
        AccountId: defaultAccountID,
        PermissionSetArn: psArn,
      },
    ];

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { InstanceArn: instanceArn, Name: "AdminAccess" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("AdminAccess")).toBeInTheDocument();
    });
    expect(screen.getByText("Inline ✓")).toBeInTheDocument();
    expect(screen.getByText("1 assignment")).toBeInTheDocument();
  });

  it("deletes a permission set after confirming", async () => {
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess" };
    await renderLoaded();
    await switchTab("Permission Sets");
    await waitFor(() => screen.getByText("AdminAccess"));

    server.permissionSetArns = [];
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(confirmDestructive).toHaveBeenCalledWith(expect.stringContaining("AdminAccess"));
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { InstanceArn: instanceArn, PermissionSetArn: psArn },
      }),
    );
    await waitFor(() => {
      expect(screen.queryByText("AdminAccess")).not.toBeInTheDocument();
    });
  });

  it("opens the inline policy editor, showing a default policy template when none exists, and saves it", async () => {
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess" };
    await renderLoaded();
    await switchTab("Permission Sets");
    await waitFor(() => screen.getByText("AdminAccess"));

    await fireEvent.click(screen.getByRole("button", { name: "Inline Policy" }));

    await waitFor(() => {
      const textarea = document.querySelector("textarea") as HTMLTextAreaElement;
      expect(textarea.value).toContain('"Effect": "Allow"');
    });

    const textarea = document.querySelector("textarea") as HTMLTextAreaElement;
    await fireEvent.input(textarea, {
      target: {
        value:
          '{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}',
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Save Policy" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          InstanceArn: instanceArn,
          PermissionSetArn: psArn,
          InlinePolicy: JSON.stringify({
            Version: "2012-10-17",
            Statement: [{ Effect: "Deny", Action: "*", Resource: "*" }],
          }),
        },
      }),
    );
    await waitFor(() => {
      // modal closed, list button remains
      expect(screen.queryByText("Inline Policy")).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: "Save Policy" })).not.toBeInTheDocument();
  });

  it("shows a toast error including the AWS error code when loading the inline policy fails", async () => {
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess" };
    await renderLoaded();
    await switchTab("Permission Sets");
    await waitFor(() => screen.getByText("AdminAccess"));

    const error = Object.assign(new Error("Permission set not found."), {
      name: "ResourceNotFoundException",
    });
    mockSend.mockImplementationOnce(() => Promise.reject(error));
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Inline Policy" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        "Failed to load inline policy: ResourceNotFoundException: Permission set not found.",
      );
    });
  });

  it("creates an account assignment, defaulting TargetType/TargetId", async () => {
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess" };
    await renderLoaded();
    await switchTab("Assignments");
    await waitFor(() => screen.getByText("Select permission set"));

    await fireEvent.change(permissionSetSelect(), {
      target: { value: psArn },
    });
    await fireEvent.input(screen.getByPlaceholderText("Principal ID"), {
      target: { value: "user-42" },
    });

    server.assignments = [
      {
        PrincipalId: "user-42",
        PrincipalType: "USER",
        AccountId: defaultAccountID,
        PermissionSetArn: psArn,
      },
    ];
    await fireEvent.click(screen.getByRole("button", { name: "Create Assignment" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          InstanceArn: instanceArn,
          PermissionSetArn: psArn,
          PrincipalType: "USER",
          PrincipalId: "user-42",
          TargetType: "AWS_ACCOUNT",
          TargetId: defaultAccountID,
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("user-42")).toBeInTheDocument();
    });
  });

  it("deletes an account assignment after confirming", async () => {
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess" };
    server.assignments = [
      {
        PrincipalId: "user-42",
        PrincipalType: "USER",
        AccountId: defaultAccountID,
        PermissionSetArn: psArn,
      },
    ];
    await renderLoaded();
    await switchTab("Assignments");
    await fireEvent.change(permissionSetSelect(), {
      target: { value: psArn },
    });
    await waitFor(() => screen.getByText("user-42"));

    server.assignments = [];
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(confirmDestructive).toHaveBeenCalledWith(expect.stringContaining("user-42"));
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          InstanceArn: instanceArn,
          PermissionSetArn: psArn,
          PrincipalType: "USER",
          PrincipalId: "user-42",
          TargetType: "AWS_ACCOUNT",
          TargetId: defaultAccountID,
        },
      }),
    );
    await waitFor(() => {
      expect(screen.queryByText("user-42")).not.toBeInTheDocument();
    });
  });

  it("creates an application and toggles its enabled status", async () => {
    await renderLoaded();
    await switchTab("Applications");
    await waitFor(() => screen.getByPlaceholderText("Application name"));

    await fireEvent.input(screen.getByPlaceholderText("Application name"), {
      target: { value: "my-app" },
    });
    const appArn = "arn:aws:sso::123456789012:application/ssoins-instance1/apl-1";
    server.applications = [{ ApplicationArn: appArn, Name: "my-app", Status: "ENABLED" }];
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          InstanceArn: instanceArn,
          Name: "my-app",
          ApplicationProviderArn: "arn:aws:sso::123456789012:applicationProvider/custom",
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("my-app")).toBeInTheDocument();
    });
    expect(screen.getByText("ENABLED")).toBeInTheDocument();

    server.applications = [{ ApplicationArn: appArn, Name: "my-app", Status: "DISABLED" }];
    await fireEvent.click(screen.getByRole("button", { name: "Disable" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: { ApplicationArn: appArn, Status: "DISABLED" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("DISABLED")).toBeInTheDocument();
    });
  });

  it("deletes an application after confirming", async () => {
    const appArn = "arn:aws:sso::123456789012:application/ssoins-instance1/apl-1";
    server.applications = [{ ApplicationArn: appArn, Name: "my-app", Status: "ENABLED" }];
    await renderLoaded();
    await switchTab("Applications");
    await waitFor(() => screen.getByText("my-app"));

    server.applications = [];
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(confirmDestructive).toHaveBeenCalledWith(expect.stringContaining("my-app"));
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { ApplicationArn: appArn } }),
    );
    await waitFor(() => {
      expect(screen.queryByText("my-app")).not.toBeInTheDocument();
    });
  });

  it("creates and deletes a trusted token issuer with the expected OIDC_JWT configuration", async () => {
    await renderLoaded();
    await switchTab("Token Issuers");
    await waitFor(() => screen.getByPlaceholderText("Trusted token issuer name"));

    await fireEvent.input(screen.getByPlaceholderText("Trusted token issuer name"), {
      target: { value: "demo-issuer" },
    });
    const ttiArn = "arn:aws:sso::123456789012:trustedTokenIssuer/ssoins-instance1/tti-1";
    server.tokenIssuers = [
      {
        TrustedTokenIssuerArn: ttiArn,
        Name: "demo-issuer",
        TrustedTokenIssuerType: "OIDC_JWT",
        TrustedTokenIssuerConfiguration: {
          OidcJwtConfiguration: { IssuerUrl: "https://issuer.example.com" },
        },
      },
    ];
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          InstanceArn: instanceArn,
          Name: "demo-issuer",
          TrustedTokenIssuerType: "OIDC_JWT",
          TrustedTokenIssuerConfiguration: {
            OidcJwtConfiguration: {
              IssuerUrl: "https://issuer.example.com",
              ClaimAttributePath: "sub",
              IdentityStoreAttributePath: "UserName",
              JwksRetrievalOption: "OPEN_ID_DISCOVERY",
            },
          },
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("demo-issuer")).toBeInTheDocument();
    });
    expect(
      screen.getByText((_, el) => el?.textContent === "Issuer: https://issuer.example.com"),
    ).toBeInTheDocument();

    server.tokenIssuers = [];
    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    expect(confirmDestructive).toHaveBeenCalledWith(expect.stringContaining("demo-issuer"));
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { TrustedTokenIssuerArn: ttiArn } }),
    );
    await waitFor(() => {
      expect(screen.queryByText("demo-issuer")).not.toBeInTheDocument();
    });
  });

  it("loads provisioning statuses across all three status lists", async () => {
    server.creationStatuses = [{ RequestId: "req-1", Status: "SUCCEEDED" }];
    server.deletionStatuses = [{ RequestId: "req-2", Status: "IN_PROGRESS" }];
    server.provisioningStatuses = [{ RequestId: "req-3", Status: "FAILED" }];
    await renderLoaded();

    await switchTab("Provisioning");

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: { InstanceArn: instanceArn },
        }),
      );
    });
    await waitFor(() => {
      expect(screen.getByText("req-1")).toBeInTheDocument();
    });
    expect(screen.getByText("req-2")).toBeInTheDocument();
    expect(screen.getByText("req-3")).toBeInTheDocument();
  });

  it("shows computed totals on the Metrics tab", async () => {
    const psArn = "arn:aws:sso:::permissionSet/ssoins-instance1/ps-1";
    server.permissionSetArns = [psArn];
    server.permissionSets[psArn] = { Name: "AdminAccess" };
    server.applications = [{ ApplicationArn: "arn:1", Name: "app1", Status: "ENABLED" }];
    await renderLoaded();

    await switchTab("Metrics");

    await waitFor(() => {
      const instancesCard = screen.getByText("Instances", { selector: "p" }).parentElement;
      expect(within(instancesCard!).getByText("1")).toBeInTheDocument();
    });
    const permSetsCard = screen.getByText("Permission Sets", { selector: "p" }).parentElement;
    expect(within(permSetsCard!).getByText("1")).toBeInTheDocument();
    const appsCard = screen.getByText("Applications", { selector: "p" }).parentElement;
    expect(within(appsCard!).getByText("1")).toBeInTheDocument();
  });

  it("lists supported operations on the Docs tab", async () => {
    render(SsoAdminPage);
    await switchTab("Docs");
    expect(screen.getByText("API Documentation")).toBeInTheDocument();
    expect(screen.getByText("CreateInstance")).toBeInTheDocument();
    expect(screen.getByText("PutInlinePolicyToPermissionSet")).toBeInTheDocument();
  });
});
