import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import Wafv2Page from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getWAFV2Client: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

const exampleAcl = {
  Name: "my-acl",
  Id: "acl-id-1",
  ARN: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/my-acl/acl-id-1",
  Description: "Protects my app",
  LockToken: "lock-token-1",
};

const exampleAclDetail = {
  ...exampleAcl,
  DefaultAction: { Allow: {} },
  Capacity: 50,
  Rules: [
    {
      Name: "block-bad-ips",
      Priority: 0,
      Action: { Block: {} },
      Statement: { IPSetReferenceStatement: { ARN: "arn:aws:wafv2:...:ipset/x" } },
    },
  ],
};

const exampleIpSet = { Name: "my-ipset", Id: "ipset-1", ARN: "arn:aws:wafv2:...:ipset/my-ipset" };
const exampleIpSetDetail = {
  ...exampleIpSet,
  IPAddressVersion: "IPV4",
  Addresses: ["10.0.0.1/32"],
};
const exampleRegexSet = {
  Name: "my-regex",
  Description: "digits only",
  ARN: "arn:aws:wafv2:...:regexset/x",
};
const exampleRuleGroup = {
  Name: "my-rulegroup",
  Description: "shared rules",
  ARN: "arn:aws:wafv2:...:rulegroup/x",
};
const exampleLoggingConfig = {
  ResourceArn: exampleAcl.ARN,
  LogDestinationConfigs: [
    "arn:aws:kinesisfirehose:us-east-1:123456789012:deliverystream/aws-waf-logs",
  ],
};

async function renderWithAcl() {
  mockSend.mockResolvedValueOnce({ WebACLs: [exampleAcl] });
  render(Wafv2Page);
  await waitFor(() => screen.getByText("my-acl"));
}

describe("WAF v2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title and all tabs", () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    expect(screen.getByText("AWS WAF v2")).toBeInTheDocument();
    expect(screen.getByText("IP Sets")).toBeInTheDocument();
    expect(screen.getByText("Regex Sets")).toBeInTheDocument();
    expect(screen.getByText("Rule Groups")).toBeInTheDocument();
    expect(screen.getByText("Logging")).toBeInTheDocument();
  });

  it("lists Web ACLs on mount with the default REGIONAL scope and no pagination token", async () => {
    await renderWithAcl();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { Scope: "REGIONAL", Limit: 100 } }),
    );
    expect(screen.getByText("my-acl")).toBeInTheDocument();
    expect(screen.getByText("Protects my app")).toBeInTheDocument();
    expect(screen.getByText(exampleAcl.ARN)).toBeInTheDocument();
  });

  it("shows the AWS error code in the toast when loading Web ACLs fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    render(Wafv2Page);

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("ThrottlingException: Rate exceeded."),
      );
    });
  });

  it("creates a Web ACL via the labeled modal with the exact CreateWebACL shape", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    await fireEvent.click(screen.getByText("Create Web ACL"));
    await fireEvent.input(screen.getByLabelText("Web ACL Name"), {
      target: { value: "new-acl" },
    });
    await fireEvent.input(screen.getByLabelText("Description (optional)"), {
      target: { value: "my new acl" },
    });

    mockSend.mockResolvedValueOnce({ Summary: exampleAcl });
    mockSend.mockResolvedValueOnce({ WebACLs: [exampleAcl] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => screen.getByText("my-acl"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          Name: "new-acl",
          Scope: "REGIONAL",
          Description: "my new acl",
          DefaultAction: { Allow: {} },
          VisibilityConfig: {
            SampledRequestsEnabled: true,
            CloudWatchMetricsEnabled: true,
            MetricName: "newacl",
          },
          Rules: [],
        },
      }),
    );
  });

  it("shows the AWS error code in the toast when creating a Web ACL fails", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    await fireEvent.click(screen.getByText("Create Web ACL"));
    await fireEvent.input(screen.getByLabelText("Web ACL Name"), {
      target: { value: "new-acl" },
    });

    const error = Object.assign(new Error("A Web ACL with this name already exists."), {
      name: "WAFDuplicateItemException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining(
          "WAFDuplicateItemException: A Web ACL with this name already exists.",
        ),
      );
    });
  });

  it("deletes a Web ACL (including its LockToken) after confirming", async () => {
    await renderWithAcl();

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ WebACLs: [] });

    await fireEvent.click(screen.getByTitle("Delete Web ACL"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          input: {
            Name: "my-acl",
            Scope: "REGIONAL",
            Id: "acl-id-1",
            LockToken: "lock-token-1",
          },
        }),
      );
    });
    await waitFor(() => screen.getByText("No Web ACLs found"));
  });

  it("does not delete the Web ACL when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderWithAcl();

    await fireEvent.click(screen.getByTitle("Delete Web ACL"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText("my-acl")).toBeInTheDocument();
  });

  it("selects a Web ACL and renders its rules table and stats", async () => {
    await renderWithAcl();

    mockSend.mockResolvedValueOnce({ WebACL: exampleAclDetail });
    await fireEvent.click(screen.getByText("my-acl"));

    await waitFor(() => screen.getByText("block-bad-ips"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: { Name: "my-acl", Scope: "REGIONAL", Id: "acl-id-1" },
      }),
    );
    expect(screen.getByText("IP Set")).toBeInTheDocument();
  });

  it("shows the AWS error code in the toast when loading Web ACL details fails", async () => {
    await renderWithAcl();

    const error = Object.assign(new Error("Web ACL not found."), {
      name: "WAFNonexistentItemException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText("my-acl"));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining("WAFNonexistentItemException: Web ACL not found."),
      );
    });
  });

  it("loads sampled requests for a selected Web ACL and renders a request row", async () => {
    await renderWithAcl();
    mockSend.mockResolvedValueOnce({ WebACL: exampleAclDetail });
    await fireEvent.click(screen.getByText("my-acl"));
    await waitFor(() => screen.getByText("block-bad-ips"));

    mockSend.mockResolvedValueOnce({
      SampledRequests: [
        {
          Request: { ClientIP: "203.0.113.5", Method: "GET", URI: "/login" },
          Action: "BLOCK",
          RuleNameWithinRuleGroup: "block-bad-ips",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Sample Requests"));

    await waitFor(() => screen.getByText("203.0.113.5"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: expect.objectContaining({
          WebAclArn: exampleAcl.ARN,
          Scope: "REGIONAL",
          MaxItems: 20,
        }),
      }),
    );
    expect(screen.getByText("/login")).toBeInTheDocument();
  });

  it("switches to IP Sets, lists them, and selects one to view its addresses", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    mockSend.mockResolvedValueOnce({ IPSets: [exampleIpSet] });
    await fireEvent.click(screen.getByText("IP Sets"));
    await waitFor(() => screen.getByText("my-ipset"));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { Scope: "REGIONAL", Limit: 100 } }),
    );

    mockSend.mockResolvedValueOnce({ IPSet: exampleIpSetDetail });
    await fireEvent.click(screen.getByText("my-ipset"));

    await waitFor(() => screen.getByText("10.0.0.1/32"));
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { Name: "my-ipset", Scope: "REGIONAL", Id: "ipset-1" },
      }),
    );
  });

  it("switches to Regex Sets and renders name/description/ARN cells", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    mockSend.mockResolvedValueOnce({ RegexPatternSets: [exampleRegexSet] });
    await fireEvent.click(screen.getByText("Regex Sets"));

    await waitFor(() => screen.getByText("my-regex"));
    expect(screen.getByText("digits only")).toBeInTheDocument();
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { Scope: "REGIONAL", Limit: 100 } }),
    );
  });

  it("switches to Rule Groups and renders name/description/ARN cells", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    mockSend.mockResolvedValueOnce({ RuleGroups: [exampleRuleGroup] });
    await fireEvent.click(screen.getByText("Rule Groups"));

    await waitFor(() => screen.getByText("my-rulegroup"));
    expect(screen.getByText("shared rules")).toBeInTheDocument();
  });

  it("switches to Logging, adds a config via the labeled modal, and deletes it after confirming", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    mockSend.mockResolvedValueOnce({ LoggingConfigurations: [] });
    await fireEvent.click(screen.getByText("Logging"));
    await waitFor(() => screen.getByText("No logging configurations"));

    await fireEvent.click(screen.getByText("Add Logging"));
    await fireEvent.input(screen.getByLabelText("Web ACL ARN"), {
      target: { value: exampleLoggingConfig.ResourceArn },
    });
    await fireEvent.input(
      screen.getByLabelText("Log Destination ARN (Kinesis, S3, or CloudWatch)"),
      {
        target: { value: exampleLoggingConfig.LogDestinationConfigs[0] },
      },
    );

    mockSend.mockResolvedValueOnce({ LoggingConfiguration: exampleLoggingConfig });
    mockSend.mockResolvedValueOnce({ LoggingConfigurations: [exampleLoggingConfig] });
    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => screen.getByText(exampleLoggingConfig.ResourceArn));
    // Call order: 1) initial ListWebACLs, 2) ListLoggingConfigurations (tab
    // click), 3) PutLoggingConfiguration (this Save), 4) reload.
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          LoggingConfiguration: {
            ResourceArn: exampleLoggingConfig.ResourceArn,
            LogDestinationConfigs: exampleLoggingConfig.LogDestinationConfigs,
          },
        },
      }),
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LoggingConfigurations: [] });
    await fireEvent.click(screen.getByTitle("Remove logging"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        5,
        expect.objectContaining({ input: { ResourceArn: exampleLoggingConfig.ResourceArn } }),
      );
    });
    await waitFor(() => screen.getByText("No logging configurations"));
  });

  it("does not delete the logging config when the confirm dialog is declined", async () => {
    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("No Web ACLs found"));

    mockSend.mockResolvedValueOnce({ LoggingConfigurations: [exampleLoggingConfig] });
    await fireEvent.click(screen.getByText("Logging"));
    await waitFor(() => screen.getByText(exampleLoggingConfig.ResourceArn));

    confirmDestructive.mockResolvedValue(false);
    await fireEvent.click(screen.getByTitle("Remove logging"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(screen.getByText(exampleLoggingConfig.ResourceArn)).toBeInTheDocument();
  });

  it("reloads the active tab's data with the new scope when the Scope select changes", async () => {
    await renderWithAcl();

    mockSend.mockResolvedValueOnce({ WebACLs: [] });
    await fireEvent.change(screen.getByLabelText("Scope:"), { target: { value: "CLOUDFRONT" } });

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ input: { Scope: "CLOUDFRONT", Limit: 100 } }),
      );
    });
  });

  it("filters the Web ACL list by the search box", async () => {
    mockSend.mockResolvedValueOnce({
      WebACLs: [exampleAcl, { ...exampleAcl, Name: "other-acl", Id: "acl-id-2" }],
    });
    render(Wafv2Page);
    await waitFor(() => screen.getByText("my-acl"));
    expect(screen.getByText("other-acl")).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Search..."), {
      target: { value: "other" },
    });

    expect(screen.getByText("other-acl")).toBeInTheDocument();
    expect(screen.queryByText("my-acl")).not.toBeInTheDocument();
  });
});
