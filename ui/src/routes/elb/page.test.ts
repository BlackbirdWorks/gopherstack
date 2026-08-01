import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ElbPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getELBClient: () => ({ send: mockSend }),
}));

const toastError = vi.fn();
const toastSuccess = vi.fn();
vi.mock("svelte-sonner", () => ({
  toast: {
    success: (...a: unknown[]) => toastSuccess(...a),
    error: (...a: unknown[]) => toastError(...a),
  },
}));

const exampleLB = {
  LoadBalancerName: "my-lb",
  DNSName: "my-lb-123456.us-east-1.elb.amazonaws.com",
  Scheme: "internet-facing",
  VPCId: "vpc-abc123",
  AvailabilityZones: ["us-east-1a"],
  Subnets: [],
  SecurityGroups: [],
  ListenerDescriptions: [
    {
      Listener: {
        Protocol: "HTTP",
        LoadBalancerPort: 80,
        InstanceProtocol: "HTTP",
        InstancePort: 8080,
      },
      PolicyNames: [],
    },
  ],
  Instances: [],
  HealthCheck: {
    Target: "HTTP:8080/health",
    Interval: 30,
    Timeout: 5,
    HealthyThreshold: 2,
    UnhealthyThreshold: 2,
  },
};

describe("ELB (Classic Load Balancers) Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [] });
    render(ElbPage);
    expect(screen.getByText("Classic Load Balancers (ELB)")).toBeInTheDocument();
  });

  it("loads load balancers on mount with an empty DescribeLoadBalancersCommand input", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => {
      expect(screen.getByText("my-lb")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(expect.objectContaining({ input: {} }));
  });

  it("shows an empty state when there are no load balancers", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [] });
    render(ElbPage);
    await waitFor(() => {
      expect(screen.getByText("No load balancers found.")).toBeInTheDocument();
    });
    expect(screen.getByText("Select a load balancer to view details")).toBeInTheDocument();
  });

  it("selects a load balancer and loads its attributes and policies", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({
      LoadBalancerAttributes: {
        CrossZoneLoadBalancing: { Enabled: true },
        ConnectionDraining: { Enabled: true, Timeout: 120 },
        ConnectionSettings: { IdleTimeout: 45 },
      },
    });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });

    await fireEvent.click(screen.getByText("my-lb"));

    await waitFor(() => {
      expect(screen.getByText(exampleLB.DNSName)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { LoadBalancerName: "my-lb" } }),
    );
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { LoadBalancerName: "my-lb" } }),
    );
  });

  it("creates a load balancer via the modal, then auto-selects it", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [] });
    render(ElbPage);
    await waitFor(() => screen.getByText("No load balancers found."));

    await fireEvent.click(screen.getByText("+ Create Load Balancer"));
    // This page's modal <label> elements have no `for` attribute pointing
    // at their inputs (confirmed by grepping the whole file: every <label>
    // is a plain sibling, not a wrapper, and only one input in the entire
    // page even has an `id`) -- a real accessibility bug, but out of scope
    // to fix here (it spans ~25 fields across every modal). Select by
    // placeholder instead of getByLabelText to work around it.
    await fireEvent.input(screen.getByPlaceholderText("my-lb"), { target: { value: "new-lb" } });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LoadBalancerDescriptions: [{ ...exampleLB, LoadBalancerName: "new-lb" }],
    });
    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(exampleLB.DNSName)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: {
          LoadBalancerName: "new-lb",
          AvailabilityZones: ["us-east-1a"],
          Scheme: "internet-facing",
          Listeners: [
            { Protocol: "HTTP", LoadBalancerPort: 80, InstanceProtocol: "HTTP", InstancePort: 80 },
          ],
        },
      }),
    );
  });

  it("deletes a load balancer immediately, with no confirmation dialog", async () => {
    // Unlike dms/detective (confirmDestructive) and sesv2 (same) and
    // lakeformation (its own inline confirm modal), this page has no
    // confirmation step at all before DeleteLoadBalancerCommand fires --
    // confirmed by grepping the page for confirmDestructive / a custom
    // confirm modal and finding neither. Test reflects the actual (no
    // confirmation) behavior rather than one this page doesn't implement.
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    await waitFor(() => screen.getByText("No load balancers found."));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { LoadBalancerName: "my-lb" } }),
    );
  });

  it("shows a toast with the AWS error message when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(ElbPage);

    await waitFor(() => {
      expect(toastError).toHaveBeenCalledWith("Failed to load balancers: Rate exceeded.");
    });
  });

  it("adds a listener from the Listeners tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    await fireEvent.click(screen.getByRole("button", { name: "listeners" }));
    await waitFor(() => screen.getByText("Listeners"));

    await fireEvent.click(screen.getByText("+ Add"));
    // The Add Listener modal has exactly one <select> (Protocol); its
    // default LB Port (443) and Instance Port (8443) already match what
    // this test expects, so only Protocol and the cert ARN need changing.
    // (See the "creates a load balancer" test above for why this isn't
    // getByLabelText.)
    await fireEvent.change(screen.getByRole("combobox"), { target: { value: "HTTPS" } });
    await fireEvent.input(screen.getByPlaceholderText("arn:aws:acm:…"), {
      target: { value: "arn:aws:acm:us-east-1:123456789012:certificate/abc" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LoadBalancerDescriptions: [
        {
          ...exampleLB,
          ListenerDescriptions: [
            ...exampleLB.ListenerDescriptions,
            {
              Listener: {
                Protocol: "HTTPS",
                LoadBalancerPort: 443,
                InstanceProtocol: "HTTP",
                InstancePort: 8443,
              },
              PolicyNames: [],
            },
          ],
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            LoadBalancerName: "my-lb",
            Listeners: [
              {
                Protocol: "HTTPS",
                LoadBalancerPort: 443,
                InstanceProtocol: "HTTP",
                InstancePort: 8443,
              },
            ],
          },
        }),
      );
    });
    // SetLoadBalancerListenerSSLCertificateCommand follows because a cert
    // ARN was provided for an HTTPS listener.
    expect(mockSend).toHaveBeenNthCalledWith(
      5,
      expect.objectContaining({
        input: {
          LoadBalancerName: "my-lb",
          LoadBalancerPort: 443,
          SSLCertificateId: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
        },
      }),
    );
  });

  it("removes a listener immediately from the Listeners tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    await fireEvent.click(screen.getByRole("button", { name: "listeners" }));
    await waitFor(() => screen.getByText("HTTP"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LoadBalancerDescriptions: [{ ...exampleLB, ListenerDescriptions: [] }],
    });

    await fireEvent.click(screen.getByText("Remove"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: { LoadBalancerName: "my-lb", LoadBalancerPorts: [80] },
        }),
      );
    });
  });

  it("saves health check settings from the Health Check tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    await fireEvent.click(screen.getByRole("button", { name: "Health Check" }));
    // The Target label/input pair has no `for`/`id` and no placeholder;
    // it's the sole text input in this tab (the rest are type="number"),
    // and is its label's next DOM sibling per the markup.
    const targetInput = screen.getByText("Target (e.g. HTTP:80/health)")
      .nextElementSibling as HTMLInputElement;
    await fireEvent.input(targetInput, {
      target: { value: "HTTP:9090/status" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            LoadBalancerName: "my-lb",
            HealthCheck: {
              Target: "HTTP:9090/status",
              Interval: 30,
              Timeout: 5,
              HealthyThreshold: 2,
              UnhealthyThreshold: 2,
            },
          },
        }),
      );
    });
  });

  it("saves attributes from the Attributes tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    await fireEvent.click(screen.getByRole("button", { name: "attributes" }));
    await fireEvent.click(screen.getByLabelText("Cross-Zone Load Balancing"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LoadBalancerAttributes: { CrossZoneLoadBalancing: { Enabled: true } },
    });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        4,
        expect.objectContaining({
          input: {
            LoadBalancerName: "my-lb",
            LoadBalancerAttributes: {
              CrossZoneLoadBalancing: { Enabled: true },
              ConnectionDraining: { Enabled: false, Timeout: 300 },
              ConnectionSettings: { IdleTimeout: 60 },
            },
          },
        }),
      );
    });
  });

  it("registers and deregisters an instance from the Instances tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    await fireEvent.click(screen.getByRole("button", { name: "instances" }));
    await waitFor(() => screen.getByText("No instances registered."));

    await fireEvent.input(screen.getByPlaceholderText("i-0123456789abcdef0"), {
      target: { value: "i-0123456789abcdef0" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LoadBalancerDescriptions: [
        { ...exampleLB, Instances: [{ InstanceId: "i-0123456789abcdef0" }] },
      ],
    });
    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Register" }));

    await waitFor(() => screen.getByText("i-0123456789abcdef0"));
    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: {
          LoadBalancerName: "my-lb",
          Instances: [{ InstanceId: "i-0123456789abcdef0" }],
        },
      }),
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [{ ...exampleLB, Instances: [] }] });
    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });

    await fireEvent.click(screen.getByText("Deregister"));

    await waitFor(() => screen.getByText("No instances registered."));
    expect(mockSend).toHaveBeenNthCalledWith(
      8,
      expect.objectContaining({
        input: {
          LoadBalancerName: "my-lb",
          Instances: [{ InstanceId: "i-0123456789abcdef0" }],
        },
      }),
    );
  });

  it("creates a stickiness policy from the Policies tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    await fireEvent.click(screen.getByRole("button", { name: "policies" }));
    await waitFor(() => screen.getByText("No policies defined."));

    await fireEvent.click(screen.getByText("+ Create Policy"));
    await fireEvent.input(screen.getByPlaceholderText("my-policy"), {
      target: { value: "sticky-1" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({
      PolicyDescriptions: [
        { PolicyName: "sticky-1", PolicyTypeName: "LBCookieStickinessPolicyType" },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText("sticky-1")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: {
          LoadBalancerName: "my-lb",
          PolicyName: "sticky-1",
          CookieExpirationPeriod: undefined,
        },
      }),
    );
    expect(screen.getByText("LBCookieStickinessPolicyType")).toBeInTheDocument();
  });

  it("enables and disables an availability zone from the Overview tab", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancerDescriptions: [exampleLB] });
    render(ElbPage);
    await waitFor(() => screen.getByText("my-lb"));

    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });
    await fireEvent.click(screen.getByText("my-lb"));
    await waitFor(() => screen.getByText(exampleLB.DNSName));

    const azContainer = screen.getByText("Availability Zones").closest("div") as HTMLElement;
    await fireEvent.input(within(azContainer).getByPlaceholderText("us-east-1c"), {
      target: { value: "us-east-1b" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      LoadBalancerDescriptions: [{ ...exampleLB, AvailabilityZones: ["us-east-1a", "us-east-1b"] }],
    });
    mockSend.mockResolvedValueOnce({ LoadBalancerAttributes: {} });
    mockSend.mockResolvedValueOnce({ PolicyDescriptions: [] });

    await fireEvent.click(within(azContainer).getByText("+"));

    await waitFor(() => screen.getByText("us-east-1b"));
    expect(mockSend).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        input: { LoadBalancerName: "my-lb", AvailabilityZones: ["us-east-1b"] },
      }),
    );
  });
});
