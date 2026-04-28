import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import ELBv2Page from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getELBv2Client: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

describe("ELBv2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ LoadBalancers: [] });
    render(ELBv2Page);
    expect(screen.getByText("Elastic Load Balancing v2")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ LoadBalancers: [] });
    render(ELBv2Page);
    expect(screen.getByText("Load Balancers")).toBeInTheDocument();
    expect(screen.getByText("Target Groups")).toBeInTheDocument();
    expect(screen.getByText("Listeners & Rules")).toBeInTheDocument();
    expect(screen.getByText("Trust Stores")).toBeInTheDocument();
  });

  it("displays load balancers", async () => {
    mockSend.mockResolvedValueOnce({
      LoadBalancers: [
        {
          LoadBalancerArn: "arn:lb:1",
          LoadBalancerName: "my-alb",
          Type: "application",
          Scheme: "internet-facing",
          State: { Code: "active" },
          DNSName: "my-alb.elb.amazonaws.com",
        },
      ],
    });

    render(ELBv2Page);

    await waitFor(() => {
      expect(screen.getByText("my-alb")).toBeInTheDocument();
    });
  });

  it("shows create modal", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancers: [] });
    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Create"));
    expect(screen.getByText("Create Load Balancer")).toBeInTheDocument();
  });

  it("switches to target groups tab", async () => {
    mockSend
      .mockResolvedValueOnce({ LoadBalancers: [] })
      .mockResolvedValueOnce({ TargetGroups: [] });
    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Target Groups"));
    await waitFor(() => {
      expect(screen.getByText("No target groups found")).toBeInTheDocument();
    });
  });

  it("switches to listeners tab and shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ LoadBalancers: [] }).mockResolvedValueOnce({ Listeners: [] });
    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Listeners & Rules"));
    await waitFor(() => {
      expect(screen.getByText("No listeners found")).toBeInTheDocument();
    });
  });

  it("displays listeners with rules and certs when one is selected", async () => {
    const listenerArn = "arn:aws:elasticloadbalancing:us-east-1:123:listener/app/my-alb/abc/80";
    mockSend
      // onMount parallel loads
      .mockResolvedValueOnce({ LoadBalancers: [] })
      .mockResolvedValueOnce({ TargetGroups: [] })
      .mockResolvedValueOnce({ Listeners: [] })
      .mockResolvedValueOnce({ TrustStores: [] })
      // tab click "Listeners & Rules"
      .mockResolvedValueOnce({
        Listeners: [
          {
            ListenerArn: listenerArn,
            Protocol: "HTTP",
            Port: 80,
            DefaultActions: [{ Type: "forward", TargetGroupArn: "arn:tg:1" }],
          },
        ],
      })
      // select listener: rules + certs in parallel
      .mockResolvedValueOnce({
        Rules: [
          {
            RuleArn: "arn:rule:1",
            Priority: "1",
            IsDefault: false,
            Conditions: [{ Field: "host-header", HostHeaderConfig: { Values: ["example.com"] } }],
            Actions: [{ Type: "forward", TargetGroupArn: "arn:tg:1" }],
          },
        ],
      })
      .mockResolvedValueOnce({ Certificates: [] });

    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Listeners & Rules"));

    await waitFor(() => {
      expect(screen.getByText("HTTP:80")).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("HTTP:80"));

    await waitFor(() => {
      expect(screen.getByText(/priority 1/)).toBeInTheDocument();
      expect(screen.getByText(/Host: example\.com/)).toBeInTheDocument();
    });
  });

  it("switches to trust stores tab and shows empty state", async () => {
    mockSend
      .mockResolvedValueOnce({ LoadBalancers: [] })
      .mockResolvedValueOnce({ TrustStores: [] });
    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Trust Stores"));
    await waitFor(() => {
      expect(screen.getByText("No trust stores found")).toBeInTheDocument();
    });
  });

  it("shows trust store revocations when one is selected", async () => {
    mockSend
      // onMount parallel loads
      .mockResolvedValueOnce({ LoadBalancers: [] })
      .mockResolvedValueOnce({ TargetGroups: [] })
      .mockResolvedValueOnce({ Listeners: [] })
      .mockResolvedValueOnce({ TrustStores: [] })
      // tab click "Trust Stores"
      .mockResolvedValueOnce({
        TrustStores: [
          { TrustStoreArn: "arn:ts:1", Name: "my-ts", Status: "ACTIVE", TotalRevokedEntries: 2 },
        ],
      })
      // select trust store revocations
      .mockResolvedValueOnce({
        TrustStoreRevocations: [{ RevocationId: "rev-001", RevocationType: "CRL" }],
      });
    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Trust Stores"));
    await waitFor(() => {
      expect(screen.getByText("my-ts")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("my-ts"));
    await waitFor(() => {
      expect(screen.getByText(/rev-001/)).toBeInTheDocument();
    });
  });

  it("opens rule editor when Add Rule is clicked", async () => {
    const listenerArn = "arn:aws:elasticloadbalancing:us-east-1:123:listener/app/my-alb/abc/80";
    mockSend
      // onMount parallel loads
      .mockResolvedValueOnce({ LoadBalancers: [] })
      .mockResolvedValueOnce({ TargetGroups: [] })
      .mockResolvedValueOnce({ Listeners: [] })
      .mockResolvedValueOnce({ TrustStores: [] })
      // tab click "Listeners & Rules"
      .mockResolvedValueOnce({
        Listeners: [
          {
            ListenerArn: listenerArn,
            Protocol: "HTTPS",
            Port: 443,
            DefaultActions: [{ Type: "forward" }],
          },
        ],
      })
      // select listener: rules + certs in parallel
      .mockResolvedValueOnce({ Rules: [] })
      .mockResolvedValueOnce({ Certificates: [] });

    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Listeners & Rules"));
    await waitFor(() => expect(screen.getByText("HTTPS:443")).toBeInTheDocument());
    await fireEvent.click(screen.getByText("HTTPS:443"));
    await waitFor(() => expect(screen.getByText("Add Rule")).toBeInTheDocument());
    await fireEvent.click(screen.getByText("Add Rule"));
    expect(screen.getByText("Create Rule")).toBeInTheDocument();
    expect(screen.getByText("Condition Type")).toBeInTheDocument();
  });

  it("opens add certificate modal", async () => {
    const listenerArn = "arn:aws:elasticloadbalancing:us-east-1:123:listener/app/my-alb/abc/443";
    mockSend
      // onMount parallel loads
      .mockResolvedValueOnce({ LoadBalancers: [] })
      .mockResolvedValueOnce({ TargetGroups: [] })
      .mockResolvedValueOnce({ Listeners: [] })
      .mockResolvedValueOnce({ TrustStores: [] })
      // tab click "Listeners & Rules"
      .mockResolvedValueOnce({
        Listeners: [{ ListenerArn: listenerArn, Protocol: "HTTPS", Port: 443, DefaultActions: [] }],
      })
      // select listener: rules + certs in parallel
      .mockResolvedValueOnce({ Rules: [] })
      .mockResolvedValueOnce({ Certificates: [] });

    render(ELBv2Page);
    await fireEvent.click(screen.getByText("Listeners & Rules"));
    await waitFor(() => expect(screen.getByText("HTTPS:443")).toBeInTheDocument());
    await fireEvent.click(screen.getByText("HTTPS:443"));
    await waitFor(() =>
      expect(screen.getByText("No additional certificates.")).toBeInTheDocument(),
    );
    const addButtons = screen.getAllByText("Add");
    await fireEvent.click(addButtons.at(-1)!);
    expect(screen.getByText("Add Certificate")).toBeInTheDocument();
  });
});
