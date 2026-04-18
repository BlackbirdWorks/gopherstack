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

describe("ELBv2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ LoadBalancers: [] });
    render(ELBv2Page);
    expect(screen.getByText("Elastic Load Balancing")).toBeInTheDocument();
  });

  it("shows both tabs", () => {
    mockSend.mockResolvedValueOnce({ LoadBalancers: [] });
    render(ELBv2Page);
    expect(screen.getByText("Load Balancers")).toBeInTheDocument();
    expect(screen.getByText("Target Groups")).toBeInTheDocument();
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
});
