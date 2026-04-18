import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import AutoScalingPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAutoScalingClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Auto Scaling Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ AutoScalingGroups: [] });
    render(AutoScalingPage);
    expect(screen.getByText("Auto Scaling Groups")).toBeInTheDocument();
  });

  it("shows Create Group button", () => {
    mockSend.mockResolvedValue({ AutoScalingGroups: [] });
    render(AutoScalingPage);
    expect(screen.getByText("Create Group")).toBeInTheDocument();
  });

  it("shows empty state when no groups", async () => {
    mockSend.mockResolvedValue({ AutoScalingGroups: [] });
    render(AutoScalingPage);
    await waitFor(
      () => {
        expect(screen.getByText("No Auto Scaling Groups found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded groups", async () => {
    mockSend.mockResolvedValue({
      AutoScalingGroups: [
        {
          AutoScalingGroupName: "web-tier-asg",
          MinSize: 1,
          MaxSize: 10,
          DesiredCapacity: 3,
          AvailabilityZones: ["us-east-1a"],
          Instances: [],
        },
        {
          AutoScalingGroupName: "api-tier-asg",
          MinSize: 2,
          MaxSize: 20,
          DesiredCapacity: 4,
          AvailabilityZones: ["us-east-1a", "us-east-1b"],
          Instances: [{ InstanceId: "i-abc", LifecycleState: "InService" }],
        },
      ],
    });
    render(AutoScalingPage);
    await waitFor(
      () => {
        expect(screen.getByText("web-tier-asg")).toBeInTheDocument();
        expect(screen.getByText("api-tier-asg")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create group modal", async () => {
    mockSend.mockResolvedValue({ AutoScalingGroups: [] });
    render(AutoScalingPage);
    await fireEvent.click(screen.getByText("Create Group"));
    expect(screen.getByText("Group Name")).toBeInTheDocument();
    expect(screen.getByText("Launch Template Name")).toBeInTheDocument();
  });

  it("cancels create group modal", async () => {
    mockSend.mockResolvedValue({ AutoScalingGroups: [] });
    render(AutoScalingPage);
    await fireEvent.click(screen.getByText("Create Group"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Group Name")).not.toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ AutoScalingGroups: [] });
    render(AutoScalingPage);
    expect(screen.getByPlaceholderText("Search groups...")).toBeInTheDocument();
  });

  it("shows capacity columns", async () => {
    mockSend.mockResolvedValue({
      AutoScalingGroups: [
        {
          AutoScalingGroupName: "test-asg",
          MinSize: 1,
          MaxSize: 5,
          DesiredCapacity: 2,
          AvailabilityZones: ["us-east-1a"],
          Instances: [],
        },
      ],
    });
    render(AutoScalingPage);
    await waitFor(
      () => {
        expect(screen.getByText("Desired / Min / Max")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
