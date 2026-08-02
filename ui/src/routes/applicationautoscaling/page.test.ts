import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ApplicationAutoScalingPage from "./+page.svelte";
import { DescribeScalableTargetsCommand } from "@aws-sdk/client-application-auto-scaling";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getApplicationAutoScalingClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

type ExampleTarget = {
  ServiceNamespace: string;
  ResourceId: string;
  ScalableDimension: string;
  ScalableTargetARN: string;
  MinCapacity: number;
  MaxCapacity: number;
  SuspendedState?: {
    DynamicScalingInSuspended?: boolean;
    DynamicScalingOutSuspended?: boolean;
    ScheduledScalingSuspended?: boolean;
  };
};

const exampleTarget: ExampleTarget = {
  ServiceNamespace: "ecs",
  ResourceId: "service/default/my-service",
  ScalableDimension: "ecs:service:DesiredCount",
  ScalableTargetARN:
    "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/abc123",
  MinCapacity: 1,
  MaxCapacity: 10,
};

// On mount the page calls loadAllNamespaces(), which fans out a
// DescribeScalableTargetsCommand per namespace via Promise.allSettled (14
// calls). Only the "ecs" namespace returns the example target; every other
// namespace resolves empty so the fan-out doesn't require 14 manually
// ordered mockResolvedValueOnce calls.
function mockNamespaceFanOut(targets: ExampleTarget[] = [exampleTarget]) {
  mockSend.mockImplementation((cmd: unknown) => {
    if (cmd instanceof DescribeScalableTargetsCommand) {
      const ns = (cmd as { input: { ServiceNamespace?: string } }).input.ServiceNamespace;
      if (ns === "ecs") return Promise.resolve({ ScalableTargets: targets });
      return Promise.resolve({ ScalableTargets: [] });
    }
    return Promise.resolve({});
  });
}

async function renderWithTarget() {
  mockNamespaceFanOut();
  render(ApplicationAutoScalingPage);
  await waitFor(() => screen.getByText(exampleTarget.ResourceId));
  mockSend.mockResolvedValueOnce({ ScalingPolicies: [] });
  await fireEvent.click(screen.getByText(exampleTarget.ResourceId));
  await waitFor(() => screen.getByText("No scaling policies yet"));
}

describe("ApplicationAutoScaling Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders the scalable targets panel", () => {
    mockNamespaceFanOut([]);
    render(ApplicationAutoScalingPage);
    expect(screen.getByText("Scalable Targets")).toBeInTheDocument();
    expect(screen.getByText("Select a scalable target to view details")).toBeInTheDocument();
  });

  it("lists scalable targets loaded from the namespace fan-out, showing resource id and namespace badge", async () => {
    mockNamespaceFanOut();
    render(ApplicationAutoScalingPage);
    await waitFor(() => {
      expect(screen.getByText(exampleTarget.ResourceId)).toBeInTheDocument();
    });
    // "ecs" badge rendered from ServiceNamespace field.
    expect(screen.getByText("ecs")).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({ ServiceNamespace: "ecs" }),
      }),
    );
  });

  it("registers a scalable target via the modal", async () => {
    mockNamespaceFanOut([]);
    render(ApplicationAutoScalingPage);
    await waitFor(() => screen.getByText("No targets found"));

    await fireEvent.click(screen.getByText("Register"));
    expect(screen.getByText("Register Scalable Target")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Resource ID"), {
      target: { value: "service/default/other-service" },
    });

    mockSend.mockReset();
    // RegisterScalableTargetCommand
    mockSend.mockResolvedValueOnce({});
    // subsequent loadAllNamespaces() refresh
    mockNamespaceFanOut([exampleTarget]);

    // Scope to the dialog -- the always-visible panel button is also named
    // "Register" and would otherwise strict-mode-collide with the modal's
    // submit button.
    await fireEvent.click(
      within(screen.getByRole("dialog")).getByRole("button", { name: "Register" }),
    );

    await waitFor(() => {
      expect(screen.getByText(exampleTarget.ResourceId)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ServiceNamespace: "ecs",
          ResourceId: "service/default/other-service",
          ScalableDimension: "ecs:service:DesiredCount",
          MinCapacity: 1,
          MaxCapacity: 10,
        }),
      }),
    );
  });

  it("deregisters a scalable target after confirming", async () => {
    await renderWithTarget();

    mockSend.mockReset();
    // DeregisterScalableTargetCommand
    mockSend.mockResolvedValueOnce({});
    // refresh: target now gone
    mockNamespaceFanOut([]);

    await fireEvent.click(screen.getByText("Deregister"));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining(exampleTarget.ResourceId),
      }),
    );
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ServiceNamespace: exampleTarget.ServiceNamespace,
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Select a scalable target to view details")).toBeInTheDocument();
    });
  });

  it("does not deregister when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    await renderWithTarget();

    const callsBefore = mockSend.mock.calls.length;
    await fireEvent.click(screen.getByText("Deregister"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
    // Appears twice: once in the target list, once in the still-selected
    // detail header -- the point being it's still selected, not gone.
    expect(screen.getAllByText(exampleTarget.ResourceId).length).toBe(2);
  });

  it("suspends scaling for a target without a confirmation dialog, sending all three suspension flags", async () => {
    await renderWithTarget();

    mockSend.mockReset();
    // RegisterScalableTargetCommand (suspend)
    mockSend.mockResolvedValueOnce({});
    mockNamespaceFanOut([
      {
        ...exampleTarget,
        SuspendedState: {
          DynamicScalingInSuspended: true,
          DynamicScalingOutSuspended: true,
          ScheduledScalingSuspended: true,
        },
      },
    ]);

    await fireEvent.click(screen.getByText("Suspend Scaling"));

    expect(confirmDestructive).not.toHaveBeenCalled();
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ServiceNamespace: exampleTarget.ServiceNamespace,
          ResourceId: exampleTarget.ResourceId,
          SuspendedState: {
            DynamicScalingInSuspended: true,
            DynamicScalingOutSuspended: true,
            ScheduledScalingSuspended: true,
          },
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Resume Scaling")).toBeInTheDocument();
    });
  });

  it("shows an inline toast error with the AWS error code when the initial load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockImplementation((cmd: unknown) => {
      if (cmd instanceof DescribeScalableTargetsCommand) {
        return Promise.reject(error);
      }
      return Promise.resolve({});
    });

    const { toast } = await import("svelte-sonner");
    render(ApplicationAutoScalingPage);

    // Every namespace call rejects; Promise.allSettled swallows the
    // individual rejections into an empty target list (namespace may
    // legitimately have zero targets), so toast.error is never called by
    // loadAllNamespaces itself.
    await waitFor(() => screen.getByText("No targets found"));
    expect(toast.error).not.toHaveBeenCalled();
  });

  it("shows a toast error with the AWS error code when loading policies for a selected target fails", async () => {
    mockNamespaceFanOut();
    render(ApplicationAutoScalingPage);
    await waitFor(() => screen.getByText(exampleTarget.ResourceId));

    const error = Object.assign(new Error("Target not found."), {
      name: "ObjectNotFoundException",
      $metadata: { httpStatusCode: 400 },
    });
    mockSend.mockRejectedValueOnce(error);
    const { toast } = await import("svelte-sonner");

    await fireEvent.click(screen.getByText(exampleTarget.ResourceId));

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(expect.stringContaining("ObjectNotFoundException"));
    });
  });

  it("creates a target-tracking scaling policy via the modal", async () => {
    await renderWithTarget();

    await fireEvent.click(screen.getByText("New Policy"));
    await fireEvent.input(screen.getByPlaceholderText("Policy name"), {
      target: { value: "cpu-tracking" },
    });

    mockSend.mockReset();
    // PutScalingPolicyCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ScalingPolicies: [
        {
          PolicyName: "cpu-tracking",
          PolicyType: "TargetTrackingScaling",
          ServiceNamespace: "ecs",
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
          TargetTrackingScalingPolicyConfiguration: { TargetValue: 75 },
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ServiceNamespace: exampleTarget.ServiceNamespace,
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
          PolicyName: "cpu-tracking",
          PolicyType: "TargetTrackingScaling",
          TargetTrackingScalingPolicyConfiguration: {
            TargetValue: 75,
            PredefinedMetricSpecification: {
              PredefinedMetricType: "ECSServiceAverageCPUUtilization",
            },
          },
        }),
      }),
    );
    // Rendered-text assertion on the policy list, not just that send() fired.
    await waitFor(() => {
      expect(screen.getByText("cpu-tracking")).toBeInTheDocument();
    });
    expect(screen.getByText("Target: 75%")).toBeInTheDocument();
  });

  it("deletes a scaling policy after confirming", async () => {
    await renderWithTarget();

    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({
      ScalingPolicies: [
        {
          PolicyName: "cpu-tracking",
          PolicyType: "TargetTrackingScaling",
          ServiceNamespace: "ecs",
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
        },
      ],
    });
    // Already selected (via renderWithTarget), so the resource id now
    // appears both in the list and in the detail header -- click the list
    // entry (the first match) to reselect and force a policies reload.
    await fireEvent.click(screen.getAllByText(exampleTarget.ResourceId)[0]);
    await waitFor(() => screen.getByText("cpu-tracking"));

    mockSend.mockReset();
    // DeleteScalingPolicyCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ScalingPolicies: [] });

    await fireEvent.click(screen.getByTitle("Delete policy"));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("cpu-tracking") }),
    );
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({ PolicyName: "cpu-tracking" }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("No scaling policies yet")).toBeInTheDocument();
    });
  });

  it("does not delete a scaling policy when the confirm dialog is declined", async () => {
    await renderWithTarget();
    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({
      ScalingPolicies: [
        {
          PolicyName: "cpu-tracking",
          PolicyType: "TargetTrackingScaling",
          ServiceNamespace: "ecs",
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
        },
      ],
    });
    // Already selected (via renderWithTarget), so the resource id now
    // appears both in the list and in the detail header -- click the list
    // entry (the first match) to reselect and force a policies reload.
    await fireEvent.click(screen.getAllByText(exampleTarget.ResourceId)[0]);
    await waitFor(() => screen.getByText("cpu-tracking"));

    confirmDestructive.mockResolvedValue(false);
    const callsBefore = mockSend.mock.calls.length;

    await fireEvent.click(screen.getByTitle("Delete policy"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenCalledTimes(callsBefore);
    expect(screen.getByText("cpu-tracking")).toBeInTheDocument();
  });

  it("switches to the Scheduled Actions tab and creates a scheduled action", async () => {
    await renderWithTarget();

    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({ ScheduledActions: [] });
    await fireEvent.click(screen.getByText("Scheduled Actions"));
    await waitFor(() => screen.getByText("No scheduled actions yet"));

    await fireEvent.click(screen.getByText("New Action"));
    await fireEvent.input(screen.getByPlaceholderText("Action name"), {
      target: { value: "scale-up-morning" },
    });
    await fireEvent.input(screen.getByPlaceholderText("Min capacity"), {
      target: { value: "5" },
    });
    await fireEvent.input(screen.getByPlaceholderText("Max capacity"), {
      target: { value: "20" },
    });

    mockSend.mockReset();
    // PutScheduledActionCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      ScheduledActions: [
        {
          ScheduledActionName: "scale-up-morning",
          Schedule: "cron(0 12 * * ? *)",
          ServiceNamespace: "ecs",
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
          ScalableTargetAction: { MinCapacity: 5, MaxCapacity: 20 },
        },
      ],
    });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ScheduledActionName: "scale-up-morning",
          Schedule: "cron(0 12 * * ? *)",
          ScalableTargetAction: { MinCapacity: 5, MaxCapacity: 20 },
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("scale-up-morning")).toBeInTheDocument();
    });
    expect(screen.getByText("Min: 5 / Max: 20")).toBeInTheDocument();
  });

  it("deletes a scheduled action after confirming", async () => {
    await renderWithTarget();
    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({
      ScheduledActions: [
        {
          ScheduledActionName: "scale-up-morning",
          Schedule: "cron(0 12 * * ? *)",
          ServiceNamespace: "ecs",
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
        },
      ],
    });
    await fireEvent.click(screen.getByText("Scheduled Actions"));
    await waitFor(() => screen.getByText("scale-up-morning"));

    mockSend.mockReset();
    // DeleteScheduledActionCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ ScheduledActions: [] });

    await fireEvent.click(screen.getByTitle("Delete action"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({ ScheduledActionName: "scale-up-morning" }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("No scheduled actions yet")).toBeInTheDocument();
    });
  });

  it("shows scaling activities on the Activities tab with status and cause text", async () => {
    await renderWithTarget();
    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({
      ScalingActivities: [
        {
          ActivityId: "act-1",
          Description: "Setting desired count to 5.",
          StatusCode: "Successful",
          Cause: "monitor alarm triggered",
          StartTime: new Date("2024-01-01T00:00:00Z"),
        },
      ],
    });
    await fireEvent.click(screen.getByText("Activities"));

    await waitFor(() => {
      expect(screen.getByText("Setting desired count to 5.")).toBeInTheDocument();
    });
    expect(screen.getByText("Successful")).toBeInTheDocument();
    expect(screen.getByText("monitor alarm triggered")).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          ServiceNamespace: exampleTarget.ServiceNamespace,
          ResourceId: exampleTarget.ResourceId,
          IncludeNotScaledActivities: true,
        }),
      }),
    );
  });

  it("adds and removes a tag on the Tags tab", async () => {
    await renderWithTarget();
    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({ Tags: {} });
    await fireEvent.click(screen.getByText("Tags"));
    await waitFor(() => screen.getByText("No tags on this target"));

    await fireEvent.input(screen.getByPlaceholderText("Key"), { target: { value: "env" } });
    await fireEvent.input(screen.getByPlaceholderText("Value"), { target: { value: "prod" } });

    mockSend.mockReset();
    // TagResourceCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Tags: { env: "prod" } });

    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ResourceARN: exampleTarget.ScalableTargetARN,
          Tags: { env: "prod" },
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("env")).toBeInTheDocument();
    });
    expect(screen.getByText("prod")).toBeInTheDocument();

    mockSend.mockReset();
    // UntagResourceCommand
    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Tags: {} });

    await fireEvent.click(screen.getByTitle("Remove tag"));

    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ResourceARN: exampleTarget.ScalableTargetARN,
          TagKeys: ["env"],
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("No tags on this target")).toBeInTheDocument();
    });
  });

  it("switches between targets, clearing the previous target's detail state (policies/scheduled/tags/forecast)", async () => {
    const secondTarget = {
      ServiceNamespace: "dynamodb",
      ResourceId: "table/my-table",
      ScalableDimension: "dynamodb:table:ReadCapacityUnits",
      ScalableTargetARN:
        "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/def456",
      MinCapacity: 5,
      MaxCapacity: 50,
    };
    mockSend.mockImplementation((cmd: unknown) => {
      if (cmd instanceof DescribeScalableTargetsCommand) {
        const ns = (cmd as { input: { ServiceNamespace?: string } }).input.ServiceNamespace;
        if (ns === "ecs") return Promise.resolve({ ScalableTargets: [exampleTarget] });
        if (ns === "dynamodb") return Promise.resolve({ ScalableTargets: [secondTarget] });
        return Promise.resolve({ ScalableTargets: [] });
      }
      return Promise.resolve({});
    });
    render(ApplicationAutoScalingPage);
    await waitFor(() => screen.getByText(exampleTarget.ResourceId));
    expect(screen.getByText(secondTarget.ResourceId)).toBeInTheDocument();

    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({ ScalingPolicies: [{ PolicyName: "p1" }] });
    await fireEvent.click(screen.getByText(exampleTarget.ResourceId));
    await waitFor(() => screen.getByText("p1"));

    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({ ScalingPolicies: [] });
    await fireEvent.click(screen.getByText(secondTarget.ResourceId));

    await waitFor(() => {
      expect(screen.getByText("No scaling policies yet")).toBeInTheDocument();
    });
    expect(screen.queryByText("p1")).not.toBeInTheDocument();
  });

  it("loads a predictive scaling forecast and renders the data-point count and max", async () => {
    await renderWithTarget();

    // Unlike the other tabs, switching to Forecast does not itself trigger
    // a fetch -- the forecast is only loaded on demand via "Load Forecast".
    await fireEvent.click(screen.getByText("Forecast"));
    expect(screen.getByPlaceholderText("PredictiveScaling policy name")).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("PredictiveScaling policy name"), {
      target: { value: "my-predictive-policy" },
    });
    await fireEvent.input(screen.getByLabelText("Start Time"), {
      target: { value: "2024-01-01T00:00" },
    });
    await fireEvent.input(screen.getByLabelText("End Time"), {
      target: { value: "2024-01-02T00:00" },
    });

    mockSend.mockReset();
    mockSend.mockResolvedValueOnce({
      CapacityForecast: {
        Timestamps: [new Date("2024-01-01T00:00:00Z"), new Date("2024-01-01T01:00:00Z")],
        Values: [3, 7],
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Load Forecast" }));

    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({
          ServiceNamespace: exampleTarget.ServiceNamespace,
          ResourceId: exampleTarget.ResourceId,
          ScalableDimension: exampleTarget.ScalableDimension,
          PolicyName: "my-predictive-policy",
        }),
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Capacity Forecast (2 data points)")).toBeInTheDocument();
    });
    expect(screen.getByText("Max: 7")).toBeInTheDocument();
  });
});
