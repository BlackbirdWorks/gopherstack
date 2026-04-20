import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CloudWatchPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCloudWatchClient: () => ({ send: mockSend }),
  getCloudWatchLogsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("CloudWatch Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ MetricAlarms: [] });
    render(CloudWatchPage);
    expect(screen.getByText("CloudWatch")).toBeInTheDocument();
  });

  it("shows Create Alarm button on alarms tab", () => {
    mockSend.mockResolvedValue({ MetricAlarms: [] });
    render(CloudWatchPage);
    expect(screen.getByText("Create Alarm")).toBeInTheDocument();
  });

  it("shows demo alarms when api returns empty", async () => {
    mockSend.mockResolvedValue({ MetricAlarms: [] });
    render(CloudWatchPage);
    await waitFor(
      () => {
        expect(screen.getByText("demo-high-cpu")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded alarms", async () => {
    mockSend.mockResolvedValue({
      MetricAlarms: [
        {
          AlarmName: "high-cpu",
          StateValue: "ALARM",
          Namespace: "AWS/EC2",
          MetricName: "CPUUtilization",
          Threshold: 80,
          ComparisonOperator: "GreaterThanThreshold",
          Period: 300,
          EvaluationPeriods: 1,
        },
        {
          AlarmName: "low-disk",
          StateValue: "OK",
          Namespace: "AWS/EBS",
          MetricName: "VolumeReadOps",
          Threshold: 10,
          ComparisonOperator: "LessThanThreshold",
          Period: 60,
          EvaluationPeriods: 3,
        },
      ],
    });
    render(CloudWatchPage);
    await waitFor(
      () => {
        expect(screen.getByText("high-cpu")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("low-disk")).toBeInTheDocument();
  });

  it("filters alarms via search", async () => {
    mockSend.mockResolvedValue({
      MetricAlarms: [
        {
          AlarmName: "cpu-alarm",
          StateValue: "OK",
          Namespace: "AWS/EC2",
          MetricName: "CPUUtilization",
          Threshold: 80,
          ComparisonOperator: "GreaterThanThreshold",
          Period: 300,
          EvaluationPeriods: 1,
        },
        {
          AlarmName: "disk-alarm",
          StateValue: "ALARM",
          Namespace: "AWS/EBS",
          MetricName: "DiskRead",
          Threshold: 90,
          ComparisonOperator: "GreaterThanThreshold",
          Period: 300,
          EvaluationPeriods: 1,
        },
      ],
    });
    render(CloudWatchPage);
    await waitFor(
      () => {
        expect(screen.getByText("cpu-alarm")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search alarms...");
    await fireEvent.input(searchInput, { target: { value: "cpu" } });
    await waitFor(() => {
      expect(screen.queryByText("disk-alarm")).not.toBeInTheDocument();
    });
    expect(screen.getByText("cpu-alarm")).toBeInTheDocument();
  });

  it("opens create alarm modal", async () => {
    mockSend.mockResolvedValue({ MetricAlarms: [] });
    render(CloudWatchPage);
    await fireEvent.click(screen.getByText("Create Alarm"));
    await waitFor(() => {
      expect(screen.getByText("Create Metric Alarm")).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. high-cpu-alarm")).toBeInTheDocument();
  });

  it("closes alarm modal on cancel", async () => {
    mockSend.mockResolvedValue({ MetricAlarms: [] });
    render(CloudWatchPage);
    await fireEvent.click(screen.getByText("Create Alarm"));
    await waitFor(() => {
      expect(screen.getByText("Create Metric Alarm")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByText("Create Metric Alarm")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(CloudWatchPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
