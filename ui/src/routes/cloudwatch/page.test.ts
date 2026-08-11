import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import CloudWatchPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

// The client factories forward the region they were built for as a second
// argument to mockSend, so All-mode tests can dispatch a response by
// (command type, region) instead of relying on call order -- this page's
// `$effect` and `onRegionChange` both fire on mount, so load functions run
// twice per render and a strict sequential mock queue is not reliable here.
vi.mock("$lib/aws-client", () => ({
  getCloudWatchClient: (region?: string) => ({
    send: (cmd: unknown) => mockSend(cmd, region),
  }),
  getCloudWatchLogsClient: (region?: string) => ({
    send: (cmd: unknown) => mockSend(cmd, region),
  }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

describe("CloudWatch Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    // Every test below predates "All" mode and assumes exactly one
    // DescribeAlarms call per load against a single region (and the demo
    // data fallback, which is single-region-only), so pin single-region
    // mode here; the "All regions mode" describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
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

  describe("All regions mode", () => {
    // DescribeAlarms is dispatched by region (not call order): this page's
    // `$effect` and `onRegionChange` both fire loadData() on mount, so
    // DescribeAlarms fires twice per region -- a sequential mockResolvedValueOnce
    // queue can't express that reliably, but a per-region response can.
    function alarmsByRegion(byRegion: Record<string, { AlarmName: string }[]>): void {
      mockSend.mockImplementation((cmd: { constructor: { name: string } }, region: string) => {
        if (cmd.constructor.name === "DescribeAlarmsCommand") {
          return Promise.resolve({ MetricAlarms: byRegion[region] ?? [] });
        }
        return Promise.resolve({});
      });
    }

    it("fans DescribeAlarms out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      alarmsByRegion({
        "us-east-1": [{ AlarmName: "us-alarm" }],
        "eu-west-1": [{ AlarmName: "eu-alarm" }],
      });

      render(CloudWatchPage);

      await waitFor(() => expect(screen.getByText("us-alarm")).toBeInTheDocument());
      expect(screen.getByText("eu-alarm")).toBeInTheDocument();
      // No demo-data fallback under All, even though both regions returned data anyway here.
      expect(screen.queryByText("demo-high-cpu")).not.toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      alarmsByRegion({ [DEFAULT_REGION]: [{ AlarmName: "solo-alarm" }] });

      render(CloudWatchPage);

      await waitFor(() => expect(screen.getByText("solo-alarm")).toBeInTheDocument());
      const describeCalls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "DescribeAlarmsCommand",
      );
      const regionsCalled = new Set(describeCalls.map(([, region]) => region));
      expect(regionsCalled).toEqual(new Set([DEFAULT_REGION]));

      vi.unstubAllGlobals();
    });

    it("issues DescribeAlarms against only the single selected region", async () => {
      alarmsByRegion({ [DEFAULT_REGION]: [{ AlarmName: "solo-alarm" }] });
      render(CloudWatchPage);
      await waitFor(() => expect(screen.getByText("solo-alarm")).toBeInTheDocument());
      const describeCalls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "DescribeAlarmsCommand",
      );
      const regionsCalled = new Set(describeCalls.map(([, region]) => region));
      expect(regionsCalled).toEqual(new Set([DEFAULT_REGION]));
    });

    it("renders the same alarm name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      alarmsByRegion({
        "us-east-1": [{ AlarmName: "shared-alarm" }],
        "eu-west-1": [{ AlarmName: "shared-alarm" }],
      });

      render(CloudWatchPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-alarm");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".overflow-hidden") as HTMLElement).getByTestId("region-chip")
            .textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("deletes the row's own region, not the picker's, when two regions share an alarm name", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      const byRegion: Record<string, { AlarmName: string }[]> = {
        "us-east-1": [{ AlarmName: "shared-alarm" }],
        "eu-west-1": [{ AlarmName: "shared-alarm" }],
      };
      mockSend.mockImplementation((cmd: { constructor: { name: string } }, region: string) => {
        if (cmd.constructor.name === "DescribeAlarmsCommand") {
          return Promise.resolve({ MetricAlarms: byRegion[region] ?? [] });
        }
        if (cmd.constructor.name === "DeleteAlarmsCommand") {
          byRegion[region] = [];
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      render(CloudWatchPage);
      await waitFor(() => expect(screen.getAllByText("shared-alarm")).toHaveLength(2));

      const rows = screen.getAllByText("shared-alarm");
      const euRow = rows
        .map((r) => r.closest(".overflow-hidden") as HTMLElement)
        .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
      // The row's buttons are (in order): toggle actions, edit state, view
      // history, delete -- delete has no title/testid to select by, so use
      // its position as the last button in the row.
      const rowButtons = within(euRow).getAllByRole("button");
      await fireEvent.click(rowButtons.at(-1)!);

      await waitFor(() => {
        const remaining = screen.getAllByText("shared-alarm");
        expect(remaining).toHaveLength(1);
        expect(
          within(remaining[0].closest(".overflow-hidden") as HTMLElement).getByTestId("region-chip")
            .textContent,
        ).toBe("us-east-1");
      });

      vi.unstubAllGlobals();
    });
  });
});
