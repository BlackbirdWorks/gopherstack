import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import BatchPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getBatchClient: () => ({ send: mockSend }),
  getCloudWatchLogsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
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

describe("Batch Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // Describe*Command call per action against a single region, so pin
    // single-region mode here; the "All regions mode" describe block below
    // opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    expect(screen.getByText("AWS Batch")).toBeInTheDocument();
  });

  it("shows queue/job/definition tabs", () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    expect(screen.getByText("Job Queues")).toBeInTheDocument();
    expect(screen.getByText("Jobs")).toBeInTheDocument();
    expect(screen.getByText("Job Definitions")).toBeInTheDocument();
  });

  it("shows Create Queue button on queues tab", () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    expect(screen.getByText("Create Queue")).toBeInTheDocument();
  });

  it("shows empty state when no queues", async () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    await waitFor(
      () => {
        expect(screen.getByText("No job queues found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded job queues", async () => {
    mockSend.mockResolvedValue({
      jobQueues: [
        {
          jobQueueName: "high-priority-queue",
          state: "ENABLED",
          status: "VALID",
          priority: 100,
          computeEnvironmentOrder: [],
        },
        {
          jobQueueName: "low-priority-queue",
          state: "DISABLED",
          status: "VALID",
          priority: 10,
          computeEnvironmentOrder: [],
        },
      ],
    });
    render(BatchPage);
    await waitFor(
      () => {
        expect(screen.getByText("high-priority-queue")).toBeInTheDocument();
        expect(screen.getByText("low-priority-queue")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create queue modal", async () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    await fireEvent.click(screen.getByText("Create Queue"));
    expect(screen.getByText("Queue Name")).toBeInTheDocument();
    expect(screen.getByText("Compute Environment ARN")).toBeInTheDocument();
  });

  it("cancels create queue modal", async () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    await fireEvent.click(screen.getByText("Create Queue"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Queue Name")).not.toBeInTheDocument();
  });

  it("switches to jobs tab and shows Submit Job button", async () => {
    mockSend.mockResolvedValue({ jobQueues: [] });
    render(BatchPage);
    const jobsTab = screen.getByText("Jobs");
    await fireEvent.click(jobsTab);
    await waitFor(
      () => {
        expect(screen.getByText("Submit Job")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    const queue = {
      jobQueueName: "shared-queue",
      state: "ENABLED",
      status: "VALID",
      priority: 100,
      computeEnvironmentOrder: [],
    };

    it("fans DescribeJobQueues out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ jobQueues: [queue] });
      mockSend.mockResolvedValueOnce({ jobQueues: [{ ...queue, jobQueueName: "eu-queue" }] });
      mockSend.mockResolvedValue({ jobSummaryList: [] });

      render(BatchPage);

      await waitFor(() => expect(screen.getByText("shared-queue")).toBeInTheDocument());
      expect(screen.getByText("eu-queue")).toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeJobQueues call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ jobQueues: [queue] });
      mockSend.mockResolvedValue({ jobSummaryList: [] });
      render(BatchPage);
      await waitFor(() => expect(screen.getByText("shared-queue")).toBeInTheDocument());
      const describeCalls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "DescribeJobQueuesCommand",
      );
      expect(describeCalls).toHaveLength(1);
    });

    it("renders the same queue name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ jobQueues: [queue] });
      mockSend.mockResolvedValueOnce({ jobQueues: [queue] });
      mockSend.mockResolvedValue({ jobSummaryList: [] });

      render(BatchPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-queue");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
