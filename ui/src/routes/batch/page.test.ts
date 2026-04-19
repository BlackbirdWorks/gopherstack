import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import BatchPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getBatchClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Batch Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
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
});
