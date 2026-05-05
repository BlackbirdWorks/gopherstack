import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import MediaConvertPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMediaConvertClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("MediaConvert Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Jobs: [] });
    render(MediaConvertPage);
    expect(screen.getAllByText("AWS MediaConvert")[0]).toBeInTheDocument();
  });

  it("shows subtitle", () => {
    mockSend.mockResolvedValue({ Jobs: [] });
    render(MediaConvertPage);
    expect(screen.getAllByText("File-based video transcoding service")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ Jobs: [] });
    render(MediaConvertPage);
    expect(screen.getAllByText("Total Jobs")[0]).toBeInTheDocument();
    expect(screen.getAllByText("Queues").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Submitted")[0]).toBeInTheDocument();
    expect(screen.getAllByText("Progressing")[0]).toBeInTheDocument();
  });

  it("shows tab navigation", () => {
    mockSend.mockResolvedValue({ Jobs: [] });
    render(MediaConvertPage);
    expect(screen.getByRole("button", { name: /Jobs/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Queues/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Templates/ })).toBeInTheDocument();
  });

  it("shows empty state when no jobs", async () => {
    mockSend.mockResolvedValue({ Queues: [] });
    render(MediaConvertPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("No queues found")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded jobs", async () => {
    mockSend.mockResolvedValueOnce({ Queues: [] });
    mockSend.mockResolvedValueOnce({
      Jobs: [
        {
          Id: "1234567890123-abc123",
          Status: "COMPLETE",
          Queue: "arn:aws:mediaconvert:us-east-1:123:queues/Default",
          Role: "arn:aws:iam::123:role/MediaConvertRole",
          CreatedAt: new Date("2024-01-15"),
          OutputGroupDetails: [],
        },
      ],
    });
    render(MediaConvertPage);
    const jobsTab = screen.getByRole("button", { name: /Jobs/ });
    await fireEvent.click(jobsTab);
    await waitFor(
      () => {
        expect(screen.getAllByText("1234567890123-abc123")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ Queues: [] });
    render(MediaConvertPage);
    expect(screen.getByPlaceholderText("Search queues...")).toBeInTheDocument();
  });

  it("selects job and shows details", async () => {
    const job = {
      Id: "job-abc-123",
      Status: "COMPLETE",
      Queue: "arn:aws:mediaconvert:us-east-1:123:queues/Default",
      Role: "arn:aws:iam::123:role/MediaConvertRole",
      CreatedAt: new Date("2024-01-15"),
      FinishTime: new Date("2024-01-15T01:00:00Z"),
      OutputGroupDetails: [{ OutputDetails: [] }],
    };
    mockSend.mockResolvedValueOnce({ Queues: [] });
    mockSend.mockResolvedValueOnce({ Jobs: [job] });
    render(MediaConvertPage);
    const jobsTab = screen.getByRole("button", { name: /Jobs/ });
    await fireEvent.click(jobsTab);
    await waitFor(() => expect(screen.getAllByText("job-abc-123")[0]).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("job-abc-123"));
    await waitFor(
      () => {
        expect(screen.getAllByText("Queue")[0]).toBeInTheDocument();
        expect(screen.getAllByText("Role")[0]).toBeInTheDocument();
        expect(screen.getAllByText("Priority")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to queues tab", async () => {
    mockSend.mockResolvedValueOnce({ Queues: [] });
    render(MediaConvertPage);
    await waitFor(() => expect(screen.getAllByText("No queues found")[0]).toBeInTheDocument(), {
      timeout: 3000,
    });
    const queuesTab = screen.getByRole("button", { name: /Queues/ });
    await fireEvent.click(queuesTab);
    await waitFor(
      () => {
        expect(screen.getAllByText("No queues found")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays queue details on click", async () => {
    mockSend.mockResolvedValueOnce({
      Queues: [
        {
          Name: "Default",
          Arn: "arn:aws:mediaconvert:us-east-1:123:queues/Default",
          Status: "ACTIVE",
          Type: "SYSTEM",
          PricingPlan: "ON_DEMAND",
          ProgressingJobsCount: 0,
          SubmittedJobsCount: 0,
          CreatedAt: new Date("2023-01-01"),
        },
      ],
    });
    render(MediaConvertPage);
    await waitFor(() => expect(screen.getAllByText("Default")[0]).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Default"));
    await waitFor(
      () => {
        expect(screen.getAllByText("Type")[0]).toBeInTheDocument();
        expect(screen.getAllByText("Pricing Plan")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to templates tab", async () => {
    mockSend.mockResolvedValueOnce({ Queues: [] });
    mockSend.mockResolvedValueOnce({ JobTemplates: [] });
    render(MediaConvertPage);
    await waitFor(() => expect(screen.getAllByText("No queues found")[0]).toBeInTheDocument(), {
      timeout: 3000,
    });
    const templatesTab = screen.getByRole("button", { name: /Templates/ });
    await fireEvent.click(templatesTab);
    await waitFor(
      () => {
        expect(screen.getAllByText("No templates found")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
