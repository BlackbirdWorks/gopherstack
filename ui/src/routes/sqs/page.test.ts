import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import SQSPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSQSClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("SQS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ QueueUrls: [], Attributes: {} });
    render(SQSPage);
    expect(screen.getByText("SQS Queues")).toBeInTheDocument();
  });

  it("shows Create Queue button", () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    expect(screen.getByText("Create Queue")).toBeInTheDocument();
  });

  it("shows empty state when no queues", async () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No queues found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded queues", async () => {
    mockSend.mockResolvedValueOnce({
      QueueUrls: [
        "https://sqs.us-east-1.amazonaws.com/123/order-queue",
        "https://sqs.us-east-1.amazonaws.com/123/dead-letter",
      ],
    });
    mockSend.mockResolvedValue({
      Attributes: { ApproximateNumberOfMessages: "5", VisibilityTimeout: "30" },
    });
    render(SQSPage);
    await waitFor(
      () => {
        expect(screen.getByText("order-queue")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("dead-letter")).toBeInTheDocument();
  });

  it("filters queues via search", async () => {
    mockSend.mockResolvedValueOnce({
      QueueUrls: [
        "https://sqs.us-east-1.amazonaws.com/123/order-queue",
        "https://sqs.us-east-1.amazonaws.com/123/payment-queue",
      ],
    });
    mockSend.mockResolvedValue({ Attributes: {} });
    render(SQSPage);
    await waitFor(
      () => {
        expect(screen.getByText("order-queue")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search queues...");
    await fireEvent.input(searchInput, { target: { value: "payment" } });
    await waitFor(() => {
      expect(screen.queryByText("order-queue")).not.toBeInTheDocument();
    });
    expect(screen.getByText("payment-queue")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    await fireEvent.click(screen.getByText("Create Queue"));
    await waitFor(() => {
      expect(screen.getByText("Create Queue", { selector: "h2" })).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. order-processing")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    await fireEvent.click(screen.getByText("Create Queue"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. order-processing")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. order-processing")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("permission denied"));
    render(SQSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
