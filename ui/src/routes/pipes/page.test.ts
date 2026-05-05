import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import PipesPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getPipesClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

describe("Pipes Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and new pipe button", () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    expect(screen.getByText("EventBridge Pipes")).toBeInTheDocument();
    expect(screen.getByText("New Pipe")).toBeInTheDocument();
  });

  it("displays loaded pipes", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [
        { Name: "sqs-to-lambda", CurrentState: "RUNNING" },
        { Name: "dynamo-to-sqs", CurrentState: "STOPPED" },
      ],
    });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("sqs-to-lambda")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("dynamo-to-sqs")).toBeInTheDocument();
  });

  it("filters pipes via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [
        { Name: "sqs-to-lambda", CurrentState: "RUNNING" },
        { Name: "dynamo-to-sqs", CurrentState: "STOPPED" },
        { Name: "kinesis-to-firehose", CurrentState: "RUNNING" },
      ],
    });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("sqs-to-lambda")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search pipes...");
    await fireEvent.input(searchInput, { target: { value: "sqs" } });

    await waitFor(() => {
      expect(screen.queryByText("kinesis-to-firehose")).not.toBeInTheDocument();
    });
    expect(screen.getByText("sqs-to-lambda")).toBeInTheDocument();
  });

  it("opens create modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    await fireEvent.click(screen.getByText("New Pipe"));

    expect(screen.getByText("Configure Pipe")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("e.g. sqs-to-lambda-processor")).toBeInTheDocument();
  });

  it("closes create modal on cancel click", async () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    await fireEvent.click(screen.getByText("New Pipe"));
    expect(screen.getByText("Configure Pipe")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Cancel"));

    await waitFor(() => {
      expect(screen.queryByText("Configure Pipe")).not.toBeInTheDocument();
    });
  });

  it("creates a pipe via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });
    mockSend.mockResolvedValueOnce({ Name: "new-pipe", Arn: "arn:aws:pipes:us-east-1:000000000000:pipe/new-pipe" });
    mockSend.mockResolvedValueOnce({ Pipes: [{ Name: "new-pipe", CurrentState: "RUNNING" }] });

    render(PipesPage);

    await fireEvent.click(screen.getByText("New Pipe"));

    const nameInput = screen.getByPlaceholderText("e.g. sqs-to-lambda-processor");
    await fireEvent.input(nameInput, { target: { value: "new-pipe" } });

    const form = nameInput.closest("form")!;
    await fireEvent.submit(form);

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(3);
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalled();
  });

  it("selects a pipe and loads details", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [{ Name: "my-pipe", CurrentState: "RUNNING" }],
    });
    mockSend.mockResolvedValueOnce({
      Name: "my-pipe",
      Arn: "arn:aws:pipes:us-east-1:000000000000:pipe/my-pipe",
      CurrentState: "RUNNING",
      Source: "arn:aws:sqs:us-east-1:000000000000:source-queue",
      Target: "arn:aws:lambda:us-east-1:000000000000:function:target-fn",
    });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-pipe")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("my-pipe"));

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(2);
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no pipes exist", async () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("No pipes configured.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network error"));

    render(PipesPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
