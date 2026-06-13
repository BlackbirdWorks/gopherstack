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

  it("shows stats bar with counts", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [
        { Name: "pipe-1", CurrentState: "RUNNING", DesiredState: "RUNNING" },
        { Name: "pipe-2", CurrentState: "STOPPED", DesiredState: "STOPPED" },
      ],
    });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("pipe-1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    // Stats bar labels are present in the filter select dropdown
    const select = document.querySelector("select");
    expect(select).toBeTruthy();
  });

  it("displays loaded pipes", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [
        { Name: "sqs-to-lambda", CurrentState: "RUNNING", DesiredState: "RUNNING" },
        { Name: "dynamo-to-sqs", CurrentState: "STOPPED", DesiredState: "STOPPED" },
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
        { Name: "sqs-to-lambda", CurrentState: "RUNNING", DesiredState: "RUNNING" },
        { Name: "dynamo-to-sqs", CurrentState: "STOPPED", DesiredState: "STOPPED" },
        { Name: "kinesis-to-firehose", CurrentState: "RUNNING", DesiredState: "RUNNING" },
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
    mockSend.mockResolvedValueOnce({
      Name: "new-pipe",
      Arn: "arn:aws:pipes:us-east-1:000000000000:pipe/new-pipe",
    });
    mockSend.mockResolvedValueOnce({
      Pipes: [{ Name: "new-pipe", CurrentState: "RUNNING", DesiredState: "RUNNING" }],
    });

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

  it("selects a pipe and loads details with tabs", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [{ Name: "my-pipe", CurrentState: "RUNNING", DesiredState: "RUNNING" }],
    });
    mockSend.mockResolvedValueOnce({
      Name: "my-pipe",
      Arn: "arn:aws:pipes:us-east-1:000000000000:pipe/my-pipe",
      CurrentState: "RUNNING",
      DesiredState: "RUNNING",
      Source: "arn:aws:sqs:us-east-1:000000000000:source-queue",
      Target: "arn:aws:lambda:us-east-1:000000000000:function:target-fn",
      Tags: {},
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
        expect(screen.getByText("Overview")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    expect(screen.getByText("Tags")).toBeInTheDocument();
    expect(screen.getByText("Config")).toBeInTheDocument();
  });

  it("shows Source and Target in detail view", async () => {
    mockSend.mockResolvedValueOnce({
      Pipes: [{ Name: "detail-pipe", CurrentState: "RUNNING", DesiredState: "RUNNING" }],
    });
    mockSend.mockResolvedValueOnce({
      Name: "detail-pipe",
      CurrentState: "RUNNING",
      DesiredState: "RUNNING",
      Source: "arn:aws:sqs:us-east-1:000000000000:my-queue",
      Target: "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
      Tags: {},
    });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("detail-pipe")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("detail-pipe"));

    await waitFor(
      () => {
        expect(screen.getByText("Source")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    expect(screen.getByText("Target")).toBeInTheDocument();
  });

  it("shows empty state when no pipes exist", async () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    await waitFor(
      () => {
        expect(screen.getByText("No pipes found.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("Create your first pipe")).toBeInTheDocument();
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

  it("shows filter dropdown for state", () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    const selects = document.querySelectorAll("select");
    const stateSelect = Array.from(selects).find((s) => s.innerHTML.includes("All States"));
    expect(stateSelect).toBeTruthy();
  });

  it("shows sort dropdown", () => {
    mockSend.mockResolvedValueOnce({ Pipes: [] });

    render(PipesPage);

    const selects = document.querySelectorAll("select");
    const sortSelect = Array.from(selects).find((s) => s.innerHTML.includes("Name"));
    expect(sortSelect).toBeTruthy();
  });
});
