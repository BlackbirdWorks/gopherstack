import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import LambdaPage from "./+page.svelte";
import { setMockPageUrl } from "$lib/mock-page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getLambdaClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
  },
}));

describe("Lambda Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ Functions: [] });

    render(LambdaPage);

    expect(screen.getByText("Lambda Functions")).toBeInTheDocument();
    expect(screen.getByText("Create Function")).toBeInTheDocument();
  });

  it("honors ?q= and ?runtime= from the URL on load", async () => {
    setMockPageUrl(new URL("http://localhost/lambda?q=api&runtime=python"));
    mockSend.mockResolvedValueOnce({
      Functions: [
        {
          FunctionName: "my-handler",
          FunctionArn: "arn:1",
          Runtime: "nodejs20.x",
          MemorySize: 128,
        },
        {
          FunctionName: "api-processor",
          FunctionArn: "arn:2",
          Runtime: "python3.12",
          MemorySize: 256,
        },
      ],
    });

    render(LambdaPage);

    await waitFor(() => {
      expect(screen.getByText("api-processor")).toBeInTheDocument();
    });
    expect(screen.queryByText("my-handler")).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText("Search functions...")).toHaveValue("api");
  });

  it("displays loaded functions", async () => {
    mockSend.mockResolvedValueOnce({
      Functions: [
        {
          FunctionName: "my-handler",
          FunctionArn: "arn:1",
          Runtime: "nodejs20.x",
          MemorySize: 128,
        },
        {
          FunctionName: "api-processor",
          FunctionArn: "arn:2",
          Runtime: "python3.12",
          MemorySize: 256,
        },
      ],
    });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-handler")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("api-processor")).toBeInTheDocument();
  });

  it("filters functions via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Functions: [
        { FunctionName: "my-handler", FunctionArn: "arn:1", Runtime: "nodejs20.x" },
        { FunctionName: "api-processor", FunctionArn: "arn:2", Runtime: "python3.12" },
        { FunctionName: "data-transformer", FunctionArn: "arn:3", Runtime: "go1.x" },
      ],
    });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-handler")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search functions...");
    await fireEvent.input(searchInput, { target: { value: "api" } });

    await waitFor(() => {
      expect(screen.queryByText("my-handler")).not.toBeInTheDocument();
    });
    expect(screen.getByText("api-processor")).toBeInTheDocument();
    expect(screen.queryByText("data-transformer")).not.toBeInTheDocument();
  });

  it("selects a function when row is clicked", async () => {
    mockSend.mockResolvedValueOnce({
      Functions: [
        {
          FunctionName: "my-handler",
          FunctionArn: "arn:1",
          Runtime: "nodejs20.x",
          MemorySize: 128,
        },
      ],
    });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-handler")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    // Click the function row
    await fireEvent.click(screen.getByText("my-handler"));

    // The page renders without errors after selection
    expect(screen.getByText("my-handler")).toBeInTheDocument();
  });

  it("shows empty state when no functions exist", async () => {
    mockSend.mockResolvedValueOnce({ Functions: [] });

    render(LambdaPage);

    await waitFor(
      () => {
        expect(screen.getByText("No functions found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));

    render(LambdaPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
