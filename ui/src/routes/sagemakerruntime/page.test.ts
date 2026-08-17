import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SageMakerRuntimePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSageMakerRuntimeClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

describe("SageMaker Runtime Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and all tabs", () => {
    render(SageMakerRuntimePage);
    expect(screen.getByText("SageMaker Runtime")).toBeInTheDocument();
    const tabBar = screen.getByText("Streaming Invocation").closest("div.flex") as HTMLElement;
    expect(within(tabBar).getByText("Invoke Endpoint")).toBeInTheDocument();
    expect(within(tabBar).getByText("Streaming Invocation")).toBeInTheDocument();
    expect(within(tabBar).getByText("Async Tracking")).toBeInTheDocument();
  });

  it("invokes sync endpoint successfully", async () => {
    const encodedBody = new TextEncoder().encode(JSON.stringify({ output: "test-prediction" }));
    mockSend.mockResolvedValueOnce({
      Body: encodedBody,
      ContentType: "application/json",
    });

    render(SageMakerRuntimePage);

    const endpointInput = screen.getByPlaceholderText("my-sagemaker-endpoint");
    await fireEvent.input(endpointInput, { target: { value: "test-endpoint" } });

    const invokeBtn = screen.getByText("Invoke");
    await fireEvent.click(invokeBtn);

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(1);
      expect(screen.getByText(/test-prediction/)).toBeInTheDocument();
    });
  });

  it("switches to async tab and executes async inference", async () => {
    mockSend.mockResolvedValueOnce({
      InferenceId: "inf-12345",
      OutputLocation: "s3://my-bucket/output/inf-12345.out",
    });

    render(SageMakerRuntimePage);

    const asyncTab = screen.getByText("Async Tracking");
    await fireEvent.click(asyncTab);

    const endpointInput = screen.getByPlaceholderText("my-async-endpoint");
    await fireEvent.input(endpointInput, { target: { value: "test-async-ep" } });

    const submitBtn = screen.getByText("Submit Async Request");
    await fireEvent.click(submitBtn);

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(1);
      expect(screen.getByText("inf-12345")).toBeInTheDocument();
      expect(screen.getByText("s3://my-bucket/output/inf-12345.out")).toBeInTheDocument();
    });
  });
});
