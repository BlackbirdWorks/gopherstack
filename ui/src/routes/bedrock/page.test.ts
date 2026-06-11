import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import BedrockPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getBedrockClient: () => ({ send: mockSend }),
  getBedrockRuntimeClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("Bedrock Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ modelSummaries: [] });
    render(BedrockPage);
    expect(screen.getByText("Amazon Bedrock")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend.mockResolvedValueOnce({ modelSummaries: [] });
    render(BedrockPage);
    expect(screen.getByText("Foundation Models")).toBeInTheDocument();
    expect(screen.getByText("Custom Models")).toBeInTheDocument();
  });

  it("displays foundation models", async () => {
    mockSend.mockResolvedValueOnce({
      modelSummaries: [
        {
          modelId: "anthropic.claude-3-sonnet-20240229-v1:0",
          modelName: "Claude 3 Sonnet",
          providerName: "Anthropic",
          inputModalities: ["TEXT", "IMAGE"],
          outputModalities: ["TEXT"],
          responseStreamingSupported: true,
        },
      ],
    });

    render(BedrockPage);

    await waitFor(() => {
      expect(screen.getByText("Claude 3 Sonnet")).toBeInTheDocument();
    });
  });

  it("shows empty state for foundation models", async () => {
    mockSend.mockResolvedValueOnce({ modelSummaries: [] });
    render(BedrockPage);
    await waitFor(() => {
      expect(screen.getByText("No foundation models found")).toBeInTheDocument();
    });
  });

  it("switches to custom models tab", async () => {
    mockSend
      .mockResolvedValueOnce({ modelSummaries: [] })
      .mockResolvedValueOnce({ modelSummaries: [] });
    render(BedrockPage);
    await fireEvent.click(screen.getByText("Custom Models"));
    await waitFor(() => {
      expect(screen.getByText("No custom models found")).toBeInTheDocument();
    });
  });
});
