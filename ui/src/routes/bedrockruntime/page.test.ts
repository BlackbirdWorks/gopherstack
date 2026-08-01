import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import BedrockRuntimePage from "./+page.svelte";

function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getBedrockRuntimeClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("Bedrock Runtime Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    render(BedrockRuntimePage);
    expect(screen.getAllByText("Amazon Bedrock Runtime")[0]).toBeInTheDocument();
  });

  it("shows stat cards and the invoke form by default", () => {
    render(BedrockRuntimePage);
    expect(screen.getAllByText("Supported Models")[0]).toBeInTheDocument();
    expect(screen.getByText("Invoke Foundation Model")).toBeInTheDocument();
  });

  it("invokes a model with the correct request body and shows the response", async () => {
    render(BedrockRuntimePage);

    mockSend.mockResolvedValueOnce({
      body: new TextEncoder().encode(
        JSON.stringify({ results: [{ outputText: "AWS is a cloud platform." }] }),
      ),
    });

    await fireEvent.click(screen.getByRole("button", { name: /Invoke Model/ }));

    await waitFor(() => {
      expect(screen.getByText("AWS is a cloud platform.")).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[0][0];
    expect(call.input.modelId).toBe("amazon.titan-text-express-v1");
    const body = JSON.parse(new TextDecoder().decode(call.input.body));
    expect(body).toEqual({
      inputText: "What is Amazon Web Services?",
      textGenerationConfig: { maxTokenCount: 512, temperature: 0.7 },
    });
  });

  it("counts tokens for the current prompt", async () => {
    render(BedrockRuntimePage);

    mockSend.mockResolvedValueOnce({ inputTokens: 42 });
    await fireEvent.click(screen.getByRole("button", { name: /Count Tokens/ }));

    await waitFor(() => {
      expect(screen.getByText("42 input tokens")).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[0][0];
    expect(call.input.modelId).toBe("amazon.titan-text-express-v1");
    const body = JSON.parse(new TextDecoder().decode(call.input.input.invokeModel.body));
    expect(body).toEqual({
      inputText: "What is Amazon Web Services?",
      textGenerationConfig: { maxTokenCount: 512, temperature: 0.7 },
    });
  });

  it("shows an inline error when InvokeModel fails", async () => {
    const error = Object.assign(new Error("Model not ready."), {
      name: "ModelNotReadyException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);
    render(BedrockRuntimePage);

    await fireEvent.click(screen.getByRole("button", { name: /Invoke Model/ }));

    await waitFor(() => {
      expect(
        screen.getByText("ModelNotReadyException (HTTP 429): Model not ready."),
      ).toBeInTheDocument();
    });
  });

  it("sends a Converse turn and appends the assistant reply", async () => {
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Converse Playground" }));

    await fireEvent.input(screen.getByPlaceholderText(/Type a message/), {
      target: { value: "Hello there" },
    });

    mockSend.mockResolvedValueOnce({
      output: { message: { content: [{ text: "Hi! How can I help?" }] } },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Send" }));

    await waitFor(() => {
      expect(screen.getByText("Hi! How can I help?")).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[0][0];
    expect(call.input).toEqual({
      modelId: "anthropic.claude-instant-v1",
      messages: [{ role: "user", content: [{ text: "Hello there" }] }],
    });
  });

  it("applies a guardrail with the correct input and shows the action", async () => {
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Guardrails" }));

    await fireEvent.input(screen.getByLabelText("Guardrail identifier"), {
      target: { value: "gr-abc123" },
    });
    await fireEvent.input(screen.getByLabelText("Guardrail version"), { target: { value: "1" } });
    await fireEvent.input(screen.getByLabelText("Content"), {
      target: { value: "some harmful text" },
    });

    mockSend.mockResolvedValueOnce({
      action: "GUARDRAIL_INTERVENED",
      assessments: [{ wordPolicy: { customWords: [{ match: "harmful" }] } }],
      usage: {
        topicPolicyUnits: 0,
        contentPolicyUnits: 0,
        wordPolicyUnits: 0,
        sensitiveInformationPolicyUnits: 0,
        sensitiveInformationPolicyFreeUnits: 0,
        contextualGroundingPolicyUnits: 0,
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: /Apply Guardrail/ }));

    await waitFor(() => {
      expect(screen.getByText("Action: GUARDRAIL_INTERVENED")).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[0][0];
    expect(call.input).toEqual({
      guardrailIdentifier: "gr-abc123",
      guardrailVersion: "1",
      source: "INPUT",
      content: [{ text: { text: "some harmful text" } }],
    });
  });

  it("runs guardrail checks for sensitive information", async () => {
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Guardrails" }));

    await fireEvent.input(screen.getByLabelText("Message text"), {
      target: { value: "contact me at a@example.com" },
    });

    mockSend.mockResolvedValueOnce({
      results: { sensitiveInformation: { results: [{ type: "EMAIL", confidenceScore: 1 }] } },
      usage: {},
    });

    await fireEvent.click(screen.getByRole("button", { name: /Run Checks/ }));

    await waitFor(() => {
      expect(screen.getByText(/EMAIL \(confidence 1\)/)).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[0][0];
    expect(call.input.messages).toEqual([
      { role: "user", content: [{ text: "contact me at a@example.com" }] },
    ]);
    expect(call.input.checks.sensitiveInformation.entities).toContainEqual({ type: "EMAIL" });
  });

  it("lists async invocations", async () => {
    mockSend.mockResolvedValueOnce({
      asyncInvokeSummaries: [
        {
          invocationArn: "arn:aws:bedrock:us-east-1:123456789012:async-invoke/abc",
          modelArn: "amazon.titan-text-express-v1",
          status: "InProgress",
        },
      ],
    });
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Async Invocations" }));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:bedrock:us-east-1:123456789012:async-invoke/abc"),
      ).toBeInTheDocument();
    });
  });

  it("shows empty state when there are no async invocations", async () => {
    mockSend.mockResolvedValueOnce({ asyncInvokeSummaries: [] });
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Async Invocations" }));

    await waitFor(() => {
      expect(screen.getByText("No async invocations found")).toBeInTheDocument();
    });
  });

  it("starts an async invocation with the correct input", async () => {
    mockSend.mockResolvedValueOnce({ asyncInvokeSummaries: [] });
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Async Invocations" }));
    await waitFor(() => screen.getByText("No async invocations found"));

    await fireEvent.click(screen.getByText("Start async invocation"));
    await fireEvent.input(within(openDialog()).getByLabelText("Model ID"), {
      target: { value: "amazon.titan-text-express-v1" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("S3 output URI"), {
      target: { value: "s3://my-bucket/output/" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Model input (JSON)"), {
      target: { value: '{"inputText": "hello"}' },
    });

    mockSend.mockResolvedValueOnce({
      invocationArn: "arn:aws:bedrock:us-east-1:123456789012:async-invoke/xyz",
    });
    mockSend.mockResolvedValueOnce({
      asyncInvokeSummaries: [
        {
          invocationArn: "arn:aws:bedrock:us-east-1:123456789012:async-invoke/xyz",
          modelArn: "amazon.titan-text-express-v1",
          status: "InProgress",
        },
      ],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Start" }));

    await waitFor(() => {
      expect(
        screen.getByText("arn:aws:bedrock:us-east-1:123456789012:async-invoke/xyz"),
      ).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[1][0];
    expect(call.input).toEqual({
      modelId: "amazon.titan-text-express-v1",
      modelInput: { inputText: "hello" },
      outputDataConfig: { s3OutputDataConfig: { s3Uri: "s3://my-bucket/output/" } },
    });
  });

  it("views an async invocation's detail", async () => {
    mockSend.mockResolvedValueOnce({
      asyncInvokeSummaries: [
        {
          invocationArn: "arn:aws:bedrock:us-east-1:123456789012:async-invoke/abc",
          modelArn: "amazon.titan-text-express-v1",
          status: "Completed",
        },
      ],
    });
    render(BedrockRuntimePage);
    await fireEvent.click(screen.getByRole("tab", { name: "Async Invocations" }));
    await waitFor(() =>
      screen.getByText("arn:aws:bedrock:us-east-1:123456789012:async-invoke/abc"),
    );

    mockSend.mockResolvedValueOnce({
      invocationArn: "arn:aws:bedrock:us-east-1:123456789012:async-invoke/abc",
      modelArn: "amazon.titan-text-express-v1",
      status: "Completed",
      submitTime: new Date("2026-01-01T00:00:00Z"),
    });

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(within(openDialog()).getByText("Completed")).toBeInTheDocument();
    });

    const call = mockSend.mock.calls[1][0];
    expect(call.input).toEqual({
      invocationArn: "arn:aws:bedrock:us-east-1:123456789012:async-invoke/abc",
    });
  });
});
