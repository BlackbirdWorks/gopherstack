import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import AppRunnerPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAppRunnerClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("App Runner Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ ServiceSummaryList: [] });
    render(AppRunnerPage);
    expect(screen.getByText("App Runner")).toBeInTheDocument();
  });

  it("shows create service button", () => {
    mockSend.mockResolvedValueOnce({ ServiceSummaryList: [] });
    render(AppRunnerPage);
    expect(screen.getByText("Create Service")).toBeInTheDocument();
  });

  it("displays services", async () => {
    mockSend.mockResolvedValueOnce({
      ServiceSummaryList: [
        {
          ServiceArn: "arn:apprunner:service/my-app",
          ServiceId: "svc-123",
          ServiceName: "my-app",
          ServiceUrl: "my-app.us-east-1.awsapprunner.com",
          Status: "RUNNING",
        },
      ],
    });

    render(AppRunnerPage);

    await waitFor(() => {
      expect(screen.getByText("my-app")).toBeInTheDocument();
    });
  });

  it("shows create service modal", async () => {
    mockSend.mockResolvedValueOnce({ ServiceSummaryList: [] });
    render(AppRunnerPage);
    await fireEvent.click(screen.getByText("Create Service"));
    expect(screen.getByText("Create App Runner Service")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ ServiceSummaryList: [] });
    render(AppRunnerPage);
    await waitFor(() => {
      expect(screen.getByText("No App Runner services found")).toBeInTheDocument();
    });
  });
});
