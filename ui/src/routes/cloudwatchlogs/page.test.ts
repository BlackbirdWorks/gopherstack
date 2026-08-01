import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CloudWatchLogsPage from "./+page.svelte";
import { setMockPageUrl } from "$lib/mock-page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCloudWatchLogsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("CloudWatch Logs Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ logGroups: [] });
    render(CloudWatchLogsPage);
    expect(screen.getByText("CloudWatch Logs")).toBeInTheDocument();
  });

  it("shows Create Log Group button", () => {
    mockSend.mockResolvedValue({ logGroups: [] });
    render(CloudWatchLogsPage);
    expect(screen.getByText("Create Log Group")).toBeInTheDocument();
  });

  it("shows empty state when no log groups", async () => {
    mockSend.mockResolvedValue({ logGroups: [] });
    render(CloudWatchLogsPage);
    await waitFor(
      () => {
        expect(screen.getByText("No log groups found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("honors ?tab=insights from the URL on load, without needing a click", async () => {
    setMockPageUrl(new URL("http://localhost/cloudwatchlogs?tab=insights"));
    mockSend.mockResolvedValue({ logGroups: [], queryDefinitions: [] });

    render(CloudWatchLogsPage);

    await waitFor(() => {
      expect(screen.getByLabelText("Query")).toBeInTheDocument();
    });
    expect(screen.queryByText("Create Log Group")).not.toBeInTheDocument();
  });

  it("honors ?q= from the URL on load", async () => {
    setMockPageUrl(new URL("http://localhost/cloudwatchlogs?q=lambda"));
    mockSend.mockResolvedValue({
      logGroups: [
        { logGroupName: "/aws/lambda/my-function", storedBytes: 1024, retentionInDays: 30 },
        { logGroupName: "/aws/rds/my-db", storedBytes: 2048 },
      ],
    });

    render(CloudWatchLogsPage);

    await waitFor(() => {
      expect(screen.getByText("/aws/lambda/my-function")).toBeInTheDocument();
    });
    expect(screen.queryByText("/aws/rds/my-db")).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText("Search log groups...")).toHaveValue("lambda");
  });

  it("displays loaded log groups", async () => {
    mockSend.mockResolvedValue({
      logGroups: [
        { logGroupName: "/aws/lambda/my-function", storedBytes: 1024, retentionInDays: 30 },
        { logGroupName: "/aws/rds/my-db", storedBytes: 2048 },
      ],
    });
    render(CloudWatchLogsPage);
    await waitFor(
      () => {
        expect(screen.getByText("/aws/lambda/my-function")).toBeInTheDocument();
        expect(screen.getByText("/aws/rds/my-db")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create log group modal", async () => {
    mockSend.mockResolvedValue({ logGroups: [] });
    render(CloudWatchLogsPage);
    const btn = screen.getByText("Create Log Group");
    await fireEvent.click(btn);
    expect(screen.getByText("Group Name")).toBeInTheDocument();
    expect(screen.getByText("Retention Period")).toBeInTheDocument();
  });

  it("cancels create log group modal", async () => {
    mockSend.mockResolvedValue({ logGroups: [] });
    render(CloudWatchLogsPage);
    await fireEvent.click(screen.getByText("Create Log Group"));
    const cancelBtn = screen.getByText("Cancel");
    await fireEvent.click(cancelBtn);
    expect(screen.queryByText("Group Name")).not.toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ logGroups: [] });
    render(CloudWatchLogsPage);
    expect(screen.getByPlaceholderText("Search log groups...")).toBeInTheDocument();
  });

  it("navigates into group to see streams", async () => {
    mockSend.mockResolvedValueOnce({
      logGroups: [{ logGroupName: "/aws/lambda/test" }],
    });
    mockSend.mockResolvedValueOnce({
      logStreams: [{ logStreamName: "stream-1", lastEventTimestamp: Date.now() }],
    });
    render(CloudWatchLogsPage);
    await waitFor(
      () => {
        expect(screen.getByText("/aws/lambda/test")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    await fireEvent.click(screen.getByText("/aws/lambda/test"));
    await waitFor(
      () => {
        expect(screen.getByText("Create Stream")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
