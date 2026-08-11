import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import CloudWatchLogsPage from "./+page.svelte";
import { setMockPageUrl } from "$lib/mock-page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCloudWatchLogsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

describe("CloudWatch Logs Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
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

  describe("All regions mode", () => {
    const group = { logGroupName: "/aws/lambda/shared", storedBytes: 1024 };

    it("fans DescribeLogGroups out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ logGroups: [group] });
      mockSend.mockResolvedValueOnce({
        logGroups: [{ ...group, logGroupName: "/aws/lambda/eu-only" }],
      });

      render(CloudWatchLogsPage);

      await waitFor(() => expect(screen.getByText("/aws/lambda/shared")).toBeInTheDocument());
      expect(screen.getByText("/aws/lambda/eu-only")).toBeInTheDocument();

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeLogGroups call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ logGroups: [group] });
      render(CloudWatchLogsPage);
      await waitFor(() => expect(screen.getByText("/aws/lambda/shared")).toBeInTheDocument());
      const calls = mockSend.mock.calls.filter(
        ([cmd]) => cmd?.constructor?.name === "DescribeLogGroupsCommand",
      );
      expect(calls).toHaveLength(1);
    });

    it("renders the same log group name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ logGroups: [group] });
      mockSend.mockResolvedValueOnce({ logGroups: [group] });

      render(CloudWatchLogsPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("/aws/lambda/shared");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
