import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SNSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSNSClient: () => ({ send: mockSend }),
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

describe("SNS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Topics: [], Attributes: {} });
    render(SNSPage);
    expect(screen.getByText("SNS Topics")).toBeInTheDocument();
  });

  it("shows Create Topic button", () => {
    mockSend.mockResolvedValue({ Topics: [] });
    render(SNSPage);
    expect(screen.getByText("Create Topic")).toBeInTheDocument();
  });

  it("shows empty state when no topics", async () => {
    mockSend.mockResolvedValue({ Topics: [] });
    render(SNSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No topics found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded topics", async () => {
    mockSend.mockResolvedValueOnce({
      Topics: [
        { TopicArn: "arn:aws:sns:us-east-1:123:my-alerts" },
        { TopicArn: "arn:aws:sns:us-east-1:123:notifications.fifo" },
      ],
    });
    // GetTopicAttributes for each
    mockSend.mockResolvedValue({ Attributes: { SubscriptionsConfirmed: "2" } });
    render(SNSPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-alerts")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("notifications.fifo")).toBeInTheDocument();
  });

  it("filters topics via search", async () => {
    mockSend.mockResolvedValueOnce({
      Topics: [
        { TopicArn: "arn:aws:sns:us-east-1:123:billing-alerts" },
        { TopicArn: "arn:aws:sns:us-east-1:123:order-events" },
      ],
    });
    mockSend.mockResolvedValue({ Attributes: {} });
    render(SNSPage);
    await waitFor(
      () => {
        expect(screen.getByText("billing-alerts")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search topics...");
    await fireEvent.input(searchInput, { target: { value: "billing" } });
    await waitFor(() => {
      expect(screen.queryByText("order-events")).not.toBeInTheDocument();
    });
    expect(screen.getByText("billing-alerts")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ Topics: [] });
    render(SNSPage);
    await fireEvent.click(screen.getByText("Create Topic"));
    await waitFor(() => {
      expect(screen.getByText("Create Topic", { selector: "h2" })).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. my-notifications")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ Topics: [] });
    render(SNSPage);
    await fireEvent.click(screen.getByText("Create Topic"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. my-notifications")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. my-notifications")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(SNSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  // Both regions' ListTopics calls fire before either's per-ARN
  // GetTopicAttributes calls (Promise.all starts every region's async
  // function synchronously up to its first await), so an ordered
  // mockResolvedValueOnce queue would be racy. Key ListTopics off call
  // count instead, which is order-independent.
  function mockTopicsPerRegion(arnsByCallOrder: string[][]): void {
    let listCalls = 0;
    mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
      if (cmd.constructor.name === "ListTopicsCommand") {
        const arns = arnsByCallOrder[listCalls] ?? [];
        listCalls++;
        return Promise.resolve({ Topics: arns.map((TopicArn) => ({ TopicArn })) });
      }
      if (cmd.constructor.name === "GetTopicAttributesCommand") {
        return Promise.resolve({ Attributes: { SubscriptionsConfirmed: "0" } });
      }
      return Promise.resolve({});
    });
  }

  describe("All regions mode", () => {
    it("fans ListTopics out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockTopicsPerRegion([
        ["arn:aws:sns:us-east-1:123:my-alerts"],
        ["arn:aws:sns:eu-west-1:123:eu-alerts"],
      ]);

      render(SNSPage);

      await waitFor(() => expect(screen.getByText("my-alerts")).toBeInTheDocument());
      expect(screen.getByText("eu-alerts")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(4);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListTopics call in single-region mode", async () => {
      mockTopicsPerRegion([["arn:aws:sns:us-east-1:123:my-alerts"]]);
      render(SNSPage);
      await waitFor(() => expect(screen.getByText("my-alerts")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("renders the same topic name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockTopicsPerRegion([
        ["arn:aws:sns:us-east-1:123:shared-topic"],
        ["arn:aws:sns:eu-west-1:123:shared-topic"],
      ]);

      render(SNSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-topic");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".rounded-lg") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
