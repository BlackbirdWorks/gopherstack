import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SQSPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSQSClient: () => ({ send: mockSend }),
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

describe("SQS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ QueueUrls: [], Attributes: {} });
    render(SQSPage);
    expect(screen.getByText("SQS Queues")).toBeInTheDocument();
  });

  it("shows Create Queue button", () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    expect(screen.getByText("Create Queue")).toBeInTheDocument();
  });

  it("shows empty state when no queues", async () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    await waitFor(
      () => {
        expect(screen.getByText("No queues found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded queues", async () => {
    mockSend.mockResolvedValueOnce({
      QueueUrls: [
        "https://sqs.us-east-1.amazonaws.com/123/order-queue",
        "https://sqs.us-east-1.amazonaws.com/123/dead-letter",
      ],
    });
    mockSend.mockResolvedValue({
      Attributes: { ApproximateNumberOfMessages: "5", VisibilityTimeout: "30" },
    });
    render(SQSPage);
    await waitFor(
      () => {
        expect(screen.getByText("order-queue")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("dead-letter")).toBeInTheDocument();
  });

  it("filters queues via search", async () => {
    mockSend.mockResolvedValueOnce({
      QueueUrls: [
        "https://sqs.us-east-1.amazonaws.com/123/order-queue",
        "https://sqs.us-east-1.amazonaws.com/123/payment-queue",
      ],
    });
    mockSend.mockResolvedValue({ Attributes: {} });
    render(SQSPage);
    await waitFor(
      () => {
        expect(screen.getByText("order-queue")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search queues...");
    await fireEvent.input(searchInput, { target: { value: "payment" } });
    await waitFor(() => {
      expect(screen.queryByText("order-queue")).not.toBeInTheDocument();
    });
    expect(screen.getByText("payment-queue")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    await fireEvent.click(screen.getByText("Create Queue"));
    await waitFor(() => {
      expect(screen.getByText("Create Queue", { selector: "h2" })).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. order-processing")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ QueueUrls: [] });
    render(SQSPage);
    await fireEvent.click(screen.getByText("Create Queue"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. order-processing")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. order-processing")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("permission denied"));
    render(SQSPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  // Both regions' ListQueues calls fire before either's per-URL
  // GetQueueAttributes calls (Promise.all starts every region's async
  // function synchronously up to its first await), so an ordered
  // mockResolvedValueOnce queue would be racy. Key ListQueues off call
  // count instead, which is order-independent; GetQueueAttributes returns
  // generic attributes since the tests below only care about queue identity.
  function mockQueuesPerRegion(urlsByCallOrder: string[][]): void {
    let listCalls = 0;
    mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
      if (cmd.constructor.name === "ListQueuesCommand") {
        const urls = urlsByCallOrder[listCalls] ?? [];
        listCalls++;
        return Promise.resolve({ QueueUrls: urls });
      }
      if (cmd.constructor.name === "GetQueueAttributesCommand") {
        return Promise.resolve({ Attributes: { ApproximateNumberOfMessages: "0" } });
      }
      return Promise.resolve({});
    });
  }

  describe("All regions mode", () => {
    it("fans ListQueues out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockQueuesPerRegion([
        ["https://sqs.us-east-1.amazonaws.com/123/order-queue"],
        ["https://sqs.eu-west-1.amazonaws.com/123/eu-queue"],
      ]);

      render(SQSPage);

      await waitFor(() => expect(screen.getByText("order-queue")).toBeInTheDocument());
      expect(screen.getByText("eu-queue")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(4);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListQueues call in single-region mode", async () => {
      mockQueuesPerRegion([["https://sqs.us-east-1.amazonaws.com/123/order-queue"]]);
      render(SQSPage);
      await waitFor(() => expect(screen.getByText("order-queue")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("renders the same queue name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockQueuesPerRegion([
        ["https://sqs.us-east-1.amazonaws.com/123/shared-queue"],
        ["https://sqs.eu-west-1.amazonaws.com/123/shared-queue"],
      ]);

      render(SQSPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-queue");
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
