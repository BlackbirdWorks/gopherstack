import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import KinesisPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getKinesisClient: () => ({ send: mockSend }),
  getCloudWatchClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Kinesis Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    expect(screen.getByText("Kinesis Data Streams")).toBeInTheDocument();
  });

  it("shows Create Stream button", () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    expect(screen.getByText("Create Stream")).toBeInTheDocument();
  });

  it("shows empty state when no streams", async () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    await waitFor(
      () => {
        expect(screen.getByText("No streams found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded streams", async () => {
    mockSend.mockResolvedValueOnce({ StreamNames: ["user-events", "order-stream"] });
    render(KinesisPage);
    await waitFor(
      () => {
        expect(screen.getByText("user-events")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("order-stream")).toBeInTheDocument();
  });

  it("filters streams via search", async () => {
    mockSend.mockResolvedValueOnce({
      StreamNames: ["user-events", "order-stream", "click-stream"],
    });
    render(KinesisPage);
    await waitFor(
      () => {
        expect(screen.getByText("user-events")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search streams...");
    await fireEvent.input(searchInput, { target: { value: "order" } });
    await waitFor(() => {
      expect(screen.queryByText("user-events")).not.toBeInTheDocument();
    });
    expect(screen.getByText("order-stream")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    await fireEvent.click(screen.getByText("Create Stream"));
    await waitFor(() => {
      expect(screen.getByText("Create Data Stream")).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. user-events")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    await fireEvent.click(screen.getByText("Create Stream"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. user-events")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. user-events")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("permission denied"));
    render(KinesisPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("shows 4 stat cards", () => {
    mockSend.mockResolvedValue({ StreamNames: [] });
    render(KinesisPage);
    expect(screen.getByText("Total Streams")).toBeInTheDocument();
    expect(screen.getByText("Open Shards")).toBeInTheDocument();
    expect(screen.getByText("Consumers")).toBeInTheDocument();
    expect(screen.getByText("Shards Used/Limit")).toBeInTheDocument();
  });
});
