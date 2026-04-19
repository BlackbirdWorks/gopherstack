import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import EventBridgePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEventBridgeClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("EventBridge Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    expect(screen.getByText("EventBridge")).toBeInTheDocument();
  });

  it("shows Create Bus button", () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    expect(screen.getByText("Create Bus")).toBeInTheDocument();
  });

  it("shows empty state when no buses", async () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    await waitFor(
      () => {
        expect(screen.getByText("No event buses found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded event buses", async () => {
    mockSend.mockResolvedValueOnce({
      EventBuses: [
        { Name: "default", Arn: "arn:aws:events:us-east-1:123:event-bus/default" },
        { Name: "my-custom-bus", Arn: "arn:aws:events:us-east-1:123:event-bus/my-custom-bus" },
      ],
    });
    render(EventBridgePage);
    await waitFor(
      () => {
        expect(screen.getByText("default")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("my-custom-bus")).toBeInTheDocument();
  });

  it("filters buses via search", async () => {
    mockSend.mockResolvedValueOnce({
      EventBuses: [
        { Name: "default", Arn: "arn:1" },
        { Name: "app-events", Arn: "arn:2" },
      ],
    });
    render(EventBridgePage);
    await waitFor(
      () => {
        expect(screen.getByText("app-events")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search event buses...");
    await fireEvent.input(searchInput, { target: { value: "app" } });
    await waitFor(() => {
      expect(screen.queryByText("default")).not.toBeInTheDocument();
    });
    expect(screen.getByText("app-events")).toBeInTheDocument();
  });

  it("opens create bus modal", async () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    await fireEvent.click(screen.getByText("Create Bus"));
    await waitFor(() => {
      expect(screen.getByText("Create Event Bus")).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. my-app-events")).toBeInTheDocument();
  });

  it("opens put events modal", async () => {
    mockSend.mockResolvedValue({ EventBuses: [] });
    render(EventBridgePage);
    await fireEvent.click(screen.getByText("Put Events"));
    await waitFor(() => {
      expect(screen.getByText("Put Events", { selector: "h2" })).toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(EventBridgePage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
