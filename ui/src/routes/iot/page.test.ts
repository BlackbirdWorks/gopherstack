import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import IoTPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getIoTClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("IoT Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ things: [] });
    render(IoTPage);
    expect(screen.getByText("AWS IoT Core")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend.mockResolvedValueOnce({ things: [] });
    render(IoTPage);
    expect(screen.getByText("Things")).toBeInTheDocument();
    expect(screen.getByText("Thing Groups")).toBeInTheDocument();
    expect(screen.getByText("Topic Rules")).toBeInTheDocument();
  });

  it("displays things", async () => {
    mockSend.mockResolvedValueOnce({
      things: [
        {
          thingName: "my-sensor",
          thingArn: "arn:iot:thing/my-sensor",
          thingTypeName: "TemperatureSensor",
        },
      ],
    });

    render(IoTPage);

    await waitFor(() => {
      expect(screen.getByText("my-sensor")).toBeInTheDocument();
    });
  });

  it("shows create thing modal", async () => {
    mockSend.mockResolvedValueOnce({ things: [] });
    render(IoTPage);
    await fireEvent.click(screen.getByText("Create Thing"));
    expect(screen.getByText("Create IoT Thing")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ things: [] });
    render(IoTPage);
    await waitFor(() => {
      expect(screen.getByText("No IoT things found")).toBeInTheDocument();
    });
  });
});
