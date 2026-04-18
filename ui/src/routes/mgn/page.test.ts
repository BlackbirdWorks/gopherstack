import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import MGNPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMGNClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("MGN Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    expect(screen.getAllByText("AWS Application Migration Service")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    expect(screen.getAllByText("Source Servers")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no source servers/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded servers", async () => {
    mockSend.mockResolvedValue({
      items: [
        {
          sourceServerID: "s-abc123",
          dataReplicationInfo: { dataReplicationState: "REPLICATED" },
          lifeCycle: { state: "READY_FOR_TEST" },
        },
      ],
    });
    render(MGNPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("s-abc123")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Applications tab", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    expect(screen.getAllByText("Applications")[0]).toBeInTheDocument();
  });

  it("shows Waves tab", () => {
    mockSend.mockResolvedValue({ items: [] });
    render(MGNPage);
    expect(screen.getAllByText("Waves")[0]).toBeInTheDocument();
  });
});
