import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import OutpostsPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getOutpostsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Outposts Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Outposts: [], Sites: [] });
    render(OutpostsPage);
    expect(screen.getAllByText("AWS Outposts")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getAllByText("Outposts")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({ Outposts: [], Sites: [] });
    render(OutpostsPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no outposts/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded outposts", async () => {
    mockSend.mockResolvedValue({
      Outposts: [
        {
          Name: "my-outpost",
          OutpostId: "op-123",
          AvailabilityZone: "us-east-1a",
          LifeCycleStatus: "ACTIVE",
        },
      ],
      Sites: [],
    });
    render(OutpostsPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-outpost")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Sites tab", () => {
    mockSend.mockResolvedValue({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getAllByText("Sites")[0]).toBeInTheDocument();
  });

  it("shows Active stat", () => {
    mockSend.mockResolvedValue({ Outposts: [] });
    render(OutpostsPage);
    expect(screen.getAllByText("Active")[0]).toBeInTheDocument();
  });
});
