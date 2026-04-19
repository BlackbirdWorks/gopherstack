import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import FISPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getFISClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe("FIS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and Ignite Chaos button", () => {
    mockSend.mockResolvedValueOnce({ experiments: [] });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });

    render(FISPage);

    expect(screen.getByText("FIS")).toBeInTheDocument();
    expect(screen.getByText("Ignite Chaos")).toBeInTheDocument();
  });

  it("renders tabs for experiments and templates", () => {
    mockSend.mockResolvedValueOnce({ experiments: [] });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });

    render(FISPage);

    expect(screen.getByText("Live Experiments")).toBeInTheDocument();
    expect(screen.getByText("Experiment Templates")).toBeInTheDocument();
  });

  it("displays loaded experiments", async () => {
    mockSend.mockResolvedValueOnce({
      experiments: [
        { id: "exp-abc123", state: { status: "running" } },
        { id: "exp-def456", state: { status: "completed" } },
      ],
    });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });

    render(FISPage);

    await waitFor(
      () => {
        expect(screen.getByText("exp-abc123")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("exp-def456")).toBeInTheDocument();
  });

  it("displays templates when Templates tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ experiments: [] });
    mockSend.mockResolvedValueOnce({
      experimentTemplates: [{ id: "tmpl-111", description: "EC2 termination test" }],
    });

    render(FISPage);

    await waitFor(
      () => {
        expect(screen.getByText("Experiment Templates")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("Experiment Templates"));

    await waitFor(
      () => {
        expect(screen.getByText("EC2 termination test")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("filters experiments via search input", async () => {
    mockSend.mockResolvedValueOnce({
      experiments: [
        { id: "exp-alpha", state: { status: "running" } },
        { id: "exp-beta", state: { status: "completed" } },
      ],
    });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });

    render(FISPage);

    await waitFor(
      () => {
        expect(screen.getByText("exp-alpha")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search chaos assets...");
    await fireEvent.input(searchInput, { target: { value: "alpha" } });

    await waitFor(() => {
      expect(screen.queryByText("exp-beta")).not.toBeInTheDocument();
    });
    expect(screen.getByText("exp-alpha")).toBeInTheDocument();
  });

  it("shows empty state when no chaos assets exist", async () => {
    mockSend.mockResolvedValueOnce({ experiments: [] });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });

    render(FISPage);

    await waitFor(
      () => {
        expect(screen.getByText("No registered chaos assets.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("permission denied"));

    render(FISPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
