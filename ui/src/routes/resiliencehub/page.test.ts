import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import ResilienceHubPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getResiliencehubClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Resilience Hub Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ appSummaries: [], assessmentSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getAllByText("AWS Resilience Hub")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getAllByText("Applications")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValue({ appSummaries: [], assessmentSummaries: [] });
    render(ResilienceHubPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no applications/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded apps", async () => {
    mockSend.mockResolvedValue({
      appSummaries: [
        {
          name: "my-resilient-app",
          appArn: "arn:aws:resiliencehub:us-east-1:123:app/abc",
          complianceStatus: "PolicyMet",
        },
      ],
      assessmentSummaries: [],
    });
    render(ResilienceHubPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-resilient-app")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Assessments tab", () => {
    mockSend.mockResolvedValue({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getAllByText("Assessments")[0]).toBeInTheDocument();
  });

  it("shows Policy Met stat", () => {
    mockSend.mockResolvedValue({ appSummaries: [] });
    render(ResilienceHubPage);
    expect(screen.getAllByText("Policy Met")[0]).toBeInTheDocument();
  });
});
