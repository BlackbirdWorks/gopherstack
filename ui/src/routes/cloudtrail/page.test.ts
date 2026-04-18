import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CloudTrailPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCloudTrailClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("CloudTrail Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ trailList: [] });
    render(CloudTrailPage);
    expect(screen.getByText("CloudTrail")).toBeInTheDocument();
  });

  it("shows both tabs", () => {
    mockSend.mockResolvedValueOnce({ trailList: [] });
    render(CloudTrailPage);
    expect(screen.getByText("Trails")).toBeInTheDocument();
    expect(screen.getByText("Event History")).toBeInTheDocument();
  });

  it("displays loaded trails", async () => {
    mockSend
      .mockResolvedValueOnce({
        trailList: [
          {
            Name: "my-trail",
            TrailARN: "arn:aws:cloudtrail:us-east-1:123:trail/my-trail",
            HomeRegion: "us-east-1",
            S3BucketName: "my-bucket",
          },
        ],
      })
      .mockResolvedValueOnce({ IsLogging: true });

    render(CloudTrailPage);

    await waitFor(() => {
      expect(screen.getByText("my-trail")).toBeInTheDocument();
    });
  });

  it("shows create trail modal", async () => {
    mockSend.mockResolvedValueOnce({ trailList: [] });
    render(CloudTrailPage);
    await fireEvent.click(screen.getByText("Create Trail"));
    expect(screen.getByText("Create Trail", { selector: "h2" })).toBeInTheDocument();
  });

  it("switches to event history tab", async () => {
    mockSend.mockResolvedValueOnce({ trailList: [] }).mockResolvedValueOnce({ Events: [] });
    render(CloudTrailPage);
    await fireEvent.click(screen.getByText("Event History"));
    expect(screen.getByText("Search Events")).toBeInTheDocument();
  });
});
