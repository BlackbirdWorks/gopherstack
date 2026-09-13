import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CodeCommitPage from "./+page.svelte";
import { DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCodeCommitClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("CodeCommit Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    expect(screen.getByText("AWS CodeCommit")).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    expect(screen.getByText("Repositories")).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    expect(screen.getByPlaceholderText(/search repositories/i)).toBeInTheDocument();
  });

  it("shows empty state when no repositories", async () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    await waitFor(
      () => {
        expect(screen.getByText(/no repositories/i)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded repositories", async () => {
    mockSend.mockResolvedValue({
      repositories: [{ repositoryName: "my-repo", repositoryId: "abc-123" }],
    });
    render(CodeCommitPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-repo")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Branches stat card", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    expect(screen.getByText("Branches")).toBeInTheDocument();
  });

  it("shows detail placeholder when no repo selected", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(CodeCommitPage);
    expect(screen.getByText(/select a repository/i)).toBeInTheDocument();
  });

  // codecommit's real PullRequestStatusEnum has exactly two members, OPEN and
  // CLOSED (gopherstack-cx05) -- a merged PR now correctly carries CLOSED, so
  // this asserts both an open PR and a merged-then-closed PR render with the
  // real statuses and neither ever renders the fabricated MERGED label.
  it("renders OPEN and CLOSED pull request badges without a fabricated MERGED status", async () => {
    mockSend.mockResolvedValueOnce({ repositories: [{ repositoryName: "repo1" }] });
    mockSend.mockResolvedValueOnce({
      repositoryMetadata: { repositoryName: "repo1", repositoryId: "id1", defaultBranch: "main" },
    });
    mockSend.mockResolvedValueOnce({ branches: ["main"] });
    mockSend.mockResolvedValueOnce({ pullRequestIds: ["1", "2"] });
    mockSend.mockResolvedValueOnce({
      pullRequest: {
        pullRequestId: "1",
        title: "Open feature PR",
        pullRequestStatus: "OPEN",
        pullRequestTargets: [{ sourceReference: "feature", destinationReference: "main" }],
      },
    });
    mockSend.mockResolvedValueOnce({
      pullRequest: {
        pullRequestId: "2",
        title: "Merged feature PR",
        pullRequestStatus: "CLOSED",
        pullRequestTargets: [{ sourceReference: "old-feature", destinationReference: "main" }],
      },
    });

    render(CodeCommitPage);

    await waitFor(() => expect(screen.getByText("repo1")).toBeInTheDocument());
    await fireEvent.click(screen.getByText("repo1"));
    await waitFor(() => expect(screen.getByText("Pull Requests")).toBeInTheDocument());
    await fireEvent.click(screen.getByText("Pull Requests"));

    await waitFor(() => expect(screen.getByText("Open feature PR")).toBeInTheDocument());
    expect(screen.getByText("Merged feature PR")).toBeInTheDocument();

    const openBadge = screen.getByText("OPEN");
    expect(openBadge.className).toContain("bg-green-100");

    const closedBadge = screen.getByText("CLOSED");
    expect(closedBadge.className).toContain("bg-gray-100");
    expect(closedBadge.className).not.toContain("purple");

    expect(screen.queryByText("MERGED")).not.toBeInTheDocument();
  });
});
