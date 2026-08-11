import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ECRPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getECRClient: () => ({ send: mockSend }),
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

describe("ECR Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // DescribeRepositories call per action against a single region, so pin
    // single-region mode here; the "All regions mode" describe block below
    // opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(ECRPage);
    expect(screen.getByText("ECR Repositories")).toBeInTheDocument();
  });

  it("shows Create Repository button", () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(ECRPage);
    expect(screen.getByText("Create Repository")).toBeInTheDocument();
  });

  it("shows empty state when no repos", async () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(ECRPage);
    await waitFor(
      () => {
        expect(screen.getByText("No repositories found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded repositories", async () => {
    mockSend.mockResolvedValueOnce({
      repositories: [
        {
          repositoryName: "my-service/api",
          repositoryUri: "123.dkr.ecr.us-east-1.amazonaws.com/my-service/api",
          imageTagMutability: "MUTABLE",
          createdAt: new Date(),
        },
        {
          repositoryName: "frontend",
          repositoryUri: "123.dkr.ecr.us-east-1.amazonaws.com/frontend",
          imageTagMutability: "IMMUTABLE",
          createdAt: new Date(),
        },
      ],
    });
    render(ECRPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-service/api")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("frontend")).toBeInTheDocument();
  });

  it("filters repositories via search", async () => {
    mockSend.mockResolvedValueOnce({
      repositories: [
        {
          repositoryName: "backend-service",
          repositoryUri: "uri1",
          imageTagMutability: "MUTABLE",
          createdAt: new Date(),
        },
        {
          repositoryName: "frontend-app",
          repositoryUri: "uri2",
          imageTagMutability: "MUTABLE",
          createdAt: new Date(),
        },
      ],
    });
    render(ECRPage);
    await waitFor(
      () => {
        expect(screen.getByText("backend-service")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search repositories...");
    await fireEvent.input(searchInput, { target: { value: "frontend" } });
    await waitFor(() => {
      expect(screen.queryByText("backend-service")).not.toBeInTheDocument();
    });
    expect(screen.getByText("frontend-app")).toBeInTheDocument();
  });

  it("opens create repository modal", async () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(ECRPage);
    await fireEvent.click(screen.getByText("Create Repository"));
    await waitFor(() => {
      expect(screen.getByText("Create Repository", { selector: "h2" })).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. my-service/api")).toBeInTheDocument();
  });

  it("closes create modal on cancel", async () => {
    mockSend.mockResolvedValue({ repositories: [] });
    render(ECRPage);
    await fireEvent.click(screen.getByText("Create Repository"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. my-service/api")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. my-service/api")).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(ECRPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  // ECR's onRegionChange also fires loadRegistryFeatures (6 unrelated
  // calls), so these tests key responses off the command name rather than
  // call order -- an ordered mockResolvedValueOnce queue would be racy
  // against those unrelated calls.
  function describeReposCallCount(): number {
    return mockSend.mock.calls.filter(
      ([cmd]) => cmd?.constructor?.name === "DescribeRepositoriesCommand",
    ).length;
  }

  describe("All regions mode", () => {
    it("fans DescribeRepositories out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
        if (cmd.constructor.name === "DescribeRepositoriesCommand") {
          return Promise.resolve({ repositories: [{ repositoryName: "my-service/api" }] });
        }
        return Promise.resolve({});
      });

      render(ECRPage);

      await waitFor(() => expect(screen.getAllByText("my-service/api")).toHaveLength(2));
      expect(describeReposCallCount()).toBe(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeRepositories call in single-region mode", async () => {
      mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
        if (cmd.constructor.name === "DescribeRepositoriesCommand") {
          return Promise.resolve({ repositories: [{ repositoryName: "my-service/api" }] });
        }
        return Promise.resolve({});
      });
      render(ECRPage);
      await waitFor(() => expect(screen.getByText("my-service/api")).toBeInTheDocument());
      expect(describeReposCallCount()).toBe(1);
    });

    it("renders the same repository name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
        if (cmd.constructor.name === "DescribeRepositoriesCommand") {
          return Promise.resolve({ repositories: [{ repositoryName: "shared-repo" }] });
        }
        return Promise.resolve({});
      });

      render(ECRPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-repo");
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
