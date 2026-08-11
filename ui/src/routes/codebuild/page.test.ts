import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import CodeBuildPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCodeBuildClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
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

describe("CodeBuild Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ projects: [] });

    render(CodeBuildPage);

    expect(screen.getByText("CodeBuild Operations")).toBeInTheDocument();
    expect(screen.getByText("Deploy Build Project")).toBeInTheDocument();
  });

  it("displays loaded projects", async () => {
    mockSend.mockResolvedValueOnce({ projects: ["my-service-build", "api-tests"] });
    mockSend.mockResolvedValueOnce({
      projects: [
        { name: "my-service-build", source: { type: "GITHUB" } },
        { name: "api-tests", source: { type: "GITHUB" } },
      ],
    });

    render(CodeBuildPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-service-build")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("api-tests")).toBeInTheDocument();
  });

  it("filters projects via search input", async () => {
    mockSend.mockResolvedValueOnce({ projects: ["alpha-build", "beta-build", "gamma-build"] });
    mockSend.mockResolvedValueOnce({
      projects: [
        { name: "alpha-build", source: { type: "GITHUB" } },
        { name: "beta-build", source: { type: "GITHUB" } },
        { name: "gamma-build", source: { type: "GITHUB" } },
      ],
    });

    render(CodeBuildPage);

    await waitFor(
      () => {
        expect(screen.getByText("alpha-build")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search projects...");
    await fireEvent.input(searchInput, { target: { value: "beta" } });

    await waitFor(() => {
      expect(screen.queryByText("alpha-build")).not.toBeInTheDocument();
    });
    expect(screen.getByText("beta-build")).toBeInTheDocument();
    expect(screen.queryByText("gamma-build")).not.toBeInTheDocument();
  });

  it("opens create modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ projects: [] });

    render(CodeBuildPage);

    await fireEvent.click(screen.getByText("Deploy Build Project"));

    expect(screen.getByText("Assemble Build Blueprint")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("e.g. monolith-release-pipeline")).toBeInTheDocument();
  });

  it("closes create modal on abort click", async () => {
    mockSend.mockResolvedValueOnce({ projects: [] });

    render(CodeBuildPage);

    await fireEvent.click(screen.getByText("Deploy Build Project"));
    expect(screen.getByText("Assemble Build Blueprint")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Abort"));

    await waitFor(() => {
      expect(screen.queryByText("Assemble Build Blueprint")).not.toBeInTheDocument();
    });
  });

  it("creates a project via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ projects: [] });
    mockSend.mockResolvedValueOnce({ project: { name: "new-build" } });
    mockSend.mockResolvedValueOnce({ projects: ["new-build"] });
    mockSend.mockResolvedValueOnce({
      projects: [{ name: "new-build", source: { type: "GITHUB" } }],
    });

    render(CodeBuildPage);

    await fireEvent.click(screen.getByText("Deploy Build Project"));

    const nameInput = screen.getByPlaceholderText("e.g. monolith-release-pipeline");
    await fireEvent.input(nameInput, { target: { value: "new-build" } });

    const form = nameInput.closest("form")!;
    await fireEvent.submit(form);

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(4);
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalled();
  });

  it("selects a project and loads builds", async () => {
    mockSend.mockResolvedValueOnce({ projects: ["service-build"] });
    mockSend.mockResolvedValueOnce({
      projects: [{ name: "service-build", source: { type: "GITHUB" } }],
    });
    // selectProject: ListBuildsForProject
    mockSend.mockResolvedValueOnce({ ids: ["build-1", "build-2"] });
    // BatchGetBuilds
    mockSend.mockResolvedValueOnce({
      builds: [
        { id: "build-1", buildStatus: "SUCCEEDED", startTime: new Date() },
        { id: "build-2", buildStatus: "FAILED", startTime: new Date() },
      ],
    });

    render(CodeBuildPage);

    await waitFor(
      () => {
        expect(screen.getByText("service-build")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("service-build"));

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(4);
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no projects exist", async () => {
    mockSend.mockResolvedValueOnce({ projects: [] });

    render(CodeBuildPage);

    await waitFor(
      () => {
        expect(screen.getByText("No build projects found.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("permission denied"));

    render(CodeBuildPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  // Both regions' ListProjects calls fire before either's BatchGetProjects
  // (Promise.all starts every region's async function synchronously up to
  // its first await), so an ordered mockResolvedValueOnce queue would be
  // racy here. Key ListProjects off call count and BatchGetProjects off the
  // requested names instead, which is order-independent.
  function mockProjectsPerRegion(namesByCallOrder: string[][]): void {
    let listCalls = 0;
    mockSend.mockImplementation(
      (cmd: { constructor: { name: string }; input?: { names?: string[] } }) => {
        if (cmd.constructor.name === "ListProjectsCommand") {
          const names = namesByCallOrder[listCalls] ?? [];
          listCalls++;
          return Promise.resolve({ projects: names });
        }
        if (cmd.constructor.name === "BatchGetProjectsCommand") {
          const names = cmd.input?.names ?? [];
          return Promise.resolve({ projects: names.map((name) => ({ name })) });
        }
        return Promise.resolve({});
      },
    );
  }

  describe("All regions mode", () => {
    it("fans ListProjects out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockProjectsPerRegion([["my-service-build"], ["eu-build"]]);

      render(CodeBuildPage);

      await waitFor(() => expect(screen.getByText("my-service-build")).toBeInTheDocument());
      expect(screen.getByText("eu-build")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(4);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListProjects call in single-region mode", async () => {
      mockProjectsPerRegion([["my-service-build"]]);
      render(CodeBuildPage);
      await waitFor(() => expect(screen.getByText("my-service-build")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("renders the same project name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockProjectsPerRegion([["shared-build"], ["shared-build"]]);

      render(CodeBuildPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-build");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest('[role="button"]') as HTMLElement).getByTestId("region-chip")
            .textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
