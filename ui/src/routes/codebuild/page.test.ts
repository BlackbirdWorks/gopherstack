import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CodeBuildPage from "./+page.svelte";

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

describe("CodeBuild Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
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
});
