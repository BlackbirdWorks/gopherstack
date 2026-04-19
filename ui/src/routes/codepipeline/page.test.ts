import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CodePipelinePage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCodePipelineClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe("CodePipeline Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ pipelines: [] });

    render(CodePipelinePage);

    expect(screen.getByText("CodePipeline Engine")).toBeInTheDocument();
    expect(screen.getByText("Deploy Pipeline")).toBeInTheDocument();
  });

  it("displays loaded pipelines", async () => {
    mockSend.mockResolvedValueOnce({
      pipelines: [{ name: "prod-delivery" }, { name: "staging-pipeline" }],
    });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "prod-delivery", stages: [] } });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "staging-pipeline", stages: [] } });

    render(CodePipelinePage);

    await waitFor(
      () => {
        expect(screen.getByText("prod-delivery")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("staging-pipeline")).toBeInTheDocument();
  });

  it("filters pipelines via search input", async () => {
    mockSend.mockResolvedValueOnce({
      pipelines: [{ name: "prod-delivery" }, { name: "staging-pipeline" }, { name: "dev-tests" }],
    });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "prod-delivery", stages: [] } });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "staging-pipeline", stages: [] } });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "dev-tests", stages: [] } });

    render(CodePipelinePage);

    await waitFor(
      () => {
        expect(screen.getByText("prod-delivery")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search pipelines...");
    await fireEvent.input(searchInput, { target: { value: "staging" } });

    await waitFor(() => {
      expect(screen.queryByText("prod-delivery")).not.toBeInTheDocument();
    });
    expect(screen.getByText("staging-pipeline")).toBeInTheDocument();
    expect(screen.queryByText("dev-tests")).not.toBeInTheDocument();
  });

  it("opens create modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ pipelines: [] });

    render(CodePipelinePage);

    await fireEvent.click(screen.getByText("Deploy Pipeline"));

    expect(screen.getByText("Assemble Pipeline")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("e.g. core-services-delivery-prod")).toBeInTheDocument();
  });

  it("closes create modal on abort click", async () => {
    mockSend.mockResolvedValueOnce({ pipelines: [] });

    render(CodePipelinePage);

    await fireEvent.click(screen.getByText("Deploy Pipeline"));
    expect(screen.getByText("Assemble Pipeline")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Abort"));

    await waitFor(() => {
      expect(screen.queryByText("Assemble Pipeline")).not.toBeInTheDocument();
    });
  });

  it("creates a pipeline via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ pipelines: [] });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "new-pipeline", stages: [] } });
    mockSend.mockResolvedValueOnce({
      pipelines: [{ name: "new-pipeline" }],
    });
    mockSend.mockResolvedValueOnce({ pipeline: { name: "new-pipeline", stages: [] } });

    render(CodePipelinePage);

    await fireEvent.click(screen.getByText("Deploy Pipeline"));

    const nameInput = screen.getByPlaceholderText("e.g. core-services-delivery-prod");
    await fireEvent.input(nameInput, { target: { value: "new-pipeline" } });

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

  it("selects a pipeline and loads state", async () => {
    mockSend.mockResolvedValueOnce({ pipelines: [{ name: "my-pipeline" }] });
    mockSend.mockResolvedValueOnce({
      pipeline: { name: "my-pipeline", stages: [{ name: "Source", actions: [] }] },
    });
    // selectPipeline: GetPipelineStateCommand
    mockSend.mockResolvedValueOnce({ stageStates: [] });

    render(CodePipelinePage);

    await waitFor(
      () => {
        expect(screen.getByText("my-pipeline")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("my-pipeline"));

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(3);
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no pipelines exist", async () => {
    mockSend.mockResolvedValueOnce({ pipelines: [] });

    render(CodePipelinePage);

    await waitFor(
      () => {
        expect(screen.getByText("No pipelines provisioned.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network error"));

    render(CodePipelinePage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
