import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/svelte";
import SageMakerPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSageMakerClient: () => ({ send: mockSend }),
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

describe("SageMaker Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({
      NotebookInstances: [],
      TrainingJobSummaries: [],
      Models: [],
      Endpoints: [],
    });
    render(SageMakerPage);
    expect(screen.getAllByText("Amazon SageMaker")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ NotebookInstances: [] });
    render(SageMakerPage);
    expect(screen.getAllByText("Notebooks")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ NotebookInstances: [] });
    render(SageMakerPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no notebooks", async () => {
    mockSend.mockResolvedValue({
      NotebookInstances: [],
      TrainingJobSummaries: [],
      Models: [],
      Endpoints: [],
    });
    render(SageMakerPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no notebook instances/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded notebooks", async () => {
    mockSend.mockResolvedValue({
      NotebookInstances: [
        {
          NotebookInstanceName: "my-notebook",
          NotebookInstanceStatus: "InService",
          InstanceType: "ml.t3.medium",
        },
      ],
      TrainingJobSummaries: [],
      Models: [],
      Endpoints: [],
    });
    render(SageMakerPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-notebook")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ NotebookInstances: [] });
    render(SageMakerPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Notebooks tab", () => {
    mockSend.mockResolvedValue({ NotebookInstances: [] });
    render(SageMakerPage);
    expect(screen.getAllByText("Notebooks").length).toBeGreaterThanOrEqual(1);
  });

  it("shows Models tab", () => {
    mockSend.mockResolvedValue({ NotebookInstances: [] });
    render(SageMakerPage);
    expect(screen.getAllByText("Models")[0]).toBeInTheDocument();
  });

  // loadData fans out ListNotebookInstances/ListTrainingJobs/ListModels/
  // ListEndpoints/ListPipelines together, so these tests key responses off
  // the command name rather than call order.
  function notebookCallCount(): number {
    return mockSend.mock.calls.filter(
      ([cmd]) => cmd?.constructor?.name === "ListNotebookInstancesCommand",
    ).length;
  }

  describe("All regions mode", () => {
    it("fans ListNotebookInstances out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
        if (cmd.constructor.name === "ListNotebookInstancesCommand") {
          return Promise.resolve({ NotebookInstances: [{ NotebookInstanceName: "my-notebook" }] });
        }
        return Promise.resolve({});
      });

      render(SageMakerPage);

      await waitFor(() => expect(screen.getAllByText("my-notebook")).toHaveLength(2));
      expect(notebookCallCount()).toBe(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListNotebookInstances call in single-region mode", async () => {
      mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
        if (cmd.constructor.name === "ListNotebookInstancesCommand") {
          return Promise.resolve({ NotebookInstances: [{ NotebookInstanceName: "my-notebook" }] });
        }
        return Promise.resolve({});
      });
      render(SageMakerPage);
      await waitFor(() => expect(screen.getAllByText("my-notebook")[0]).toBeInTheDocument());
      expect(notebookCallCount()).toBe(1);
    });

    it("renders the same notebook name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockImplementation((cmd: { constructor: { name: string } }) => {
        if (cmd.constructor.name === "ListNotebookInstancesCommand") {
          return Promise.resolve({
            NotebookInstances: [{ NotebookInstanceName: "shared-notebook" }],
          });
        }
        return Promise.resolve({});
      });

      render(SageMakerPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-notebook");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest("div.flex.items-center.gap-3") as HTMLElement).getByTestId("region-chip")
            .textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
