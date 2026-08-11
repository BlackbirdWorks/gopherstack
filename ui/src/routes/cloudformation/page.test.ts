import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import CloudFormationPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCloudFormationClient: () => ({ send: mockSend }),
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

describe("CloudFormation Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Stacks: [] });
    render(CloudFormationPage);
    expect(screen.getByText("CloudFormation")).toBeInTheDocument();
  });

  it("shows Create Stack button", () => {
    mockSend.mockResolvedValue({ Stacks: [] });
    render(CloudFormationPage);
    expect(screen.getByText("Create Stack")).toBeInTheDocument();
  });

  it("shows empty state when no stacks", async () => {
    mockSend.mockResolvedValue({ Stacks: [] });
    render(CloudFormationPage);
    await waitFor(
      () => {
        expect(screen.getByText("No stacks found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded stacks", async () => {
    mockSend.mockResolvedValue({
      Stacks: [
        { StackName: "my-web-stack", StackStatus: "CREATE_COMPLETE", CreationTime: new Date() },
        { StackName: "my-api-stack", StackStatus: "UPDATE_IN_PROGRESS", CreationTime: new Date() },
      ],
    });
    render(CloudFormationPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-web-stack")).toBeInTheDocument();
        expect(screen.getByText("my-api-stack")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create stack modal", async () => {
    mockSend.mockResolvedValue({ Stacks: [] });
    render(CloudFormationPage);
    await fireEvent.click(screen.getByText("Create Stack"));
    expect(screen.getByText("Stack Name")).toBeInTheDocument();
    expect(screen.getByText("Template Body (JSON/YAML)")).toBeInTheDocument();
  });

  it("cancels create stack modal", async () => {
    mockSend.mockResolvedValue({ Stacks: [] });
    render(CloudFormationPage);
    await fireEvent.click(screen.getByText("Create Stack"));
    const cancelBtn = screen.getByText("Cancel");
    await fireEvent.click(cancelBtn);
    expect(screen.queryByText("Stack Name")).not.toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ Stacks: [] });
    render(CloudFormationPage);
    expect(screen.getByPlaceholderText("Search stacks...")).toBeInTheDocument();
  });

  it("shows stack status badges", async () => {
    mockSend.mockResolvedValue({
      Stacks: [
        { StackName: "test-stack", StackStatus: "CREATE_COMPLETE", CreationTime: new Date() },
      ],
    });
    render(CloudFormationPage);
    await waitFor(
      () => {
        expect(screen.getByText("CREATE_COMPLETE")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans DescribeStacks out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Stacks: [{ StackName: "my-web-stack" }] });
      mockSend.mockResolvedValueOnce({ Stacks: [{ StackName: "eu-stack" }] });

      render(CloudFormationPage);

      await waitFor(() => expect(screen.getByText("my-web-stack")).toBeInTheDocument());
      expect(screen.getByText("eu-stack")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeStacks call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ Stacks: [{ StackName: "my-web-stack" }] });
      render(CloudFormationPage);
      await waitFor(() => expect(screen.getByText("my-web-stack")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same stack name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Stacks: [{ StackName: "shared-stack" }] });
      mockSend.mockResolvedValueOnce({ Stacks: [{ StackName: "shared-stack" }] });

      render(CloudFormationPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared-stack");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("tr") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
