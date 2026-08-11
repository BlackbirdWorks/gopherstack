import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SSMPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSSMClient: () => ({ send: mockSend }),
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

function describeParametersCalls() {
  return mockSend.mock.calls.filter(
    ([cmd]) => cmd?.constructor?.name === "DescribeParametersCommand",
  );
}

describe("SSM Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    // Every test below predates "All" mode and assumes exactly one
    // DescribeParameters/DescribeMaintenanceWindows call per load against a
    // single region, so pin single-region mode here; the "All regions mode"
    // describe block below opts back in.
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    expect(screen.getByText("AWS Systems Manager")).toBeInTheDocument();
  });

  it("shows Create Parameter button", () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    expect(screen.getByText("Create Parameter")).toBeInTheDocument();
  });

  it("shows empty state when no parameters", async () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    await waitFor(
      () => {
        expect(screen.getByText("No parameters found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded parameters", async () => {
    mockSend.mockResolvedValueOnce({
      Parameters: [
        {
          Name: "/myapp/db/password",
          Type: "SecureString",
          LastModifiedDate: new Date(),
          Version: 3,
        },
        { Name: "/myapp/config/region", Type: "String", LastModifiedDate: new Date(), Version: 1 },
      ],
    });
    render(SSMPage);
    await waitFor(
      () => {
        expect(screen.getByText("/myapp/db/password")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("/myapp/config/region")).toBeInTheDocument();
  });

  it("opens create modal", async () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    await fireEvent.click(screen.getByText("Create Parameter"));
    await waitFor(() => {
      expect(screen.getByText("Create Parameter", { selector: "h2" })).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText("e.g. /myapp/database/password")).toBeInTheDocument();
  });

  it("closes modal on cancel", async () => {
    mockSend.mockResolvedValue({ Parameters: [] });
    render(SSMPage);
    await fireEvent.click(screen.getByText("Create Parameter"));
    await waitFor(() => {
      expect(screen.getByPlaceholderText("e.g. /myapp/database/password")).toBeInTheDocument();
    });
    await fireEvent.click(screen.getByText("Cancel"));
    await waitFor(() => {
      expect(
        screen.queryByPlaceholderText("e.g. /myapp/database/password"),
      ).not.toBeInTheDocument();
    });
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(SSMPage);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans DescribeParameters out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Parameters: [{ Name: "/us/param" }] });
      mockSend.mockResolvedValueOnce({ Parameters: [{ Name: "/eu/param" }] });
      mockSend.mockResolvedValue({ WindowIdentities: [] });

      render(SSMPage);

      await waitFor(() => expect(screen.getByText("/us/param")).toBeInTheDocument());
      expect(screen.getByText("/eu/param")).toBeInTheDocument();
      expect(describeParametersCalls()).toHaveLength(2);

      vi.unstubAllGlobals();
    });

    it("falls back to just the default region when no region has data", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData([]);
      mockSend.mockResolvedValueOnce({ Parameters: [{ Name: "/solo/param" }] });
      mockSend.mockResolvedValue({ WindowIdentities: [] });

      render(SSMPage);

      await waitFor(() => expect(screen.getByText("/solo/param")).toBeInTheDocument());
      expect(describeParametersCalls()).toHaveLength(1);

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeParameters call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ Parameters: [{ Name: "/solo/param" }] });
      mockSend.mockResolvedValue({ WindowIdentities: [] });
      render(SSMPage);
      await waitFor(() => expect(screen.getByText("/solo/param")).toBeInTheDocument());
      expect(describeParametersCalls()).toHaveLength(1);
    });

    it("renders the same parameter name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Parameters: [{ Name: "/shared/param" }] });
      mockSend.mockResolvedValueOnce({ Parameters: [{ Name: "/shared/param" }] });
      mockSend.mockResolvedValue({ WindowIdentities: [] });

      render(SSMPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("/shared/param");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) => within(r.closest("button") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });
  });
});
