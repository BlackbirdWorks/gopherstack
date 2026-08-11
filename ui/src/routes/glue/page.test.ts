import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import GluePage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getGlueClient: () => ({ send: mockSend }),
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

describe("Glue Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    expect(screen.getByText("AWS Glue")).toBeInTheDocument();
  });

  it("shows catalog/jobs/crawlers/connections tabs", () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    expect(screen.getByText("Data Catalog")).toBeInTheDocument();
    expect(screen.getByText("ETL Jobs")).toBeInTheDocument();
    expect(screen.getByText("Crawlers")).toBeInTheDocument();
    expect(screen.getByText("Connections")).toBeInTheDocument();
  });

  it("shows empty state when no databases", async () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    await waitFor(
      () => {
        expect(screen.getByText("No databases found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded databases", async () => {
    mockSend.mockResolvedValue({
      DatabaseList: [
        { Name: "analytics_db", Description: "Analytics database", CreateTime: new Date() },
        { Name: "raw_data", Description: "Raw data lake", CreateTime: new Date() },
      ],
    });
    render(GluePage);
    await waitFor(
      () => {
        expect(screen.getByText("analytics_db")).toBeInTheDocument();
        expect(screen.getByText("raw_data")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to ETL jobs tab", async () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    mockSend.mockResolvedValue({ Jobs: [] });
    await fireEvent.click(screen.getByText("ETL Jobs"));
    await waitFor(
      () => {
        expect(screen.getByText("No ETL jobs found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays jobs in ETL tab", async () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    mockSend.mockResolvedValue({
      Jobs: [
        {
          Name: "transform-sales",
          Command: { Name: "glueetl" },
          Role: "arn:aws:iam::123:role/glue",
          CreatedOn: new Date(),
        },
      ],
    });
    await fireEvent.click(screen.getByText("ETL Jobs"));
    await waitFor(
      () => {
        expect(screen.getByText("transform-sales")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to crawlers tab", async () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    mockSend.mockResolvedValue({ Crawlers: [] });
    await fireEvent.click(screen.getByText("Crawlers"));
    await waitFor(
      () => {
        expect(screen.getByText("No crawlers found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ DatabaseList: [] });
    render(GluePage);
    expect(screen.getByPlaceholderText("Search...")).toBeInTheDocument();
  });

  describe("All regions mode", () => {
    it("fans GetDatabases out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DatabaseList: [{ Name: "analytics_db" }] });
      mockSend.mockResolvedValueOnce({ DatabaseList: [{ Name: "eu_db" }] });

      render(GluePage);

      await waitFor(() => expect(screen.getByText("analytics_db")).toBeInTheDocument());
      expect(screen.getByText("eu_db")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one GetDatabases call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ DatabaseList: [{ Name: "analytics_db" }] });
      render(GluePage);
      await waitFor(() => expect(screen.getByText("analytics_db")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same database name from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ DatabaseList: [{ Name: "shared_db" }] });
      mockSend.mockResolvedValueOnce({ DatabaseList: [{ Name: "shared_db" }] });

      render(GluePage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared_db");
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
