import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import MetricsPage from "./+page.svelte";

vi.mock("svelte-sonner", () => ({
  toast: { error: vi.fn(), success: vi.fn() },
}));

vi.mock("$lib/api/connect-client", () => {
  return {
    dashboardClient: {
      streamMetrics: vi.fn().mockImplementation(async function* () {
        yield {
          dashboard: {
            runtime: {
              goroutines: 42,
              heapAllocMb: 10.5,
              heapSysMb: 20.0,
              heapInuseMb: 8.0,
              numServices: 3,
              totalAllocMb: 50.0,
            },
            operations: [
              {
                service: "DynamoDB",
                operation: "DynamoDB::ListTables",
                count: 100,
                errorCount: 0,
                avgMs: 15.5,
                p50Ms: 12.0,
                p95Ms: 25.0,
                p99Ms: 30.0,
                maxMs: 45.0,
              },
            ],
            workers: [],
          },
        };
        await new Promise<void>((resolve) => {
          setTimeout(resolve, 5000);
        });
      }),
    },
  };
});

describe("Metrics Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders metrics from the stream", async () => {
    render(MetricsPage);

    expect(screen.getByText("Performance Metrics")).toBeInTheDocument();
    expect(screen.getByText("Gathering insights...")).toBeInTheDocument();

    await waitFor(
      () => {
        expect(screen.queryByText("Gathering insights...")).not.toBeInTheDocument();
      },
      { timeout: 5000 },
    );

    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("10.50 MB")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText("ListTables")).toBeInTheDocument();
    expect(screen.getByText("DynamoDB")).toBeInTheDocument();
    expect(screen.getByText("100")).toBeInTheDocument();
  });
});
