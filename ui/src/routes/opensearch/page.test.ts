import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import OpenSearchPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getOpenSearchClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("OpenSearch Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ DomainNames: [] });
    render(OpenSearchPage);
    expect(screen.getByText("Amazon OpenSearch Service")).toBeInTheDocument();
  });

  it("shows Create Domain button", () => {
    mockSend.mockResolvedValue({ DomainNames: [] });
    render(OpenSearchPage);
    expect(screen.getByText("Create Domain")).toBeInTheDocument();
  });

  it("shows empty state when no domains", async () => {
    mockSend.mockResolvedValue({ DomainNames: [] });
    render(OpenSearchPage);
    await waitFor(
      () => {
        expect(screen.getByText("No domains found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded domains", async () => {
    mockSend.mockResolvedValue({
      DomainNames: [{ DomainName: "prod-search" }, { DomainName: "dev-analytics" }],
    });
    render(OpenSearchPage);
    await waitFor(
      () => {
        expect(screen.getByText("prod-search")).toBeInTheDocument();
        expect(screen.getByText("dev-analytics")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens create domain modal", async () => {
    mockSend.mockResolvedValue({ DomainNames: [] });
    render(OpenSearchPage);
    await fireEvent.click(screen.getByText("Create Domain"));
    expect(screen.getByText("Domain Name")).toBeInTheDocument();
    expect(screen.getByText("Engine Version")).toBeInTheDocument();
  });

  it("cancels create domain modal", async () => {
    mockSend.mockResolvedValue({ DomainNames: [] });
    render(OpenSearchPage);
    await fireEvent.click(screen.getByText("Create Domain"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Domain Name")).not.toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ DomainNames: [] });
    render(OpenSearchPage);
    expect(screen.getByPlaceholderText("Search domains...")).toBeInTheDocument();
  });

  it("navigates to domain detail on click", async () => {
    mockSend.mockResolvedValueOnce({ DomainNames: [{ DomainName: "my-domain" }] });
    mockSend.mockResolvedValueOnce({
      DomainStatus: {
        DomainName: "my-domain",
        ARN: "arn:aws:es:us-east-1:123:domain/my-domain",
        EngineVersion: "OpenSearch_2.11",
        ClusterConfig: { InstanceType: "t3.small.search", InstanceCount: 1 },
        EBSOptions: { EBSEnabled: true, VolumeSize: 10 },
        Processing: false,
      },
    });
    render(OpenSearchPage);
    await waitFor(() => expect(screen.getByText("my-domain")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("my-domain"));
    await waitFor(
      () => {
        expect(screen.getByText("Update Config")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
