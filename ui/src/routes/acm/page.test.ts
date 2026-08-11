import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import ACMPage from "./+page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getACMClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
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

describe("ACM Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
    render(ACMPage);
    expect(screen.getByText("Certificate Manager")).toBeInTheDocument();
  });

  it("shows request certificate button", () => {
    mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
    render(ACMPage);
    expect(screen.getByText("Request Certificate")).toBeInTheDocument();
  });

  it("displays certificates", async () => {
    mockSend.mockResolvedValueOnce({
      CertificateSummaryList: [
        {
          CertificateArn: "arn:acm:cert/1",
          DomainName: "example.com",
          Status: "ISSUED",
          Type: "AMAZON_ISSUED",
        },
      ],
    });

    render(ACMPage);

    await waitFor(() => {
      expect(screen.getByText("example.com")).toBeInTheDocument();
    });
  });

  it("shows request modal", async () => {
    mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
    render(ACMPage);
    await fireEvent.click(screen.getByText("Request Certificate"));
    expect(screen.getByText("Request Certificate", { selector: "h2" })).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ CertificateSummaryList: [] });
    render(ACMPage);
    await waitFor(() => {
      expect(screen.getByText("No certificates found")).toBeInTheDocument();
    });
  });

  describe("All regions mode", () => {
    it("fans ListCertificates out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        CertificateSummaryList: [{ CertificateArn: "arn:acm:us/1", DomainName: "example.com" }],
      });
      mockSend.mockResolvedValueOnce({
        CertificateSummaryList: [{ CertificateArn: "arn:acm:eu/1", DomainName: "eu.example.com" }],
      });

      render(ACMPage);

      await waitFor(() => expect(screen.getByText("example.com")).toBeInTheDocument());
      expect(screen.getByText("eu.example.com")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one ListCertificates call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({
        CertificateSummaryList: [{ CertificateArn: "arn:acm:us/1", DomainName: "example.com" }],
      });
      render(ACMPage);
      await waitFor(() => expect(screen.getByText("example.com")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same domain from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({
        CertificateSummaryList: [
          { CertificateArn: "arn:acm:us/1", DomainName: "shared.example.com" },
        ],
      });
      mockSend.mockResolvedValueOnce({
        CertificateSummaryList: [
          { CertificateArn: "arn:acm:eu/1", DomainName: "shared.example.com" },
        ],
      });

      render(ACMPage);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("shared.example.com");
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
