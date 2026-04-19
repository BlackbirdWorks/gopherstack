import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import SecurityHubPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSecurityHubClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const criticalFinding = {
  Id: "finding-critical",
  Title: "Root account activity detected",
  Severity: { Label: "CRITICAL" },
  ProductName: "GuardDuty",
  Workflow: { Status: "NEW" },
  Types: ["Effects/Data Exfiltration"],
};
const highFinding = {
  Id: "finding-1",
  Title: "Security group allows unrestricted access",
  Severity: { Label: "HIGH" },
  ProductName: "Security Hub",
  Workflow: { Status: "NEW" },
  Types: ["Software and Configuration Checks"],
};
const resolvedFinding = {
  Id: "finding-resolved",
  Title: "Old resolved finding",
  Severity: { Label: "MEDIUM" },
  ProductName: "Security Hub",
  Workflow: { Status: "RESOLVED" },
  Types: [],
};

describe("Security Hub Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend
      .mockRejectedValueOnce(new Error("not enabled"))
      .mockResolvedValueOnce({ Findings: [] });
    render(SecurityHubPage);
    expect(screen.getByText("Security Hub")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend.mockRejectedValueOnce(new Error("not enabled"));
    render(SecurityHubPage);
    expect(screen.getByText("Findings")).toBeInTheDocument();
    expect(screen.getByText("Insights")).toBeInTheDocument();
  });

  it("shows not enabled state", async () => {
    mockSend.mockRejectedValueOnce(new Error("Hub not enabled"));
    render(SecurityHubPage);
    await waitFor(() => {
      expect(screen.getByText("Security Hub is not enabled")).toBeInTheDocument();
    });
  });

  it("displays findings when enabled", async () => {
    // DescribeHub
    mockSend.mockResolvedValueOnce({}).mockResolvedValueOnce({ Findings: [highFinding] });
    render(SecurityHubPage);
    await waitFor(() => {
      expect(screen.getByText("Security group allows unrestricted access")).toBeInTheDocument();
    });
  });

  it("shows empty findings state", async () => {
    mockSend.mockResolvedValueOnce({}).mockResolvedValueOnce({ Findings: [] });
    render(SecurityHubPage);
    await waitFor(() => {
      expect(screen.getByText("No findings found")).toBeInTheDocument();
    });
  });

  it("shows 6 stat cards when enabled with findings", async () => {
    mockSend
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ Findings: [criticalFinding, highFinding, resolvedFinding] });
    render(SecurityHubPage);
    await waitFor(() => screen.getByText("Security group allows unrestricted access"), {
      timeout: 3000,
    });
    const cards = document.querySelectorAll(".uppercase.tracking-wide");
    const labels = [...cards].map((c) => c.textContent?.trim());
    expect(labels).toContain("Critical");
    expect(labels).toContain("High");
    expect(labels).toContain("Medium");
    expect(labels).toContain("Resolved");
  });

  it("filters by severity", async () => {
    mockSend
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ Findings: [criticalFinding, highFinding] });
    render(SecurityHubPage);
    await waitFor(() => screen.getByText("Root account activity detected"), { timeout: 3000 });
    const selects = screen.getAllByRole("combobox");
    await fireEvent.change(selects[0], { target: { value: "HIGH" } });
    await waitFor(() => {
      expect(screen.queryByText("Root account activity detected")).not.toBeInTheDocument();
      expect(screen.getByText("Security group allows unrestricted access")).toBeInTheDocument();
    });
  });

  it("shows top sources bar when multiple products", async () => {
    mockSend
      .mockResolvedValueOnce({})
      .mockResolvedValueOnce({ Findings: [criticalFinding, highFinding] });
    render(SecurityHubPage);
    await waitFor(() => screen.getByText("Root account activity detected"), { timeout: 3000 });
    expect(screen.getByText(/Top Sources/i)).toBeInTheDocument();
  });
});
