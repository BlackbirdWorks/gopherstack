import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import AccessAnalyzerPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAccessAnalyzerClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleAnalyzer = {
  arn: "arn:aws:access-analyzer:us-east-1:123456789012:analyzer/example",
  name: "example",
  type: "ACCOUNT",
  status: "ACTIVE",
  createdAt: new Date("2024-01-01T00:00:00Z"),
};

describe("Access Analyzer Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ analyzers: [] });
    render(AccessAnalyzerPage);
    expect(screen.getByText("IAM Access Analyzer")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ analyzers: [] });
    render(AccessAnalyzerPage);
    expect(screen.getByText("Analyzers")).toBeInTheDocument();
    expect(screen.getByText("Archive Rules")).toBeInTheDocument();
    expect(screen.getByText("Findings")).toBeInTheDocument();
    expect(screen.getByText("Analyzed Resources")).toBeInTheDocument();
    expect(screen.getByText("Access Previews")).toBeInTheDocument();
    expect(screen.getByText("Policy Generations")).toBeInTheDocument();
  });

  it("shows empty state when no analyzers", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [] });
    render(AccessAnalyzerPage);
    await waitFor(() => {
      expect(screen.getByText("No analyzers found")).toBeInTheDocument();
    });
  });

  it("lists analyzers", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });
    render(AccessAnalyzerPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
    });
  });

  it("creates an analyzer via the modal", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [] });
    render(AccessAnalyzerPage);
    await waitFor(() => screen.getByText("No analyzers found"));

    await fireEvent.click(screen.getByText("Create analyzer"));
    expect(screen.getByText("Create Analyzer")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("Name"), { target: { value: "new-analyzer" } });

    mockSend.mockResolvedValueOnce({ arn: exampleAnalyzer.arn });
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes an analyzer after confirming", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });
    render(AccessAnalyzerPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ analyzers: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No analyzers found")).toBeInTheDocument();
    });
  });

  it("does not delete an analyzer when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });
    render(AccessAnalyzerPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListAnalyzers call -- no DeleteAnalyzer, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "example" })).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Analyzer not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(AccessAnalyzerPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Analyzer not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens an analyzer's detail view", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });
    render(AccessAnalyzerPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({ analyzer: exampleAnalyzer });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getAllByText(exampleAnalyzer.arn).length).toBeGreaterThan(0);
    });
  });

  it("switches to the archive rules tab and loads rules for the selected analyzer", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });
    render(AccessAnalyzerPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    mockSend.mockResolvedValueOnce({
      archiveRules: [
        {
          ruleName: "archive-old-buckets",
          filter: { resourceType: { eq: ["AWS::S3::Bucket"] } },
          createdAt: new Date("2024-02-01T00:00:00Z"),
          updatedAt: new Date("2024-02-01T00:00:00Z"),
        },
      ],
    });
    await fireEvent.click(screen.getByText("Archive Rules"));

    await waitFor(() => {
      expect(screen.getByText("archive-old-buckets")).toBeInTheDocument();
    });
  });
});
