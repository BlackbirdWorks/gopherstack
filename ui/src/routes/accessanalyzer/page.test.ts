import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import AccessAnalyzerPage from "./+page.svelte";

// The Edit Analyzer and Edit Archive Rule modals (and the Create Archive
// Rule modal) all stay mounted in the DOM once rendered -- closed <dialog>
// elements just lose the `open` attribute rather than unmounting -- so
// getByLabelText/getByRole across the whole document is ambiguous once more
// than one modal has been opened. Scope to the dialog that is actually open,
// same pattern as dlm/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

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

  it("edits an analyzer's configuration via UpdateAnalyzer", async () => {
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });
    render(AccessAnalyzerPage);
    await waitFor(() => screen.getByRole("cell", { name: "example" }));

    // Opening the edit modal re-fetches via GetAnalyzer to prefill the
    // current configuration, since ListAnalyzers/AnalyzerSummary never
    // carries it.
    mockSend.mockResolvedValueOnce({
      analyzer: { ...exampleAnalyzer, configuration: { unusedAccess: { unusedAccessAge: 90 } } },
    });
    await fireEvent.click(screen.getByTitle("Edit"));
    expect(screen.getByText("Edit Analyzer")).toBeInTheDocument();

    await waitFor(() => {
      expect(within(openDialog()).getByLabelText("Configuration (JSON)")).toHaveValue(
        JSON.stringify({ unusedAccess: { unusedAccessAge: 90 } }, null, 2),
      );
    });

    await fireEvent.input(within(openDialog()).getByLabelText("Configuration (JSON)"), {
      target: { value: JSON.stringify({ unusedAccess: { unusedAccessAge: 120 } }) },
    });

    mockSend.mockResolvedValueOnce({ configuration: { unusedAccess: { unusedAccessAge: 120 } } });
    mockSend.mockResolvedValueOnce({ analyzers: [exampleAnalyzer] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          analyzerName: "example",
          configuration: { unusedAccess: { unusedAccessAge: 120 } },
        },
      }),
    );
    // Render-snippet assertion: the Status column still renders correctly
    // after the post-edit refresh (the render-snippet class of bug renders
    // this cell blank instead).
    expect(screen.getByText("ACTIVE")).toBeInTheDocument();
  });

  it("edits an archive rule's filter via UpdateArchiveRule", async () => {
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
    await waitFor(() => screen.getByText("archive-old-buckets"));

    await fireEvent.click(screen.getByTitle("Edit"));
    expect(screen.getByText("Edit Archive Rule")).toBeInTheDocument();
    // ArchiveRuleSummary always carries "filter", so the edit modal
    // prefills straight from the row with no extra fetch.
    expect(within(openDialog()).getByLabelText("Updated filter (JSON)")).toHaveValue(
      JSON.stringify({ resourceType: { eq: ["AWS::S3::Bucket"] } }, null, 2),
    );

    const newFilter = { resourceType: { eq: ["AWS::S3::Bucket", "AWS::S3::AccessPoint"] } };
    await fireEvent.input(within(openDialog()).getByLabelText("Updated filter (JSON)"), {
      target: { value: JSON.stringify(newFilter) },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      archiveRules: [
        {
          ruleName: "archive-old-buckets",
          filter: newFilter,
          createdAt: new Date("2024-02-01T00:00:00Z"),
          updatedAt: new Date("2024-02-02T00:00:00Z"),
        },
      ],
    });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          analyzerName: "example",
          ruleName: "archive-old-buckets",
          filter: newFilter,
        },
      }),
    );
    // Render-snippet assertion: the Filter column (defined with `render:
    // ruleFilterCell`) reflects the updated filter -- this is the assertion
    // class that catches a malformed `render` entry rendering a blank cell.
    await waitFor(() => {
      expect(screen.getByText(JSON.stringify(newFilter))).toBeInTheDocument();
    });
  });
});
