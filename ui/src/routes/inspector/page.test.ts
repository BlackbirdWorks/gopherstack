import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import InspectorPage from "./+page.svelte";

// The Edit Filter modal stays mounted in the DOM once rendered -- a closed
// <dialog> just loses the `open` attribute rather than unmounting -- so
// getByLabelText("Name") across the whole document is ambiguous once more
// than one modal has been opened. Scope to the dialog that is actually open.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getInspectorClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// DataTable renders "Loading..." while a tab's fetch is in flight and swaps
// to the empty-state message only once it resolves. Waiting for "Loading..."
// to disappear (rather than waiting directly on the empty-state text, which
// can be present transiently before the tab loader's effect ever sets
// loading=true) is the reliable sync point for empty-state assertions.
async function waitForLoadingToFinish(): Promise<void> {
  await waitFor(() => {
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
  });
}

const exampleFinding = {
  findingArn: "arn:aws:inspector2:us-east-1:123456789012:finding/abc123",
  title: "CVE-2024-0001 - openssl",
  type: "PACKAGE_VULNERABILITY",
  severity: "CRITICAL",
  status: "ACTIVE",
  fixAvailable: "YES",
  resources: [{ id: "i-0123456789abcdef0" }],
  firstObservedAt: new Date("2024-01-01T00:00:00Z"),
  lastObservedAt: new Date("2024-01-02T00:00:00Z"),
};

const exampleFilter = {
  arn: "arn:aws:inspector2:us-east-1:123456789012:owner/123456789012/filter/example",
  name: "suppress-dev-findings",
  action: "SUPPRESS",
  ownerId: "123456789012",
  criteria: {},
  createdAt: new Date("2024-01-01T00:00:00Z"),
  updatedAt: new Date("2024-01-01T00:00:00Z"),
};

describe("Inspector Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    expect(screen.getByText("Amazon Inspector")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    expect(screen.getByText("Findings")).toBeInTheDocument();
    expect(screen.getByText("Coverage")).toBeInTheDocument();
    expect(screen.getByText("Filters & Suppression Rules")).toBeInTheDocument();
    expect(screen.getByText("Enablement")).toBeInTheDocument();
  });

  it("lists findings and renders the severity badge via its render snippet", async () => {
    mockSend.mockResolvedValueOnce({ findings: [exampleFinding] });
    render(InspectorPage);
    await waitFor(() => {
      expect(screen.getByText("CVE-2024-0001 - openssl")).toBeInTheDocument();
    });
    // Render-snippet assertion: the Severity column is defined with
    // `render: severityCell`, which is exactly the class of bug that
    // renders blank when a column array is built with `as Column<T>[]`.
    expect(screen.getByText("CRITICAL")).toBeInTheDocument();
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: { maxResults: 50, nextToken: undefined } }),
    );
  });

  it("shows empty state when no findings", async () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    await waitForLoadingToFinish();
    expect(screen.getByText("No findings found")).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Throttled."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(InspectorPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(screen.getByText("ThrottlingException (HTTP 429): Throttled.")).toBeInTheDocument();
    });
  });

  it("opens a finding's detail view (list + detail only, no create/delete)", async () => {
    mockSend.mockResolvedValueOnce({ findings: [exampleFinding] });
    render(InspectorPage);
    await waitFor(() => screen.getByText("CVE-2024-0001 - openssl"));

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleFinding.findingArn)).toBeInTheDocument();
    });
    // No GetFinding round-trip: ListFindings' Finding is already the full
    // shape, so opening detail must not issue another send() call.
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("switches to the filters tab and lists filters", async () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    await waitForLoadingToFinish();

    mockSend.mockResolvedValueOnce({ filters: [exampleFilter] });
    await fireEvent.click(screen.getByText("Filters & Suppression Rules"));

    await waitFor(() => {
      expect(screen.getByText("suppress-dev-findings")).toBeInTheDocument();
    });
    // Render-snippet assertion for the "Kind" column.
    expect(screen.getByText("Suppression rule")).toBeInTheDocument();
  });

  it("creates a filter via CreateFilter with exact input", async () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    await waitForLoadingToFinish();

    mockSend.mockResolvedValueOnce({ filters: [] });
    await fireEvent.click(screen.getByText("Filters & Suppression Rules"));
    await waitFor(() => screen.getByText("No filters found"));

    await fireEvent.click(screen.getByText("Create filter"));
    expect(screen.getByText("Create Filter")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Name"), {
      target: { value: "suppress-test-findings" },
    });

    mockSend.mockResolvedValueOnce({ arn: exampleFilter.arn });
    mockSend.mockResolvedValueOnce({ filters: [exampleFilter] });

    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: {
          name: "suppress-test-findings",
          action: "SUPPRESS",
          reason: undefined,
          filterCriteria: {},
        },
      }),
    );
  });

  it("deletes a filter after confirming, with exact DeleteFilter input", async () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    await waitForLoadingToFinish();

    mockSend.mockResolvedValueOnce({ filters: [exampleFilter] });
    await fireEvent.click(screen.getByText("Filters & Suppression Rules"));
    await waitFor(() => screen.getByText("suppress-dev-findings"));

    mockSend.mockResolvedValueOnce({ arn: exampleFilter.arn });
    mockSend.mockResolvedValueOnce({ filters: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No filters found")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { arn: exampleFilter.arn } }),
    );
  });

  it("does not delete a filter when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    await waitForLoadingToFinish();

    mockSend.mockResolvedValueOnce({ filters: [exampleFilter] });
    await fireEvent.click(screen.getByText("Filters & Suppression Rules"));
    await waitFor(() => screen.getByText("suppress-dev-findings"));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only ListFindings + ListFilters -- no DeleteFilter, no reload.
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(screen.getByText("suppress-dev-findings")).toBeInTheDocument();
  });

  it("enables scanning for the selected resource types with exact Enable input", async () => {
    mockSend.mockResolvedValueOnce({ findings: [] });
    render(InspectorPage);
    await waitForLoadingToFinish();

    mockSend.mockResolvedValueOnce({ accounts: [] });
    await fireEvent.click(screen.getByText("Enablement"));
    await waitFor(() => screen.getByText("No account status found"));

    mockSend.mockResolvedValueOnce({ accounts: [] });
    mockSend.mockResolvedValueOnce({ accounts: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Enable" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(4);
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { resourceTypes: ["EC2", "ECR"] } }),
    );
  });
});
