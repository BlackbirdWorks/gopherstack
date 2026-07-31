import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DLMPage from "./+page.svelte";

// The create and edit modals both have "Description"/"Execution role ARN"
// fields, and closed <dialog> elements stay in the DOM (just without the
// `open` attribute), so getByLabelText across the whole document is
// ambiguous once both modals have been opened at least once. Scope to the
// dialog that is actually open.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getDLMClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const examplePolicy = {
  PolicyId: "policy-0000000000000001",
  Description: "Daily EBS snapshots",
  State: "ENABLED",
  PolicyType: "EBS_SNAPSHOT_MANAGEMENT",
  Tags: {},
};

const exampleFullPolicy = {
  PolicyId: "policy-0000000000000001",
  PolicyArn: "arn:aws:dlm:us-east-1:123456789012:policy/policy-0000000000000001",
  Description: "Daily EBS snapshots",
  ExecutionRoleArn: "arn:aws:iam::123456789012:role/DLM-Role",
  State: "ENABLED",
  DateCreated: new Date("2024-01-01T00:00:00Z"),
  DateModified: new Date("2024-01-02T00:00:00Z"),
  Tags: {},
  PolicyDetails: { PolicyType: "EBS_SNAPSHOT_MANAGEMENT" },
};

describe("DLM Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [] });
    render(DLMPage);
    expect(screen.getByText("Data Lifecycle Manager")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No lifecycle policies found"));
  });

  it("shows empty state when no lifecycle policies", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [] });
    render(DLMPage);
    await waitFor(() => {
      expect(screen.getByText("No lifecycle policies found")).toBeInTheDocument();
    });
  });

  it("lists lifecycle policies", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    render(DLMPage);
    await waitFor(() => {
      expect(screen.getByText(examplePolicy.PolicyId)).toBeInTheDocument();
    });
    expect(screen.getByText(examplePolicy.Description)).toBeInTheDocument();
  });

  it("creates a lifecycle policy via the modal", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [] });
    render(DLMPage);
    await waitFor(() => screen.getByText("No lifecycle policies found"));

    await fireEvent.click(screen.getByText("Create policy"));
    expect(screen.getByText("Create Lifecycle Policy")).toBeInTheDocument();

    await fireEvent.input(within(openDialog()).getByLabelText("Description"), {
      target: { value: "Daily EBS snapshots" },
    });
    await fireEvent.input(within(openDialog()).getByLabelText("Execution role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/DLM-Role" },
    });

    mockSend.mockResolvedValueOnce({ PolicyId: examplePolicy.PolicyId });
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(examplePolicy.PolicyId)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a lifecycle policy after confirming", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    render(DLMPage);
    await waitFor(() => screen.getByText(examplePolicy.PolicyId));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Policies: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No lifecycle policies found")).toBeInTheDocument();
    });
  });

  it("does not delete a policy when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    render(DLMPage);
    await waitFor(() => screen.getByText(examplePolicy.PolicyId));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial GetLifecyclePolicies call -- no DeleteLifecyclePolicy, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText(examplePolicy.PolicyId)).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Policy not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DLMPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Policy not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a policy's detail view", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    render(DLMPage);
    await waitFor(() => screen.getByText(examplePolicy.PolicyId));

    mockSend.mockResolvedValueOnce({ Policy: exampleFullPolicy });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleFullPolicy.PolicyArn)).toBeInTheDocument();
    });
    expect(screen.getByText(exampleFullPolicy.ExecutionRoleArn)).toBeInTheDocument();
  });

  it("edits a policy's top-level fields via the edit modal", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    render(DLMPage);
    await waitFor(() => screen.getByText(examplePolicy.PolicyId));

    mockSend.mockResolvedValueOnce({ Policy: exampleFullPolicy });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByText(exampleFullPolicy.PolicyArn));

    await fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    expect(screen.getByText("Edit Lifecycle Policy")).toBeInTheDocument();

    const descriptionInput = within(openDialog()).getByLabelText("Description") as HTMLInputElement;
    expect(descriptionInput.value).toBe("Daily EBS snapshots");

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    mockSend.mockResolvedValueOnce({ Policy: exampleFullPolicy });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(5);
    });
  });

  it("adds a tag to a policy from the detail view", async () => {
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });
    render(DLMPage);
    await waitFor(() => screen.getByText(examplePolicy.PolicyId));

    mockSend.mockResolvedValueOnce({ Policy: exampleFullPolicy });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByText(exampleFullPolicy.PolicyArn));

    await fireEvent.input(screen.getByLabelText("New tag key"), {
      target: { value: "env" },
    });
    await fireEvent.input(screen.getByLabelText("New tag value"), {
      target: { value: "prod" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Policy: { ...exampleFullPolicy, Tags: { env: "prod" } },
    });
    mockSend.mockResolvedValueOnce({ Policies: [examplePolicy] });

    await fireEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => {
      expect(screen.getByText("env = prod")).toBeInTheDocument();
    });
  });
});
