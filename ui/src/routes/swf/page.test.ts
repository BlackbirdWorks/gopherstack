import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import SwfPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSWFClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

const exampleDomain = {
  name: "my-domain",
  status: "REGISTERED",
  description: "Order processing domain",
  arn: "arn:aws:swf:us-east-1:123456789012:/domain/my-domain",
};

const exampleWorkflowType = {
  workflowType: { name: "order-workflow", version: "1.0" },
  status: "REGISTERED",
  description: "Processes orders",
  creationDate: new Date("2026-01-01T00:00:00Z"),
};

const exampleActivityType = {
  activityType: { name: "charge-card", version: "1.0" },
  status: "REGISTERED",
  description: "Charges a card",
  creationDate: new Date("2026-01-01T00:00:00Z"),
};

const exampleOpenExecution = {
  execution: { workflowId: "order-1", runId: "run-abc123456" },
  workflowType: { name: "order-workflow", version: "1.0" },
  executionStatus: "OPEN",
  startTimestamp: new Date("2026-01-02T00:00:00Z"),
};

const exampleClosedExecution = {
  execution: { workflowId: "order-2", runId: "run-def123456" },
  workflowType: { name: "order-workflow", version: "1.0" },
  executionStatus: "CLOSED",
  closeStatus: "COMPLETED",
  startTimestamp: new Date("2026-01-02T00:00:00Z"),
  closeTimestamp: new Date("2026-01-02T01:00:00Z"),
};

// Renders the page and resolves the mandatory initial ListDomains(REGISTERED)
// + ListDomains(DEPRECATED) pair issued by onRegionChange on mount.
async function renderWithDomains(
  registered: unknown[] = [],
  deprecated: unknown[] = [],
): Promise<void> {
  mockSend.mockResolvedValueOnce({ domainInfos: registered });
  mockSend.mockResolvedValueOnce({ domainInfos: deprecated });
  render(SwfPage);
  await waitFor(() => screen.getByText("Domains"));
}

async function selectDomainAndSwitchTab(tabLabel: string): Promise<void> {
  await fireEvent.click(screen.getByText(tabLabel));
  const select = screen.getByDisplayValue("— select domain —");
  await fireEvent.change(select, { target: { value: "my-domain" } });
}

describe("SWF Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    await renderWithDomains();
    expect(screen.getByText("Simple Workflow Service")).toBeInTheDocument();
  });

  it("shows all tabs", async () => {
    await renderWithDomains();
    expect(screen.getByText("Domains")).toBeInTheDocument();
    expect(screen.getByText("Workflow Types")).toBeInTheDocument();
    expect(screen.getByText("Activity Types")).toBeInTheDocument();
    expect(screen.getByText("Executions")).toBeInTheDocument();
  });

  it("lists domains from the merged REGISTERED + DEPRECATED calls", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => {
      expect(screen.getByText("my-domain")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: expect.objectContaining({ registrationStatus: "REGISTERED" }),
      }),
    );
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: expect.objectContaining({ registrationStatus: "DEPRECATED" }),
      }),
    );
  });

  it("registers a domain via the modal", async () => {
    await renderWithDomains();
    await waitFor(() => screen.getByText("No domains found"));

    await fireEvent.click(screen.getByText("Register domain"));
    await fireEvent.input(screen.getByLabelText("Domain Name"), {
      target: { value: "new-domain" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ domainInfos: [{ ...exampleDomain, name: "new-domain" }] });
    mockSend.mockResolvedValueOnce({ domainInfos: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Register" }));

    await waitFor(() => {
      expect(screen.getByText("new-domain")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: expect.objectContaining({
          name: "new-domain",
          workflowExecutionRetentionPeriodInDays: "30",
        }),
      }),
    );
  });

  it("deprecates a domain after confirming the cascade warning", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ domainInfos: [{ ...exampleDomain, status: "DEPRECATED" }] });
    mockSend.mockResolvedValueOnce({ domainInfos: [] });

    await fireEvent.click(screen.getByTitle("Deprecate"));

    expect(confirmDestructive).toHaveBeenCalledWith(
      expect.objectContaining({ title: "Deprecate domain" }),
    );
    await waitFor(() => {
      expect(screen.getByText("DEPRECATED")).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(SwfPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("opens a domain's detail view with tags", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({
      domainInfo: exampleDomain,
      configuration: { workflowExecutionRetentionPeriodInDays: "30" },
    });
    mockSend.mockResolvedValueOnce({ tags: [{ key: "team", value: "checkout" }] });

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText("team")).toBeInTheDocument();
    });
  });

  it("requires selecting a domain before showing workflow types", async () => {
    await renderWithDomains();
    await fireEvent.click(screen.getByText("Workflow Types"));
    expect(screen.getByText("Select a domain to view workflow types")).toBeInTheDocument();
  });

  it("lists workflow types once a domain is selected", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ typeInfos: [exampleWorkflowType] });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });

    await selectDomainAndSwitchTab("Workflow Types");

    await waitFor(() => {
      expect(screen.getByText("order-workflow")).toBeInTheDocument();
    });
  });

  it("registers a workflow type via the modal", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ typeInfos: [] });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });
    await selectDomainAndSwitchTab("Workflow Types");
    await waitFor(() => screen.getByText("No workflow types registered in this domain"));

    await fireEvent.click(screen.getByText("Register workflow type"));
    await fireEvent.input(screen.getByLabelText("Workflow Type Name"), {
      target: { value: "new-workflow" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      typeInfos: [
        { ...exampleWorkflowType, workflowType: { name: "new-workflow", version: "1.0" } },
      ],
    });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Register" }));

    await waitFor(() => {
      expect(screen.getByText("new-workflow")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      5,
      expect.objectContaining({
        input: expect.objectContaining({
          domain: "my-domain",
          name: "new-workflow",
          version: "1.0",
          defaultChildPolicy: "TERMINATE",
        }),
      }),
    );
  });

  it("deletes a workflow type after confirming", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ typeInfos: [exampleWorkflowType] });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });
    await selectDomainAndSwitchTab("Workflow Types");
    await waitFor(() => screen.getByText("order-workflow"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ typeInfos: [] });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No workflow types registered in this domain")).toBeInTheDocument();
    });
  });

  it("lists activity types once a domain is selected", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ typeInfos: [exampleActivityType] });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });

    await selectDomainAndSwitchTab("Activity Types");

    await waitFor(() => {
      expect(screen.getByText("charge-card")).toBeInTheDocument();
    });
  });

  it("lists open and closed executions using nextPageToken pagination", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({
      executionInfos: [exampleOpenExecution],
      nextPageToken: "open-next",
    });
    mockSend.mockResolvedValueOnce({ executionInfos: [exampleClosedExecution] });

    await selectDomainAndSwitchTab("Executions");

    await waitFor(() => {
      expect(screen.getByText("order-1")).toBeInTheDocument();
      expect(screen.getByText("order-2")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: expect.objectContaining({ domain: "my-domain", nextPageToken: undefined }),
      }),
    );
    expect(screen.getByText("Load More")).toBeInTheDocument();
  });

  it("starts a workflow execution via the modal", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ typeInfos: [exampleWorkflowType] });
    mockSend.mockResolvedValueOnce({ typeInfos: [] });
    await selectDomainAndSwitchTab("Workflow Types");
    await waitFor(() => screen.getByText("order-workflow"));

    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    await fireEvent.click(screen.getByText("Executions"));
    await waitFor(() => screen.getByText("No open executions"));

    await fireEvent.click(screen.getByText("Start execution"));
    await fireEvent.input(screen.getByLabelText("Workflow ID"), { target: { value: "order-99" } });

    mockSend.mockResolvedValueOnce({ runId: "run-new" });
    mockSend.mockResolvedValueOnce({ executionInfos: [exampleOpenExecution] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });

    await fireEvent.click(screen.getByRole("button", { name: "Start" }));

    await waitFor(() => {
      expect(screen.getByText("order-1")).toBeInTheDocument();
    });
  });

  it("terminates an execution after confirming, without a childPolicy field", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ executionInfos: [exampleOpenExecution] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    await selectDomainAndSwitchTab("Executions");
    await waitFor(() => screen.getByText("order-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });

    await fireEvent.click(screen.getByTitle("Terminate"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(7);
    });
    const terminateCall = mockSend.mock.calls[4][0];
    expect(terminateCall.input).toEqual(
      expect.objectContaining({ domain: "my-domain", workflowId: "order-1" }),
    );
    expect(terminateCall.input.childPolicy).toBeUndefined();
  });

  it("requests cancellation for an open execution", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ executionInfos: [exampleOpenExecution] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    await selectDomainAndSwitchTab("Executions");
    await waitFor(() => screen.getByText("order-1"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ executionInfos: [exampleOpenExecution] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });

    await fireEvent.click(screen.getByTitle("Request Cancel"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalledTimes(7);
    });
  });

  it("signals a running execution", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ executionInfos: [exampleOpenExecution] });
    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    await selectDomainAndSwitchTab("Executions");
    await waitFor(() => screen.getByText("order-1"));

    await fireEvent.click(screen.getByTitle("Signal"));
    await fireEvent.input(screen.getByLabelText("Signal Name"), { target: { value: "approve" } });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByRole("button", { name: "Send Signal" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        5,
        expect.objectContaining({
          input: expect.objectContaining({
            domain: "my-domain",
            workflowId: "order-1",
            signalName: "approve",
          }),
        }),
      );
    });
  });

  it("opens an execution's detail view with history and warns on a run mismatch", async () => {
    await renderWithDomains([exampleDomain], []);
    await waitFor(() => screen.getByText("my-domain"));

    mockSend.mockResolvedValueOnce({ executionInfos: [] });
    mockSend.mockResolvedValueOnce({ executionInfos: [exampleClosedExecution] });
    await selectDomainAndSwitchTab("Executions");
    await waitFor(() => screen.getByText("order-2"));

    // The backend ignores runId and always returns the CURRENT run for this
    // workflowId (services/swf/handler_workflow_executions.go,
    // handler_history.go) -- simulate that by returning a different runId
    // than the row's, and assert the mismatch banner appears.
    mockSend.mockResolvedValueOnce({
      executionInfo: {
        execution: { workflowId: "order-2", runId: "run-NEWER" },
        executionStatus: "OPEN",
      },
      executionConfiguration: {},
      openCounts: {
        openActivityTasks: 0,
        openDecisionTasks: 1,
        openTimers: 0,
        openChildWorkflowExecutions: 0,
      },
    });
    mockSend.mockResolvedValueOnce({
      events: [{ eventId: 1, eventType: "WorkflowExecutionStarted" }],
    });

    await fireEvent.click(within(screen.getByText("order-2").closest("tr")!).getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(/keys execution state by workflow ID only/)).toBeInTheDocument();
    });
  });
});
