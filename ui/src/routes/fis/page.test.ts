import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import FISPage from "./+page.svelte";
import { formatDate } from "$lib/format";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getFISClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleTemplate = {
  id: "EXT123",
  arn: "arn:aws:fis:us-east-1:123456789012:experiment-template/EXT123",
  description: "Terminate one EC2 instance",
  roleArn: "arn:aws:iam::123456789012:role/fis-role",
  creationTime: new Date("2024-01-01T00:00:00Z"),
  lastUpdateTime: new Date("2024-01-02T00:00:00Z"),
};

const exampleExperiment = {
  id: "EXP123",
  arn: "arn:aws:fis:us-east-1:123456789012:experiment/EXP123",
  experimentTemplateId: "EXT123",
  state: { status: "running" },
};

const exampleTAC = {
  accountId: "123456789012",
  roleArn: "arn:aws:iam::123456789012:role/fis-target-role",
  description: "Primary target account",
};

const exampleAction = {
  id: "aws:ec2:terminate-instances",
  arn: "arn:aws:fis:us-east-1:aws:action/aws:ec2:terminate-instances",
  description: "Terminates the specified EC2 instances",
};

const exampleResourceType = {
  resourceType: "aws:ec2:instance",
  description: "An EC2 instance",
};

const exampleSafetyLever = {
  id: "default",
  arn: "arn:aws:fis:us-east-1:123456789012:safety-lever/default",
  state: { status: "disengaged", reason: "" },
};

describe("FIS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    expect(screen.getByText("AWS Fault Injection Simulator")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    expect(screen.getByRole("tab", { name: "Experiment Templates" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Experiments" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Target Account Configs" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Actions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Target Resource Types" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Safety Lever" })).toBeInTheDocument();
  });

  it("lists experiment templates using nextToken pagination", async () => {
    mockSend.mockResolvedValueOnce({
      experimentTemplates: [exampleTemplate],
      nextToken: "next-page",
    });
    render(FISPage);

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "EXT123" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledWith(
      expect.objectContaining({ input: expect.objectContaining({ nextToken: undefined }) }),
    );
    expect(screen.getByText("Load More")).toBeInTheDocument();
  });

  // Regression test for a shipped bug: the "Created"/"Updated" columns used
  // `render: (t) => formatDate(...)` -- a plain value-returning function --
  // instead of a `{#snippet}`. `{@render column.render(row)}` requires a
  // real Snippet (which has an anchor-node calling convention); handed a
  // plain function it renders nothing, so the cells were silently blank.
  // Earlier tests in this file only assert on the ID cell, so they never
  // exercised these two columns. Assert on the actual rendered text here,
  // not just presence of the row.
  it("renders the template's Created and Updated date columns", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "EXT123" })).toBeInTheDocument();
    });

    expect(
      screen.getByRole("cell", { name: formatDate(exampleTemplate.creationTime) }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("cell", { name: formatDate(exampleTemplate.lastUpdateTime) }),
    ).toBeInTheDocument();
  });

  it("creates an experiment template via the modal", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    // Synchronize on the tab bar (not the empty-state text, which can race
    // the still-in-flight initial ListExperimentTemplates call).
    await waitFor(() => screen.getByText("Experiment Templates"));

    await fireEvent.click(screen.getByText("Create template"));
    expect(screen.getByText("Create Experiment Template")).toBeInTheDocument();

    await fireEvent.input(screen.getByLabelText("IAM Role ARN"), {
      target: { value: "arn:aws:iam::123456789012:role/fis-role" },
    });

    mockSend.mockResolvedValueOnce({ experimentTemplate: exampleTemplate });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "EXT123" })).toBeInTheDocument();
    });
    // ListExperimentTemplates (initial) + CreateExperimentTemplate + ListExperimentTemplates (refresh).
    expect(mockSend).toHaveBeenCalledTimes(3);
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: expect.objectContaining({
          roleArn: "arn:aws:iam::123456789012:role/fis-role",
          stopConditions: [{ source: "none", value: undefined }],
        }),
      }),
    );
  });

  it("deletes an experiment template after confirming", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No experiment templates found")).toBeInTheDocument();
    });
  });

  it("does not delete a template when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial List call -- no Delete, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("cell", { name: "EXT123" })).toBeInTheDocument();
  });

  it("opens an experiment template's detail view", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    mockSend.mockResolvedValueOnce({
      experimentTemplate: {
        ...exampleTemplate,
        targets: {},
        actions: {},
        stopConditions: [{ source: "none" }],
      },
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleTemplate.arn)).toBeInTheDocument();
    });
  });

  it("updates an experiment template via the modal", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    mockSend.mockResolvedValueOnce({
      experimentTemplate: {
        ...exampleTemplate,
        targets: {},
        actions: {},
        stopConditions: [{ source: "none" }],
      },
    });
    await fireEvent.click(screen.getByTitle("Update"));
    await waitFor(() => screen.getByText("Update Experiment Template"));

    const descriptionInput = screen.getByLabelText("Description") as HTMLInputElement;
    await fireEvent.input(descriptionInput, { target: { value: "Updated description" } });

    const updated = { ...exampleTemplate, description: "Updated description" };
    mockSend.mockResolvedValueOnce({ experimentTemplate: updated });
    mockSend.mockResolvedValueOnce({ experimentTemplates: [updated] });

    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "Updated description" })).toBeInTheDocument();
    });
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Rate exceeded."), {
      name: "ThrottlingException",
      $metadata: { httpStatusCode: 429 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(FISPage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("switches to Experiments and starts an experiment from a template", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    mockSend.mockResolvedValueOnce({ experiments: [] });
    await fireEvent.click(screen.getByText("Experiments"));
    await waitFor(() => screen.getByText("No experiments found"));

    await fireEvent.click(screen.getByText("Start experiment"));
    await waitFor(() => screen.getByText("Start Experiment"));

    mockSend.mockResolvedValueOnce({ experiment: exampleExperiment });
    mockSend.mockResolvedValueOnce({ experiments: [exampleExperiment] });

    await fireEvent.click(screen.getByRole("button", { name: "Start" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "EXP123" })).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: expect.objectContaining({
          experimentTemplateId: "EXT123",
          experimentOptions: { actionsMode: "run-all" },
        }),
      }),
    );
  });

  it("stops a running experiment after confirming", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    await waitFor(() => screen.getByText("Experiment Templates"));

    mockSend.mockResolvedValueOnce({ experiments: [exampleExperiment] });
    await fireEvent.click(screen.getByText("Experiments"));
    await waitFor(() => screen.getByRole("cell", { name: "EXP123" }));

    mockSend.mockResolvedValueOnce({
      experiment: { ...exampleExperiment, state: { status: "stopped" } },
    });
    mockSend.mockResolvedValueOnce({ experiments: [] });

    await fireEvent.click(screen.getByTitle("Stop"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No experiments found")).toBeInTheDocument();
    });
  });

  it("opens an experiment's detail view including resolved targets", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    await waitFor(() => screen.getByText("Experiment Templates"));

    mockSend.mockResolvedValueOnce({ experiments: [exampleExperiment] });
    await fireEvent.click(screen.getByText("Experiments"));
    await waitFor(() => screen.getByRole("cell", { name: "EXP123" }));

    mockSend.mockResolvedValueOnce({
      experiment: { ...exampleExperiment, targets: {}, actions: {}, stopConditions: [] },
    });
    mockSend.mockResolvedValueOnce({
      resolvedTargets: [
        { targetName: "myInstances", resourceType: "aws:ec2:instance", targetResourcesCount: 2 },
      ],
    });
    mockSend.mockResolvedValueOnce({ targetAccountConfigurations: [] });

    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(/myInstances: 2 resource/)).toBeInTheDocument();
    });
  });

  it("lists target account configurations for the selected template and creates a new one", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    mockSend.mockResolvedValueOnce({ targetAccountConfigurations: [exampleTAC] });
    await fireEvent.click(screen.getByText("Target Account Configs"));
    await waitFor(() => screen.getByRole("cell", { name: "123456789012" }));
    expect(mockSend).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: expect.objectContaining({ experimentTemplateId: "EXT123" }),
      }),
    );

    await fireEvent.click(screen.getByText("Create config"));
    await waitFor(() => screen.getByText("Create Target Account Configuration"));

    await fireEvent.input(screen.getByLabelText("Target Account ID"), {
      target: { value: "999999999999" },
    });
    await fireEvent.input(screen.getByLabelText("Target Account IAM Role ARN"), {
      target: { value: "arn:aws:iam::999999999999:role/fis-role" },
    });

    const newTAC = {
      accountId: "999999999999",
      roleArn: "arn:aws:iam::999999999999:role/fis-role",
    };
    mockSend.mockResolvedValueOnce({ targetAccountConfiguration: newTAC });
    mockSend.mockResolvedValueOnce({ targetAccountConfigurations: [exampleTAC, newTAC] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "999999999999" })).toBeInTheDocument();
    });
  });

  it("deletes a target account configuration after confirming", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [exampleTemplate] });
    render(FISPage);
    await waitFor(() => screen.getByRole("cell", { name: "EXT123" }));

    mockSend.mockResolvedValueOnce({ targetAccountConfigurations: [exampleTAC] });
    await fireEvent.click(screen.getByText("Target Account Configs"));
    await waitFor(() => screen.getByRole("cell", { name: "123456789012" }));

    mockSend.mockResolvedValueOnce({ targetAccountConfiguration: exampleTAC });
    mockSend.mockResolvedValueOnce({ targetAccountConfigurations: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No target account configurations found")).toBeInTheDocument();
    });
  });

  it("shows the read-only actions catalog and a detail view, with no create/delete affordances", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    await waitFor(() => screen.getByText("Experiment Templates"));

    mockSend.mockResolvedValueOnce({ actions: [exampleAction], nextToken: "next" });
    await fireEvent.click(screen.getByRole("tab", { name: "Actions" }));
    await waitFor(() => screen.getByRole("cell", { name: exampleAction.id }));
    expect(screen.getByText("Load More")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ action: { ...exampleAction, parameters: {}, targets: {} } });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText(exampleAction.arn)).toBeInTheDocument();
    });
  });

  it("shows the read-only target resource types catalog", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    await waitFor(() => screen.getByText("Experiment Templates"));

    mockSend.mockResolvedValueOnce({ targetResourceTypes: [exampleResourceType] });
    await fireEvent.click(screen.getByText("Target Resource Types"));

    await waitFor(() => {
      expect(
        screen.getByRole("cell", { name: exampleResourceType.resourceType }),
      ).toBeInTheDocument();
    });
  });

  it("shows the singleton safety lever and engages it", async () => {
    mockSend.mockResolvedValueOnce({ experimentTemplates: [] });
    render(FISPage);
    await waitFor(() => screen.getByText("Experiment Templates"));

    mockSend.mockResolvedValueOnce({ safetyLever: exampleSafetyLever });
    await fireEvent.click(screen.getByText("Safety Lever"));
    await waitFor(() => screen.getByText("disengaged"));

    mockSend.mockResolvedValueOnce({
      safetyLever: {
        ...exampleSafetyLever,
        state: { status: "engaged", reason: "Incident response" },
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Engage" }));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("engaged")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Disengage" })).toBeInTheDocument();
    });
  });
});
