import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DetectivePage from "./+page.svelte";

// Modals stay mounted in the DOM once rendered -- a closed <dialog> just
// loses the `open` attribute rather than unmounting -- so labels reused
// across modals (e.g. "Account ID" on both the invite-member and
// enable-admin forms) are ambiguous via a document-wide query. Scope to the
// dialog that is actually open, same pattern as accessanalyzer/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDetectiveClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleGraph = {
  Arn: "arn:aws:detective:us-east-1:123456789012:graph:abc123",
  CreatedTime: new Date("2024-01-01T00:00:00Z"),
};

describe("Detective Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    expect(screen.getByText("Amazon Detective")).toBeInTheDocument();
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    expect(screen.getByText("Behavior Graphs")).toBeInTheDocument();
    expect(screen.getByText("Members")).toBeInTheDocument();
    expect(screen.getByText("Invitations")).toBeInTheDocument();
    expect(screen.getByText("Investigations")).toBeInTheDocument();
    expect(screen.getByText("Organization")).toBeInTheDocument();
  });

  it("shows empty state when no behavior graphs", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    await waitFor(() => {
      expect(screen.getByText("No behavior graphs found")).toBeInTheDocument();
    });
  });

  it("lists behavior graphs", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => {
      expect(screen.getByText(exampleGraph.Arn)).toBeInTheDocument();
    });
  });

  it("creates a behavior graph via the modal", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [] });
    render(DetectivePage);
    await waitFor(() => screen.getByText("No behavior graphs found"));

    await fireEvent.click(screen.getByText("Create graph"));
    expect(screen.getByText("Create Behavior Graph")).toBeInTheDocument();

    mockSend.mockResolvedValueOnce({ GraphArn: exampleGraph.Arn });
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });

    await fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(screen.getByText(exampleGraph.Arn)).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenCalledTimes(3);
  });

  it("deletes a behavior graph after confirming", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ GraphList: [] });

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    await waitFor(() => {
      expect(screen.getByText("No behavior graphs found")).toBeInTheDocument();
    });
  });

  it("does not delete a graph when the confirm dialog is declined", async () => {
    confirmDestructive.mockResolvedValue(false);
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    await fireEvent.click(screen.getByTitle("Delete"));

    expect(confirmDestructive).toHaveBeenCalled();
    // Only the initial ListGraphs call — no DeleteGraph, no reload.
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(screen.getByText(exampleGraph.Arn)).toBeInTheDocument();
  });

  it("shows an inline error with the AWS error code when a load fails", async () => {
    const error = Object.assign(new Error("Graph not found."), {
      name: "ResourceNotFoundException",
      $metadata: { httpStatusCode: 404 },
    });
    mockSend.mockRejectedValueOnce(error);

    render(DetectivePage);

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ResourceNotFoundException (HTTP 404): Graph not found."),
      ).toBeInTheDocument();
    });
  });

  it("opens a graph's detail view", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    await fireEvent.click(screen.getByTitle("View"));

    expect(screen.getAllByText(exampleGraph.Arn).length).toBeGreaterThan(1);
  });

  it("switches to the members tab and loads members for the selected graph", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({
      MemberDetails: [
        {
          AccountId: "222233334444",
          EmailAddress: "member@example.com",
          Status: "ENABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Members"));

    await waitFor(() => {
      expect(screen.getByText("222233334444")).toBeInTheDocument();
    });
  });

  it("manages tags, datasource packages, and org config from the graph detail view", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    // openGraphDetail fires ListTagsForResource, ListDatasourcePackages, and
    // DescribeOrganizationConfiguration together via Promise.all, in that order.
    mockSend.mockResolvedValueOnce({ Tags: {} });
    mockSend.mockResolvedValueOnce({ DatasourcePackages: {} });
    mockSend.mockResolvedValueOnce({ AutoEnable: false });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText("No tags")).toBeInTheDocument();
    });

    await fireEvent.input(screen.getByLabelText("New tag key"), {
      target: { value: "env" },
    });
    await fireEvent.input(screen.getByLabelText("New tag value"), {
      target: { value: "prod" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Tags: { env: "prod" } });
    await fireEvent.click(screen.getByText("Add"));

    await waitFor(() => {
      expect(screen.getByText("env = prod")).toBeInTheDocument();
    });
    expect(mockSend).toHaveBeenNthCalledWith(
      5,
      expect.objectContaining({
        input: { ResourceArn: exampleGraph.Arn, Tags: { env: "prod" } },
      }),
    );

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      DatasourcePackages: {
        DETECTIVE_CORE: { DatasourcePackageIngestState: "STARTED" },
      },
    });
    // "Start" is ambiguous with the Start Investigation modal's submit
    // button once both modals are mounted -- scope to the open dialog.
    await fireEvent.click(within(openDialog()).getByText("Start"));

    await waitFor(() => {
      expect(screen.getByText("DETECTIVE_CORE: STARTED")).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({});
    await fireEvent.click(screen.getByLabelText("Auto-enable new organization accounts"));

    await waitFor(() => {
      expect(mockSend).toHaveBeenNthCalledWith(
        9,
        expect.objectContaining({
          input: { GraphArn: exampleGraph.Arn, AutoEnable: true },
        }),
      );
    });
  });

  it("starts monitoring a member whose monitoring is paused", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({
      MemberDetails: [
        {
          AccountId: "222233334444",
          EmailAddress: "member@example.com",
          Status: "ACCEPTED_BUT_DISABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Members"));
    await waitFor(() => screen.getByText("222233334444"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      MemberDetails: [
        {
          AccountId: "222233334444",
          EmailAddress: "member@example.com",
          Status: "ENABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByTitle("Start monitoring"));

    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({
        input: { GraphArn: exampleGraph.Arn, AccountId: "222233334444" },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("ENABLED")).toBeInTheDocument();
    });
  });

  it("shows a member's datasource ingest history in its detail view", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({
      MemberDetails: [
        {
          AccountId: "222233334444",
          EmailAddress: "member@example.com",
          Status: "ENABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Members"));
    await waitFor(() => screen.getByText("222233334444"));

    mockSend.mockResolvedValueOnce({
      MemberDetails: [
        {
          AccountId: "222233334444",
          EmailAddress: "member@example.com",
          Status: "ENABLED",
        },
      ],
      UnprocessedAccounts: [],
    });
    mockSend.mockResolvedValueOnce({
      MemberDatasources: [
        {
          AccountId: "222233334444",
          GraphArn: exampleGraph.Arn,
          DatasourcePackageIngestHistory: {
            DETECTIVE_CORE: { STARTED: new Date("2024-03-01T00:00:00Z") },
          },
        },
      ],
    });
    await fireEvent.click(screen.getByTitle("View"));

    await waitFor(() => {
      expect(screen.getByText("Datasource ingest history")).toBeInTheDocument();
      expect(screen.getByText(/DETECTIVE_CORE/)).toBeInTheDocument();
    });
  });

  it("leaves a behavior graph via DisassociateMembership from the invitations tab", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({
      Invitations: [
        {
          GraphArn: exampleGraph.Arn,
          AdministratorId: "999900001111",
          Status: "ENABLED",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Invitations"));
    await waitFor(() => screen.getByTitle("Leave"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ Invitations: [] });
    await fireEvent.click(screen.getByTitle("Leave"));

    expect(confirmDestructive).toHaveBeenCalled();
    expect(mockSend).toHaveBeenNthCalledWith(
      3,
      expect.objectContaining({ input: { GraphArn: exampleGraph.Arn } }),
    );
    await waitFor(() => {
      expect(screen.queryByTitle("Leave")).not.toBeInTheDocument();
    });
  });

  it("archives an investigation via UpdateInvestigationState", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({
      InvestigationDetails: [
        {
          InvestigationId: "inv-1",
          EntityArn: "arn:aws:iam::123456789012:user/example",
          Status: "RUNNING",
          State: "ACTIVE",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Investigations"));
    await waitFor(() => screen.getByText("inv-1"));

    mockSend.mockResolvedValueOnce({ State: "ACTIVE" });
    mockSend.mockResolvedValueOnce({ Indicators: [] });
    await fireEvent.click(screen.getByTitle("View"));
    await waitFor(() => screen.getByText("Archive"));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      InvestigationDetails: [
        {
          InvestigationId: "inv-1",
          EntityArn: "arn:aws:iam::123456789012:user/example",
          State: "ARCHIVED",
        },
      ],
    });
    await fireEvent.click(screen.getByText("Archive"));

    expect(mockSend).toHaveBeenNthCalledWith(
      5,
      expect.objectContaining({
        input: {
          GraphArn: exampleGraph.Arn,
          InvestigationId: "inv-1",
          State: "ARCHIVED",
        },
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("Reactivate")).toBeInTheDocument();
    });
  });

  it("shows the organization tab and enables a delegated admin account", async () => {
    mockSend.mockResolvedValueOnce({ GraphList: [exampleGraph] });
    render(DetectivePage);
    await waitFor(() => screen.getByText(exampleGraph.Arn));

    mockSend.mockResolvedValueOnce({ Administrators: [] });
    await fireEvent.click(screen.getByText("Organization"));
    await waitFor(() => {
      expect(screen.getByText("No organization admin account is delegated")).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("Enable admin account"));
    await fireEvent.input(within(openDialog()).getByLabelText("Account ID"), {
      target: { value: "123456789012" },
    });

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({
      Administrators: [{ AccountId: "123456789012", GraphArn: exampleGraph.Arn }],
    });
    await fireEvent.click(within(openDialog()).getByRole("button", { name: "Enable" }));

    await waitFor(() => {
      expect(screen.getByText("123456789012")).toBeInTheDocument();
    });
  });
});
