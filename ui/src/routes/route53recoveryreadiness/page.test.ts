import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import Page from "./+page.svelte";

type Handler = (input: Record<string, unknown>) => unknown;
const h: Record<string, Handler> = {};

vi.mock("#lib/aws-client.ts", () => ({
  getRoute53RecoveryReadinessClient: () => ({
    send: (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
      const fn = h[cmd.constructor.name];
      if (!fn) return Promise.reject(new Error(`unexpected ${cmd.constructor.name}`));
      try {
        return Promise.resolve(fn(cmd.input));
      } catch (e) {
        return Promise.reject(e);
      }
    },
  }),
}));
const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("#lib/confirm-dialog.ts", () => ({
  confirmDestructive: (...a: unknown[]) => confirmDestructive(...a),
}));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

const cellArn = "arn:aws:route53-recovery-readiness::0:cell/cell_west";

describe("Route 53 Recovery Readiness page", () => {
  beforeEach(() => {
    for (const k of Object.keys(h)) delete h[k];
    h.ListCellsCommand = () => ({
      Cells: [{ CellArn: cellArn, CellName: "cell_west", Cells: [], ParentReadinessScopes: [] }],
    });
    h.ListRecoveryGroupsCommand = () => ({
      RecoveryGroups: [
        { RecoveryGroupArn: "arn:g", RecoveryGroupName: "app_rg", Cells: [cellArn] },
      ],
    });
    h.GetRecoveryGroupReadinessSummaryCommand = () => ({ Readiness: "NOT_READY" });
    h.GetCellReadinessSummaryCommand = () => ({ Readiness: "READY" });
    h.ListResourceSetsCommand = () => ({
      ResourceSets: [
        {
          ResourceSetArn: "arn:rs",
          ResourceSetName: "orders_queues",
          ResourceSetType: "AWS::SQS::Queue",
          Resources: [
            { ResourceArn: "arn:aws:sqs:us-west-2:0:orders", ReadinessScopes: [cellArn] },
          ],
        },
      ],
    });
    h.ListReadinessChecksCommand = () => ({
      ReadinessChecks: [
        {
          ReadinessCheckArn: "arn:rc",
          ReadinessCheckName: "orders_check",
          ResourceSet: "orders_queues",
        },
      ],
    });
    h.GetReadinessCheckStatusCommand = () => ({
      Readiness: "READY",
      Resources: [
        {
          ResourceArn: "arn:aws:sqs:us-west-2:0:orders",
          Readiness: "READY",
          LastCheckedTimestamp: new Date("2026-01-01T00:00:00Z"),
        },
      ],
    });
    confirmDestructive.mockClear();
  });

  it("shows recovery group readiness status", async () => {
    render(Page);
    await waitFor(() =>
      expect(screen.getByTestId("readiness-group-app_rg")).toHaveTextContent("NOT_READY"),
    );
    expect(screen.getAllByText("cell_west").length).toBeGreaterThan(0);
  });

  it.each([
    ["Cells", "No cells found", "readiness-cell-cell_west", "READY"],
    ["Readiness Checks", "No readiness checks found", "readiness-check-orders_check", "READY"],
  ])("%s tab shows readiness", async (tab, _empty, testId, want) => {
    render(Page);
    await waitFor(() => screen.getByText("app_rg"));
    await fireEvent.click(screen.getByRole("tab", { name: tab }));
    await waitFor(() => expect(screen.getByTestId(testId)).toHaveTextContent(want));
  });

  it("lists resource sets with type and scopes", async () => {
    render(Page);
    await waitFor(() => screen.getByText("app_rg"));
    await fireEvent.click(screen.getByRole("tab", { name: "Resource Sets" }));
    await waitFor(() => expect(screen.getByText("orders_queues")).toBeInTheDocument());
    expect(screen.getAllByText("AWS::SQS::Queue").length).toBeGreaterThan(0);
  });

  it("opens the readiness check detail with resource status", async () => {
    render(Page);
    await waitFor(() => screen.getByText("app_rg"));
    await fireEvent.click(screen.getByRole("tab", { name: "Readiness Checks" }));
    await waitFor(() => screen.getByLabelText("View readiness check orders_check"));
    await fireEvent.click(screen.getByLabelText("View readiness check orders_check"));
    await waitFor(() =>
      expect(screen.getAllByText("arn:aws:sqs:us-west-2:0:orders").length).toBeGreaterThan(0),
    );
  });

  it("creates a cell", async () => {
    const created: Record<string, unknown>[] = [];
    h.CreateCellCommand = (i) => {
      created.push(i);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("app_rg"));
    await fireEvent.click(screen.getByRole("tab", { name: "Cells" }));
    await waitFor(() => screen.getByTestId("readiness-cell-cell_west"));
    await fireEvent.click(screen.getByRole("button", { name: /Create cell/ }));
    await fireEvent.input(screen.getByLabelText("Cell name"), { target: { value: "cell_east" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => expect(created).toEqual([{ CellName: "cell_east" }]));
  });

  it("deletes a recovery group after confirmation", async () => {
    const deleted: Record<string, unknown>[] = [];
    h.DeleteRecoveryGroupCommand = (i) => {
      deleted.push(i);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("app_rg"));
    await fireEvent.click(screen.getByLabelText("Delete recovery group app_rg"));
    await waitFor(() => expect(deleted).toEqual([{ RecoveryGroupName: "app_rg" }]));
  });
});
