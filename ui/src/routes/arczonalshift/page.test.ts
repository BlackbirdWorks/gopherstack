import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import { DEFAULT_REGION, setStoredRegion } from "#lib/region.svelte.ts";
import Page from "./+page.svelte";

type Handler = (input: Record<string, unknown>) => unknown;
const h: Record<string, Handler> = {};

vi.mock("#lib/aws-client.ts", () => ({
  getARCZonalShiftClient: () => ({
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

const lb = "arn:aws:elasticloadbalancing:us-west-2:0:loadbalancer/app/api-alb/1";
const shift = (id: string, type: string, extra = {}) => ({
  zonalShiftId: id,
  resourceIdentifier: lb,
  awayFrom: "usw2-az1",
  status: "ACTIVE",
  comment: "c",
  shiftType: type,
  startTime: new Date("2026-01-01T00:00:00Z"),
  expiryTime: new Date("2026-01-01T02:00:00Z"),
  ...extra,
});

describe("ARC Zonal Shift page", () => {
  beforeEach(() => {
    for (const k of Object.keys(h)) delete h[k];
    h.ListManagedResourcesCommand = () => ({
      items: [
        {
          arn: lb,
          name: "api-alb",
          availabilityZones: ["usw2-az1", "usw2-az2"],
          appliedWeights: { "usw2-az1": 0, "usw2-az2": 1 },
          zonalShifts: [{ appliedStatus: "APPLIED" }],
          zonalAutoshiftStatus: "DISABLED",
          practiceRunStatus: "ENABLED",
        },
      ],
    });
    h.ListZonalShiftsCommand = () => ({
      items: [
        shift("zs-1", "ZONAL_SHIFT"),
        shift("pr-1", "PRACTICE_RUN", { practiceRunOutcome: "PENDING" }),
      ],
    });
    h.ListAutoshiftsCommand = () => ({ items: [] });
    confirmDestructive.mockClear();
  });

  it("lists managed resources with zones and weights", async () => {
    render(Page);
    await waitFor(() => expect(screen.getByText("api-alb")).toBeInTheDocument());
    expect(screen.getByText("usw2-az1 (0)")).toBeInTheDocument();
    expect(screen.getByText("usw2-az2 (1)")).toBeInTheDocument();
  });

  it.each([
    ["Zonal Shifts", "zs-1", "pr-1"],
    ["Practice Runs", "pr-1", "zs-1"],
  ])("%s tab filters by shift type", async (tab, shown, hidden) => {
    render(Page);
    await waitFor(() => screen.getByText("api-alb"));
    await fireEvent.click(screen.getByRole("tab", { name: tab }));
    await waitFor(() => expect(screen.getByText(shown)).toBeInTheDocument());
    expect(screen.queryByText(hidden)).not.toBeInTheDocument();
  });

  it("starts a zonal shift", async () => {
    const started: Record<string, unknown>[] = [];
    h.StartZonalShiftCommand = (i) => {
      started.push(i);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("api-alb"));
    await fireEvent.click(screen.getByLabelText("Start zonal shift for api-alb"));
    await fireEvent.input(screen.getByLabelText("Comment"), { target: { value: "evacuate" } });
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    await waitFor(() =>
      expect(started).toEqual([
        { resourceIdentifier: lb, awayFrom: "usw2-az1", expiresIn: "1h", comment: "evacuate" },
      ]),
    );
  });

  it("starts a practice run", async () => {
    const started: Record<string, unknown>[] = [];
    h.StartPracticeRunCommand = (i) => {
      started.push(i);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("api-alb"));
    await fireEvent.click(screen.getByLabelText("Start practice run for api-alb"));
    await fireEvent.input(screen.getByLabelText("Comment"), { target: { value: "drill" } });
    await fireEvent.click(screen.getByRole("button", { name: "Start" }));
    await waitFor(() =>
      expect(started).toEqual([{ resourceIdentifier: lb, awayFrom: "usw2-az1", comment: "drill" }]),
    );
  });

  it("cancels an active zonal shift after confirmation", async () => {
    const canceled: Record<string, unknown>[] = [];
    h.CancelZonalShiftCommand = (i) => {
      canceled.push(i);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("api-alb"));
    await fireEvent.click(screen.getByRole("tab", { name: "Zonal Shifts" }));
    await waitFor(() => screen.getByLabelText("Cancel zonal shift zs-1"));
    await fireEvent.click(screen.getByLabelText("Cancel zonal shift zs-1"));
    await waitFor(() => expect(canceled).toEqual([{ zonalShiftId: "zs-1" }]));
  });

  it("shows a load error with the AWS error code", async () => {
    h.ListManagedResourcesCommand = () => {
      throw Object.assign(new Error("denied"), {
        name: "AccessDeniedException",
        $metadata: { httpStatusCode: 403 },
      });
    };
    render(Page);
    await waitFor(() =>
      expect(screen.getByText("AccessDeniedException (HTTP 403): denied")).toBeInTheDocument(),
    );
  });

  it("reloads every loaded tab when the region changes", async () => {
    let shiftLists = 0;
    const list = h.ListZonalShiftsCommand;
    h.ListZonalShiftsCommand = (input) => {
      shiftLists += 1;
      return list(input);
    };
    render(Page);
    await waitFor(() => screen.getByText("api-alb"));
    await fireEvent.click(screen.getByRole("tab", { name: "Zonal Shifts" }));
    await waitFor(() => screen.getByText("zs-1"));
    await fireEvent.click(screen.getByRole("tab", { name: "Managed Resources" }));
    expect(shiftLists).toBe(1);

    setStoredRegion("us-west-2");
    try {
      await waitFor(() => expect(shiftLists).toBe(2));
    } finally {
      setStoredRegion(DEFAULT_REGION);
    }
  });
});
