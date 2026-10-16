import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import { DEFAULT_REGION, setStoredRegion } from "#lib/region.svelte.ts";
import Page from "./+page.svelte";

type Handler = (input: Record<string, unknown>) => unknown;
const configHandlers: Record<string, Handler> = {};
const clusterHandlers: Record<string, Handler> = {};
const clusterRegions: string[] = [];

function dispatch(table: Record<string, Handler>) {
  return (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
    const h = table[cmd.constructor.name];
    if (!h) {
      return Promise.reject(new Error(`unexpected command ${cmd.constructor.name}`));
    }
    try {
      return Promise.resolve(h(cmd.input));
    } catch (e) {
      return Promise.reject(e);
    }
  };
}

vi.mock("#lib/aws-client.ts", () => ({
  getRoute53RecoveryControlConfigClient: () => ({ send: dispatch(configHandlers) }),
  getRoute53RecoveryClusterClient: (_path: string, region: string) => {
    clusterRegions.push(region);
    return { send: dispatch(clusterHandlers) };
  },
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("#lib/confirm-dialog.ts", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

const clusterArn = "arn:aws:route53-recovery-control::000000000000:cluster/c1";
const panelArn = "arn:aws:route53-recovery-control::000000000000:controlpanel/p1";
const rcEast = `${panelArn}/routingcontrol/east`;
const rcWest = `${panelArn}/routingcontrol/west`;

const cluster = {
  ClusterArn: clusterArn,
  Name: "prod-cluster",
  Status: "DEPLOYED",
  NetworkType: "IPV4",
  ClusterEndpoints: [
    { Endpoint: "http://h/route53-recovery-cluster/c1/us-east-1", Region: "us-east-1" },
    { Endpoint: "http://h/route53-recovery-cluster/c1/us-west-2", Region: "us-west-2" },
  ],
};
const panel = {
  ControlPanelArn: panelArn,
  ClusterArn: clusterArn,
  Name: "app-panel",
  Status: "DEPLOYED",
  RoutingControlCount: 2,
  DefaultControlPanel: false,
};

function seed(states: Record<string, string>) {
  configHandlers.ListClustersCommand = () => ({ Clusters: [cluster] });
  configHandlers.ListControlPanelsCommand = () => ({ ControlPanels: [panel] });
  configHandlers.ListRoutingControlsCommand = () => ({
    RoutingControls: [
      {
        RoutingControlArn: rcEast,
        Name: "east-active",
        Status: "DEPLOYED",
        ControlPanelArn: panelArn,
      },
      {
        RoutingControlArn: rcWest,
        Name: "west-active",
        Status: "DEPLOYED",
        ControlPanelArn: panelArn,
      },
    ],
  });
  configHandlers.ListSafetyRulesCommand = () => ({
    SafetyRules: [
      {
        ASSERTION: {
          SafetyRuleArn: `${panelArn}/safetyrule/a1`,
          Name: "at-least-one",
          Status: "DEPLOYED",
          AssertedControls: [rcEast, rcWest],
          RuleConfig: { Type: "ATLEAST", Threshold: 1, Inverted: false },
          WaitPeriodMs: 0,
        },
      },
      {
        GATING: {
          SafetyRuleArn: `${panelArn}/safetyrule/g1`,
          Name: "gate-west",
          Status: "DEPLOYED",
          GatingControls: [rcEast],
          TargetControls: [rcWest],
          RuleConfig: { Type: "AND", Threshold: 1, Inverted: false },
          WaitPeriodMs: 5,
        },
      },
    ],
  });
  clusterHandlers.ListRoutingControlsCommand = () => ({
    RoutingControls: Object.entries(states).map(([arn, s]) => ({
      RoutingControlArn: arn,
      RoutingControlState: s,
    })),
  });
}

async function openTab(name: string) {
  await fireEvent.click(screen.getByRole("tab", { name }));
}

describe("Route 53 Recovery Control page", () => {
  beforeEach(() => {
    for (const t of [configHandlers, clusterHandlers]) {
      for (const k of Object.keys(t)) delete t[k];
    }
    clusterRegions.length = 0;
    confirmDestructive.mockClear();
  });

  it("lists clusters with status and endpoint count", async () => {
    seed({});
    render(Page);
    await waitFor(() => expect(screen.getByText("prod-cluster")).toBeInTheDocument());
    expect(screen.getByText("DEPLOYED")).toBeInTheDocument();
    await fireEvent.click(screen.getByLabelText("Toggle endpoints for prod-cluster"));
    expect(screen.getByText("http://h/route53-recovery-cluster/c1/us-west-2")).toBeInTheDocument();
  });

  it("shows live routing control state read through the data plane", async () => {
    seed({ [rcEast]: "On", [rcWest]: "Off" });
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Routing Controls");
    await waitFor(() => expect(screen.getByTestId("state-east-active")).toHaveTextContent("On"));
    expect(screen.getByTestId("state-west-active")).toHaveTextContent("Off");
  });

  it("flips a routing control through the data plane", async () => {
    seed({ [rcEast]: "On", [rcWest]: "Off" });
    const updates: Record<string, unknown>[] = [];
    clusterHandlers.UpdateRoutingControlStateCommand = (input) => {
      updates.push(input);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Routing Controls");
    await waitFor(() => screen.getByLabelText("Turn west-active On"));
    await fireEvent.click(screen.getByLabelText("Turn west-active On"));
    await waitFor(() =>
      expect(updates).toEqual([{ RoutingControlArn: rcWest, RoutingControlState: "On" }]),
    );
  });

  it("shows the ConflictException when a safety rule blocks the flip", async () => {
    seed({ [rcEast]: "Off", [rcWest]: "On" });
    clusterHandlers.UpdateRoutingControlStateCommand = () => {
      throw Object.assign(new Error("assertion rule at-least-one blocks this change"), {
        name: "ConflictException",
        $metadata: { httpStatusCode: 409 },
      });
    };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Routing Controls");
    await waitFor(() => screen.getByLabelText("Turn west-active Off"));
    await fireEvent.click(screen.getByLabelText("Turn west-active Off"));
    await waitFor(() =>
      expect(screen.getByTestId("flip-error")).toHaveTextContent(
        "ConflictException (HTTP 409): assertion rule at-least-one blocks this change",
      ),
    );
    expect(screen.getByTestId("state-west-active")).toHaveTextContent("On");
  });

  it("keeps loaded panels and does not refetch clusters when switching tabs", async () => {
    seed({});
    let clusterLists = 0;
    configHandlers.ListClustersCommand = () => {
      clusterLists += 1;
      return { Clusters: [cluster] };
    };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Safety Rules");
    await waitFor(() => screen.getByText("at-least-one"));
    expect(screen.getByRole("option", { name: "app-panel" })).toBeInTheDocument();
    expect(clusterLists).toBe(1);
  });

  it("renders assertion and gating safety rules", async () => {
    seed({});
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Safety Rules");
    await waitFor(() => expect(screen.getByText("at-least-one")).toBeInTheDocument());
    expect(screen.getByText("gate-west")).toBeInTheDocument();
    expect(screen.getByText("Assertion")).toBeInTheDocument();
    expect(screen.getByText("Gating")).toBeInTheDocument();
  });

  it("creates a cluster from the modal", async () => {
    seed({});
    const created: Record<string, unknown>[] = [];
    configHandlers.CreateClusterCommand = (input) => {
      created.push(input);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await fireEvent.click(screen.getByRole("button", { name: /Create cluster/ }));
    await fireEvent.input(screen.getByLabelText("Cluster name"), { target: { value: "dr" } });
    await fireEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => expect(created).toEqual([{ ClusterName: "dr", NetworkType: "IPV4" }]));
  });

  it("deletes a cluster only after confirmation", async () => {
    seed({});
    const deleted: Record<string, unknown>[] = [];
    configHandlers.DeleteClusterCommand = (input) => {
      deleted.push(input);
      return {};
    };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    confirmDestructive.mockResolvedValueOnce(false);
    await fireEvent.click(screen.getByLabelText("Delete cluster prod-cluster"));
    expect(deleted).toEqual([]);
    await fireEvent.click(screen.getByLabelText("Delete cluster prod-cluster"));
    await waitFor(() => expect(deleted).toEqual([{ ClusterArn: clusterArn }]));
  });

  it("resolves safety rule control names across paginated routing controls", async () => {
    seed({});
    configHandlers.ListRoutingControlsCommand = (input) =>
      input.NextToken
        ? { RoutingControls: [{ RoutingControlArn: rcWest, Name: "west-active" }] }
        : {
            RoutingControls: [{ RoutingControlArn: rcEast, Name: "east-active" }],
            NextToken: "page2",
          };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Safety Rules");
    await waitFor(() => screen.getByText("at-least-one"));
    expect(screen.getByText(/Asserted: east-active, west-active/)).toBeInTheDocument();
  });

  it("reloads every loaded tab when the region changes", async () => {
    seed({});
    const calls = { clusters: 0, rules: 0 };
    configHandlers.ListClustersCommand = () => {
      calls.clusters += 1;
      return { Clusters: [cluster] };
    };
    const rules = configHandlers.ListSafetyRulesCommand;
    configHandlers.ListSafetyRulesCommand = (input) => {
      calls.rules += 1;
      return rules(input);
    };
    render(Page);
    await waitFor(() => screen.getByText("prod-cluster"));
    await openTab("Safety Rules");
    await waitFor(() => screen.getByText("at-least-one"));
    expect(calls).toEqual({ clusters: 1, rules: 1 });

    setStoredRegion("us-west-2");
    try {
      await waitFor(() => expect(calls.rules).toBe(2));
      await waitFor(() => expect(calls.clusters).toBe(2));
    } finally {
      setStoredRegion(DEFAULT_REGION);
    }
  });
});
