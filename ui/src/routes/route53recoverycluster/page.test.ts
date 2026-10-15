import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import Page from "./+page.svelte";

type Handler = (input: Record<string, unknown>) => unknown;
const cfg: Record<string, Handler> = {};
const data: Record<string, Handler> = {};
const regionsUsed: string[] = [];

function dispatch(table: Record<string, Handler>) {
  return (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
    const h = table[cmd.constructor.name];
    if (!h) return Promise.reject(new Error(`unexpected ${cmd.constructor.name}`));
    try {
      return Promise.resolve(h(cmd.input));
    } catch (e) {
      return Promise.reject(e);
    }
  };
}

vi.mock("#lib/aws-client.ts", () => ({
  getRoute53RecoveryControlConfigClient: () => ({ send: dispatch(cfg) }),
  getRoute53RecoveryClusterClient: (_p: string, region: string) => {
    regionsUsed.push(region);
    return { send: dispatch(data) };
  },
}));
vi.mock("svelte-sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

const rc = "arn:aws:route53-recovery-control::000000000000:controlpanel/p/routingcontrol/r1";

describe("Route 53 Recovery Cluster page", () => {
  beforeEach(() => {
    for (const t of [cfg, data]) for (const k of Object.keys(t)) delete t[k];
    regionsUsed.length = 0;
    cfg.ListClustersCommand = () => ({
      Clusters: [
        {
          ClusterArn: "arn:c",
          Name: "prod",
          Status: "DEPLOYED",
          ClusterEndpoints: [
            { Endpoint: "http://h/route53-recovery-cluster/c/us-east-1", Region: "us-east-1" },
            { Endpoint: "http://h/route53-recovery-cluster/c/us-west-2", Region: "us-west-2" },
          ],
        },
      ],
    });
  });

  it("reads routing control state from the cluster endpoint", async () => {
    data.ListRoutingControlsCommand = () => ({
      RoutingControls: [
        {
          RoutingControlArn: rc,
          RoutingControlName: "east",
          ControlPanelName: "panel",
          RoutingControlState: "On",
        },
      ],
    });
    render(Page);
    await waitFor(() => expect(screen.getByTestId("state-east")).toHaveTextContent("On"));
    expect(regionsUsed.length).toBeGreaterThan(0);
  });

  it("surfaces ConflictException from UpdateRoutingControlState", async () => {
    data.ListRoutingControlsCommand = () => ({
      RoutingControls: [
        { RoutingControlArn: rc, RoutingControlName: "east", RoutingControlState: "On" },
      ],
    });
    data.UpdateRoutingControlStateCommand = () => {
      throw Object.assign(new Error("blocked by rule"), {
        name: "ConflictException",
        $metadata: { httpStatusCode: 409 },
      });
    };
    render(Page);
    await waitFor(() => screen.getByLabelText("Turn east Off"));
    await fireEvent.click(screen.getByLabelText("Turn east Off"));
    await waitFor(() =>
      expect(screen.getByTestId("flip-error")).toHaveTextContent(
        "ConflictException (HTTP 409): blocked by rule",
      ),
    );
  });

  it("shows an empty state when there are no clusters", async () => {
    cfg.ListClustersCommand = () => ({ Clusters: [] });
    render(Page);
    await waitFor(() => expect(screen.getByText("No routing controls found")).toBeInTheDocument());
  });
});
