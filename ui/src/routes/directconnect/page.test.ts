import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import DirectConnectPage from "./+page.svelte";

// Every Modal on this page stays mounted in the DOM once rendered -- a closed
// <dialog> just loses the `open` attribute rather than unmounting -- so
// getByLabelText/getByRole across the whole document goes ambiguous once more
// than one modal shares a label or footer button. Scope to the dialog that is
// actually open, the same pattern as accessanalyzer/dlm/page.test.ts.
function openDialog(): HTMLElement {
  const dialog = document.querySelector("dialog[open]");
  if (!dialog) throw new Error("no open dialog found");
  return dialog as HTMLElement;
}

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getDirectConnectClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const exampleConnection = {
  connectionId: "dxcon-abcd1234",
  connectionName: "example-connection",
  connectionState: "available",
  bandwidth: "1Gbps",
  location: "EqDC2",
  region: "us-east-1",
  ownerAccount: "000000000000",
  vlan: 0,
  tags: [{ key: "env", value: "test" }],
};

const exampleLag = {
  lagId: "dxlag-abcd1234",
  lagName: "example-lag",
  lagState: "available",
  connectionsBandwidth: "1Gbps",
  numberOfConnections: 1,
  minimumLinks: 0,
  connections: [],
};

const exampleInterconnect = {
  interconnectId: "dxcon-ic001122",
  interconnectName: "example-interconnect",
  interconnectState: "available",
  bandwidth: "1Gbps",
  location: "EqDC2",
};

const exampleVif = {
  virtualInterfaceId: "dxvif-abcd1234",
  virtualInterfaceName: "example-vif",
  virtualInterfaceType: "private",
  virtualInterfaceState: "available",
  connectionId: "dxcon-abcd1234",
  vlan: 100,
  asn: 65000,
  bgpPeers: [{ bgpPeerId: "bgp-1234abcd", asn: 65000, bgpPeerState: "available", bgpStatus: "up" }],
};

const exampleGateway = {
  directConnectGatewayId: "11111111-1111-1111-1111-111111111111",
  directConnectGatewayName: "example-gateway",
  directConnectGatewayState: "available",
  ownerAccount: "000000000000",
};

const exampleProposal = {
  proposalId: "22222222-2222-2222-2222-222222222222",
  directConnectGatewayId: "11111111-1111-1111-1111-111111111111",
  directConnectGatewayOwnerAccount: "000000000000",
  proposalState: "requested",
};

describe("Direct Connect Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
  });

  it("renders page title", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getByText("AWS Direct Connect")).toBeInTheDocument();
    await waitFor(() => screen.getByText("No connections found"));
  });

  it("shows all tabs", () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    expect(screen.getByText("Connections")).toBeInTheDocument();
    expect(screen.getByText("LAGs")).toBeInTheDocument();
    expect(screen.getByText("Interconnects")).toBeInTheDocument();
    expect(screen.getByText("Virtual Interfaces")).toBeInTheDocument();
    expect(screen.getByText("Gateways")).toBeInTheDocument();
    expect(screen.getByText("Gateway Associations")).toBeInTheDocument();
    expect(screen.getByText("Association Proposals")).toBeInTheDocument();
    expect(screen.getByText("Gateway Attachments")).toBeInTheDocument();
    expect(screen.getByText("Virtual Gateways")).toBeInTheDocument();
    expect(screen.getByText("Locations")).toBeInTheDocument();
    expect(screen.getByText("VIF Test History")).toBeInTheDocument();
  });

  it("shows empty state when no connections", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => {
      expect(screen.getByText("No connections found")).toBeInTheDocument();
    });
  });

  it("lists connections with a state badge", async () => {
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    render(DirectConnectPage);
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-connection" })).toBeInTheDocument();
    });
    expect(screen.getByText("available")).toBeInTheDocument();
  });

  it("creates a connection", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    await fireEvent.click(screen.getByRole("button", { name: "Create Connection" }));
    const dialog = within(openDialog());
    await fireEvent.input(dialog.getByLabelText("Name"), { target: { value: "new-connection" } });
    await fireEvent.input(dialog.getByLabelText("Location"), { target: { value: "EqDC2" } });

    mockSend.mockResolvedValueOnce({ connectionId: "dxcon-new00001" });
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    await fireEvent.click(dialog.getByRole("button", { name: "Create" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalled();
    });
  });

  it("deletes a connection after confirmation", async () => {
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-connection" }));

    mockSend.mockResolvedValueOnce({});
    mockSend.mockResolvedValueOnce({ connections: [] });
    await fireEvent.click(
      screen.getByRole("button", { name: "Delete connection example-connection" }),
    );

    await waitFor(() => {
      expect(confirmDestructive).toHaveBeenCalled();
    });
  });

  it("opens connection detail and shows tags", async () => {
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-connection" }));

    mockSend.mockResolvedValueOnce({ connections: [exampleConnection] });
    await fireEvent.click(
      screen.getByRole("button", { name: "View connection example-connection" }),
    );

    const dialog = within(openDialog());
    await waitFor(() => {
      expect(dialog.getByText("env=test")).toBeInTheDocument();
    });
  });

  it("lists LAGs on the LAGs tab", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    mockSend.mockResolvedValueOnce({ lags: [exampleLag] });
    await fireEvent.click(screen.getByText("LAGs"));
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-lag" })).toBeInTheDocument();
    });
  });

  it("shows a bookkeeping-only note on the Interconnects tab", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    mockSend.mockResolvedValueOnce({ interconnects: [exampleInterconnect] });
    await fireEvent.click(screen.getByText("Interconnects"));
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-interconnect" })).toBeInTheDocument();
    });
    expect(screen.getByText(/bookkeeping only/)).toBeInTheDocument();
  });

  it("lists virtual interfaces and shows nested BGP peers in detail", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    mockSend.mockResolvedValueOnce({ virtualInterfaces: [exampleVif] });
    await fireEvent.click(screen.getByText("Virtual Interfaces"));
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-vif" })).toBeInTheDocument();
    });

    mockSend.mockResolvedValueOnce({ virtualInterfaces: [exampleVif] });
    await fireEvent.click(
      screen.getByRole("button", { name: "View virtual interface example-vif" }),
    );
    const dialog = within(openDialog());
    await waitFor(() => {
      expect(dialog.getByText(/bgp-1234abcd/)).toBeInTheDocument();
    });
  });

  it("lists Direct Connect gateways and notes the global-ARN behavior", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    mockSend.mockResolvedValueOnce({ directConnectGateways: [exampleGateway] });
    await fireEvent.click(screen.getByText("Gateways"));
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "example-gateway" })).toBeInTheDocument();
    });
    expect(screen.getByText(/GLOBAL resource/)).toBeInTheDocument();
  });

  it("shows a requested proposal and accepts it", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    mockSend.mockResolvedValueOnce({ directConnectGatewayAssociationProposals: [exampleProposal] });
    await fireEvent.click(screen.getByText("Association Proposals"));
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: exampleProposal.proposalId })).toBeInTheDocument();
    });

    await fireEvent.click(
      screen.getByRole("button", { name: `View proposal ${exampleProposal.proposalId}` }),
    );
    const dialog = within(openDialog());
    await fireEvent.input(dialog.getByLabelText("Associated gateway owner account"), {
      target: { value: "000000000099" },
    });

    mockSend.mockResolvedValueOnce({ directConnectGatewayAssociation: {} });
    mockSend.mockResolvedValueOnce({ directConnectGatewayAssociationProposals: [] });
    mockSend.mockResolvedValueOnce({ directConnectGatewayAssociations: [] });
    await fireEvent.click(dialog.getByRole("button", { name: "Accept Proposal" }));

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalled();
    });
  });

  it("shows the customer metadata panel on the Locations tab", async () => {
    mockSend.mockResolvedValueOnce({ connections: [] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByText("No connections found"));

    mockSend.mockResolvedValueOnce({ locations: [] });
    mockSend.mockResolvedValueOnce({ agreements: [], nniPartnerType: "nonPartner" });
    await fireEvent.click(screen.getByText("Locations"));
    await waitFor(() => {
      expect(screen.getByText("Customer Metadata")).toBeInTheDocument();
    });
    expect(screen.getByText(/NNI partner type: nonPartner/)).toBeInTheDocument();
  });

  it("filters connections by search query", async () => {
    const other = {
      ...exampleConnection,
      connectionId: "dxcon-other0001",
      connectionName: "other-connection",
    };
    mockSend.mockResolvedValueOnce({ connections: [exampleConnection, other] });
    render(DirectConnectPage);
    await waitFor(() => screen.getByRole("cell", { name: "example-connection" }));
    expect(screen.getByRole("cell", { name: "other-connection" })).toBeInTheDocument();

    await fireEvent.input(screen.getByPlaceholderText("Search..."), {
      target: { value: "other-connection" },
    });

    await waitFor(() => {
      expect(screen.queryByRole("cell", { name: "example-connection" })).not.toBeInTheDocument();
      expect(screen.getByRole("cell", { name: "other-connection" })).toBeInTheDocument();
    });
  });
});
