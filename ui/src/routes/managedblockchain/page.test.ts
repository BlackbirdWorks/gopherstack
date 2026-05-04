import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import ManagedBlockchainPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getManagedBlockchainClient: () => ({ send: mockSend }),
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
    info: vi.fn(),
  },
}));

const exampleNetwork = {
  Id: "net-abc123",
  Name: "test-network",
  Status: "AVAILABLE",
  Framework: "HYPERLEDGER_FABRIC",
  FrameworkVersion: "2.2",
  Description: "Test network",
};

const exampleMember = {
  Id: "mem-def456",
  Name: "test-member",
  Status: "AVAILABLE",
  Description: "Test member",
};

const exampleAccessor = {
  Id: "acc-jkl012",
  Type: "BILLING_TOKEN",
  NetworkType: "ETHEREUM_MAINNET",
  Status: "AVAILABLE",
};

const exampleProposal = {
  ProposalId: "prop-mno345",
  Status: "IN_PROGRESS",
  Description: "Test proposal",
  ProposedByMemberName: "test-member",
  YesVoteCount: 0,
  NoVoteCount: 0,
  OutstandingVoteCount: 1,
};

const exampleInvitation = {
  InvitationId: "inv-pqr678",
  Status: "PENDING",
  CreationDate: new Date("2024-01-01"),
};

describe("ManagedBlockchainPage", () => {
  beforeEach(() => {
    mockSend.mockReset();
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
  });

  it("renders the page header", () => {
    render(ManagedBlockchainPage);
    expect(screen.getByText("Managed Blockchain")).toBeTruthy();
  });

  it("loads and displays networks on mount", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);
    await waitFor(() => {
      expect(screen.getByText("test-network")).toBeTruthy();
    });
  });

  it("shows empty state when no networks", async () => {
    mockSend.mockResolvedValue({ Networks: [] });
    render(ManagedBlockchainPage);
    await waitFor(() => {
      expect(screen.getByText(/No networks found/)).toBeTruthy();
    });
  });

  it("renders all tab buttons", () => {
    render(ManagedBlockchainPage);
    expect(screen.getByText("Networks")).toBeTruthy();
    expect(screen.getByText("Accessors")).toBeTruthy();
    expect(screen.getByText("Proposals")).toBeTruthy();
    expect(screen.getByText("Invitations")).toBeTruthy();
    expect(screen.getByText("Topology")).toBeTruthy();
  });

  it("shows create network form when button clicked", async () => {
    mockSend.mockResolvedValue({ Networks: [] });
    render(ManagedBlockchainPage);
    await waitFor(() => screen.getByText("Create Network"));

    const createBtn = screen.getByText("Create Network");
    await fireEvent.click(createBtn);

    expect(screen.getByText("Network Name *")).toBeTruthy();
    expect(screen.getByText("Initial Member Name *")).toBeTruthy();
  });

  it("cancels create network form", async () => {
    mockSend.mockResolvedValue({ Networks: [] });
    render(ManagedBlockchainPage);
    await waitFor(() => screen.getByText("Create Network"));

    await fireEvent.click(screen.getByText("Create Network"));
    expect(screen.getByText("Network Name *")).toBeTruthy();

    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Network Name *")).toBeNull();
  });

  it("switches to accessors tab and loads accessors", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend.mockResolvedValue({ Accessors: [exampleAccessor] });
    await fireEvent.click(screen.getByText("Accessors"));

    await waitFor(() => {
      expect(screen.getByText("acc-jkl012")).toBeTruthy();
    });
  });

  it("shows empty accessors state", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend.mockResolvedValue({ Accessors: [] });
    await fireEvent.click(screen.getByText("Accessors"));

    await waitFor(() => {
      expect(screen.getByText(/No accessors found/)).toBeTruthy();
    });
  });

  it("shows create accessor form", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend.mockResolvedValue({ Accessors: [] });
    await fireEvent.click(screen.getByText("Accessors"));
    await waitFor(() => screen.getByText("Create Accessor"));

    await fireEvent.click(screen.getByText("Create Accessor"));
    expect(screen.getByText("Accessor Type")).toBeTruthy();
  });

  it("switches to proposals tab", async () => {
    render(ManagedBlockchainPage);
    await fireEvent.click(screen.getByText("Proposals"));
    expect(screen.getByText("Network ID:")).toBeTruthy();
  });

  it("loads proposals when network ID entered and load clicked", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    await fireEvent.click(screen.getByText("Proposals"));

    const input = screen.getByPlaceholderText("Enter network ID");
    await fireEvent.input(input, { target: { value: "net-abc123" } });

    mockSend.mockResolvedValue({ Proposals: [exampleProposal] });
    const loadBtn = screen.getByText("Load");
    await fireEvent.click(loadBtn);

    await waitFor(() => {
      expect(screen.getByText("Test proposal")).toBeTruthy();
    });
  });

  it("switches to invitations tab and loads invitations", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend.mockResolvedValue({ Invitations: [exampleInvitation] });
    await fireEvent.click(screen.getByText("Invitations"));

    await waitFor(() => {
      expect(screen.getAllByText("inv-pqr678").length).toBeGreaterThan(0);
    });
  });

  it("shows empty invitations state", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend.mockResolvedValue({ Invitations: [] });
    await fireEvent.click(screen.getByText("Invitations"));

    await waitFor(() => {
      expect(screen.getByText(/No pending invitations/)).toBeTruthy();
    });
  });

  it("switches to topology tab and builds topology", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend
      .mockResolvedValueOnce({ Networks: [exampleNetwork] })
      .mockResolvedValueOnce({ Members: [exampleMember] });

    await fireEvent.click(screen.getByText("Topology"));

    await waitFor(() => {
      expect(screen.getByText("Network Topology")).toBeTruthy();
    });
  });

  it("expands network to show members", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    await waitFor(() => screen.getByText("test-network"));

    mockSend.mockResolvedValue({ Members: [exampleMember] });

    const expandBtn = screen.getByRole("button", {
      name: /Expand test-network/i,
    });
    await fireEvent.click(expandBtn);

    await waitFor(() => {
      expect(mockSend).toHaveBeenCalled();
    });
  });

  it("shows network count in networks tab", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    await waitFor(() => {
      expect(screen.getByText("1 network")).toBeTruthy();
    });
  });

  it("shows accessor count in accessors tab", async () => {
    mockSend.mockResolvedValue({ Networks: [exampleNetwork] });
    render(ManagedBlockchainPage);

    mockSend.mockResolvedValue({ Accessors: [exampleAccessor] });
    await fireEvent.click(screen.getByText("Accessors"));

    await waitFor(() => {
      expect(screen.getByText("1 accessor")).toBeTruthy();
    });
  });
});
