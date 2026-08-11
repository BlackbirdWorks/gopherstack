import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/svelte";
import EC2Page from "./+page.svelte";
import { setMockPageUrl } from "$lib/mock-page.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "$lib/region.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getEC2Client: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

function stubRegionsWithData(regions: string[]): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ regions }),
    }),
  );
}

const mockInstance = {
  InstanceId: "i-1234567890abcdef0",
  InstanceType: "t3.micro",
  State: { Name: "running" },
  PublicIpAddress: "54.1.2.3",
  PrivateIpAddress: "10.0.1.5",
  Tags: [{ Key: "Name", Value: "my-server" }],
  LaunchTime: new Date("2024-01-15"),
  Platform: "Linux/UNIX",
  VpcId: "vpc-abc123",
  SubnetId: "subnet-def456",
};

const mockStoppedInstance = {
  InstanceId: "i-stopped",
  InstanceType: "t2.small",
  State: { Name: "stopped" },
  Tags: [{ Key: "Name", Value: "dev-server" }],
};

describe("EC2 Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    setStoredRegion(DEFAULT_REGION);
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ Reservations: [] });
    render(EC2Page);
    expect(screen.getByRole("heading", { name: "EC2", level: 1 })).toBeInTheDocument();
  });

  it("shows Launch Instance button", () => {
    mockSend.mockResolvedValue({ Reservations: [] });
    render(EC2Page);
    expect(screen.getByText("Launch Instance")).toBeInTheDocument();
  });

  it("shows empty state when no instances", async () => {
    mockSend.mockResolvedValue({ Reservations: [] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("No EC2 instances found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded instances by name tag", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("my-server")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("t3.micro")).toBeInTheDocument();
  });

  it("shows instance state badge", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("running")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows stopped instance with start button", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockStoppedInstance] }] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("dev-server")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("Start")).toBeInTheDocument();
  });

  it("shows running instance with stop button", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("Stop")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows instance IP addresses", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("54.1.2.3")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("10.0.1.5")).toBeInTheDocument();
  });

  it("filters instances via search", async () => {
    mockSend.mockResolvedValueOnce({
      Reservations: [{ Instances: [mockInstance, mockStoppedInstance] }],
    });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("my-server")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const searchInput = screen.getByPlaceholderText("Search instances...");
    await fireEvent.input(searchInput, { target: { value: "dev" } });
    await waitFor(() => {
      expect(screen.queryByText("my-server")).not.toBeInTheDocument();
    });
    expect(screen.getByText("dev-server")).toBeInTheDocument();
  });

  it("calls stop API and shows toast", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
    // stop response
    mockSend.mockResolvedValueOnce({ StoppingInstances: [] });
    // reload
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("Stop")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    await fireEvent.click(screen.getByText("Stop"));
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.success)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("calls start API on stopped instance", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockStoppedInstance] }] });
    mockSend.mockResolvedValueOnce({ StartingInstances: [] });
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText("Start")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    await fireEvent.click(screen.getByText("Start"));
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.success)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("access denied"));
    render(EC2Page);
    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("shows instance count summary", async () => {
    mockSend.mockResolvedValueOnce({
      Reservations: [{ Instances: [mockInstance, mockStoppedInstance] }],
    });
    render(EC2Page);
    await waitFor(
      () => {
        expect(screen.getByText(/2/)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows extended EC2 resource tabs", () => {
    mockSend.mockResolvedValue({ Reservations: [] });
    render(EC2Page);
    expect(screen.getByText("Security Groups")).toBeInTheDocument();
    expect(screen.getByText("Key Pairs")).toBeInTheDocument();
    expect(screen.getByText("AMIs")).toBeInTheDocument();
    expect(screen.getByText("Launch Templates")).toBeInTheDocument();
    expect(screen.getByText("VPC Endpoints")).toBeInTheDocument();
    expect(screen.getByText("Network ACLs")).toBeInTheDocument();
  });

  it("loads security groups when tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    mockSend.mockResolvedValueOnce({
      SecurityGroups: [
        {
          GroupName: "default",
          GroupId: "sg-123",
          VpcId: "vpc-abc",
          Description: "Default SG",
          IpPermissions: [],
          IpPermissionsEgress: [],
        },
      ],
    });
    render(EC2Page);
    await waitFor(() => screen.getByText("No EC2 instances found"), { timeout: 3000 });
    const sgButtons = screen.getAllByText(/Security Groups/i);
    const tabBtn = sgButtons.find((el) => el.tagName === "BUTTON" || el.closest("button"));
    if (tabBtn) await fireEvent.click(tabBtn.closest("button") ?? tabBtn);
    await waitFor(
      () => {
        expect(screen.getByText("default")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("honors ?tab=secgroups from the URL on load, without needing a click", async () => {
    setMockPageUrl(new URL("http://localhost/ec2?tab=secgroups"));
    // Only the active tab's data loads on mount (see the onRegionChange
    // effect in +page.svelte) — with the URL selecting "secgroups", that's
    // the ONLY describe call expected, not instances.
    mockSend.mockResolvedValueOnce({
      SecurityGroups: [
        {
          GroupName: "url-selected-sg",
          GroupId: "sg-url",
          VpcId: "vpc-abc",
          IpPermissions: [],
          IpPermissionsEgress: [],
        },
      ],
    });

    render(EC2Page);

    await waitFor(() => {
      expect(screen.getByText("url-selected-sg")).toBeInTheDocument();
    });
    expect(screen.queryByText("No EC2 instances found")).not.toBeInTheDocument();
  });

  it("honors ?q= and ?state= filters from the URL on load", async () => {
    setMockPageUrl(new URL("http://localhost/ec2?tab=instances&q=dev&state=stopped"));
    mockSend.mockResolvedValueOnce({
      Reservations: [{ Instances: [mockInstance] }, { Instances: [mockStoppedInstance] }],
    });

    render(EC2Page);

    await waitFor(() => {
      expect(screen.getByText("dev-server")).toBeInTheDocument();
    });
    expect(screen.queryByText("my-server")).not.toBeInTheDocument();
  });

  it("loads key pairs when tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    mockSend.mockResolvedValueOnce({
      KeyPairs: [{ KeyName: "my-keypair", KeyPairId: "key-123", KeyFingerprint: "abc123" }],
    });
    render(EC2Page);
    await waitFor(() => screen.getByText("No EC2 instances found"), { timeout: 3000 });
    const kpButtons = screen.getAllByText(/Key Pairs/i);
    const tabBtn = kpButtons.find((el) => el.tagName === "BUTTON" || el.closest("button"));
    if (tabBtn) await fireEvent.click(tabBtn.closest("button") ?? tabBtn);
    await waitFor(
      () => {
        expect(screen.getByText("my-keypair")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads AMIs when tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    mockSend.mockResolvedValueOnce({
      Images: [{ ImageId: "ami-123", Name: "base-ami", State: "available" }],
    });
    render(EC2Page);
    await waitFor(() => screen.getByText("No EC2 instances found"), { timeout: 3000 });
    await fireEvent.click(screen.getByRole("button", { name: /AMIs/i }));
    await waitFor(
      () => {
        expect(screen.getByText("base-ami")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads launch templates when tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    mockSend.mockResolvedValueOnce({
      LaunchTemplates: [{ LaunchTemplateId: "lt-123", LaunchTemplateName: "web" }],
    });
    render(EC2Page);
    await waitFor(() => screen.getByText("No EC2 instances found"), { timeout: 3000 });
    await fireEvent.click(screen.getByRole("button", { name: /Launch Templates/i }));
    await waitFor(
      () => {
        expect(screen.getByText("web")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads VPC endpoints when tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    mockSend.mockResolvedValueOnce({
      VpcEndpoints: [{ VpcEndpointId: "vpce-123", ServiceName: "com.amazonaws.us-east-1.s3" }],
    });
    render(EC2Page);
    await waitFor(() => screen.getByText("No EC2 instances found"), { timeout: 3000 });
    await fireEvent.click(screen.getByRole("button", { name: /VPC Endpoints/i }));
    await waitFor(
      () => {
        expect(screen.getByText("com.amazonaws.us-east-1.s3")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("loads network ACLs when tab clicked", async () => {
    mockSend.mockResolvedValueOnce({ Reservations: [] });
    mockSend.mockResolvedValueOnce({
      NetworkAcls: [{ NetworkAclId: "acl-123", VpcId: "vpc-abc", IsDefault: true }],
    });
    render(EC2Page);
    await waitFor(() => screen.getByText("No EC2 instances found"), { timeout: 3000 });
    await fireEvent.click(screen.getByRole("button", { name: /Network ACLs/i }));
    await waitFor(
      () => {
        expect(screen.getByText("acl-123")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  describe("All regions mode", () => {
    it("fans DescribeInstances out across every region with data and tags each row", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
      mockSend.mockResolvedValueOnce({
        Reservations: [
          {
            Instances: [
              {
                ...mockInstance,
                InstanceId: "i-eu-1",
                Tags: [{ Key: "Name", Value: "eu-server" }],
              },
            ],
          },
        ],
      });

      render(EC2Page);

      await waitFor(() => expect(screen.getByText("my-server")).toBeInTheDocument());
      expect(screen.getByText("eu-server")).toBeInTheDocument();
      expect(mockSend).toHaveBeenCalledTimes(2);

      vi.unstubAllGlobals();
    });

    it("issues exactly one DescribeInstances call in single-region mode", async () => {
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
      render(EC2Page);
      await waitFor(() => expect(screen.getByText("my-server")).toBeInTheDocument());
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("renders the same instance id from two different regions as two distinct rows, each tagged with its own region", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });

      render(EC2Page);

      const rows = await waitFor(() => {
        const found = screen.getAllByText("my-server");
        expect(found).toHaveLength(2);
        return found;
      });
      const chips = rows.map(
        (r) =>
          within(r.closest(".rounded-lg") as HTMLElement).getByTestId("region-chip").textContent,
      );
      expect(chips.toSorted()).toEqual(["eu-west-1", "us-east-1"]);

      vi.unstubAllGlobals();
    });

    it("stops the row's own region, not the picker's, when two regions share an instance id", async () => {
      setStoredRegion(ALL_REGIONS);
      stubRegionsWithData(["us-east-1", "eu-west-1"]);
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });

      render(EC2Page);
      await waitFor(() => expect(screen.getAllByText("my-server")).toHaveLength(2));

      mockSend.mockResolvedValueOnce({});
      mockSend.mockResolvedValueOnce({ Reservations: [{ Instances: [mockInstance] }] });
      mockSend.mockResolvedValueOnce({
        Reservations: [{ Instances: [{ ...mockInstance, State: { Name: "stopping" } }] }],
      });

      const rows = screen.getAllByText("my-server");
      const euRow = rows
        .map((r) => r.closest(".rounded-lg") as HTMLElement)
        .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
      await fireEvent.click(within(euRow).getByRole("button", { name: /stop/i }));

      await waitFor(() => {
        const remaining = screen.getAllByText("my-server");
        const stillEu = remaining
          .map((r) => r.closest(".rounded-lg") as HTMLElement)
          .find((r) => within(r).getByTestId("region-chip").textContent === "eu-west-1")!;
        expect(within(stillEu).getByText("stopping")).toBeInTheDocument();
      });

      vi.unstubAllGlobals();
    });
  });
});
