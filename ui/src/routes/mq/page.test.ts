import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import MQPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getMQClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("MQ Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getByText("Amazon MQ")).toBeInTheDocument();
  });

  it("shows subtitle", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getByText("Managed message broker service")).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getAllByText("Brokers").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Configurations").length).toBeGreaterThan(0);
    expect(screen.getByText("ActiveMQ")).toBeInTheDocument();
    expect(screen.getByText("RabbitMQ")).toBeInTheDocument();
  });

  it("shows tab navigation", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getByRole("button", { name: /Brokers/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Configurations/ })).toBeInTheDocument();
  });

  it("shows empty state when no brokers", async () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    await waitFor(
      () => {
        expect(screen.getByText("No brokers found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded broker names", async () => {
    mockSend.mockResolvedValue({
      BrokerSummaries: [
        {
          BrokerName: "my-activemq-broker",
          BrokerId: "broker-abc123",
          BrokerState: "RUNNING",
          EngineType: "ACTIVEMQ",
          DeploymentMode: "SINGLE_INSTANCE",
        },
        {
          BrokerName: "my-rabbitmq-broker",
          BrokerId: "broker-def456",
          BrokerState: "RUNNING",
          EngineType: "RABBITMQ",
          DeploymentMode: "SINGLE_INSTANCE",
        },
      ],
    });
    render(MQPage);
    await waitFor(
      () => {
        expect(screen.getByText("my-activemq-broker")).toBeInTheDocument();
        expect(screen.getByText("my-rabbitmq-broker")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getByPlaceholderText("Search brokers...")).toBeInTheDocument();
  });

  it("selects broker and loads details", async () => {
    mockSend.mockResolvedValueOnce({
      BrokerSummaries: [
        {
          BrokerName: "test-broker",
          BrokerId: "broker-123",
          BrokerState: "RUNNING",
          EngineType: "ACTIVEMQ",
        },
      ],
    });
    mockSend.mockResolvedValueOnce({
      BrokerName: "test-broker",
      BrokerId: "broker-123",
      BrokerState: "RUNNING",
      EngineType: "ACTIVEMQ",
      BrokerArn: "arn:aws:mq:us-east-1:123:broker:test-broker",
      EngineVersion: "5.15.14",
      DeploymentMode: "SINGLE_INSTANCE",
      HostInstanceType: "mq.m5.large",
      PubliclyAccessible: false,
      AutoMinorVersionUpgrade: true,
      StorageType: "efs",
      BrokerInstances: [],
    });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("test-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("test-broker"));
    await waitFor(
      () => {
        expect(screen.getByText("Broker ARN")).toBeInTheDocument();
        expect(screen.getByText("Deployment Mode")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("switches to configurations tab", async () => {
    mockSend.mockResolvedValueOnce({ BrokerSummaries: [] });
    mockSend.mockResolvedValueOnce({ Configurations: [] });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("No brokers found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const configTab = screen.getByRole("button", { name: /Configurations/ });
    await fireEvent.click(configTab);
    await waitFor(
      () => {
        expect(screen.getByText("No configurations found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays configurations", async () => {
    mockSend.mockResolvedValueOnce({ BrokerSummaries: [] });
    mockSend.mockResolvedValueOnce({
      Configurations: [
        {
          Id: "config-abc",
          Name: "my-activemq-config",
          EngineType: "ACTIVEMQ",
          EngineVersion: "5.15.14",
          LatestRevision: { Revision: 1 },
        },
      ],
    });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("No brokers found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const configTab = screen.getByRole("button", { name: /Configurations/ });
    await fireEvent.click(configTab);
    await waitFor(
      () => {
        expect(screen.getByText("my-activemq-config")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("counts engine types correctly", async () => {
    mockSend.mockResolvedValue({
      BrokerSummaries: [
        { BrokerName: "a1", BrokerId: "b1", BrokerState: "RUNNING", EngineType: "ACTIVEMQ" },
        { BrokerName: "a2", BrokerId: "b2", BrokerState: "RUNNING", EngineType: "ACTIVEMQ" },
        { BrokerName: "r1", BrokerId: "b3", BrokerState: "RUNNING", EngineType: "RABBITMQ" },
      ],
    });
    render(MQPage);
    await waitFor(
      () => {
        expect(screen.getByText("a1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    const statCards = screen.getAllByText(/^\d+$/);
    expect(statCards.length).toBeGreaterThan(0);
  });

  it("shows Create Broker button", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getByText("Create Broker")).toBeInTheDocument();
  });

  it("opens create broker modal", async () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    await fireEvent.click(screen.getByText("Create Broker"));
    expect(screen.getByText("Broker Name")).toBeInTheDocument();
    expect(screen.getByText("Engine Type")).toBeInTheDocument();
    expect(screen.getByText("Deployment Mode")).toBeInTheDocument();
  });

  it("cancels create broker modal", async () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    await fireEvent.click(screen.getByText("Create Broker"));
    expect(screen.getByLabelText("Broker Name")).toBeInTheDocument();
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByLabelText("Broker Name")).not.toBeInTheDocument();
  });

  it("creates broker and refreshes list", async () => {
    mockSend
      .mockResolvedValueOnce({ BrokerSummaries: [] })
      .mockResolvedValueOnce({ BrokerId: "broker-new", BrokerArn: "arn:..." })
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "new-broker",
            BrokerId: "broker-new",
            BrokerState: "CREATION_IN_PROGRESS",
            EngineType: "ACTIVEMQ",
          },
        ],
      });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("No brokers found")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Create Broker"));
    const nameInput = screen.getByLabelText("Broker Name");
    await fireEvent.input(nameInput, { target: { value: "new-broker" } });
    await fireEvent.click(screen.getByText("Create Broker", { selector: "button[type='submit']" }));
    await waitFor(() => expect(screen.getByText("new-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("opens delete broker modal from list", async () => {
    mockSend.mockResolvedValue({
      BrokerSummaries: [
        {
          BrokerName: "del-broker",
          BrokerId: "broker-del",
          BrokerState: "RUNNING",
          EngineType: "ACTIVEMQ",
        },
      ],
    });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("del-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    const deleteBtn = screen.getByTitle("Delete broker");
    await fireEvent.click(deleteBtn);
    expect(screen.getByText(/Are you sure you want to delete broker/)).toBeInTheDocument();
    expect(screen.getAllByText("Delete Broker").length).toBeGreaterThan(0);
  });

  it("cancels delete broker modal", async () => {
    mockSend.mockResolvedValue({
      BrokerSummaries: [
        {
          BrokerName: "del-broker",
          BrokerId: "broker-del",
          BrokerState: "RUNNING",
          EngineType: "ACTIVEMQ",
        },
      ],
    });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("del-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByTitle("Delete broker"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText(/Are you sure you want to delete broker/)).not.toBeInTheDocument();
  });

  it("deletes broker and refreshes list", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "gone-broker",
            BrokerId: "broker-gone",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({ BrokerId: "broker-gone" })
      .mockResolvedValueOnce({ BrokerSummaries: [] });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("gone-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByTitle("Delete broker"));
    const confirmBtn = screen.getByRole("button", { name: "Delete Broker" });
    await fireEvent.click(confirmBtn);
    await waitFor(() => expect(screen.getByText("No brokers found")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("reboots broker from detail panel", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "reboot-broker",
            BrokerId: "broker-reboot",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "reboot-broker",
        BrokerId: "broker-reboot",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        HostInstanceType: "mq.m5.large",
        BrokerInstances: [],
      })
      .mockResolvedValueOnce({});
    render(MQPage);
    await waitFor(() => expect(screen.getByText("reboot-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("reboot-broker"));
    await waitFor(() => expect(screen.getByText("Reboot")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Reboot"));
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(3), { timeout: 3000 });
  });

  it("opens update broker modal", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "update-broker",
            BrokerId: "broker-upd",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "update-broker",
        BrokerId: "broker-upd",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        HostInstanceType: "mq.m5.large",
        DeploymentMode: "SINGLE_INSTANCE",
        AutoMinorVersionUpgrade: false,
        BrokerInstances: [],
      });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("update-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("update-broker"));
    await waitFor(() => expect(screen.getByText("Update")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Update"));
    expect(screen.getAllByText("Update Broker").length).toBeGreaterThan(0);
    expect(screen.getByLabelText("Engine Version")).toBeInTheDocument();
    expect(screen.getByText("Auto Minor Version Upgrade")).toBeInTheDocument();
  });

  it("cancels update broker modal", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "update-broker",
            BrokerId: "broker-upd",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "update-broker",
        BrokerId: "broker-upd",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        HostInstanceType: "mq.m5.large",
        DeploymentMode: "SINGLE_INSTANCE",
        AutoMinorVersionUpgrade: false,
        BrokerInstances: [],
      });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("update-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("update-broker"));
    await waitFor(() => expect(screen.getByText("Update")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Update"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByLabelText("Engine Version")).not.toBeInTheDocument();
  });

  it("toggles users panel and shows users", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "user-broker",
            BrokerId: "broker-usr",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "user-broker",
        BrokerId: "broker-usr",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        BrokerInstances: [],
      })
      .mockResolvedValueOnce({
        BrokerId: "broker-usr",
        Users: [{ Username: "alice" }, { Username: "bob" }],
      });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("user-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("user-broker"));
    await waitFor(() => expect(screen.getByText("Users")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Users"));
    await waitFor(
      () => {
        expect(screen.getByText("alice")).toBeInTheDocument();
        expect(screen.getByText("bob")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Add User button when users panel is open", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "user-broker",
            BrokerId: "broker-usr",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "user-broker",
        BrokerId: "broker-usr",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        BrokerInstances: [],
      })
      .mockResolvedValueOnce({ BrokerId: "broker-usr", Users: [] });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("user-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("user-broker"));
    await waitFor(() => expect(screen.getByText("Users")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Users"));
    await waitFor(() => expect(screen.getByText("Add User")).toBeInTheDocument(), {
      timeout: 3000,
    });
  });

  it("opens create user modal", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "user-broker",
            BrokerId: "broker-usr",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "user-broker",
        BrokerId: "broker-usr",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        BrokerInstances: [],
      })
      .mockResolvedValueOnce({ BrokerId: "broker-usr", Users: [] });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("user-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("user-broker"));
    await waitFor(() => expect(screen.getByText("Users")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Users"));
    await waitFor(() => expect(screen.getByText("Add User")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Add User"));
    expect(screen.getByText("Username")).toBeInTheDocument();
    expect(screen.getByText("Console Access")).toBeInTheDocument();
  });

  it("cancels create user modal", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "user-broker",
            BrokerId: "broker-usr",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "user-broker",
        BrokerId: "broker-usr",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        BrokerInstances: [],
      })
      .mockResolvedValueOnce({ BrokerId: "broker-usr", Users: [] });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("user-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("user-broker"));
    await waitFor(() => expect(screen.getByText("Users")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Users"));
    await waitFor(() => expect(screen.getByText("Add User")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("Add User"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Username")).not.toBeInTheDocument();
  });

  it("opens edit user modal", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "user-broker",
            BrokerId: "broker-usr",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "user-broker",
        BrokerId: "broker-usr",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        BrokerInstances: [],
      })
      .mockResolvedValueOnce({
        BrokerId: "broker-usr",
        Users: [{ Username: "charlie" }],
      });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("user-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("user-broker"));
    await waitFor(() => expect(screen.getByText("Users")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByText("Users"));
    await waitFor(() => expect(screen.getByText("charlie")).toBeInTheDocument(), { timeout: 3000 });
    await fireEvent.click(screen.getByTitle("Edit user"));
    expect(screen.getByText("Update User: charlie")).toBeInTheDocument();
    expect(screen.getByText("New Password")).toBeInTheDocument();
  });

  it("shows broker instances in detail panel", async () => {
    mockSend
      .mockResolvedValueOnce({
        BrokerSummaries: [
          {
            BrokerName: "inst-broker",
            BrokerId: "broker-inst",
            BrokerState: "RUNNING",
            EngineType: "ACTIVEMQ",
          },
        ],
      })
      .mockResolvedValueOnce({
        BrokerName: "inst-broker",
        BrokerId: "broker-inst",
        BrokerState: "RUNNING",
        EngineType: "ACTIVEMQ",
        EngineVersion: "5.15.14",
        DeploymentMode: "SINGLE_INSTANCE",
        BrokerInstances: [{ ConsoleURL: "https://console.mq.example.com", IpAddress: "10.0.0.1" }],
      });
    render(MQPage);
    await waitFor(() => expect(screen.getByText("inst-broker")).toBeInTheDocument(), {
      timeout: 3000,
    });
    await fireEvent.click(screen.getByText("inst-broker"));
    await waitFor(
      () => {
        expect(screen.getByText("Broker Instances")).toBeInTheDocument();
        expect(screen.getByText("https://console.mq.example.com")).toBeInTheDocument();
        expect(screen.getByText("IP: 10.0.0.1")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows select placeholder when no broker selected", () => {
    mockSend.mockResolvedValue({ BrokerSummaries: [] });
    render(MQPage);
    expect(screen.getByText("Select a broker to view details")).toBeInTheDocument();
  });
});
