import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/svelte";
import ServiceDiscoveryPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getServiceDiscoveryClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

const defaultMockResponse = {
  Namespaces: [],
  Services: [],
  Operations: [],
  Instances: [],
};

describe("Service Discovery Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockSend.mockResolvedValue(defaultMockResponse);
  });

  it("renders page title", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getAllByText("AWS Cloud Map")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getAllByText("Namespaces")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no namespaces", async () => {
    render(ServiceDiscoveryPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no namespaces/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded namespaces", async () => {
    mockSend.mockResolvedValue({
      Namespaces: [{ Name: "my-namespace", Id: "ns-123", Type: "DNS_PRIVATE", ServiceCount: 3 }],
      Services: [],
      Operations: [],
    });
    render(ServiceDiscoveryPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("my-namespace")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Services tab", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getAllByText("Services")[0]).toBeInTheDocument();
  });

  it("shows DNS Namespaces stat", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getAllByText("DNS Namespaces")[0]).toBeInTheDocument();
  });

  it("shows Instances and Operations tabs", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getAllByText("Instances")[0]).toBeInTheDocument();
    expect(screen.getAllByText("Operations")[0]).toBeInTheDocument();
  });

  it("shows Operations stat card", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getAllByText("Operations")[0]).toBeInTheDocument();
  });

  it("shows operations from initial load", async () => {
    mockSend.mockResolvedValue({
      Namespaces: [],
      Services: [],
      Operations: [{ Id: "op-abc123", Status: "SUCCESS" }],
    });
    render(ServiceDiscoveryPage);
    // Click Operations tab
    const opsTabs = await screen.findAllByText("Operations");
    fireEvent.click(opsTabs.at(-1)!);
    await waitFor(
      () => {
        expect(screen.getByText("op-abc123")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Create Namespace button on namespaces tab", () => {
    render(ServiceDiscoveryPage);
    expect(screen.getByText("Create Namespace")).toBeInTheDocument();
  });

  it("opens create namespace modal on button click", async () => {
    render(ServiceDiscoveryPage);
    fireEvent.click(screen.getByText("Create Namespace"));
    await waitFor(
      () => {
        expect(
          screen.getByLabelText(/create namespace/i) ?? screen.getByRole("dialog"),
        ).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Create Service button on services tab", async () => {
    render(ServiceDiscoveryPage);
    const serviceTabs = screen.getAllByText("Services");
    fireEvent.click(serviceTabs.at(-1)!);
    await waitFor(
      () => {
        expect(screen.getByText("Create Service")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Register Instance button on instances tab", async () => {
    render(ServiceDiscoveryPage);
    const instancesTabs = screen.getAllByText("Instances");
    fireEvent.click(instancesTabs.at(-1)!);
    await waitFor(
      () => {
        expect(screen.getByText("Register Instance")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("instances tab shows service selector empty state", async () => {
    render(ServiceDiscoveryPage);
    const instancesTabs = screen.getAllByText("Instances");
    fireEvent.click(instancesTabs.at(-1)!);
    await waitFor(
      () => {
        expect(screen.getByText(/select a service to view its instances/i)).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("opens register instance modal", async () => {
    render(ServiceDiscoveryPage);
    const instancesTabs = screen.getAllByText("Instances");
    fireEvent.click(instancesTabs.at(-1)!);
    await waitFor(() => screen.getByText("Register Instance"), { timeout: 3000 });
    fireEvent.click(screen.getByText("Register Instance"));
    await waitFor(
      () => {
        expect(
          screen.getByText("Register Instance", { selector: "h3" }) ?? screen.getByRole("dialog"),
        ).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
