import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CodeDeployPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCodeDeployClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("CodeDeploy Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ applications: [] });
    render(CodeDeployPage);
    expect(screen.getByText("AWS CodeDeploy")).toBeInTheDocument();
  });

  it("shows empty state when no applications", async () => {
    mockSend.mockResolvedValue({ applications: [] });
    render(CodeDeployPage);
    await waitFor(
      () => {
        expect(screen.getByText("No applications found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded applications", async () => {
    mockSend.mockResolvedValue({
      applications: ["web-app", "api-service", "worker"],
    });
    render(CodeDeployPage);
    await waitFor(
      () => {
        expect(screen.getByText("web-app")).toBeInTheDocument();
        expect(screen.getByText("api-service")).toBeInTheDocument();
        expect(screen.getByText("worker")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ applications: [] });
    render(CodeDeployPage);
    expect(screen.getByPlaceholderText("Search applications...")).toBeInTheDocument();
  });

  it("filters apps by search", async () => {
    mockSend.mockResolvedValue({ applications: ["web-app", "api-service"] });
    render(CodeDeployPage);
    await waitFor(() => screen.getByText("web-app"), { timeout: 3000 });
    const search = screen.getByPlaceholderText("Search applications...");
    await fireEvent.input(search, { target: { value: "api" } });
    expect(screen.getByText("api-service")).toBeInTheDocument();
  });

  it("navigates to app detail on click", async () => {
    mockSend.mockResolvedValue({ applications: ["my-app"] });
    render(CodeDeployPage);
    await waitFor(() => screen.getByText("my-app"), { timeout: 3000 });

    mockSend.mockResolvedValue({
      application: {
        applicationId: "abc123",
        applicationName: "my-app",
        computePlatform: "Server",
        createTime: new Date(),
      },
      deploymentGroups: ["production", "staging"],
    });

    await fireEvent.click(screen.getByText("my-app"));
    await waitFor(
      () => {
        expect(screen.getByText("Create Deployment")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Deployment Groups and Deployments tabs", async () => {
    mockSend.mockResolvedValue({ applications: ["my-app"] });
    render(CodeDeployPage);
    await waitFor(() => screen.getByText("my-app"), { timeout: 3000 });

    mockSend.mockResolvedValue({
      application: {
        applicationId: "abc",
        applicationName: "my-app",
        computePlatform: "Server",
        createTime: new Date(),
      },
      deploymentGroups: [],
    });

    await fireEvent.click(screen.getByText("my-app"));
    await waitFor(
      () => {
        expect(screen.getByText("Deployment Groups")).toBeInTheDocument();
        expect(screen.getByText("Deployments")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
