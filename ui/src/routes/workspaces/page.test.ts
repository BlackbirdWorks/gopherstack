import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import WorkspacesPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getWorkSpacesClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

describe("WorkSpaces Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ Workspaces: [] });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    render(WorkspacesPage);

    expect(screen.getByText("WorkSpaces Desktop Grid")).toBeInTheDocument();
    expect(screen.getByText("Deploy Instance")).toBeInTheDocument();
  });

  it("displays loaded workspaces", async () => {
    mockSend.mockResolvedValueOnce({
      Workspaces: [
        { WorkspaceId: "ws-abc", UserName: "alice.smith", State: "AVAILABLE", BundleId: "wsb-1" },
        { WorkspaceId: "ws-def", UserName: "bob.jones", State: "AVAILABLE", BundleId: "wsb-1" },
      ],
    });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    render(WorkspacesPage);

    await waitFor(
      () => {
        expect(screen.getByText("alice.smith")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("bob.jones")).toBeInTheDocument();
  });

  it("filters workspaces via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Workspaces: [
        { WorkspaceId: "ws-1", UserName: "alice.smith", State: "AVAILABLE", BundleId: "wsb-1" },
        { WorkspaceId: "ws-2", UserName: "bob.jones", State: "AVAILABLE", BundleId: "wsb-1" },
        { WorkspaceId: "ws-3", UserName: "carol.white", State: "STOPPED", BundleId: "wsb-2" },
      ],
    });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    render(WorkspacesPage);

    await waitFor(
      () => {
        expect(screen.getByText("alice.smith")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search desktops...");
    await fireEvent.input(searchInput, { target: { value: "bob" } });

    await waitFor(() => {
      expect(screen.queryByText("alice.smith")).not.toBeInTheDocument();
    });
    expect(screen.getByText("bob.jones")).toBeInTheDocument();
    expect(screen.queryByText("carol.white")).not.toBeInTheDocument();
  });

  it("opens create modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ Workspaces: [] });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    render(WorkspacesPage);

    await fireEvent.click(screen.getByText("Deploy Instance"));

    expect(screen.getByText("Deploy Instance", { selector: "h3" })).toBeInTheDocument();
    expect(screen.getByPlaceholderText("e.g. bishop.a")).toBeInTheDocument();
  });

  it("closes create modal on abort click", async () => {
    mockSend.mockResolvedValueOnce({ Workspaces: [] });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    render(WorkspacesPage);

    await fireEvent.click(screen.getByText("Deploy Instance"));
    expect(screen.getByPlaceholderText("e.g. bishop.a")).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Abort"));

    await waitFor(() => {
      expect(screen.queryByPlaceholderText("e.g. bishop.a")).not.toBeInTheDocument();
    });
  });

  it("creates a workspace via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ Workspaces: [] });
    mockSend.mockResolvedValueOnce({
      Bundles: [{ BundleId: "wsb-std", Name: "Standard", ComputeType: { Name: "STANDARD" } }],
    });
    // After create: reload
    mockSend.mockResolvedValueOnce({
      PendingRequests: [{ WorkspaceId: "ws-new" }],
      FailedRequests: [],
    });
    mockSend.mockResolvedValueOnce({
      Workspaces: [
        { WorkspaceId: "ws-new", UserName: "new.user", State: "PENDING", BundleId: "wsb-std" },
      ],
    });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    const { container } = render(WorkspacesPage);

    await waitFor(
      () => {
        expect(screen.getByText("Deploy Instance")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("Deploy Instance"));

    const usernameInput = screen.getByPlaceholderText("e.g. bishop.a");
    await fireEvent.input(usernameInput, { target: { value: "new.user" } });

    // Select the bundle from the dropdown
    const bundleSelect = container.querySelector("select")!;
    await fireEvent.change(bundleSelect, { target: { value: "wsb-std" } });

    const form = usernameInput.closest("form")!;
    await fireEvent.submit(form);

    await waitFor(
      () => {
        return mockSend.mock.calls.length >= 3;
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.success)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no workspaces exist", async () => {
    mockSend.mockResolvedValueOnce({ Workspaces: [] });
    mockSend.mockResolvedValueOnce({ Bundles: [] });

    render(WorkspacesPage);

    await waitFor(
      () => {
        expect(screen.getByText("No provisioned managed desktops.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("no access"));

    render(WorkspacesPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
