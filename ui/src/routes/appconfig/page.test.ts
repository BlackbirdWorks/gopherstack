import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import AppConfigPage from "./+page.svelte";
import { confirmDestructive } from "$lib/confirm-dialog";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAppConfigClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: vi.fn().mockResolvedValue(true),
}));

describe("AppConfig Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title and create button", () => {
    mockSend.mockResolvedValueOnce({ Items: [] });

    render(AppConfigPage);

    expect(screen.getByText("AppConfig Orchestration")).toBeInTheDocument();
    expect(screen.getByText("Assemble Application")).toBeInTheDocument();
  });

  it("displays loaded applications", async () => {
    mockSend.mockResolvedValueOnce({
      Items: [
        { Id: "app-1", Name: "my-service-config" },
        { Id: "app-2", Name: "payment-config" },
      ],
    });

    render(AppConfigPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-service-config")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
    expect(screen.getByText("payment-config")).toBeInTheDocument();
  });

  it("filters applications via search input", async () => {
    mockSend.mockResolvedValueOnce({
      Items: [
        { Id: "app-1", Name: "alpha-config" },
        { Id: "app-2", Name: "beta-config" },
        { Id: "app-3", Name: "gamma-config" },
      ],
    });

    render(AppConfigPage);

    await waitFor(
      () => {
        expect(screen.getByText("alpha-config")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    const searchInput = screen.getByPlaceholderText("Search applications...");
    await fireEvent.input(searchInput, { target: { value: "beta" } });

    await waitFor(() => {
      expect(screen.queryByText("alpha-config")).not.toBeInTheDocument();
    });
    expect(screen.getByText("beta-config")).toBeInTheDocument();
    expect(screen.queryByText("gamma-config")).not.toBeInTheDocument();
  });

  it("opens create modal when button is clicked", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });

    render(AppConfigPage);

    const createBtn = screen.getByText("Assemble Application");
    await fireEvent.click(createBtn);

    expect(screen.getByText("Assemble Application", { selector: "h3" })).toBeInTheDocument();
    expect(
      screen.getByPlaceholderText("e.g. gopherstack-microservices-config"),
    ).toBeInTheDocument();
  });

  it("closes create modal on abort button click", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });

    render(AppConfigPage);

    await fireEvent.click(screen.getByText("Assemble Application"));

    expect(
      screen.getByPlaceholderText("e.g. gopherstack-microservices-config"),
    ).toBeInTheDocument();

    await fireEvent.click(screen.getByText("Abort"));

    await waitFor(() => {
      expect(
        screen.queryByPlaceholderText("e.g. gopherstack-microservices-config"),
      ).not.toBeInTheDocument();
    });
  });

  it("creates an application via the modal form", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });
    mockSend.mockResolvedValueOnce({ Id: "app-new", Name: "new-config" });
    mockSend.mockResolvedValueOnce({ Items: [{ Id: "app-new", Name: "new-config" }] });

    render(AppConfigPage);

    await fireEvent.click(screen.getByText("Assemble Application"));

    const nameInput = screen.getByPlaceholderText("e.g. gopherstack-microservices-config");
    await fireEvent.input(nameInput, { target: { value: "new-config" } });

    const form = nameInput.closest("form")!;
    await fireEvent.submit(form);

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(3);
      },
      { timeout: 3000 },
    );

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalled();
  });

  it("selects an app and loads its details", async () => {
    mockSend.mockResolvedValueOnce({
      Items: [{ Id: "app-1", Name: "my-service-config" }],
    });
    // selectApp calls: ListEnvironments, ListConfigurationProfiles
    mockSend.mockResolvedValueOnce({
      Items: [{ Id: "env-1", Name: "production", State: "READY_FOR_DEPLOYMENT" }],
    });
    mockSend.mockResolvedValueOnce({
      Items: [{ Id: "prof-1", Name: "feature-flags", Type: "AWS.AppConfig.FeatureFlags" }],
    });
    // ListDeployments
    mockSend.mockResolvedValueOnce({ Items: [] });

    render(AppConfigPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-service-config")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByText("my-service-config"));

    await waitFor(
      () => {
        expect(screen.getByText("production")).toBeInTheDocument();
        expect(screen.getByText("feature-flags")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows error toast on load failure", async () => {
    mockSend.mockRejectedValueOnce(new Error("network failure"));

    render(AppConfigPage);

    const { toast } = await import("svelte-sonner");
    await waitFor(
      () => {
        expect(vi.mocked(toast.error)).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("shows empty state when no applications exist", async () => {
    mockSend.mockResolvedValueOnce({ Items: [] });

    render(AppConfigPage);

    await waitFor(
      () => {
        expect(screen.getByText("No applications detected.")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("deletes an application on confirm", async () => {
    mockSend.mockResolvedValueOnce({ Items: [{ Id: "app-1", Name: "my-config" }] });
    // selectApp calls (after clicking the app)
    // environments
    mockSend.mockResolvedValueOnce({ Items: [] });
    // profiles
    mockSend.mockResolvedValueOnce({ Items: [] });
    // delete + reload
    vi.mocked(confirmDestructive).mockResolvedValueOnce(true);
    // DeleteApplication
    mockSend.mockResolvedValueOnce({});
    // Reload
    mockSend.mockResolvedValueOnce({ Items: [] });

    render(AppConfigPage);

    await waitFor(
      () => {
        expect(screen.getByText("my-config")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    // First select the app to show the delete button
    await fireEvent.click(screen.getByText("my-config"));

    await waitFor(
      () => {
        expect(screen.getByTitle("Purge Application")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );

    await fireEvent.click(screen.getByTitle("Purge Application"));

    await waitFor(
      () => {
        expect(mockSend).toHaveBeenCalledTimes(5);
      },
      { timeout: 3000 },
    );
  });
});
