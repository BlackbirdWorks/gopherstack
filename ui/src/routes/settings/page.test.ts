import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import SettingsPage from "./+page.svelte";

vi.mock("svelte-sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warning: vi.fn(),
  },
}));

const mockLocalStorage = (() => {
  let store: Record<string, string> = {};

  return {
    getItem(key: string) {
      return store[key] ?? null;
    },
    setItem(key: string, value: string) {
      store[key] = String(value);
    },
    removeItem(key: string) {
      delete store[key];
    },
    clear() {
      store = {};
    },
  };
})();

Object.defineProperty(window, "localStorage", { value: mockLocalStorage });

const defaultServerSettings = {
  accountID: "000000000000",
  region: "us-east-1",
  latencyMs: 0,
  enforceIAM: false,
  autoPurgeTTL: "0s",
  portRangeStart: 5000,
  portRangeEnd: 10000,
  initScriptTimeout: "30s",
  persist: false,
  demo: false,
};

function mockFetch(
  options: { settingsOk?: boolean; settingsStatus?: number; settingsBody?: unknown } = {},
) {
  const { settingsOk = true, settingsStatus = 200, settingsBody = defaultServerSettings } = options;

  return vi.fn((input: RequestInfo | URL) => {
    const url = String(input);

    if (url.includes("/_gopherstack/health")) {
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ version: "dev", services: [] }),
      });
    }

    if (url.includes("/dashboard/api/system/settings")) {
      return Promise.resolve({
        ok: settingsOk,
        status: settingsStatus,
        json: () => Promise.resolve(settingsBody),
      });
    }

    return Promise.resolve({ ok: false, status: 404, json: () => Promise.resolve({}) });
  });
}

describe("Settings Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.localStorage.clear();
  });

  it("renders global settings as read-only, server-sourced values", async () => {
    vi.stubGlobal("fetch", mockFetch());
    render(SettingsPage);

    expect(screen.getByText("Settings")).toBeInTheDocument();
    expect(screen.getByText("Global Settings")).toBeInTheDocument();

    const accountID = (await screen.findByLabelText("Account ID")) as HTMLInputElement;
    const region = screen.getByLabelText("Default Region") as HTMLInputElement;
    const iam = screen.getByLabelText("IAM Execution") as HTMLInputElement;

    expect(accountID.value).toBe("000000000000");
    expect(accountID).toBeDisabled();
    expect(region.value).toBe("us-east-1");
    expect(region).toBeDisabled();
    expect(iam).toBeDisabled();

    expect(screen.getByRole("button", { name: "Save Preferences" })).toBeDisabled();
  });

  it("shows a clear message when the settings endpoint is unreachable", async () => {
    vi.stubGlobal("fetch", mockFetch({ settingsOk: false, settingsStatus: 404 }));
    render(SettingsPage);

    await waitFor(() => {
      expect(screen.getByText(/Could not reach/)).toBeInTheDocument();
    });
    expect(screen.queryByLabelText("Account ID")).not.toBeInTheDocument();
  });

  it("shows an unavailable message when the server has no config manager wired up", async () => {
    vi.stubGlobal("fetch", mockFetch({ settingsBody: {} }));
    render(SettingsPage);

    await waitFor(() => {
      expect(screen.getByText(/no configuration manager wired up/)).toBeInTheDocument();
    });
    expect(screen.queryByLabelText("Account ID")).not.toBeInTheDocument();
  });

  it("switches to service settings tab and shows service controls", async () => {
    vi.stubGlobal("fetch", mockFetch());
    render(SettingsPage);

    const serviceTab = screen.getByRole("button", { name: "Service Settings" });
    await fireEvent.click(serviceTab);

    expect(screen.getByText("Dashboard Service Settings")).toBeInTheDocument();
    expect(screen.getByLabelText("Auto-refresh Service Tables")).toBeInTheDocument();
    expect(screen.getByLabelText("Refresh Interval (seconds)")).toBeInTheDocument();
    expect(screen.getByLabelText("Max Console Entries Limit")).toBeInTheDocument();
  });

  it("updates and persists browser-local preferences on save", async () => {
    vi.stubGlobal("fetch", mockFetch());
    render(SettingsPage);

    const serviceTab = screen.getByRole("button", { name: "Service Settings" });
    await fireEvent.click(serviceTab);

    const refreshInterval = screen.getByLabelText("Refresh Interval (seconds)") as HTMLInputElement;
    await fireEvent.input(refreshInterval, { target: { value: "42" } });

    const saveButton = screen.getByRole("button", { name: "Save Preferences" });
    expect(saveButton).toBeEnabled();
    await fireEvent.click(saveButton);

    const raw = window.localStorage.getItem("gopherstack_settings");
    expect(raw).toBeTruthy();
    const parsed = JSON.parse(raw!);
    expect(parsed.refreshInterval).toBe(42);
    // Server-owned fields must never be written to localStorage.
    expect(parsed.accountID).toBeUndefined();
    expect(parsed.region).toBeUndefined();

    const { toast } = await import("svelte-sonner");
    expect(toast.success).toHaveBeenCalledWith("Preferences updated successfully");
  });

  it("resets local preferences to defaults when reset is clicked", async () => {
    vi.stubGlobal("fetch", mockFetch());
    render(SettingsPage);

    const serviceTab = screen.getByRole("button", { name: "Service Settings" });
    await fireEvent.click(serviceTab);

    const refreshInterval = screen.getByLabelText("Refresh Interval (seconds)") as HTMLInputElement;
    await fireEvent.input(refreshInterval, { target: { value: "42" } });

    const resetButton = screen.getByRole("button", { name: "Reset Defaults" });
    await fireEvent.click(resetButton);

    expect((screen.getByLabelText("Refresh Interval (seconds)") as HTMLInputElement).value).toBe(
      "5",
    );
  });
});
