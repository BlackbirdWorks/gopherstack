import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import AppConfigDataPage from "./+page.svelte";

// StartConfigurationSession / GetLatestConfiguration go through the typed
// AppConfigDataClient (mocked below, same as every other page's SDK client);
// the gopherstack-only admin endpoints (Sessions/Fixtures/Stats tabs) go
// through plain fetch(), so both mocks are needed.

const mockSend = vi.fn();

vi.mock("$lib/aws-client", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getAppConfigDataClient: () => ({ send: mockSend }),
}));

const confirmDestructive = vi.fn().mockResolvedValue(true);
vi.mock("$lib/confirm-dialog", () => ({
  confirmDestructive: (...args: unknown[]) => confirmDestructive(...args),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

type Route = {
  match: (url: string, method: string) => boolean;
  handle: (
    url: string,
    init?: RequestInit,
  ) => {
    ok: boolean;
    status: number;
    statusText?: string;
    headers?: Record<string, string>;
    json?: () => Promise<unknown>;
    text?: () => Promise<string>;
  };
};

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: "",
    headers,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  };
}

function installFetchMock(routes: Route[]) {
  vi.stubGlobal(
    "fetch",
    vi.fn((input: string | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      const route = routes.find((r) => r.match(url, method));
      const result = route ? route.handle(url, init) : jsonResponse(200, {});
      return {
        ok: result.ok,
        status: result.status,
        statusText: result.statusText ?? "",
        headers: {
          get: (name: string) => (result.headers ?? {})[name] ?? null,
        },
        json: result.json ?? (() => Promise.resolve({})),
        text: result.text ?? (() => Promise.resolve("")),
      };
    }),
  );
}

const EMPTY_STATS = {
  lastSweepAt: "0001-01-01T00:00:00Z",
  sessionTtl: "1h0m0s",
  janitorPeriod: "1h0m0s",
  sessionCount: 0,
  profileCount: 0,
  totalPollCount: 0,
  totalPollFailures: 0,
  configurationChangeCount: 0,
};

function defaultRoutes(overrides: Partial<Record<string, Route["handle"]>> = {}): Route[] {
  return [
    {
      match: (url, method) =>
        method === "GET" && url.includes("/dashboard/api/appconfigdata/stats"),
      handle: overrides.stats ?? (() => jsonResponse(200, EMPTY_STATS)),
    },
    {
      match: (url, method) =>
        method === "GET" && url.includes("/dashboard/api/appconfigdata/sessions"),
      handle: overrides.sessions ?? (() => jsonResponse(200, { sessions: [] })),
    },
    {
      match: (url, method) =>
        method === "GET" && url.includes("/dashboard/api/appconfigdata/profiles"),
      handle: overrides.profiles ?? (() => jsonResponse(200, { profiles: [] })),
    },
    {
      match: (url, method) =>
        method === "POST" && url.includes("/dashboard/api/appconfigdata/profiles"),
      handle:
        overrides.seedProfile ??
        (() => ({
          ok: true,
          status: 204,
          json: () => Promise.resolve({}),
          text: () => Promise.resolve(""),
        })),
    },
    {
      match: (url, method) =>
        method === "DELETE" && url.includes("/dashboard/api/appconfigdata/sessions/"),
      handle:
        overrides.endSession ??
        (() => ({
          ok: true,
          status: 204,
          json: () => Promise.resolve({}),
          text: () => Promise.resolve(""),
        })),
    },
    {
      match: (url, method) =>
        method === "DELETE" && url.includes("/dashboard/api/appconfigdata/profiles"),
      handle:
        overrides.deleteProfile ??
        (() => ({
          ok: true,
          status: 204,
          json: () => Promise.resolve({}),
          text: () => Promise.resolve(""),
        })),
    },
  ];
}

const DEFAULT_START_SESSION_RESULT = { InitialConfigurationToken: "initial-token-abc123" };
const DEFAULT_POLL_RESULT = {
  NextPollConfigurationToken: "next-token-def456",
  NextPollIntervalInSeconds: 30,
  ContentType: "application/json",
  VersionLabel: "v1",
  Configuration: new TextEncoder().encode('{"featureFlag":true}'),
};

async function fillSessionForm(
  app = "my-app",
  env = "prod",
  profile = "my-profile",
): Promise<void> {
  await fireEvent.input(screen.getByLabelText("Application *"), { target: { value: app } });
  await fireEvent.input(screen.getByLabelText("Environment *"), { target: { value: env } });
  await fireEvent.input(screen.getByLabelText("Configuration Profile *"), {
    target: { value: profile },
  });
}

describe("AppConfigData Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    confirmDestructive.mockReset();
    confirmDestructive.mockResolvedValue(true);
    try {
      localStorage.clear();
    } catch {
      // jsdom always has localStorage; ignore if not
    }
  });

  it("renders page title", () => {
    installFetchMock(defaultRoutes());
    render(AppConfigDataPage);
    expect(screen.getByText("AppConfig Data")).toBeInTheDocument();
  });

  it("shows all three tabs", () => {
    installFetchMock(defaultRoutes());
    render(AppConfigDataPage);
    expect(screen.getByRole("tab", { name: "Poll Console" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Sessions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Test Fixtures" })).toBeInTheDocument();
  });

  it("disables Start Session until application/environment/profile are filled", async () => {
    installFetchMock(defaultRoutes());
    render(AppConfigDataPage);
    const btn = screen.getByRole("button", { name: /Start Session/ });
    expect(btn).toBeDisabled();
    await fillSessionForm();
    expect(btn).not.toBeDisabled();
  });

  it("starts a session and shows the returned token", async () => {
    installFetchMock(defaultRoutes());
    mockSend.mockResolvedValueOnce(DEFAULT_START_SESSION_RESULT);
    render(AppConfigDataPage);
    await fillSessionForm();
    await fireEvent.click(screen.getByRole("button", { name: /Start Session/ }));

    await waitFor(() => {
      expect(screen.getByText(/initial-…c123/)).toBeInTheDocument();
    });
    expect(screen.getByRole("button", { name: "Poll Now" })).toBeInTheDocument();
  });

  it("shows a no-active-deployment hint and links to Test Fixtures on a 404", async () => {
    installFetchMock(defaultRoutes());
    mockSend.mockRejectedValueOnce(
      Object.assign(
        new Error(
          "No deployment exists for the given application, environment, and configuration profile.",
        ),
        { name: "ResourceNotFoundException", $metadata: { httpStatusCode: 404 } },
      ),
    );
    render(AppConfigDataPage);
    await fillSessionForm();
    await fireEvent.click(screen.getByRole("button", { name: /Start Session/ }));

    await waitFor(() => {
      expect(
        screen.getByText(/No active deployment for this app\/env\/profile/),
      ).toBeInTheDocument();
    });
    // The inline error banner shows err.name and the HTTP status, not just a generic message.
    expect(screen.getByText(/ResourceNotFoundException \(HTTP 404\)/)).toBeInTheDocument();

    await fireEvent.click(
      screen.getByRole("button", { name: /Seed this profile in Test Fixtures/ }),
    );
    expect(screen.getByRole("tab", { name: "Test Fixtures" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
  });

  it("polls for the latest configuration, renders the content, and rotates the token", async () => {
    installFetchMock(defaultRoutes());
    mockSend.mockResolvedValueOnce(DEFAULT_START_SESSION_RESULT);
    mockSend.mockResolvedValueOnce(DEFAULT_POLL_RESULT);
    render(AppConfigDataPage);
    await fillSessionForm();
    await fireEvent.click(screen.getByRole("button", { name: /Start Session/ }));
    await waitFor(() => screen.getByRole("button", { name: "Poll Now" }));

    await fireEvent.click(screen.getByRole("button", { name: "Poll Now" }));

    await waitFor(() => {
      expect(screen.getByText(/"featureFlag": true/)).toBeInTheDocument();
    });
    expect(screen.getByText("Content updated")).toBeInTheDocument();
    expect(screen.getByText(/Version-Label:/)).toBeInTheDocument();
    // Token rotation: the poll log records the token BEFORE this poll (the
    // initial, single-use token from StartConfigurationSession) rotating to
    // the NEW one the response returned — the real single-use/rotate-on-poll
    // semantics, not just an echoed-back token. The pre-poll token now only
    // appears in the poll log (the "current token" display above has moved
    // on to the rotated token, which is why it shows up twice: once as the
    // live current token, once as this poll's tokenAfter).
    expect(screen.getByText(/initial-…c123/)).toBeInTheDocument();
    expect(screen.getAllByText(/next-tok…f456/).length).toBeGreaterThan(0);
  });

  it("shows an explicit unchanged message on an empty poll body", async () => {
    installFetchMock(defaultRoutes());
    mockSend.mockResolvedValueOnce(DEFAULT_START_SESSION_RESULT);
    mockSend.mockResolvedValueOnce({
      NextPollConfigurationToken: "next-token-def456",
      NextPollIntervalInSeconds: 30,
    });
    render(AppConfigDataPage);
    await fillSessionForm();
    await fireEvent.click(screen.getByRole("button", { name: /Start Session/ }));
    await waitFor(() => screen.getByRole("button", { name: "Poll Now" }));
    await fireEvent.click(screen.getByRole("button", { name: "Poll Now" }));

    await waitFor(() => {
      expect(screen.getByText("No change since last poll")).toBeInTheDocument();
    });
  });

  it("loads Sessions and renders the redacted token prefix (not a full token)", async () => {
    installFetchMock(
      defaultRoutes({
        sessions: () =>
          jsonResponse(200, {
            sessions: [
              {
                createdAt: "2024-01-01T00:00:00Z",
                lastAccessedAt: "2024-01-01T00:05:00Z",
                lastPollAt: "2024-01-01T00:05:00Z",
                expiresAt: "2024-01-02T00:00:00Z",
                tokenPrefix: "abcd1234…ef12",
                tokenFamilyId: "fam-1",
                applicationIdentifier: "my-app",
                environmentIdentifier: "prod",
                configurationProfileIdentifier: "my-profile",
                pollIntervalInSeconds: 30,
                pollCount: 4,
              },
            ],
          }),
      }),
    );
    render(AppConfigDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Sessions" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "abcd1234…ef12" })).toBeInTheDocument();
    });
    expect(screen.getByText("my-app")).toBeInTheDocument();
    expect(screen.getByText("4")).toBeInTheDocument();
  });

  it("shows an empty Sessions hint pointing at the Poll Console", async () => {
    installFetchMock(defaultRoutes());
    render(AppConfigDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Sessions" }));

    await waitFor(() => {
      expect(screen.getByText(/Start one from the Poll Console tab/)).toBeInTheDocument();
    });
  });

  it("loads Test Fixtures, selects one, and renders its content through the detail snippet", async () => {
    installFetchMock(
      defaultRoutes({
        profiles: () =>
          jsonResponse(200, {
            profiles: [
              {
                updatedAt: "2024-01-01T00:00:00Z",
                applicationIdentifier: "my-app",
                environmentIdentifier: "prod",
                configurationProfileIdentifier: "my-profile",
                content: '{"featureFlag":true}',
                contentType: "application/json",
                contentHash: "abcdef1234567890",
                versionLabel: "",
                deploymentId: "",
                history: [],
                versionNumber: 1,
              },
            ],
          }),
      }),
    );
    render(AppConfigDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Test Fixtures" }));

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: /my-app/ })).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("my-app"));

    await waitFor(() => {
      expect(screen.getByText(/"featureFlag": true/)).toBeInTheDocument();
    });
    // Evidence of the appconfig control-plane wiring gap (gopherstack-uiyi):
    // DeploymentId is always empty since services/appconfig never populates it.
    expect(screen.getByText(/never populated — gopherstack-uiyi/)).toBeInTheDocument();
  });

  it("seeds a new fixture with the admin endpoint", async () => {
    const seedProfile = vi.fn(() => ({
      ok: true,
      status: 204,
      json: () => Promise.resolve({}),
      text: () => Promise.resolve(""),
    }));
    installFetchMock(defaultRoutes({ seedProfile }));
    render(AppConfigDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Test Fixtures" }));
    await waitFor(() => screen.getByText("No fixtures seeded yet."));

    await fireEvent.input(screen.getByLabelText("Application"), { target: { value: "seed-app" } });
    await fireEvent.input(screen.getByLabelText("Environment"), { target: { value: "seed-env" } });
    await fireEvent.input(screen.getByLabelText("Profile ID"), {
      target: { value: "seed-profile" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Seed Fixture" }));

    await waitFor(() => {
      expect(seedProfile).toHaveBeenCalled();
    });
  });

  it("shows an inline error banner when the admin Sessions endpoint fails", async () => {
    // /dashboard/api/appconfigdata/sessions is gopherstack's own admin
    // endpoint, not an AWS operation — it does not return the AWS
    // __type/message error shape the real StartConfigurationSession /
    // GetLatestConfiguration calls do, just a bare HTTP status.
    installFetchMock(
      defaultRoutes({
        sessions: () => ({
          ok: false,
          status: 500,
          json: () => Promise.resolve({}),
          text: () => Promise.resolve(""),
        }),
      }),
    );
    render(AppConfigDataPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Sessions" }));

    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(screen.getByText(/status 500/)).toBeInTheDocument();
    });
  });
});
