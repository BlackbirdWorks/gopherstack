import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/svelte";
import STSPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSTSClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

function commandName(command: unknown): string {
  return (command as { constructor: { name: string } }).constructor.name;
}

const IDENTITY_RESPONSE = {
  Account: "123456789012",
  Arn: "arn:aws:iam::123456789012:user/test",
  UserId: "AIDAI12345",
};

const METRICS_RESPONSE = {
  activeSessions: 2,
  expiredSessions: 1,
  sweepCount: 3,
  expiredEvictions: 4,
  totalSessionsCreated: 7,
  opsAssumeRole: 1,
};

describe("STS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    mockSend.mockImplementation((command: unknown) => {
      if (commandName(command) === "GetCallerIdentityCommand") {
        return Promise.resolve(IDENTITY_RESPONSE);
      }
      return Promise.resolve({});
    });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(METRICS_RESPONSE),
      }),
    );
  });

  it("renders page title", () => {
    render(STSPage);
    expect(screen.getByText("AWS Security Token Service")).toBeInTheDocument();
  });

  it("shows all four tabs", () => {
    render(STSPage);
    expect(screen.getByRole("tab", { name: "Identity" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Assume Role" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Get Token" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Utilities" })).toBeInTheDocument();
  });

  it("loads caller identity on the Identity tab by default", async () => {
    render(STSPage);
    await waitFor(() => {
      expect(screen.getByText("123456789012")).toBeInTheDocument();
    });
    expect(screen.getByText("Account")).toBeInTheDocument();
    expect(screen.getByText("ARN")).toBeInTheDocument();
    expect(screen.getByText("User ID")).toBeInTheDocument();
  });

  it("loads session metrics from the dashboard endpoint", async () => {
    render(STSPage);
    await waitFor(() => {
      expect(screen.getByText("Active sessions")).toBeInTheDocument();
    });
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(screen.getByText("Total created")).toBeInTheDocument();
  });

  it("shows an inline error banner with err.name, HTTP status, and message on a failed identity load", async () => {
    mockSend.mockImplementation(() =>
      Promise.reject(
        Object.assign(new Error("Rate exceeded."), {
          name: "ThrottlingException",
          $metadata: { httpStatusCode: 429 },
        }),
      ),
    );

    render(STSPage);
    await waitFor(() => {
      expect(screen.getByText("Failed to load data")).toBeInTheDocument();
      expect(
        screen.getByText("ThrottlingException (HTTP 429): Rate exceeded."),
      ).toBeInTheDocument();
    });
  });

  it("assumes a role, renders the returned credentials, and records it in the session history table", async () => {
    mockSend.mockImplementation((command: unknown) => {
      const name = commandName(command);
      if (name === "GetCallerIdentityCommand") return Promise.resolve(IDENTITY_RESPONSE);
      if (name === "AssumeRoleCommand") {
        return Promise.resolve({
          Credentials: {
            AccessKeyId: "ASIAEXAMPLEASSUMEROLE",
            SecretAccessKey: "secret",
            SessionToken: "token-abcdefghijklmnopqrstuvwxyz",
            Expiration: new Date(Date.now() + 3600_000),
          },
          AssumedRoleUser: {
            AssumedRoleId: "AROAEXAMPLE:my-session",
            Arn: "arn:aws:sts::123456789012:assumed-role/MyRole/my-session",
          },
        });
      }
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Assume Role" }));
    await fireEvent.click(screen.getByRole("button", { name: "Assume Role" }));

    // The access key shows up twice once issued: once in the credential
    // result card, once in the session-history row below — assert on the
    // unique AssumedRoleId (from the factsList snippet) to synchronize.
    await waitFor(() => {
      expect(screen.getByText("AROAEXAMPLE:my-session")).toBeInTheDocument();
    });
    expect(screen.getAllByText("ASIAEXAMPLEASSUMEROLE").length).toBeGreaterThan(0);

    // The session-history table (defineColumns + render snippets) at the
    // bottom of the page should now show one row for this call, with the
    // Operation cell rendered through histOpCell (a real {#snippet}, not a
    // plain function passed as `render`).
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "AssumeRole" })).toBeInTheDocument();
    });
    expect(screen.getByRole("cell", { name: "ASIAEXAMPLEASSUMEROLE" })).toBeInTheDocument();
  });

  it("generates a session token via GetSessionToken on the Get Token tab", async () => {
    mockSend.mockImplementation((command: unknown) => {
      const name = commandName(command);
      if (name === "GetCallerIdentityCommand") return Promise.resolve(IDENTITY_RESPONSE);
      if (name === "GetSessionTokenCommand") {
        return Promise.resolve({
          Credentials: {
            AccessKeyId: "ASIAEXAMPLESESSIONTOKEN",
            SecretAccessKey: "secret",
            SessionToken: "token-zyxwvutsrqponmlkjihgfedcba",
            Expiration: new Date(Date.now() + 3600_000),
          },
        });
      }
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Get Token" }));
    await fireEvent.click(screen.getByRole("button", { name: "Generate Session Token" }));

    // Appears both in the credential result card and the session-history
    // row below, so assert on the cell specifically.
    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "ASIAEXAMPLESESSIONTOKEN" })).toBeInTheDocument();
    });
    expect(screen.getAllByText("ASIAEXAMPLESESSIONTOKEN").length).toBeGreaterThan(0);
  });

  it("issues a GetWebIdentityToken JWT and renders it without adding it to the credentials history", async () => {
    mockSend.mockImplementation((command: unknown) => {
      const name = commandName(command);
      if (name === "GetCallerIdentityCommand") return Promise.resolve(IDENTITY_RESPONSE);
      if (name === "GetWebIdentityTokenCommand") {
        return Promise.resolve({
          WebIdentityToken: "header.payload.signature",
          Expiration: new Date(Date.now() + 300_000),
        });
      }
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Get Token" }));
    await fireEvent.click(screen.getByRole("button", { name: "Get Web Identity Token" }));

    await waitFor(() => {
      expect(screen.getByText("header.payload.signature")).toBeInTheDocument();
    });
    // A JWT is not an AccessKeyId/SecretAccessKey/SessionToken triple, so it
    // must not appear as a row in the credentials-history table below.
    expect(screen.queryByRole("cell", { name: "GetWebIdentityToken" })).not.toBeInTheDocument();
  });

  it("looks up an access key via GetAccessKeyInfo on the Utilities tab", async () => {
    mockSend.mockImplementation((command: unknown) => {
      const name = commandName(command);
      if (name === "GetCallerIdentityCommand") return Promise.resolve(IDENTITY_RESPONSE);
      if (name === "GetAccessKeyInfoCommand") return Promise.resolve({ Account: "123456789012" });
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Utilities" }));
    await fireEvent.input(screen.getByPlaceholderText("ASIA..."), {
      target: { value: "ASIAEXAMPLEACCESSKEY" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Look Up" }));

    await waitFor(() => {
      expect(screen.getByText(/Access key belongs to account/)).toBeInTheDocument();
    });
  });

  it("decodes an authorization message on the Utilities tab", async () => {
    mockSend.mockImplementation((command: unknown) => {
      const name = commandName(command);
      if (name === "GetCallerIdentityCommand") return Promise.resolve(IDENTITY_RESPONSE);
      if (name === "DecodeAuthorizationMessageCommand") {
        return Promise.resolve({ DecodedMessage: '{"error":"denied"}' });
      }
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.click(screen.getByRole("tab", { name: "Utilities" }));
    await fireEvent.input(
      screen.getByPlaceholderText("Paste base64 encoded authorization message"),
      { target: { value: "ZXJyb3I=" } },
    );
    await fireEvent.click(screen.getByRole("button", { name: "Decode" }));

    await waitFor(() => {
      expect(screen.getByText(/"error":\s*"denied"/)).toBeInTheDocument();
    });
  });

  it("shows the empty session-history message before any credentials are issued", async () => {
    render(STSPage);
    await waitFor(() => {
      expect(screen.getByText("123456789012")).toBeInTheDocument();
    });
    expect(
      screen.getByText(
        "No credentials issued yet this session. Use Assume Role or Get Token above.",
      ),
    ).toBeInTheDocument();
  });
});
