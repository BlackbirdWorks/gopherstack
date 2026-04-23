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

describe("STS Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: () => ({
          activeSessions: 2,
          expiredSessions: 1,
          sweepCount: 3,
          expiredEvictions: 4,
        }),
      }),
    );
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({
      Account: "123456789012",
      Arn: "arn:aws:iam::123:user/test",
      UserId: "AIDAI...",
    });
    render(STSPage);
    expect(screen.getByText("AWS Security Token Service")).toBeInTheDocument();
  });

  it("shows caller identity after load", async () => {
    mockSend.mockResolvedValue({
      Account: "123456789012",
      Arn: "arn:aws:iam::123:user/test",
      UserId: "AIDAI12345",
    });
    render(STSPage);
    await waitFor(
      () => {
        expect(screen.getByText("123456789012")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ Account: "123456789012" });
    render(STSPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Session Token section", () => {
    mockSend.mockResolvedValue({ Account: "123456789012" });
    render(STSPage);
    expect(screen.getByText("Session Token")).toBeInTheDocument();
  });

  it("shows generate token button", () => {
    mockSend.mockResolvedValue({ Account: "123456789012" });
    render(STSPage);
    expect(screen.getByText("Generate Session Token")).toBeInTheDocument();
  });

  it("renders validator and decode sections", () => {
    mockSend.mockResolvedValue({ Account: "123456789012" });
    render(STSPage);
    expect(screen.getByText("Session Token Validator")).toBeInTheDocument();
    expect(screen.getByText("Decode Authorization Message")).toBeInTheDocument();
    expect(screen.getByText("STS Janitor Metrics")).toBeInTheDocument();
  });

  it("validates access key info", async () => {
    mockSend.mockImplementation((command: { constructor: { name: string } }) => {
      if (command.constructor.name === "GetCallerIdentityCommand") {
        return Promise.resolve({
          Account: "123456789012",
          Arn: "arn:aws:iam::123:user/test",
          UserId: "AIDAI12345",
        });
      }
      if (command.constructor.name === "GetAccessKeyInfoCommand") {
        return Promise.resolve({ Account: "123456789012" });
      }
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.input(screen.getByPlaceholderText("ASIA..."), {
      target: { value: "ASIAEXAMPLEACCESSKEY" },
    });
    await fireEvent.click(screen.getByText("Validate"));

    await waitFor(() => {
      expect(screen.getByText(/Valid session credentials/)).toBeInTheDocument();
    });
  });

  it("decodes authorization message", async () => {
    mockSend.mockImplementation((command: { constructor: { name: string } }) => {
      if (command.constructor.name === "GetCallerIdentityCommand") {
        return Promise.resolve({
          Account: "123456789012",
          Arn: "arn:aws:iam::123:user/test",
          UserId: "AIDAI12345",
        });
      }
      if (command.constructor.name === "DecodeAuthorizationMessageCommand") {
        return Promise.resolve({ DecodedMessage: '{"error":"denied"}' });
      }
      return Promise.resolve({});
    });

    render(STSPage);
    await fireEvent.input(
      screen.getByPlaceholderText("Paste base64 encoded authorization message"),
      {
        target: { value: "ZXJyb3I=" },
      },
    );
    await fireEvent.click(screen.getByText("Decode"));

    await waitFor(() => {
      expect(screen.getByText('{"error":"denied"}')).toBeInTheDocument();
    });
  });

  it("shows Account label", async () => {
    mockSend.mockResolvedValue({
      Account: "123456789012",
      Arn: "arn:aws:iam::123:user/test",
      UserId: "AIDAI12345",
    });
    render(STSPage);
    await waitFor(
      () => {
        expect(screen.getByText("Account")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows ARN label", async () => {
    mockSend.mockResolvedValue({
      Account: "123456789012",
      Arn: "arn:aws:iam::123:user/test",
      UserId: "AIDAI12345",
    });
    render(STSPage);
    await waitFor(
      () => {
        expect(screen.getByText("ARN")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows User ID label", async () => {
    mockSend.mockResolvedValue({
      Account: "123456789012",
      Arn: "arn:aws:iam::123:user/test",
      UserId: "AIDAI12345",
    });
    render(STSPage);
    await waitFor(
      () => {
        expect(screen.getByText("User ID")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
