import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
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
