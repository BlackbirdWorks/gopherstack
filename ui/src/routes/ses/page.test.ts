import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import SESPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getSESClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("SES Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ Identities: [] });
    render(SESPage);
    expect(screen.getByText("Simple Email Service")).toBeInTheDocument();
  });

  it("shows identities tab by default", () => {
    mockSend.mockResolvedValueOnce({ Identities: [] });
    render(SESPage);
    expect(screen.getByText("Identities")).toBeInTheDocument();
    expect(screen.getByText("Templates")).toBeInTheDocument();
    expect(screen.getByText("Send Email")).toBeInTheDocument();
  });

  it("displays loaded identities", async () => {
    mockSend
      .mockResolvedValueOnce({ Identities: ["user@example.com", "admin@test.org"] })
      .mockResolvedValueOnce({
        VerificationAttributes: {
          "user@example.com": { VerificationStatus: "Success", VerificationToken: "tok1" },
          "admin@test.org": { VerificationStatus: "Pending", VerificationToken: "tok2" },
        },
      });

    render(SESPage);

    await waitFor(() => {
      expect(screen.getByText("user@example.com")).toBeInTheDocument();
      expect(screen.getByText("admin@test.org")).toBeInTheDocument();
    });
  });

  it("shows verify identity modal", async () => {
    mockSend.mockResolvedValueOnce({ Identities: [] });
    render(SESPage);
    const btn = screen.getByText("Verify Identity");
    await fireEvent.click(btn);
    expect(screen.getByText("Verify Email Identity")).toBeInTheDocument();
  });

  it("switches to send tab", async () => {
    mockSend.mockResolvedValueOnce({ Identities: [] });
    render(SESPage);
    const sendTab = screen.getAllByText("Send Email")[0];
    await fireEvent.click(sendTab);
    expect(screen.getByText("Send Email", { selector: "h2" })).toBeInTheDocument();
  });
});
