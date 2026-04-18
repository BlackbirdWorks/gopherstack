import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import WAFPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getWAFV2Client: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("WAF Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    expect(screen.getByText("AWS WAF v2")).toBeInTheDocument();
  });

  it("shows Create Web ACL button", () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    expect(screen.getByText("Create Web ACL")).toBeInTheDocument();
  });

  it("shows scope selector", () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    expect(screen.getByText("Regional")).toBeInTheDocument();
    expect(screen.getByText("CloudFront (Global)")).toBeInTheDocument();
  });

  it("shows empty state when no Web ACLs", async () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    await waitFor(
      () => {
        expect(screen.getByText("No Web ACLs found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded Web ACLs", async () => {
    mockSend.mockResolvedValue({
      WebACLs: [
        {
          Name: "production-acl",
          Id: "abc123",
          LockToken: "lt1",
          Description: "Blocks bad actors",
        },
        { Name: "staging-acl", Id: "def456", LockToken: "lt2" },
      ],
    });
    render(WAFPage);
    await waitFor(
      () => {
        expect(screen.getByText("production-acl")).toBeInTheDocument();
        expect(screen.getByText("staging-acl")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows Web ACLs / IP Sets / Rule Groups tabs", () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    expect(screen.getByText("Web ACLs")).toBeInTheDocument();
    expect(screen.getByText("IP Sets")).toBeInTheDocument();
    expect(screen.getByText("Rule Groups")).toBeInTheDocument();
  });

  it("opens create Web ACL modal", async () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    await fireEvent.click(screen.getByText("Create Web ACL"));
    expect(screen.getByText("Web ACL Name")).toBeInTheDocument();
    expect(screen.getByText("Default Action")).toBeInTheDocument();
  });

  it("cancels create modal", async () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    await fireEvent.click(screen.getByText("Create Web ACL"));
    await fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByText("Web ACL Name")).not.toBeInTheDocument();
  });

  it("switches to IP Sets tab", async () => {
    mockSend.mockResolvedValue({ WebACLs: [] });
    render(WAFPage);
    mockSend.mockResolvedValue({ IPSets: [] });
    await fireEvent.click(screen.getByText("IP Sets"));
    await waitFor(
      () => {
        expect(screen.getByText("No IP sets found")).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });
});
