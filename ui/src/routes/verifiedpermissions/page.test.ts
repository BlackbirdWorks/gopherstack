import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/svelte";
import VerifiedPermissionsPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getVerifiedPermissionsClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
}));

describe("Verified Permissions Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    expect(screen.getAllByText("Amazon Verified Permissions")[0]).toBeInTheDocument();
  });

  it("shows stat cards", () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    expect(screen.getAllByText("Policy Stores")[0]).toBeInTheDocument();
  });

  it("shows search input", () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
  });

  it("shows empty state when no policy stores", async () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    await waitFor(
      () => {
        expect(screen.getAllByText(/no policy stores/i)[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("displays loaded policy stores", async () => {
    mockSend.mockResolvedValue({
      policyStores: [
        {
          policyStoreId: "store-abc123",
          arn: "arn:aws:verifiedpermissions::123:policy-store/abc123",
        },
      ],
    });
    render(VerifiedPermissionsPage);
    await waitFor(
      () => {
        expect(screen.getAllByText("store-abc123")[0]).toBeInTheDocument();
      },
      { timeout: 3000 },
    );
  });

  it("shows refresh button", () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    expect(screen.getByTitle("Refresh")).toBeInTheDocument();
  });

  it("shows Policies tab", () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    expect(screen.getAllByText("Policies")[0]).toBeInTheDocument();
  });

  it("shows Identity Sources tab", () => {
    mockSend.mockResolvedValue({ policyStores: [] });
    render(VerifiedPermissionsPage);
    expect(screen.getAllByText("Identity Sources")[0]).toBeInTheDocument();
  });
});
