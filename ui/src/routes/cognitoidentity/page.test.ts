import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import CognitoIdentityPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getCognitoIdentityClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("Cognito Identity Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ IdentityPools: [] });
    render(CognitoIdentityPage);
    expect(screen.getByText("Cognito Identity")).toBeInTheDocument();
  });

  it("shows create pool button", () => {
    mockSend.mockResolvedValueOnce({ IdentityPools: [] });
    render(CognitoIdentityPage);
    expect(screen.getByText("Create Pool")).toBeInTheDocument();
  });

  it("renders 4 stat cards", () => {
    mockSend.mockResolvedValueOnce({ IdentityPools: [] });
    render(CognitoIdentityPage);
    expect(screen.getByText("Identity Pools")).toBeInTheDocument();
    expect(screen.getByText("Filtered Results")).toBeInTheDocument();
    expect(screen.getByText("Selected")).toBeInTheDocument();
  });

  it("displays identity pools", async () => {
    mockSend.mockResolvedValueOnce({
      IdentityPools: [
        {
          IdentityPoolId: "us-east-1:abc-123",
          IdentityPoolName: "my-app-pool",
        },
      ],
    });

    render(CognitoIdentityPage);

    await waitFor(() => {
      expect(screen.getByText("my-app-pool")).toBeInTheDocument();
    });
  });

  it("shows create modal", async () => {
    mockSend.mockResolvedValueOnce({ IdentityPools: [] });
    render(CognitoIdentityPage);
    await fireEvent.click(screen.getByText("Create Pool"));
    expect(screen.getByText("Create Identity Pool")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ IdentityPools: [] });
    render(CognitoIdentityPage);
    await waitFor(() => {
      expect(screen.getByText("No identity pools found")).toBeInTheDocument();
    });
  });

  it("filters identity pools by search", async () => {
    mockSend.mockResolvedValueOnce({
      IdentityPools: [
        { IdentityPoolId: "us-east-1:abc-123", IdentityPoolName: "prod-app-pool" },
        { IdentityPoolId: "us-east-1:def-456", IdentityPoolName: "dev-app-pool" },
      ],
    });
    render(CognitoIdentityPage);
    await waitFor(() => {
      expect(screen.getByText("prod-app-pool")).toBeInTheDocument();
    });
    const search = screen.getByPlaceholderText("Search identity pools...");
    await fireEvent.input(search, { target: { value: "dev" } });
    await waitFor(() => {
      expect(screen.queryByText("prod-app-pool")).not.toBeInTheDocument();
    });
  });

  it("shows subtitle", () => {
    mockSend.mockResolvedValueOnce({ IdentityPools: [] });
    render(CognitoIdentityPage);
    expect(screen.getByText("Federated identity pools for AWS service access")).toBeInTheDocument();
  });

  it("shows principal tag and developer identity tools in pool details", async () => {
    mockSend
      .mockResolvedValueOnce({
        IdentityPools: [{ IdentityPoolId: "us-east-1:abc-123", IdentityPoolName: "my-app-pool" }],
      })
      .mockResolvedValueOnce({
        IdentityPoolId: "us-east-1:abc-123",
        IdentityPoolName: "my-app-pool",
        AllowUnauthenticatedIdentities: true,
      })
      .mockResolvedValueOnce({ Roles: {} });

    render(CognitoIdentityPage);

    await waitFor(() => {
      expect(screen.getByText("my-app-pool")).toBeInTheDocument();
    });

    await fireEvent.click(screen.getByText("my-app-pool"));

    await waitFor(() => {
      expect(screen.getByText("Principal Tag Attribute Map")).toBeInTheDocument();
      expect(screen.getByText("Developer Identity Tools")).toBeInTheDocument();
    });
  });
});
