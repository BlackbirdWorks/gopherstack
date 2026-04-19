import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import AppSyncPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAppSyncClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("AppSync Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ graphqlApis: [] });
    render(AppSyncPage);
    expect(screen.getByText("AppSync")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend.mockResolvedValueOnce({ graphqlApis: [] });
    render(AppSyncPage);
    expect(screen.getByText("GraphQL APIs")).toBeInTheDocument();
    expect(screen.getByText("Data Sources")).toBeInTheDocument();
    expect(screen.getByText("Functions")).toBeInTheDocument();
  });

  it("displays APIs", async () => {
    mockSend.mockResolvedValueOnce({
      graphqlApis: [
        {
          apiId: "api-123",
          name: "my-api",
          authenticationType: "API_KEY",
        },
      ],
    });

    render(AppSyncPage);

    await waitFor(() => {
      expect(screen.getByText("my-api")).toBeInTheDocument();
    });
  });

  it("shows create API modal", async () => {
    mockSend.mockResolvedValueOnce({ graphqlApis: [] });
    render(AppSyncPage);
    await fireEvent.click(screen.getByText("Create API"));
    expect(screen.getByText("Create GraphQL API")).toBeInTheDocument();
  });

  it("shows empty state", async () => {
    mockSend.mockResolvedValueOnce({ graphqlApis: [] });
    render(AppSyncPage);
    await waitFor(() => {
      expect(screen.getByText("No GraphQL APIs found")).toBeInTheDocument();
    });
  });
});
