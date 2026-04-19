import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import APIGatewayPage from "./+page.svelte";

const mockSend = vi.fn();

vi.mock("$lib/aws-client", () => ({
  getAPIGatewayClient: () => ({ send: mockSend }),
}));

vi.mock("svelte-sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

describe("API Gateway Page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSend.mockReset();
  });

  it("renders page title", () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(APIGatewayPage);
    expect(screen.getByText("API Gateway")).toBeInTheDocument();
  });

  it("shows tabs", () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(APIGatewayPage);
    expect(screen.getByText("REST APIs")).toBeInTheDocument();
    expect(screen.getByText("API Keys")).toBeInTheDocument();
  });

  it("displays APIs", async () => {
    mockSend.mockResolvedValueOnce({
      items: [
        {
          id: "api123",
          name: "my-rest-api",
          description: "My test API",
          endpointConfiguration: { types: ["REGIONAL"] },
          createdDate: new Date("2024-01-01"),
        },
      ],
    });

    render(APIGatewayPage);

    await waitFor(() => {
      expect(screen.getByText("my-rest-api")).toBeInTheDocument();
    });
  });

  it("shows create modal", async () => {
    mockSend.mockResolvedValueOnce({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("Create API"));
    expect(screen.getByText("Create REST API")).toBeInTheDocument();
  });

  it("switches to API keys tab", async () => {
    mockSend.mockResolvedValueOnce({ items: [] }).mockResolvedValueOnce({ items: [] });
    render(APIGatewayPage);
    await fireEvent.click(screen.getByText("API Keys"));
    await waitFor(() => {
      expect(screen.getByText("No API keys found")).toBeInTheDocument();
    });
  });
});
